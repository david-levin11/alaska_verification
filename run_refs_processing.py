"""Batch REFS statistics into monthly local/S3 archives, or inspect a single case."""
import argparse
import json
from region_config import normalize_region, region_root, refs_limits
import pandas as pd
from ensemble_processing import assemble_refs, summarize_refs
from refs_archive_io import update_monthly, write_parquet
from refs_threshold_config import DEFAULT_CONFIG, resolve_threshold_config

VALUES = {'wind': ['wind_speed_kt','wind_gust_kt'], 'precip6hr':['precip_6h'],
          'snow6hr':['snow_6h'], 'rh':['rh'], 'temp2m':['temp_2m_f']}


def utc_time(value):
    # Support compact dates used by the existing daily shell wrapper.
    if isinstance(value,str) and value.isdigit() and len(value) in (10,12):
        return pd.to_datetime(value,format='%Y%m%d%H' if len(value)==10 else '%Y%m%d%H%M',utc=True)
    return pd.to_datetime(value,utc=True)


def process_range(start, end, *, archive_root=None, output_root=None,
                  elements=None, forecast_hours=None,
                  station_ids=None, thresholds=None, require_complete=True,
                  max_forecast_hours=None, aws_profile=None, value_column=None, threshold_config=None, region='alaska'):
    """Process [start,end) UTC cycles; cache each source month once per element.

    Monthly upserts preserve unrelated rows and make repeated cron runs safe.
    """
    region=normalize_region(region)
    archive_root=region_root(archive_root,region,'model')
    output_root=region_root(output_root,region,'derived/refs')
    if elements is None:
        elements=[e for e in VALUES if region=='alaska' or not e.startswith('snow')]
    if region=='hawaii' and any(e.lower().startswith('snow') for e in elements):
        raise ValueError('Snow archiving is disabled for Hawaii')
    start,end=utc_time(start),utc_time(end)
    if start >= end:
        raise ValueError('start must be earlier than the exclusive end')
    leads=list(range(3,61,3)) if forecast_hours is None else sorted(set(forecast_hours))
    if not leads or any(h<1 or h>60 for h in leads):
        raise ValueError('Forecast hours must be integers between 1 and 60')
    if value_column and len(elements)!=1:
        raise ValueError('--value-column requires exactly one element')
    limits=refs_limits(region,max_forecast_hours)
    config=resolve_threshold_config(threshold_config,thresholds,value_column)
    recipes={column: json.dumps(dict(**settings,require_complete=require_complete,
             max_forecast_hours=limits,percentiles=[5,10,25,50,75,90,95],
             quantile_method='linear',weighting='equal',version=2),sort_keys=True)
             for column,settings in config.items()}
    cycles=pd.date_range(start.ceil('6h'),end,freq='6h',inclusive='left')
    if not len(cycles):
        raise ValueError('No 00/06/12/18 UTC cycles in this interval')
    written=[]
    empty_cases=0
    for element in elements:
        element=element.lower()
        if element not in VALUES:
            raise ValueError(f'Unsupported element: {element}')
        if value_column and value_column not in VALUES[element]:
            raise ValueError(f'Unsupported value column {value_column!r} for {element}')
        columns=[value_column] if value_column else VALUES[element]
        cache={}
        # Flush per initialization month to bound summary memory on backfills.
        for month in sorted(set(cycles.strftime('%Y_%m'))):
            summaries=[]
            for cycle in cycles[cycles.strftime('%Y_%m')==month]:
                for lead in leads:
                    if element in ('precip6hr','snow6hr') and lead<6:
                        continue
                    members=assemble_refs(archive_root,element,cycle,cycle+pd.Timedelta(hours=lead),
                                          station_ids=station_ids,max_forecast_hours=limits,
                                          source_cache=cache,aws_profile=aws_profile,region=region)
                    if members.empty:
                        print(f'WARNING: No stations found for {element} {cycle} f{lead:03d}')
                        empty_cases+=1
                        continue
                    for column in columns:
                        # A missing field is a missing value, never a zero.
                        if column not in members:
                            members[column]=float('nan')
                        summary=summarize_refs(members,column,thresholds=config[column]["thresholds"],
                                               probability_operator=config[column]["operator"],
                                               require_complete=require_complete)
                        summary['statistics_config']=recipes[column]
                        summaries.append(summary)
                print(f'Processed {element} cycle {cycle}')
            if summaries:
                combined=pd.concat(summaries,ignore_index=True)
                print(f'{element}: {int(combined.complete.sum()):,}/{len(combined):,} complete station/lead/variable rows')
                written.extend(update_monthly(combined,output_root,element,aws_profile))
            # Retain only this month for the next month's lagged boundary.
            cache={key:frame for key,frame in cache.items() if key[3]==month}
    if not written:
        raise ValueError('No statistics were written; check source archive paths, dates, and station coverage')
    if empty_cases:
        raise ValueError(f'{empty_cases} cycle/lead cases had no source rows; available cases were saved. Re-archive missing sources and rerun.')
    return written


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive-root',default=None,help='Source local directory or S3 bucket/prefix')
    parser.add_argument('--output-root',default=None,help='Monthly statistics directory or s3://bucket/prefix')
    parser.add_argument('--aws-profile',help='Optional AWS profile; otherwise use normal AWS credential chain')
    parser.add_argument('--date',help='UTC day to process; defaults to yesterday in batch mode')
    parser.add_argument('--start',help='Inclusive UTC start for batch mode')
    parser.add_argument('--end',help='Exclusive UTC end for batch mode')
    parser.add_argument('--lookback-days',type=int,default=1,help='Number of days ending on --date (default 1)')
    parser.add_argument('--element','--elements',dest='elements',nargs='+',default=None)
    parser.add_argument('--forecast-hours',nargs='+',type=int)
    parser.add_argument('--cycle',help='Single-case mode: REFS initialization, UTC')
    parser.add_argument('--valid',help='Single-case mode: valid time, UTC')
    parser.add_argument('--value-column')
    parser.add_argument('--stations',nargs='+')
    parser.add_argument('--thresholds',nargs='*',type=float,default=None,
                        help='Strict > override for one --value-column; empty list disables probabilities')
    parser.add_argument('--threshold-config',default=str(DEFAULT_CONFIG),
                        help='Variable-specific JSON probability configuration')
    parser.add_argument('--allow-incomplete',action='store_true')
    parser.add_argument('--output',help='Single-case summary path (local/S3; replaced)')
    parser.add_argument('--members-output',help='Single-case member path (local/S3; replaced)')
    parser.add_argument('--rrfs-max-hour',type=int,default=60)
    parser.add_argument('--rrfsens-max-hour',type=int,default=60)
    parser.add_argument('--hrrr-max-hour',type=int,default=48)
    parser.add_argument('--region',choices=['alaska','hawaii','ak','hi'],default='alaska')
    args=parser.parse_args()
    limits={'rrfs':args.rrfs_max_hour,'rrfsens':args.rrfsens_max_hour,'hrrr':args.hrrr_max_hour}
    try:
        args.region=normalize_region(args.region)
        args.archive_root=region_root(args.archive_root,args.region,'model')
        args.output_root=region_root(args.output_root,args.region,'derived/refs')
        if args.cycle or args.valid:
            if not (args.cycle and args.valid and args.elements and len(args.elements)==1 and args.value_column):
                parser.error('Single-case mode requires --cycle, --valid, one --element, and --value-column')
            if args.date or args.start or args.end:
                parser.error('Do not combine single-case and batch dates')
            config=resolve_threshold_config(args.threshold_config,args.thresholds,args.value_column)
            settings=config[args.value_column]
            members=assemble_refs(args.archive_root,args.elements[0],args.cycle,args.valid,
                                  station_ids=args.stations,max_forecast_hours=limits,aws_profile=args.aws_profile,region=args.region)
            summary=summarize_refs(members,args.value_column,thresholds=settings["thresholds"],
                                   probability_operator=settings["operator"],
                                   require_complete=not args.allow_incomplete)
            print(summary.to_string(index=False))
            for frame,path in ((summary,args.output),(members,args.members_output)):
                if path:
                    path=region_root(path,args.region,'output')
                    write_parquet(frame,path,args.aws_profile)
                    print(f'Saved {len(frame):,} rows to {path}')
            return
        if args.output or args.members_output:
            parser.error('Batch mode uses --output-root; --output/--members-output are single-case options')
        if args.lookback_days<1:
            parser.error('--lookback-days must be positive')
        if args.start or args.end:
            if not (args.start and args.end) or args.date or args.lookback_days!=1:
                parser.error('Use --start and --end together, without --date or --lookback-days')
            start,end=args.start,args.end
        else:
            day=utc_time(args.date).normalize() if args.date else pd.Timestamp.now(tz='UTC').normalize()-pd.Timedelta(days=1)
            start,end=day-pd.Timedelta(days=args.lookback_days-1),day+pd.Timedelta(days=1)
        process_range(start,end,archive_root=args.archive_root,output_root=args.output_root,
                      elements=args.elements,forecast_hours=args.forecast_hours,
                      station_ids=args.stations,thresholds=args.thresholds,threshold_config=args.threshold_config,
                      require_complete=not args.allow_incomplete,max_forecast_hours=limits,
                      aws_profile=args.aws_profile,value_column=args.value_column,region=args.region)
    except (ValueError,OSError,KeyError) as exc:
        parser.exit(1,f'REFS processing failed: {exc}\n')


if __name__=='__main__':
    main()
