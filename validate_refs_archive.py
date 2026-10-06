"""Validate archived REFS inputs and repeat processing into an isolated local/S3 prefix."""
import argparse
import json
import uuid
from pathlib import Path
import numpy as np
import pandas as pd
from ensemble_processing import assemble_refs
from refs_archive_io import read_source, filesystem
from run_refs_processing import process_range, VALUES, utc_time


def require(condition, message):
    if not condition:
        raise ValueError(message)


def independent_interval(frame, station, member, init, valid, total, interval):
    """Lookup endpoints directly; do not call the production differencing helper."""
    rows=frame[(frame.station_id.astype(str)==station) & (frame.init_time==init)]
    if 'member_id' in rows:
        rows=rows[rows.member_id==member]
    end=rows[rows.valid_time==valid]
    require(len(end)<=1, f'Duplicate endpoint: {station} {member} {init} {valid}')
    if end.empty:
        return np.nan, False
    if total not in rows:
        return np.nan, False  # interval-only input cannot independently prove the window
    current=float(end.iloc[0][total])
    lead=(valid-init)/pd.Timedelta(hours=1)
    if lead==6:
        return round(max(current,0),2), True
    previous=rows[rows.valid_time==valid-pd.Timedelta(hours=6)]
    require(len(previous)<=1, f'Duplicate prior endpoint: {station} {member} {init}')
    if previous.empty:
        return np.nan, True
    difference=current-float(previous.iloc[0][total])
    require(not np.isfinite(difference) or difference>=-1e-6,
            f'Negative cumulative difference: {station} {member} {init} {valid}: {difference}')
    return round(max(difference,0),2), True


def read_parquet(path, profile):
    fs,key=filesystem(path,profile)
    with fs.open(key,'rb') as stream:
        return pd.read_parquet(stream)


def check_case(root, element, cycle, lead, stations, profile, raw_cache):
    valid=cycle+pd.Timedelta(hours=lead)
    members=assemble_refs(root,element,cycle,valid,station_ids=stations,aws_profile=profile)
    require(not members.empty, f'No members: {element} {cycle} f{lead:03d}')
    counts={3:14,6:14,12:14,42:14,45:13,48:13,51:12,54:12,57:6,60:6}
    expected=counts.get(lead,14 if lead<=42 else 13 if lead<=48 else 12 if lead<=54 else 6)
    require(members.groupby('station_id').size().eq(expected).all(),'Incorrect member count')
    require(members.source_available.all(),f'Missing sources: {element} {cycle} f{lead:03d}: '+
            str(members.loc[~members.source_available,['station_id','ensemble_member_id']].head(10).to_dict('records')))
    require((members.init_time+pd.to_timedelta(members.forecast_hour,unit='h')==valid).all(),'Valid-time alignment failed')
    require((members.init_time+pd.to_timedelta(members.lag_hours,unit='h')==cycle).all(),'Lag alignment failed')
    for col in VALUES[element]:
        require(col in members and np.isfinite(members[col]).all(),f'Missing/nonfinite {col}: {cycle} f{lead:03d}')
    checked=0; unverified=0
    if element in ('precip6hr','snow6hr'):
        total,interval=('precip_accum','precip_6h') if element=='precip6hr' else ('snow_accum','snow_6h')
        for row in members.itertuples():
            key=(row.source_model,row.init_time.strftime('%Y_%m'),element)
            if key not in raw_cache:
                raw=read_source(root,key[0],element,key[1],profile).copy()
                for col in ('init_time','valid_time'):
                    raw[col]=pd.to_datetime(raw[col],utc=True)
                raw_cache[key]=raw
            raw=raw_cache[key]
            value,verifiable=independent_interval(raw,row.station_id,row.member_id,row.init_time,valid,total,interval)
            if not verifiable:
                unverified+=1
                continue
            require(np.isclose(value,getattr(row,interval),atol=1e-8,equal_nan=True),
                    f'Interval mismatch: {element} {row.station_id} {row.ensemble_member_id} {valid}')
            # Also check the interval actually stored by the source archiver.
            selected=raw[(raw.station_id.astype(str)==row.station_id)&(raw.init_time==row.init_time)&(raw.valid_time==valid)]
            if 'member_id' in selected:
                selected=selected[selected.member_id==row.member_id]
            if interval in selected:
                require(np.isclose(value,float(selected.iloc[0][interval]),atol=1e-8,equal_nan=True),
                        f'Stored source interval mismatch: {key} {row.station_id} {valid}')
            checked+=1
    return members,checked,unverified


def check_statistics(frame,members):
    for row in frame.itertuples():
        selected=members[members.station_id==row.station_id]
        values=selected[row.value_column].to_numpy(dtype=float)
        require(row.complete and row.n_available==len(values)==row.n_expected,'Incomplete derived row')
        recipe=json.loads(row.statistics_config)
        desired={'mean':np.mean(values),'spread':np.std(values,ddof=0)}
        desired.update({f'p{p:g}':np.percentile(values,p,method='linear') for p in recipe['percentiles']})
        for threshold in recipe['thresholds']:
            op=recipe['operator'];prefix='lt' if op=='<' else 'gt'
            desired[f'prob_{prefix}_{threshold:g}']=np.mean(values<threshold if op=='<' else values>threshold)
        # A Series lookup also supports negative threshold column names.
        actual=frame.loc[row.Index]
        for col,value in desired.items():
            require(np.isclose(actual[col],value,atol=1e-10),f'Statistic mismatch: {row.station_id} {row.value_column} {col}')


def run(args):
    day=utc_time(args.date).normalize()
    root=f"{args.test_output_root.rstrip('/')}/refs-validation-{uuid.uuid4().hex}"
    print(f'Test artifacts (retained): {root}')
    results=[]
    def record(name, fn):
        try:
            detail=fn(); results.append(dict(check=name,status='PASS',detail=detail))
        except Exception as exc:
            results.append(dict(check=name,status='FAIL',detail=str(exc)))
        print(f"{results[-1]['status']}: {name}: {results[-1]['detail']}")
    raw_cache={}
    for element in args.elements:
        def validate(element=element):
            leads=[h for h in args.forecast_hours if element not in ('precip6hr','snow6hr') or h>=6]
            require(bool(leads),'No eligible forecast leads')
            cases={}; intervals=0; unverified=0
            for cycle in pd.date_range(day,day+pd.Timedelta(hours=18),freq='6h'):
                for lead in leads:
                    members,n,skipped=check_case(args.archive_root,element,cycle,lead,args.stations,args.aws_profile,raw_cache)
                    cases[(cycle,cycle+pd.Timedelta(hours=lead))]=members
                    intervals+=n;unverified+=skipped
            kwargs=dict(archive_root=args.archive_root,output_root=root,elements=[element],
                        forecast_hours=leads,station_ids=args.stations,aws_profile=args.aws_profile,
                        threshold_config=args.threshold_config)
            paths=process_range(day,day+pd.Timedelta(days=1),**kwargs)
            first={path:read_parquet(path,args.aws_profile) for path in paths}
            for path,frame in first.items():
                keys=['station_id','ensemble_init_time','valid_time','value_column']
                require(not frame.duplicated(keys).any(),'Duplicate derived keys')
                expected=sum(m.station_id.nunique()*len(VALUES[element]) for m in cases.values())
                require(len(frame)==expected,f'Expected {expected} rows, found {len(frame)}')
                for case,part in frame.groupby(['ensemble_init_time','valid_time']):
                    check_statistics(part,cases[case])
            second=process_range(day,day+pd.Timedelta(days=1),**kwargs)
            require(set(second)==set(paths),'Output paths changed on repeat')
            for path in paths:
                pd.testing.assert_frame_equal(first[path],read_parquet(path,args.aws_profile),check_exact=True)
            require(unverified==0,f'{unverified} member intervals lack cumulative totals; alignment not independently verified')
            return f'{sum(len(f) for f in first.values())} rows; {intervals} member intervals; statistics and repeat read/write identical'
        record(element,validate)
    if args.boundary_date:
        boundary=utc_time(args.boundary_date).normalize()
        require(boundary.day==1,'--boundary-date must be the first day of a month')
        for element in args.elements:
            def boundary_check(element=element):
                _,_,skipped=check_case(args.archive_root,element,boundary,6,args.stations,args.aws_profile,raw_cache)
                require(skipped==0,'Boundary accumulation lacks cumulative totals')
                paths=process_range(boundary,boundary+pd.Timedelta(hours=6),archive_root=args.archive_root,
                    output_root=root+'/boundary',elements=[element],forecast_hours=[6],station_ids=args.stations,
                    aws_profile=args.aws_profile,threshold_config=args.threshold_config)
                require(all(p.endswith(boundary.strftime('%Y_%m')+'_archive.parquet') for p in paths),'Wrong output month')
                return 'Previous-month lagged sources present; result saved in initialization month'
            record(element+' month boundary',boundary_check)
    else:
        results.append(dict(check='month boundary',status='SKIP',detail='Supply --boundary-date YYYY-MM-01 to exercise real cross-month sources'))
    print('\n'+json.dumps(results,indent=2))
    if args.report:
        Path(args.report).write_text(json.dumps(dict(output_root=root,checks=results),indent=2)+'\n')
    return 1 if any(r['status']=='FAIL' for r in results) else 2 if any(r['status']=='SKIP' for r in results) else 0


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--date',required=True,help='UTC initialization day')
    p.add_argument('--archive-root',default='model')
    p.add_argument('--test-output-root',default='validation_output',help='Local directory or s3://bucket/test-prefix; unique child created')
    p.add_argument('--elements',nargs='+',choices=list(VALUES),default=['precip6hr','snow6hr'])
    p.add_argument('--stations',nargs='+',default=['PAJN'],help='Default PAJN; --all-stations checks station union found in sources')
    p.add_argument('--all-stations',action='store_true')
    p.add_argument('--forecast-hours',type=int,nargs='+',default=[6,12,42,45,48,51,54,57,60])
    p.add_argument('--boundary-date',help='First UTC day of a month with archived sources')
    p.add_argument('--threshold-config')
    p.add_argument('--aws-profile')
    p.add_argument('--report',help='Optional local JSON report path')
    args=p.parse_args()
    if args.all_stations: args.stations=None
    try:
        require(all(1<=h<=60 for h in args.forecast_hours),'Forecast hours must be 1–60')
        raise SystemExit(run(args))
    except (ValueError,OSError) as exc:
        p.exit(1,f'Validation failed: {exc}\n')


if __name__=='__main__':
    main()
