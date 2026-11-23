"""Archive observations matching native experimental or standard 12h windows."""
import argparse
import os
from pathlib import Path
import sys
import math
import pandas as pd

from experimental_ndfd import install_plan
from import_ndfd_experimental import archive_lock
from obs_forecast_windows import native_periods,standard_periods,fetch_windows,observation_plan
from obs_minrh import utc_timestamp


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile',choices=['experimental','12hr'],default='experimental')
    parser.add_argument('--forecast-root',type=Path,help='Experimental model directory; defaults to <archive-root>/ndfd_exp')
    parser.add_argument('--archive-root',type=Path)
    parser.add_argument('--region',choices=['alaska','hawaii'],default='alaska')
    parser.add_argument('--elements',nargs='+',choices=['maxt','mint','minrh'],default=['maxt','mint','minrh'])
    parser.add_argument('--start',required=True,help='First UTC window endpoint, inclusive')
    parser.add_argument('--end',required=True,help='Last UTC window endpoint, inclusive')
    parser.add_argument('--max-gap-minutes',type=float,default=90)
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--replace-conflicts',action='store_true')
    args = parser.parse_args(argv)
    start,end = utc_timestamp(args.start),utc_timestamp(args.end)
    if start>end:
        parser.error('start must not be after end')
    if not math.isfinite(args.max_gap_minutes) or args.max_gap_minutes<=0:
        parser.error('max-gap-minutes must be positive and finite')
    import archiver_config as config
    from obs_archiver import ObsArchiver
    config.configure_region(args.region)
    if not config.API_KEY:
        parser.error('Set SYNOPTIC_API_KEY in the active environment')
    root = (args.archive_root or Path(config.MODEL_DIR)).resolve()
    forecast_root = (args.forecast_root or root/'ndfd_exp').resolve()
    model = 'obs_exp' if args.profile=='experimental' else 'obs_12hr'
    with archive_lock(root,args.apply,model=model):
        for element in args.elements:
            config.ELEMENT = element
            archiver = ObsArchiver(config)
            # Obtain zone metadata using the shared all-status discovery.
            # Native forecasts supply the actual IDs to query, including any
            # historical IDs absent from the current discovery response.
            station_ids = archiver.get_station_metadata()
            periods = (native_periods(forecast_root,element,start,end) if args.profile=='experimental'
                else standard_periods(station_ids,element,start,end))
            if periods.empty:
                print(f'{element}: no forecast windows in requested endpoint range')
                continue
            for month,subset in periods.groupby(periods.window_end.dt.strftime('%Y_%m')):
                print(f'{model}/{element} {month}: {len(subset)} station-windows; '
                    f'{len(subset[["window_start","window_end"]].drop_duplicates())} distinct periods',flush=True)
                data = fetch_windows(archiver,subset,element,max_gap_minutes=args.max_gap_minutes)
                print(f'  observations={len(data)}, missing station-windows={len(subset)-len(data)}, '
                    f'sparse={int((~data.coverage_ok).sum()) if not data.empty else 0}')
                plan = observation_plan(data,root,model,element,replace_conflicts=args.replace_conflicts)
                for path,_,details in plan:
                    print(path,details)
                if plan and args.apply:
                    report = dict(operation='observed_forecast_windows',profile=args.profile,element=element,
                        forecast_root=str(forecast_root),requested_station_windows=len(subset),returned_rows=len(data),
                        max_gap_minutes=args.max_gap_minutes)
                    print('INSTALLED:',install_plan(plan,report,root,model=model))
                else:
                    print('PREVIEW: add --apply to save' if plan else 'UNCHANGED: no new observations')
    return 0


if __name__=='__main__':
    try:
        raise SystemExit(main())
    except Exception as error:
        message = str(error)
        token = os.environ.get('SYNOPTIC_API_KEY','')
        if token:
            message = message.replace(token,'[REDACTED]')
        print(f'Observation window import failed: {message}',file=sys.stderr)
        raise SystemExit(1)
