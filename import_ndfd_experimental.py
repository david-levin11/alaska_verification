"""Import local GFE experimental exports; preview by default, no AWS access."""
import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sys

import numpy as np
import pandas as pd

from experimental_ndfd import ELEMENTS,extract_file,monthly_plan,install_plan


def load_stations(csv_path,region):
    if csv_path:
        stations = pd.read_csv(csv_path,dtype={'stid':str})
    else:
        from regional_stations import discover_regional_stations
        import requests
        token = os.environ.get('SYNOPTIC_API_KEY','')
        if not token: raise ValueError('Set SYNOPTIC_API_KEY or supply --station-csv')
        rows = discover_regional_stations('https://api.synopticdata.com/v2/stations/metadata',
            token,'hi' if region=='hawaii' else 'ak','0',get=requests.get)
        stations = pd.DataFrame([dict(stid=s['STID'],latitude=s['LATITUDE'],longitude=s['LONGITUDE']) for s in rows])
    if not {'stid','latitude','longitude'}<=set(stations):
        raise ValueError('Station CSV requires stid, latitude, longitude columns')
    stations = stations.dropna(subset=['stid']).copy()
    stations['stid'] = stations.stid.astype(str).str.strip()
    for column in ('latitude','longitude'): stations[column] = pd.to_numeric(stations[column],errors='coerce')
    valid = (np.isfinite(stations.latitude)&np.isfinite(stations.longitude)
        &stations.latitude.between(-90,90)&stations.longitude.between(-180,360)&stations.stid.ne(''))
    if not valid.all(): print(f'Ignoring {int((~valid).sum())} invalid station coordinate records')
    stations = stations.loc[valid].copy()
    stations['longitude'] = (stations.longitude+180)%360-180
    unique = stations[['stid','latitude','longitude']].drop_duplicates()
    if unique.stid.duplicated().any(): raise ValueError('Conflicting coordinates for a station ID')
    stations = unique.reset_index(drop=True)
    if stations.empty: raise ValueError('Station inventory is empty')
    return stations


@contextmanager
def archive_lock(root,apply):
    if not apply:
        yield
        return
    root = Path(root)/'ndfd_exp'
    root.mkdir(parents=True,exist_ok=True)
    lock = root/'.import.lock'
    try:
        stream = lock.open('x')
    except FileExistsError:
        raise RuntimeError(f'Importer lock exists: {lock}; check for an active importer before removing a stale lock') from None
    try:
        with stream: stream.write(str(os.getpid()))
        yield
    finally:
        lock.unlink(missing_ok=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,help='JSON configuration; CLI options override it')
    parser.add_argument('--input-dir',type=Path)
    parser.add_argument('--archive-root',type=Path,help='Parent model directory, e.g. /archive/model')
    parser.add_argument('--station-csv',type=Path,help='Optional fixed inventory; avoids any network access')
    parser.add_argument('--region',choices=['alaska','hawaii'])
    parser.add_argument('--max-distance-km',type=float)
    parser.add_argument('--elements',nargs='+',choices=ELEMENTS)
    parser.add_argument('--recursive',action='store_true',default=None)
    parser.add_argument('--apply',action='store_true',help='Install archives after validating each input file')
    parser.add_argument('--replace-conflicts',action='store_true',help='Allow replacement after backup; also works in preview')
    args = parser.parse_args(argv)
    config = {}
    if args.config:
        config = json.loads(args.config.read_text(encoding='utf-8'))
        allowed = {'input_dir','archive_root','station_csv','region','max_distance_km','elements','recursive'}
        if not isinstance(config,dict) or set(config)-allowed:
            parser.error('Unknown JSON configuration keys or invalid configuration object')
    base = args.config.resolve().parent if args.config else Path.cwd()
    def setting(name,default=None):
        value = getattr(args,name)
        return config.get(name,default) if value is None else value
    def path_setting(name,default=None):
        value = setting(name,default)
        if value is None: return None
        path = Path(value).expanduser()
        # CLI relative paths are relative to the working directory; config
        # relative paths are relative to the configuration file's directory.
        anchor = Path.cwd() if getattr(args,name) is not None else base
        return (anchor/path).resolve()
    region = setting('region','alaska')
    if region not in ('alaska','hawaii'): parser.error('Unsupported region')
    input_dir = path_setting('input_dir')
    if input_dir is None or not input_dir.is_dir(): parser.error('Supply an existing --input-dir or configure input_dir')
    default_root = Path(__file__).resolve().parent/('hawaii/model' if region=='hawaii' else 'model')
    root = path_setting('archive_root',default_root)
    elements = setting('elements',ELEMENTS)
    if not isinstance(elements,(list,tuple)) or not elements or set(elements)-set(ELEMENTS):
        parser.error('Invalid elements configuration')
    distance = setting('max_distance_km',5.)
    if not isinstance(distance,(int,float)) or not np.isfinite(distance) or distance<=0:
        parser.error('max_distance_km must be positive and finite')
    recursive = setting('recursive',False)
    if not isinstance(recursive,bool): parser.error('recursive must be a boolean')
    paths = input_dir.rglob('*') if recursive else input_dir.iterdir()
    files = sorted(p for p in paths if p.is_file() and p.name.lower().endswith(('.nc','.nc.gz','.netcdf','.netcdf.gz')))
    if not files: parser.error(f'No NetCDF exports found in {input_dir}')
    stations = load_stations(path_setting('station_csv'),region)
    cache = {}
    pending = {}
    with archive_lock(root,args.apply):
        for source in files:
            print(f'\nReading {source}',flush=True)
            frames,report = extract_file(source,stations,elements=elements,max_distance_km=distance,cache=cache)
            print(f'Initialization: {report["init_time"]}; stations selected={report["stations_selected"]}, '
                  f'rejected={len(report["stations_rejected"])}')
            for element,details in report['elements'].items(): print(f'  {element}: {details}')
            plan = monthly_plan(frames,root,replace_conflicts=args.replace_conflicts,existing=pending)
            for destination,_,details in plan: print(f'  {destination}: {details}')
            if not plan:
                print('UNCHANGED: matching forecasts already archived')
            elif args.apply:
                manifest = install_plan(plan,report,root)
                print(f'INSTALLED: {manifest}',flush=True)
            else:
                print('PREVIEW: no archive files written; add --apply to install')
            pending.update({path:data for path,data,_ in plan})
    return 0


if __name__=='__main__':
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f'Experimental NDFD import failed: {error}',file=sys.stderr)
        raise SystemExit(1)
