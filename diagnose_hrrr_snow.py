"""Read-only HRRR ASNOW diagnostic; downloads exact cumulative GRIB records."""
import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
import uuid


def select_record(index, lead):
    labels={f'0-{lead} hour acc fcst'}
    if lead%24==0:
        labels.add(f'0-{lead//24} day acc fcst')
    lines=index.strip().splitlines()
    matches=[]
    for i,line in enumerate(lines):
        fields=[part.strip() for part in line.split(':')]
        if len(fields)>5 and fields[3:5]==['ASNOW','surface'] and fields[5] in labels:
            start=int(fields[1]);end=int(lines[i+1].split(':')[1])-1 if i+1<len(lines) else None
            matches.append((line,start,end))
    if len(matches)!=1:
        raise ValueError(f'Expected exactly one cumulative ASNOW record at f{lead:03}; found {len(matches)}')
    return matches[0]


def download_record(url,lead,directory):
    import requests
    response=requests.get(url+'.idx',timeout=60);response.raise_for_status()
    (directory/f'f{lead:03}.idx').write_text(response.text)
    line,start,end=select_record(response.text,lead)
    requested=f'{start}-{end if end is not None else ""}'
    response=requests.get(url,headers={'Range':'bytes='+requested},timeout=120)
    response.raise_for_status()
    if response.status_code!=206:
        raise ValueError('Server did not honor byte-range request; refusing a full-file response')
    content_range=response.headers.get('Content-Range','')
    if not content_range.startswith(f'bytes {start}-'):
        raise ValueError(f'Unexpected Content-Range: {content_range}')
    if end is not None and len(response.content)!=end-start+1:
        raise ValueError('Truncated or incorrect GRIB byte range')
    if not response.content.startswith(b'GRIB'):
        raise ValueError('Downloaded record does not begin with GRIB')
    path=directory/f'f{lead:03}_asnow.grib2';path.write_bytes(response.content)
    return path,dict(url=url,index_record=line,byte_range=requested)


def metadata(message):
    result={}
    for key in ['name','shortName','units','typeOfLevel','level','stepType','startStep','endStep',
                'stepUnits','stepRange','dataDate','dataTime','validityDate','validityTime',
                'packingType','bitsPerValue','binaryScaleFactor','decimalScaleFactor',
                'referenceValue','discipline','parameterCategory','parameterNumber','centre','tablesVersion','localTablesVersion','packingError','unpackedError','numberOfMissing','gridType','Ni','Nj']:
        try:
            value=message[key]
            if hasattr(value,'item'): value=value.item()
            result[key]=value if isinstance(value,(str,int,float,bool)) else str(value)
        except Exception:
            result[key]=None
    return result


def resolve_units(units, assume_meters=False):
    if units == 'm':
        return 'Decoded GRIB units: meters'
    if units in (None, 'unknown', 'undef') and assume_meters:
        return 'ASSUMED meters via --assume-asnow-meters for exact ASNOW index record; decoder units unresolved'
    raise ValueError(f'Unresolved/unexpected ASNOW units: {units!r}. '
                     'For unknown units only, use --assume-asnow-meters to explicitly apply the archiver meter convention.')


def compare_bounds(difference,first,second):
    import math
    errors=[first.get('packingError'),second.get('packingError')]
    if all(isinstance(e,(float,int)) and math.isfinite(e) and e>=0 for e in errors):
        bound=sum(errors)
        return dict(combined_reported_packing_error=bound,
                    within_reported_packing_error=abs(difference)<=bound,
                    interpretation='Consistency with this bound is not proof of packing as the cause.')
    return dict(combined_reported_packing_error=None,within_reported_packing_error=None,
                interpretation='Packing-error bound unavailable; small magnitude alone does not establish rounding.')


def run(args,directory):
    import numpy as np
    import pandas as pd
    import pygrib
    # Use the production grid-cell selection, not a separate nearest-point method.
    from utils import ll_to_index
    cycle=datetime.strptime(args.cycle,'%Y%m%d%H')
    if args.latitude is not None and args.longitude is not None:
        lat,lon=args.latitude,args.longitude;origin='explicit coordinates'
    else:
        frame=pd.read_csv(args.metadata)
        idcol=next((c for c in ['stid','station_id','STID'] if c in frame),None)
        if idcol is None: raise ValueError('No station ID column in metadata')
        row=frame[frame[idcol].astype(str).str.strip()==args.station]
        if len(row)!=1: raise ValueError(f'Expected one metadata row for {args.station}, found {len(row)}')
        lat=float(row.iloc[0]['latitude']);lon=float(row.iloc[0]['longitude']);origin=args.metadata
    arrays=[];grids=[];records=[]
    for lead in args.leads:
        url=(f'{args.base_url.rstrip("/")}/hrrr.{cycle:%Y%m%d}/alaska/'
             f'hrrr.t{cycle:%H}z.wrfsfcf{lead:02}.ak.grib2')
        path,record=download_record(url,lead,directory)
        with pygrib.open(str(path)) as gribs:
            if gribs.messages!=1: raise ValueError(f'Expected one GRIB message in {path}')
            message=gribs.message(1);info=metadata(message)
            if (info['stepType']!='accum' or info['startStep']!=0 or info['endStep']!=lead
                    or str(info['stepUnits']) not in ('1','h')):
                raise ValueError(f'Unexpected decoded accumulation metadata: {info}')
            record['unit_interpretation']=resolve_units(info['units'],args.assume_asnow_meters)
            if info['dataDate']!=int(cycle.strftime('%Y%m%d')) or info['dataTime']!=cycle.hour*100:
                raise ValueError('GRIB initialization does not match requested cycle')
            valid=cycle+timedelta(hours=lead)
            if info['validityDate']!=int(valid.strftime('%Y%m%d')) or info['validityTime']!=valid.hour*100:
                raise ValueError('GRIB valid time does not match requested lead')
            arrays.append(np.ma.asarray(message.values,dtype=float).filled(np.nan))
            grids.append(message.latlons());record['metadata']=info;records.append(record)
    if arrays[0].shape!=arrays[1].shape or any(not np.allclose(a,b,equal_nan=True) for a,b in zip(grids[0],grids[1])):
        raise ValueError('Source grids differ; comparison aborted')
    iy,ix=ll_to_index(lat,lon,*grids[0]);iy=int(iy);ix=int(ix)
    difference=arrays[1]-arrays[0];point=float(difference[iy,ix])
    if not np.isfinite(point): raise ValueError('Selected station grid cell is missing')
    neighborhood=[]
    for y in range(max(0,iy-args.radius),min(arrays[0].shape[0],iy+args.radius+1)):
        for x in range(max(0,ix-args.radius),min(arrays[0].shape[1],ix+args.radius+1)):
            neighborhood.append(dict(y=y,x=x,latitude=float(grids[0][0][y,x]),
                longitude=float(grids[0][1][y,x]),first_m=float(arrays[0][y,x]),
                second_m=float(arrays[1][y,x]),difference_m=float(difference[y,x])))
    finite=difference[np.isfinite(difference)];negative=finite[finite<0]
    report=dict(station=args.station,station_latitude=lat,station_longitude=lon,coordinate_source=origin,
        cycle=cycle.isoformat(),leads=args.leads,grid_index=[iy,ix],records=records,
        point=dict(first_m=float(arrays[0][iy,ix]),second_m=float(arrays[1][iy,ix]),difference_m=point,
                   first_inches=float(arrays[0][iy,ix])*39.3701,second_inches=float(arrays[1][iy,ix])*39.3701,
                   difference_inches=point*39.3701),
        precision=compare_bounds(point,records[0]['metadata'],records[1]['metadata']),
        grid_summary=dict(finite_cells=int(finite.size),negative_cells=int(negative.size),
                          minimum_difference_m=float(finite.min()),maximum_difference_m=float(finite.max())),
        neighborhood=neighborhood,
        scope='Source GRIB comparison only. Does not alter archives or establish model numerical precision.')
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--cycle',default='2026100412',help='UTC YYYYMMDDHH')
    p.add_argument('--leads',nargs=2,type=int,default=[6,12])
    p.add_argument('--station',default='PAJN')
    p.add_argument('--metadata',default='obs/alaska_all_active_synoptic_station_metadata.csv')
    p.add_argument('--latitude',type=float);p.add_argument('--longitude',type=float)
    p.add_argument('--assume-asnow-meters',action='store_true',
                   help='Explicitly interpret unknown ASNOW units as meters; recorded in report')
    p.add_argument('--radius',type=int,default=1,help='Neighborhood radius in cells (default 3x3)')
    p.add_argument('--base-url',default='https://noaa-hrrr-bdp-pds.s3.amazonaws.com')
    p.add_argument('--output-dir',default='snow_diagnostics')
    args=p.parse_args()
    if (args.latitude is None)!=(args.longitude is None): p.error('Supply both latitude and longitude')
    if args.leads[0]<0 or args.leads[1]-args.leads[0]!=6: p.error('Leads must be nonnegative and six hours apart')
    if not 0<=args.radius<=10: p.error('Radius must be between 0 and 10')
    directory=Path(args.output_dir)/('hrrr-snow-'+uuid.uuid4().hex[:12]);directory.mkdir(parents=True)
    try:
        report=run(args,directory)
        # Strict JSON: missing grid points appear as null.
        def clean(value):
            import math
            if isinstance(value,float) and not math.isfinite(value): return None
            if isinstance(value,dict): return {k:clean(v) for k,v in value.items()}
            if isinstance(value,list): return [clean(v) for v in value]
            return value
        text=json.dumps(clean(report),indent=2,allow_nan=False)
        (directory/'report.json').write_text(text+'\n')
        (directory/'report.txt').write_text(text+'\n')
        print(text);print(f'\nAttach {directory / "report.txt"}')
    except Exception as exc:
        (directory/'error.txt').write_text(str(exc)+'\n')
        p.exit(1,f'Diagnostic failed: {exc}\nDetails/artifacts: {directory}\n')


if __name__=='__main__': main()
