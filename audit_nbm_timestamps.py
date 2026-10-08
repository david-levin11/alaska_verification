"""Read-only GRIB timestamp audit for NBM core/QMD operational and experimental.

Samples actual percentile messages used by the pygrib archive path. Core wind
is excluded because it uses the separate cfgrib path. No archives are modified.
Requires requests and pygrib; retains indexes, subsets, JSON and CSV evidence.
"""
import argparse
import csv
from datetime import datetime, timedelta
import json
from pathlib import Path
import re
from uuid import uuid4

ELEMENTS = {
    'maxt': ('TMP', '2 m above ground', 18, 'max'),
    'mint': ('TMP', '2 m above ground', 18, 'min'),
    'precip6hr': ('APCP', 'surface', 6, 'acc'),
    'precip24hr': ('APCP', 'surface', 24, 'acc'),
    'snow6hr': ('ASNOW', 'surface', 6, 'acc'),
    'snow24hr': ('ASNOW', 'surface', 24, 'acc'),
    'snow48hr': ('ASNOW', 'surface', 48, 'acc'),
    'snow72hr': ('ASNOW', 'surface', 72, 'acc'),
    'wind': ('WIND', '10 m above ground', 0, ''),
    'gust': ('GUST', '10 m above ground', 0, ''),
    'rh': ('RH', '2 m above ground', 0, ''),
}


def matching_ranges(index, element, lead, percentile):
    variable, level, duration, kind = ELEMENTS[element]
    lines = [s for s in index.splitlines() if s.strip()]
    for i, line in enumerate(lines):
        parts = line.split(':')
        if f':{variable}:{level}:' not in line:
            continue
        if not re.fullmatch(rf'{percentile}% level', parts[-1].strip()):
            continue
        matched = False
        for field in parts:
            if duration:
                m = re.fullmatch(r'(\d+)-(\d+) (hour|day) '+kind+r' fcst', field)
                if not m and kind == 'acc':
                    m = re.fullmatch(r'(\d+)-(\d+) (hour|day) acc@\(fcst,dt='+str(duration)+r' hour\),missing=\d+', field)
                if m:
                    scale = 24 if m[3] == 'day' else 1
                    matched = int(m[2])*scale == lead and (int(m[2])-int(m[1]))*scale == duration
            else:
                m = re.fullmatch(r'(\d+) (hour|day) fcst', field)
                if m:
                    matched = int(m[1])*(24 if m[2] == 'day' else 1) == lead
            if matched:
                break
        if matched:
            start = int(parts[1])
            end = next((int(s.split(':')[1])-1 for s in lines[i+1:]
                        if int(s.split(':')[1]) > start), None)
            yield line, start, end


def key(message, name):
    try:
        return message[name]
    except (KeyError, ValueError, RuntimeError):
        return None


def assess(message, init, lead, percentile):
    names = ['dataDate', 'dataTime', 'stepType', 'stepUnits', 'startStep',
             'endStep', 'forecastTime', 'stepRange', 'validityDate', 'validityTime',
             'percentileValue']
    result = {n: key(message, n) for n in names}
    if result['percentileValue'] != percentile:
        raise ValueError('Decoded message is not the requested percentile')
    reference = datetime.strptime(f"{int(result['dataDate']):08d}{int(result['dataTime']):04d}", '%Y%m%d%H%M')
    end_names = [x+'OfEndOfOverallTimeInterval' for x in ['year','month','day','hour','minute','second']]
    values = [key(message, n) for n in end_names]
    result.update(dict(zip(end_names, values)))
    if all(v is not None for v in values):
        endpoint = datetime(*map(int, values))
        endpoint_source = 'explicit interval endpoint'
    else:
        endpoint = datetime.strptime(f"{int(result['validityDate']):08d}{int(result['validityTime']):04d}", '%Y%m%d%H%M')
        endpoint_source = 'validityDate/validityTime'
    valid = message.validDate
    archived_init = valid - timedelta(hours=lead)
    from grib_timestamps import percentile_times
    try:
        corrected_init, corrected_valid = percentile_times(message, lead)
        result.update(corrected_init=corrected_init.isoformat(), corrected_valid=corrected_valid.isoformat(),
                      corrected_matches_metadata=(corrected_init == reference and corrected_valid == endpoint))
    except ValueError as exc:
        result.update(corrected_matches_metadata=False, corrected_error=str(exc))
    result.update(reference_time=reference.isoformat(), endpoint=endpoint.isoformat(),
                  endpoint_source=endpoint_source, pygrib_validDate=valid.isoformat(),
                  current_archiver_init=archived_init.isoformat(),
                  init_offset_hours=(archived_init-reference).total_seconds()/3600,
                  valid_offset_hours=(valid-endpoint).total_seconds()/3600)
    if reference != init or endpoint != init+timedelta(hours=lead):
        result['status'] = 'METADATA_MISMATCH'
    elif archived_init != reference or valid != endpoint:
        result['status'] = 'OFFSET'
    else:
        result['status'] = 'PASS'
    return result


def download_range(session, url, start, end, path):
    with session.get(url, headers={'Range': f'bytes={start}-{end if end is not None else ""}'},
                     timeout=90, stream=True) as response:
        response.raise_for_status()
        m = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+|\*)', response.headers.get('Content-Range', ''))
        if response.status_code != 206 or not m or int(m[1]) != start or (end is not None and int(m[2]) != end):
            raise ValueError('Server did not honor byte range')
        blob = response.content
        if len(blob) != int(m[2])-start+1 or not blob.startswith(b'GRIB'):
            raise ValueError('Invalid or incomplete subset')
        path.write_bytes(blob)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--dates', nargs='+', required=True, help='YYYY-MM-DD dates to sample')
    p.add_argument('--regions', nargs='+', choices=['alaska','hawaii'], default=['alaska','hawaii'])
    p.add_argument('--models', nargs='+', choices=['nbm','nbmqmd','nbm_exp','nbmqmd_exp'], default=['nbm','nbmqmd'])
    p.add_argument('--elements', nargs='+', choices=list(ELEMENTS), default=list(ELEMENTS))
    p.add_argument('--cycles', nargs='+', type=int, help='Override default core 01Z / QMD 00Z and 12Z')
    p.add_argument('--forecast-hours', nargs='+', type=int, help='Override default core 11/29/53/83 and QMD 6/18/24/30')
    p.add_argument('--percentile', type=int, choices=[5,10,25,50,75,90,95], default=50)
    p.add_argument('--output-root', default='timestamp_diagnostics')
    a = p.parse_args()
    dates = [datetime.strptime(d, '%Y-%m-%d') for d in a.dates]
    if a.cycles and any(h < 0 or h > 23 for h in a.cycles):
        p.error('cycles must be 0 through 23')
    if a.forecast_hours and any(h < 0 for h in a.forecast_hours):
        p.error('forecast hours must be nonnegative')
    import requests
    import pygrib
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1, status_forcelist=[429,500,502,503,504])))
    out = Path(a.output_root) / ('nbm-times-'+uuid4().hex[:12])
    out.mkdir(parents=True)
    rows = []
    print(f'Artifacts retained: {out}', flush=True)
    def save():
        (out/'report.json').write_text(json.dumps({'arguments':vars(a),'results':rows}, indent=2, default=str)+'\n')
        columns = ['region','model','date','cycle','lead','element','status','init_offset_hours','valid_offset_hours','detail']
        with (out/'summary.csv').open('w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
            w.writeheader()
            w.writerows(rows)
    for region in a.regions:
        for model in a.models:
            qmd = 'qmd' in model
            eligible = [e for e in a.elements if (not e.startswith('snow') if qmd else e.startswith('snow'))]
            if region == 'hawaii' and not qmd:
                eligible = []  # Hawaii snow is not archived.
            if not eligible:
                rows.append(dict(region=region, model=model, status='SKIP_UNSUPPORTED', detail='No selected elements use this archived percentile path'))
                continue
            bucket = 'noaa-nbm-para-pds' if model.endswith('_exp') else 'noaa-nbm-grib2-pds'
            suite = 'qmd' if qmd else 'core'
            domain = 'hi' if region == 'hawaii' else 'ak'
            for date in dates:
                for cycle in (a.cycles if a.cycles is not None else ([0,12] if qmd else [1])):
                    for lead in (a.forecast_hours if a.forecast_hours is not None else ([6,18,24,30] if qmd else [11,29,53,83])):
                        init = date.replace(hour=cycle)
                        url = f'https://{bucket}.s3.amazonaws.com/blend.{date:%Y%m%d}/{cycle:02d}/{suite}/blend.t{cycle:02d}z.{suite}.f{lead:03d}.{domain}.grib2'
                        case = out/f'{region}-{model}-{date:%Y%m%d}-{cycle:02d}-f{lead:03d}'
                        case.mkdir()
                        base = dict(region=region,model=model,date=f'{date:%Y-%m-%d}',cycle=cycle,lead=lead,source_url=url)
                        try:
                            response = session.get(url+'.idx',timeout=90)
                            response.raise_for_status()
                            index = response.text
                            (case/'source.idx').write_text(index)
                        except Exception as exc:
                            rows.append(dict(base,status='ERROR',detail=f'{type(exc).__name__}: {exc}'))
                            save()
                            print(f'ERROR: {case.name}: {exc}',flush=True)
                            continue
                        for element in eligible:
                            row = dict(base,element=element)
                            try:
                                matches = list(matching_ranges(index,element,lead,a.percentile))
                                if len(matches) != 1:
                                    row.update(status='NO_MATCH' if not matches else 'AMBIGUOUS',detail=f'{len(matches)} matching percentile index entries')
                                else:
                                    line,start,end = matches[0]
                                    row['index_line'] = line
                                    subset = case/f'{element}.grib2'
                                    download_range(session,url,start,end,subset)
                                    with pygrib.open(str(subset)) as grbs:
                                        messages = [g for g in grbs if key(g,'percentileValue') == a.percentile]
                                        if len(messages) != 1:
                                            raise ValueError('Expected exactly one decoded percentile message')
                                        row.update(assess(messages[0],init,lead,a.percentile))
                            except Exception as exc:
                                row.update(status='ERROR',detail=f'{type(exc).__name__}: {exc}')
                            rows.append(row)
                            print(f"{row['status']}: {case.name} {element}; init offset={row.get('init_offset_hours','n/a')} h",flush=True)
                            save()
    save()
    counts = {s:sum(r['status']==s for r in rows) for s in sorted({r['status'] for r in rows})}
    print(json.dumps(counts,indent=2))
    print(f'Report: {out / "report.json"}')
    print('OFFSET describes the legacy validDate calculation; corrected_matches_metadata tests the fixed decoder.')
    print('NO_MATCH is not a pass; daily fields may not exist at every sampled lead.')
    print('Samples diagnose this decoder, not the provenance or correctness of every historical Parquet row.')
    if any(r['status'] in ('ERROR','AMBIGUOUS','METADATA_MISMATCH','OFFSET') for r in rows):
        return 1
    return 0 if any(r['status']=='PASS' for r in rows) else 2


if __name__ == '__main__':
    raise SystemExit(main())
