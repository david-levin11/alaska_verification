"""Download one NBM QMD temperature message and retain timestamp evidence.

Requires requests and pygrib. Does not import archive config or modify archives.
"""
import argparse
from datetime import datetime, timedelta
import json
from pathlib import Path
import re
import sys
from uuid import uuid4


def select_message(index, element, lead):
    lines = [line for line in index.splitlines() if line.strip()]
    kind = 'max' if element == 'maxt' else 'min'
    label = f'{lead-18}-{lead} hour {kind} fcst'
    for i, line in enumerate(lines):
        if ':TMP:2 m above ground:' in line and label in line:
            start = int(line.split(':')[1])
            # Submessages may share an offset; use the next distinct offset.
            end = next((int(x.split(':')[1])-1 for x in lines[i+1:]
                        if int(x.split(':')[1]) > start), None)
            return line, start, end
    raise ValueError(f'No TMP 2 m {label} message in index')


def read_key(message, key):
    try:
        return message[key]
    except (KeyError, RuntimeError, ValueError):
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date', default='2026-10-06')
    parser.add_argument('--cycle', type=int, choices=range(24), default=0)
    parser.add_argument('--region', choices=['hawaii', 'alaska'], default='hawaii')
    parser.add_argument('--element', choices=['maxt', 'mint'], default='maxt')
    parser.add_argument('--forecast-hour', type=int, default=30)
    parser.add_argument('--output-root', default='temperature_diagnostics')
    args = parser.parse_args()
    if args.forecast_hour < 18:
        parser.error('--forecast-hour must be at least 18')
    import requests
    import pygrib

    init = datetime.strptime(args.date, '%Y-%m-%d').replace(hour=args.cycle)
    domain = 'hi' if args.region == 'hawaii' else 'ak'
    name = f'blend.t{args.cycle:02d}z.qmd.f{args.forecast_hour:03d}.{domain}.grib2'
    url = (f'https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.{init:%Y%m%d}/'
           f'{args.cycle:02d}/qmd/{name}')
    out = Path(args.output_root) / f'{args.element}-{uuid4().hex[:12]}'
    out.mkdir(parents=True)
    print(f'Artifacts retained: {out}', flush=True)
    report = {'source_url': url, 'element': args.element,
              'requested_init': init.isoformat(), 'forecast_hour': args.forecast_hour}
    try:
        response = requests.get(url + '.idx', timeout=90)
        response.raise_for_status()
        (out / 'source.idx').write_text(response.text)
        line, start, end = select_message(response.text, args.element, args.forecast_hour)
        report['selected_index_line'] = line
        byte_range = f'bytes={start}-{end if end is not None else ""}'
        # Require range support so a server cannot silently send the full file.
        with requests.get(url, headers={'Range': byte_range}, timeout=90, stream=True) as response:
            response.raise_for_status()
            content_range = response.headers.get('Content-Range', '')
            match = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+|\*)', content_range)
            if response.status_code != 206 or not match or int(match[1]) != start:
                raise ValueError('Server did not honor requested byte range')
            if end is not None and int(match[2]) != end:
                raise ValueError('Unexpected range endpoint')
            blob = response.content
            if len(blob) != int(match[2]) - start + 1 or not blob.startswith(b'GRIB'):
                raise ValueError('Incomplete or invalid GRIB subset')
        subset = out / 'subset.grib2'
        subset.write_bytes(blob)
        keys = ['name', 'shortName', 'dataDate', 'dataTime', 'stepType',
                'stepUnits', 'forecastTime', 'startStep', 'endStep', 'stepRange',
                'validityDate', 'validityTime', 'percentileValue',
                'yearOfEndOfOverallTimeInterval', 'monthOfEndOfOverallTimeInterval',
                'dayOfEndOfOverallTimeInterval', 'hourOfEndOfOverallTimeInterval',
                'minuteOfEndOfOverallTimeInterval', 'secondOfEndOfOverallTimeInterval']
        report['messages'] = []
        with pygrib.open(str(subset)) as messages:
            for message in messages:
                metadata = {key: read_key(message, key) for key in keys}
                valid = message.validDate
                reference = message.analDate
                current_init = valid - timedelta(hours=args.forecast_hour)
                metadata.update(analDate=reference.isoformat(), validDate=valid.isoformat(),
                                current_archiver_init=current_init.isoformat(),
                                current_archiver_init_offset_hours=(current_init-init).total_seconds()/3600,
                                reference_matches_requested_init=reference == init,
                                expected_window_start=(init+timedelta(hours=args.forecast_hour-18)).isoformat(),
                                expected_window_end=(init+timedelta(hours=args.forecast_hour)).isoformat())
                report['messages'].append(metadata)
        if not report['messages']:
            raise ValueError('No GRIB messages decoded')
        report['status'] = 'DECODED'
    except Exception as exc:
        report['status'] = 'ERROR'
        report['error'] = f'{type(exc).__name__}: {exc}'
    text = json.dumps(report, indent=2, default=str)
    (out / 'report.json').write_text(text + '\n')
    print(text)
    print(f'Report: {out / "report.json"}')
    return 1 if report['status'] == 'ERROR' else 0


if __name__ == '__main__':
    sys.exit(main())
