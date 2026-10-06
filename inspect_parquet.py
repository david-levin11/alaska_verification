"""Inspect local source, assembled-member, or summary Parquet files (read-only).

Examples:
  python inspect_parquet.py derived/refs_members.parquet --station PAOU --value-column wind_speed_kt
  python inspect_parquet.py derived/refs_summary.parquet --station PAOU
  python inspect_parquet.py model/hrrr/wind/2026_10_archive.parquet --station PAOU --init "2026-10-01 12:00" --valid "2026-10-03 12:00"

Time filters are interpreted as UTC. Memory use scales with the selected file.
"""
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq


def inspect(path, *, station=None, cycle=None, init=None, valid=None,
            value_column=None, rows=20, missing_only=False):
    path = Path(path).expanduser().resolve()
    metadata = pq.ParquetFile(path)
    print(f"File: {path}")
    print(f"Size: {path.stat().st_size / 1024**2:.2f} MiB | Rows: {metadata.metadata.num_rows:,} | Row groups: {metadata.num_row_groups}")
    print("\nSchema:")
    print(metadata.schema_arrow)
    frame = pd.read_parquet(path)
    for column, value in (("station_id", station), ("ensemble_init_time", cycle),
                          ("init_time", init), ("valid_time", valid)):
        if value is None:
            continue
        if column not in frame:
            raise ValueError(f"Cannot filter {column}: column not present in this file")
        if column == 'station_id':
            frame = frame[frame[column].astype(str) == value]
        else:
            frame = frame[pd.to_datetime(frame[column], utc=True) == pd.to_datetime(value, utc=True)]
    if value_column and value_column not in frame:
        raise ValueError(f"Column {value_column!r} not present. Choose a column from the schema above.")
    print(f"\nRows after filters: {len(frame):,}")
    if frame.empty:
        print('No matching records. Check station ID, UTC times, and initialization-month filename.')
        return
    if 'station_id' in frame:
        print(f"Stations: {frame.station_id.nunique():,}")
    for column in ('init_time','ensemble_init_time','valid_time','forecast_hour','ensemble_forecast_hour'):
        if column in frame:
            print(f"{column}: {frame[column].min()} through {frame[column].max()}")
    print('\nNull counts (filtered rows):')
    print(frame.isna().sum().to_string())
    missing = pd.Series(False, index=frame.index)
    if 'ensemble_member_id' in frame:
        available = (frame['source_available'].fillna(False).astype(bool)
                     if 'source_available' in frame else pd.Series(True,index=frame.index))
        usable = available.copy()
        if value_column:
            usable &= np.isfinite(pd.to_numeric(frame[value_column],errors='coerce'))
        missing = ~usable
        report = frame[['ensemble_member_id']].assign(
            expected_rows=1, source_rows=available.astype(int), usable_rows=usable.astype(int))
        report = report.groupby('ensemble_member_id').sum()
        report['missing_rows'] = report.expected_rows - report.usable_rows
        print('\nMember coverage' + (f' for {value_column}' if value_column else ' (source rows only; use --value-column to check values)') + ':')
        print(report.to_string())
        if missing.any():
            details = frame.loc[missing].copy()
            details['missing_reason'] = np.where(available.loc[missing], 'value missing/nonfinite', 'source record absent')
            columns = [c for c in ['station_id','ensemble_member_id','init_time','forecast_hour',
                                   'valid_time',value_column,'missing_reason'] if c and c in details]
            print(f'\nMissing member examples (first {rows}):')
            print(details[columns].head(rows).to_string(index=False))
    elif {'n_expected','n_available','complete'}.issubset(frame.columns):
        missing = ~frame.complete.fillna(False).astype(bool)
        print('\nSummary completeness:')
        print(frame.groupby(['n_expected','n_available','complete'],dropna=False).size().rename('rows').to_string())
        print('To identify the missing member, inspect the file saved with --members-output.')
    else:
        if value_column:
            missing = ~np.isfinite(pd.to_numeric(frame[value_column],errors='coerce'))
        if 'member_id' in frame:
            print('\nSource member row counts:')
            print(frame.member_id.value_counts(dropna=False).to_string())
        print('Source archive: absent runs/members cannot be inferred from these rows alone.')
    if value_column:
        values = pd.to_numeric(frame[value_column],errors='coerce')
        print(f'\nFinite-value statistics for {value_column}:')
        print(values[np.isfinite(values)].describe().to_string())
    display = frame.loc[missing] if missing_only else frame
    print(f'\nSample ({min(rows,len(display))} of {len(display):,} rows):')
    print(display.head(rows).to_string(index=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('path',help='Local Parquet file')
    parser.add_argument('--station')
    parser.add_argument('--cycle',help='Ensemble initialization, UTC')
    parser.add_argument('--init',help='Source initialization, UTC')
    parser.add_argument('--valid',help='Valid time, UTC')
    parser.add_argument('--value-column')
    parser.add_argument('--rows',type=int,default=20)
    parser.add_argument('--missing-only',action='store_true',help='Restrict the sample to missing/incomplete rows')
    args = parser.parse_args()
    if args.rows < 1:
        parser.error('--rows must be positive')
    try:
        inspect(**vars(args))
    except (OSError,ValueError,KeyError) as exc:
        parser.exit(1,f'Inspection failed: {exc}\n')


if __name__ == '__main__':
    main()
