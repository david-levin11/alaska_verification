"""Repair experimental forecasts written using startProcTime minus 84 hours.

Unmarked rows from earlier importer versions are the legacy 84-hour convention.
Move initialization forward 12h, decrease all leads by 12h, and mark the new
72-hour convention. Values and valid windows are preserved. Preview by default.
"""
import argparse
from pathlib import Path
import pandas as pd

from experimental_ndfd import KEYS,INITIALIZATION_OFFSET_HOURS,install_plan
from import_ndfd_experimental import archive_lock

LEADS = {'forecast_hour':'valid_time','forecast_hour_start':'window_start',
         'forecast_hour_end':'window_end'}


def repair_plan(root):
    base = Path(root)/'ndfd_exp'
    plan=[]
    repaired_rows=0
    directories=sorted(p for p in base.iterdir() if p.is_dir()) if base.exists() else []
    for directory in directories:
        sources = sorted(directory.glob('*_archive.parquet'))
        if not sources:
            continue
        originals = {p:pd.read_parquet(p) for p in sources}
        nonempty = [d for d in originals.values() if not d.empty]
        if not nonempty:
            continue
        data = pd.concat(nonempty,ignore_index=True)
        required = set(KEYS)|set(LEADS)|{'source_sha256','source_file','statistic'}
        if not required<=set(data):
            raise ValueError(f'Not a recognized experimental forecast archive: {directory}')
        offset = data.get('initialization_offset_hours',pd.Series(84,index=data.index)).fillna(84)
        if not offset.isin([72,84]).all():
            raise ValueError(f'Unrecognized initialization offset in {directory}')
        legacy = offset.eq(84)
        if not legacy.any():
            continue
        for lead,endpoint in LEADS.items():
            if data[[lead,endpoint,'init_time']].isna().any().any():
                raise ValueError(f'Missing timestamp/projection in {directory}')
            expected = (data[endpoint]-data.init_time).dt.total_seconds()/3600
            if (data[lead]-expected).abs().gt(1e-8).any():
                raise ValueError(f'Inconsistent timestamp/projection in {directory}')
        data.loc[legacy,'init_time'] += pd.Timedelta(hours=12)
        for column in LEADS:
            data.loc[legacy,column] -= 12
        data['initialization_offset_hours'] = INITIALIZATION_OFFSET_HOURS
        if data.duplicated(KEYS).any():
            raise ValueError(f'Repair would create duplicate station/run/window keys in {directory}; review overlapping imports first')
        repaired_rows += int(legacy.sum())
        targets = {directory/f'{month}_archive.parquet':group.reset_index(drop=True)
            for month,group in data.groupby(data.init_time.dt.strftime('%Y_%m'))}
        # Keep a validated empty file if all rows moved into the next month.
        # The original file is backed up along with every changed destination.
        for path in sorted(set(originals)|set(targets)):
            result = targets.get(path,data.iloc[:0].copy())
            if path in originals and result.equals(originals[path].reset_index(drop=True)):
                continue
            plan.append((path,result,dict(rows=len(result),initialization_offset_hours=INITIALIZATION_OFFSET_HOURS)))
    return plan,repaired_rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive-root',type=Path,default=Path('model'))
    parser.add_argument('--apply',action='store_true')
    args = parser.parse_args(argv)
    with archive_lock(args.archive_root,args.apply):
        plan,count = repair_plan(args.archive_root)
        print(f'Initialization repair: {count} legacy rows; {len(plan)} monthly files to update')
        if plan and args.apply:
            print('INSTALLED:',install_plan(plan,dict(operation='repair_initialization_offset',
                old_offset_hours=84,new_offset_hours=72,repaired_rows=count),args.archive_root))
        elif plan:
            print('PREVIEW: add --apply to repair with backups')
        else:
            print('UNCHANGED: no legacy initialization timestamps')
    return 0


if __name__=='__main__':
    raise SystemExit(main())
