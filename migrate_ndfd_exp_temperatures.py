"""Preserve pre-12h experimental temperature archives under *_native."""
import argparse
from pathlib import Path
import pandas as pd

from experimental_ndfd import monthly_plan, install_plan
from import_ndfd_experimental import archive_lock


def migration_plan(root):
    plan = []
    for element, statistic in [('maxt','maximum'),('mint','minimum')]:
        for source in sorted((Path(root)/'ndfd_exp'/element).glob('*_archive.parquet')):
            old = pd.read_parquet(source)
            if old.empty:
                continue
            if 'statistic' not in old:
                raise ValueError(f'Cannot identify temperature provenance: {source}')
            native = old.statistic.eq(statistic)
            if not native.any():
                continue
            if not {'source_sha256',element,'init_time','window_start','window_end'} <= set(old):
                raise ValueError(f'Not an experimental importer archive: {source}')
            plan.extend(monthly_plan({element+'_native':old.loc[native].reset_index(drop=True)},root))
            # An empty original is intentional until the NetCDF is reimported.
            # Keeping a validated empty Parquet also permits atomic rollback.
            remaining = old.loc[~native].reset_index(drop=True)
            plan.append((source,remaining,dict(native_rows_moved=int(native.sum()))))
    return plan


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive-root',type=Path,default=Path('model'))
    parser.add_argument('--apply',action='store_true')
    args = parser.parse_args(argv)
    with archive_lock(args.archive_root,args.apply):
        plan = migration_plan(args.archive_root)
        for path,_,details in plan:
            print(path,details)
        if plan and args.apply:
            print('INSTALLED:',install_plan(plan,{'operation':'migrate_native_temperatures'},args.archive_root))
        else:
            print('PREVIEW: add --apply to migrate' if plan else 'UNCHANGED: no legacy native temperatures')
    return 0


if __name__=='__main__':
    raise SystemExit(main())
