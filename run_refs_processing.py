"""Assemble archived REFS members and optionally persist derived statistics."""
import argparse
from pathlib import Path
from ensemble_processing import assemble_refs, summarize_refs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive-root', default='model')
    parser.add_argument('--element', required=True)
    parser.add_argument('--cycle', required=True, help='REFS initialization, UTC')
    parser.add_argument('--valid', required=True, help='Common valid time, UTC')
    parser.add_argument('--value-column', required=True, help='e.g. wind_speed_kt, wind_gust_kt, precip_6h')
    parser.add_argument('--stations', nargs='+')
    parser.add_argument('--thresholds', nargs='*', type=float, default=[])
    parser.add_argument('--allow-incomplete', action='store_true')
    parser.add_argument('--output', help='Optional summary Parquet path (replaced if present)')
    parser.add_argument('--members-output', help='Optional assembled member Parquet path')
    parser.add_argument('--rrfs-max-hour', type=int, default=60)
    parser.add_argument('--rrfsens-max-hour', type=int, default=60)
    parser.add_argument('--hrrr-max-hour', type=int, default=48)
    args = parser.parse_args()
    members = assemble_refs(args.archive_root, args.element, args.cycle, args.valid,
                            station_ids=args.stations,
                            max_forecast_hours={'rrfs': args.rrfs_max_hour,
                                                'rrfsens': args.rrfsens_max_hour,
                                                'hrrr': args.hrrr_max_hour})
    summary = summarize_refs(members, args.value_column, thresholds=args.thresholds,
                             require_complete=not args.allow_incomplete)
    print(summary.to_string(index=False) if not summary.empty else 'No stations found; supply --stations to report missing members.')
    for frame, path in ((summary, args.output), (members, args.members_output)):
        if path:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(path, index=False)
            print(f"Saved {len(frame):,} rows to {Path(path).resolve()}")


if __name__ == '__main__':
    main()
