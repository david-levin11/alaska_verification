"""Move legacy local NDFD archives into model/ndfd/<element>/YYYY_MM_archive.parquet.

Preview by default; --apply backs up originals before changing any archive.
Existing destinations are merged, removing only exact repeated rows. Records
with differing contents are preserved rather than choosing a forecast silently.
Run with archive writers stopped. S3 objects are not touched.
"""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from uuid import uuid4

import pandas as pd
import pyarrow.parquet as pq


LEGACY_NAME = re.compile(r"^(\d{4})_(0[1-9]|1[0-2])_ndfd_([A-Za-z0-9]+)_archive\.parquet$")
MONTH_NAME = re.compile(r"^(\d{4})_(0[1-9]|1[0-2])_archive\.parquet$")


def migration_plan(root, region="alaska"):
    root = Path(root).resolve()
    regions = ("alaska", "hawaii") if region == "all" else (region,)
    groups = defaultdict(list)
    for name in regions:
        regional_root = root / "hawaii" if name == "hawaii" else root
        source_root = regional_root / "ndfd"
        if not source_root.exists():
            continue
        for source in sorted(source_root.rglob("*_archive.parquet")):
            if source.is_symlink() or not source.resolve().is_relative_to(root):
                raise ValueError(f"Refusing linked/outside source: {source}")
            match = LEGACY_NAME.fullmatch(source.name)
            if match:
                year, month, element = match.groups()
                if source.parent != source_root and source.parent.name.lower() != element.lower():
                    raise ValueError(f"Filename and element directory disagree: {source}")
            else:
                match = MONTH_NAME.fullmatch(source.name)
                if not match or source.parent == source_root:
                    raise ValueError(f"Unrecognized NDFD archive name: {source}")
                year, month = match.groups()
                element = source.parent.name
            destination = regional_root / "model" / "ndfd" / element.lower() / f"{year}_{month}_archive.parquet"
            if not destination.resolve().is_relative_to(root):
                raise ValueError(f"Destination escapes archive root: {destination}")
            groups[destination].append(source)
    return dict(sorted(groups.items()))


def migrate(root=None, *, region="alaska", apply=False):
    root = Path(root or Path(__file__).resolve().parent).resolve()
    if region not in ("alaska", "hawaii", "all"):
        raise ValueError(f"Unsupported region: {region}")
    plan = migration_plan(root, region)
    if not plan:
        print("No legacy NDFD archives found; nothing to migrate.")
        return None
    for destination, sources in plan.items():
        action = "MERGE" if destination.exists() or len(sources) > 1 else "MOVE"
        print(f"{action}: {', '.join(str(p.relative_to(root)) for p in sources)} -> {destination.relative_to(root)}")
    if not apply:
        print("Preview only. Rerun with --apply to back up and migrate these files.")
        return None

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = root / f"ndfd_migration_backup_{stamp}_{uuid4().hex[:8]}"
    backup.mkdir()
    manifest = {"status": "IN_PROGRESS", "root": str(root), "files": []}
    manifest_path = backup / "manifest.json"

    def save_manifest():
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    # Finish backing up every affected original before installing any output.
    try:
        for destination, sources in plan.items():
            for original in sources + ([destination] if destination.exists() else []):
                copy = backup / original.relative_to(root)
                copy.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(original, copy)
            manifest["files"].append({"sources": [str(p.relative_to(root)) for p in sources],
                                      "destination": str(destination.relative_to(root)), "status": "BACKED_UP"})
        save_manifest()
        for entry, (destination, sources) in zip(manifest["files"], plan.items()):
            destination.parent.mkdir(parents=True, exist_ok=True)
            if not destination.exists() and len(sources) == 1:
                # Validate it is readable Parquet, then preserve the bytes exactly.
                rows = pq.read_metadata(sources[0]).num_rows
                shutil.move(str(sources[0]), str(destination))
                entry.update(rows=rows, exact_duplicates_removed=0)
            else:
                paths = ([destination] if destination.exists() else []) + sources
                frames = [pd.read_parquet(p) for p in paths]
                combined = pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True)
                before = sum(len(f) for f in frames)
                fd, temporary = tempfile.mkstemp(prefix=".ndfd_migration_", suffix=".parquet", dir=destination.parent)
                os.close(fd)
                try:
                    combined.to_parquet(temporary, index=False)
                    if pq.read_metadata(temporary).num_rows != len(combined):
                        raise ValueError(f"Staged row count mismatch: {destination}")
                    os.replace(temporary, destination)
                finally:
                    Path(temporary).unlink(missing_ok=True)
                # Only remove sources after the completed output is installed.
                for source in sources:
                    source.unlink()
                entry.update(rows=len(combined), exact_duplicates_removed=before-len(combined))
            entry["status"] = "MIGRATED"
            save_manifest()
            print(f"Saved {destination}: {entry['rows']:,} rows")
        manifest["status"] = "COMPLETE"
        save_manifest()
    except Exception as exc:
        manifest["status"] = "FAILED"
        manifest["error"] = str(exc)
        save_manifest()
        raise
    finally:
        print(f"Originals and migration record: {backup}")
    return backup


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent,
                        help="Repository/archive root containing ndfd/ and model/ (default: script directory)")
    parser.add_argument("--region", choices=["alaska", "hawaii", "all"], default="alaska")
    parser.add_argument("--apply", action="store_true", help="Back up originals and perform the migration")
    args = parser.parse_args()
    try:
        migrate(args.root, region=args.region, apply=args.apply)
    except Exception as exc:
        parser.exit(1, f"NDFD migration failed: {exc}\n")


if __name__ == "__main__":
    main()
