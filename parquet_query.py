#!/usr/bin/env python3
import argparse
import sys
from typing import Optional

import fsspec
try:
    import s3fs  # noqa: F401  # needed for s3 URLs
except Exception:
    pass
import pyarrow.parquet as pq
import pandas as pd


def get_fs(path: str, aws_profile: Optional[str], anon: bool):
    if path.startswith("s3://"):
        kw = {}
        if aws_profile:
            kw["profile"] = aws_profile
        return fsspec.filesystem("s3", anon=anon, **({"session_kwargs": kw} if kw else {}))
    return fsspec.filesystem("file")


def find_one_parquet(path: str, fs) -> str:
    if path.endswith(".parquet"):
        if not fs.exists(path):
            raise FileNotFoundError(f"File not found: {path}")
        return path
    # Treat as directory/prefix; find a parquet file inside (recursively)
    pattern = path.rstrip("/") + "/**/*.parquet"
    matches = fs.glob(pattern)
    if not matches:
        # try non-recursive as a fallback
        pattern = path.rstrip("/") + "/*.parquet"
        matches = fs.glob(pattern)
    if not matches:
        raise FileNotFoundError(f"No .parquet files found under: {path}")
    # pick the first deterministically
    return sorted(matches)[0]


def main():
    ap = argparse.ArgumentParser(description="Peek a Parquet file: schema + first N rows (local or S3).")
    ap.add_argument("path", help="Path to a .parquet file OR a folder/prefix (e.g., s3://bucket/prefix)")
    ap.add_argument("--rows", type=int, default=10, help="Number of rows to show (default: 10)")
    ap.add_argument("--aws-profile", default="", help="AWS profile name to use (optional)")
    ap.add_argument("--anon", action="store_true", help="Use anonymous S3 access")
    args = ap.parse_args()

    fs = get_fs(args.path, aws_profile=(args.aws_profile or None), anon=args.anon)
    chosen = find_one_parquet(args.path, fs)
    print(f"📄 Using file: {chosen}")

    # Open with pyarrow without downloading locally
    with fs.open(chosen, "rb") as f:
        pf = pq.ParquetFile(f)

        # Print schema (column names + types)
        schema = pf.schema_arrow
        print("\n🧱 Schema (name: type)")
        for i in range(len(schema.names)):
            print(f"  - {schema.names[i]}: {schema.types[i]}")

        # Read a small sample from the first row group only
        if pf.num_row_groups == 0:
            print("\n(No row groups found.)")
            sys.exit(0)

        tbl = pf.read_row_group(0)  # reads only RG 0
        df = tbl.to_pandas()
        print(f"\n🔎 First {args.rows} row(s) from row-group 0:")
        with pd.option_context("display.max_columns", None, "display.width", 200):
            print(df.head(args.rows))


if __name__ == "__main__":
    main()
