import argparse
from ndfd_archiver import NDFDArchiver
import archiver_config as config
import pandas as pd
from dateutil.relativedelta import relativedelta
import shutil
import os
import sys

def run_monthly_archiving(start, end, element, use_local):
    # Normalize element (e.g., wind → Wind)
    element_title = element.title()

    if element_title not in config.NDFD_FILE_STRINGS:
        print(f"❌ Element '{element}' not recognized. Valid options: {list(config.NDFD_FILE_STRINGS.keys())}")
        sys.exit(1)

    config.ELEMENT = element_title
    config.USE_CLOUD_STORAGE = not use_local

    archiver = NDFDArchiver(config, start=start.strftime("%Y%m%d%H%M"))
    current = start

    while current <= end:
        chunk_end = (current + relativedelta(months=1)) - pd.Timedelta(minutes=1)
        if chunk_end > end:
            chunk_end = end

        print(f"\n📆 Processing {element_title} from {current:%Y-%m-%d} to {chunk_end:%Y-%m-%d}")
        filtered_files = archiver.fetch_file_list(current.strftime("%Y%m%d%H%M"), chunk_end.strftime("%Y%m%d%H%M"))

        file_key = config.NDFD_FILE_STRINGS[element_title][0]
        if not filtered_files[file_key]:
            print(f"⚠️ No data for {current} to {chunk_end}")
        else:
            df = archiver.process_files(filtered_files)
            filename = f"{current.year}_{current.month:02d}_ndfd_{element_title.lower()}_archive.parquet"

            if config.USE_CLOUD_STORAGE:
                s3_url = f"{config.NDFD_S3_URL}{filename}"
                archiver.write_to_s3(df, s3_url)
            else:
                local_path = os.path.join(config.NDFD_DIR, element_title.lower(), filename)
                archiver.write_local_output(df, local_path)

        shutil.rmtree(config.TMP, ignore_errors=True)
        os.makedirs(config.TMP, exist_ok=True)

        current += relativedelta(months=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDFD Archiver")
    parser.add_argument("--start", required=True, help="Start date (e.g. 2022-01-01 or 2022-01-01 00:00)")
    parser.add_argument("--end", required=True, help="End date (e.g. 2022-02-01)")
    parser.add_argument("--element", required=True, help="Forecast element (e.g. Wind, Gust)")
    parser.add_argument("--local", action="store_true", help="Write output locally instead of to S3")

    args = parser.parse_args()
    start = pd.to_datetime(args.start)
    end = pd.to_datetime(args.end)

    run_monthly_archiving(start, end, args.element, args.local)
