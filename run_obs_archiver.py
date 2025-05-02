import argparse
import pandas as pd
from dateutil.relativedelta import relativedelta
import os
#import shutil
import sys
import archiver_config as config
from obs_archiver import ObsArchiver

def run_monthly_obs_archiving(start, end, element, use_local):
    element_title = element.title()

    if element_title not in config.OBS_VARS:
        print(f"❌ Element '{element}' not recognized. Valid options: {list(config.OBS_VARS.keys())}")
        sys.exit(1)

    if use_local:
        config.USE_CLOUD_STORAGE = False
        print("📁 Local storage enabled (S3 writing disabled).")
    else:
        config.USE_CLOUD_STORAGE = True

    config.ELEMENT = element_title
    archiver = ObsArchiver(config)
    stations = archiver.get_station_metadata()

    current = start
    while current <= end:
        chunk_end = (current + relativedelta(months=1)) - pd.Timedelta(minutes=1)
        if chunk_end > end:
            chunk_end = end

        print(f"\n📆 Fetching OBS {element_title} from {current:%Y-%m-%d} to {chunk_end:%Y-%m-%d}")
        df = archiver.fetch_observations(stations, current.strftime("%Y%m%d%H%M"), chunk_end.strftime("%Y%m%d%H%M"))

        if df.empty:
            print("⚠️ No data extracted for this chunk.")
        else:
            if config.USE_CLOUD_STORAGE:
                s3_path = f"{config.S3_URLS['obs']}{current.year}_{current.month:02d}_obs_{element.lower()}_archive.parquet"
                archiver.write_to_s3(df, s3_path)
            else:
                local_path = os.path.join(
                    config.MODEL_DIR,
                    "obs",
                    element.lower(),
                    f"{current.year}_{current.month:02d}_archive.parquet"
                )
                archiver.write_local_output(df, local_path)

        #shutil.rmtree(config.TMP, ignore_errors=True)
        #os.makedirs(config.TMP, exist_ok=True)

        current += relativedelta(months=1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Observation Archiver")
    parser.add_argument("--start", required=True, help="Start datetime (e.g. 2022-01-01)")
    parser.add_argument("--end", required=True, help="End datetime (e.g. 2022-03-01)")
    parser.add_argument("--element", required=True, help="Observation element (e.g. Wind)")
    parser.add_argument(
        "--local",
        action="store_true",
        help="If set, store output locally instead of S3"
    )

    args = parser.parse_args()
    start = pd.to_datetime(args.start)
    end = pd.to_datetime(args.end)

    run_monthly_obs_archiving(start, end, args.element, args.local)
