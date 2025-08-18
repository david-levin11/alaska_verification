import argparse
import pandas as pd
from dateutil.relativedelta import relativedelta
import os
#import shutil
import sys
import archiver_config as config
from obs_archiver import ObsArchiver

def run_monthly_obs_archiving(start, end, element, use_local):
    if element.lower() == "wind":
        element = element.capitalize()  # "wind" → "Wind", etc.
    if element not in config.OBS_VARS:
        print(f"❌ Element '{element}' not recognized. Valid options: {list(config.OBS_VARS.keys())}")
        sys.exit(1)

    if use_local:
        config.USE_CLOUD_STORAGE = False
        print("📁 Local storage enabled (S3 writing disabled).")
    else:
        config.USE_CLOUD_STORAGE = True

    config.ELEMENT = element
    archiver = ObsArchiver(config)
    stations = archiver.get_station_metadata()

    current = start
    while current <= end:
        chunk_end = (current + relativedelta(months=1)) - pd.Timedelta(minutes=1)
        if chunk_end > end:
            chunk_end = end

        print(f"\n📆 Fetching OBS {element} from {current:%Y-%m-%d} to {chunk_end:%Y-%m-%d}")
        if element == "Wind":
            df = archiver.fetch_observations(stations, current.strftime("%Y%m%d%H%M"), chunk_end.strftime("%Y%m%d%H%M"))
        elif element == "precip24hr":
            df = archiver.fetch_precip_rolling(stations, current.strftime("%Y%m%d%H%M"), chunk_end.strftime("%Y%m%d%H%M"),accum_hours=24, step_hours=12)
        elif element == "precip6hr":
            df = archiver.fetch_precip_rolling(stations, current.strftime("%Y%m%d%H%M"), chunk_end.strftime("%Y%m%d%H%M"),accum_hours=6, step_hours=6)
        elif element == "maxt":
            df = archiver.fetch_tmax_12to06_timeseries(stations,current.strftime("%Y%m%d%H%M"), chunk_end.strftime("%Y%m%d%H%M"))
        elif element == "mint":
            df = archiver.fetch_tmin_00to18_timeseries(stations,current.strftime("%Y%m%d%H%M"), chunk_end.strftime("%Y%m%d%H%M"))
        #print(df)
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
