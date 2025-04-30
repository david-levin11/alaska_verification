import argparse
from model_archiver import ModelArchiver
import archiver_config as config
import pandas as pd
from dateutil.relativedelta import relativedelta
import shutil
import os
import sys

def run_monthly_archiving(start, end, model_name, element, use_local):

    # Normalize to match config keys
    model = model_name.lower()
    element_title = element.title()  # "wind" → "Wind", etc.

    # Validate
    if model not in config.HERBIE_MODELS:
        print(f"❌ Model '{model}' not recognized. Valid options: {config.HERBIE_MODELS}")
        sys.exit(1)
    if element_title not in config.ELEMENT_DICT:
        print(f"❌ Element '{element}' not recognized. Valid options: {list(config.ELEMENT_DICT.keys())}")
        sys.exit(1)

    if use_local:
        config.USE_CLOUD_STORAGE = False
        print("📁 Local storage enabled (S3 writing disabled).")
    else:
        config.USE_CLOUD_STORAGE = True

    config.MODEL = model_name
    config.ELEMENT = element
    archiver = ModelArchiver(config, start=start.strftime("%Y%m%d%H%M"))
    current = start

    while current <= end:
        chunk_end = (current + relativedelta(months=1)) - pd.Timedelta(minutes=1)
        if chunk_end > end:
            chunk_end = end

        print(f"\n📆 Processing {model_name.upper()} {element} from {current:%Y-%m-%d} to {chunk_end:%Y-%m-%d}")
        file_urls = archiver.fetch_file_list(current, chunk_end)
        print(f'File urls are: {file_urls}')
        if not file_urls:
            print("⚠️ No files found for this chunk.")
        else:
            df = archiver.process_files(file_urls)
            #print(f'Dataframe is: {df[df['station_id']=='PAAQ'].head(10)}')
            #df.to_csv('test.csv')
            if df.empty:
                print("⚠️ No data extracted for this chunk.")
            else:
                if config.USE_CLOUD_STORAGE:
                    s3_path = f"{config.S3_URLS[config.MODEL]}{current.year}_{current.month:02d}_{model}_{element.lower()}_archive.parquet"
                    archiver.write_to_s3(df, s3_path)
                else:
                    local_path = os.path.join(
                        config.MODEL_DIR,
                        model,
                        element.lower(),
                        f"{current.year}_{current.month:02d}_archive.parquet"
                    )
                    archiver.write_local_output(df, local_path)

        shutil.rmtree(config.TMP, ignore_errors=True)
        os.makedirs(config.TMP, exist_ok=True)

        current += relativedelta(months=1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Model Archiver")
    parser.add_argument("--start", required=True, help="Start datetime (e.g. 2022-01-01)")
    parser.add_argument("--end", required=True, help="End datetime (e.g. 2022-02-01)")
    parser.add_argument("--model", required=True, help="Model name (e.g. nbm, gfs, hrrrak)")
    parser.add_argument("--element", required=True, help="Forecast element (e.g. Wind, Gust, Temperature)")
    parser.add_argument(
        "--local",
        action="store_true",
        help="If set, store output locally instead of S3 (overrides USE_CLOUD_STORAGE)"
    )

    args = parser.parse_args()
    start = pd.to_datetime(args.start)
    end = pd.to_datetime(args.end)

    if args.model.lower() not in ['nbm', 'hrrr']:
        print(f"Archiving not yet set up for models other than nbm")
        raise NotImplementedError

    if args.element.title() != "Wind":
        print(f"Still working on setting up archiving for {args.element} for the {args.model}")
        raise NotImplementedError

    run_monthly_archiving(start, end, args.model, args.element, args.local)