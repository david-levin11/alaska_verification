from ndfd_archiver import NDFDArchiver
import archiver_config as config
import pandas as pd
from dateutil.relativedelta import relativedelta
import shutil
import os

def run_monthly_archiving():
    archiver = NDFDArchiver(config)
    start = pd.to_datetime(config.OBS_START)
    end = pd.to_datetime(config.OBS_END)
    current = start

    while current <= end:
        chunk_end = (current + relativedelta(months=1)) - pd.Timedelta(minutes=1)
        if chunk_end > end:
            chunk_end = end

        print(f"Processing chunk: {current} to {chunk_end}")
        filtered_files = archiver.fetch_file_list(current.strftime("%Y%m%d%H%M"), chunk_end.strftime("%Y%m%d%H%M"))

        if not filtered_files[config.NDFD_FILE_STRINGS[config.ELEMENT][0]]:
            print(f"No data for {current} to {chunk_end}")
        else:
            df = archiver.process_files(filtered_files)
            s3_url = f'{config.NDFD_S3_URL}{current.year}_{current.month:02d}_ndfd_{config.ELEMENT.lower()}_archive.parquet'
            archiver.write_to_s3(df, s3_url)

        shutil.rmtree(config.TMP, ignore_errors=True)
        os.makedirs(config.TMP)

        current += relativedelta(months=1)

if __name__ == "__main__":
    run_monthly_archiving()
