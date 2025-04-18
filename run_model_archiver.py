from model_archiver import NBMArchiver
import archiver_config as config
import pandas as pd
from dateutil.relativedelta import relativedelta
import os
import shutil

def run_model_archiver(archiver, model_name="nbm"):
    station_df = archiver.ensure_metadata()

    start = pd.to_datetime(config.OBS_START)
    end = pd.to_datetime(config.OBS_END)
    current = start

    while current <= end:
        chunk_end = (current + relativedelta(months=1)) - pd.Timedelta(minutes=1)
        if chunk_end > end:
            chunk_end = end

        print(f"📦 Model: {model_name} — {current:%Y-%m} chunk")

        config.OBS_START = current.strftime("%Y-%m-%d %H:%M")
        config.OBS_END = chunk_end.strftime("%Y-%m-%d %H:%M")

        df = archiver.download_data(stations=station_df, model=model_name)
        #df = archiver.process_files(ds)

        s3_url = f"{config.NBM_S3_URL}{current.year}_{current.month:02d}_{model_name}_{config.ELEMENT.lower()}_archive.parquet"
        archiver.write_to_s3(df, s3_url)

        shutil.rmtree(config.TMP, ignore_errors=True)
        os.makedirs(config.TMP)

        current += relativedelta(months=1)

if __name__ == "__main__":
    archiver = NBMArchiver(config)
    run_model_archiver(archiver, model_name="nbm")
