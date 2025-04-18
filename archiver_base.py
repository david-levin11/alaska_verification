from abc import ABC, abstractmethod
import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.fs as pafs
import fsspec
import requests
import numpy as np
import xarray as xr
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd

class Archiver(ABC):
    def __init__(self, config):
        self.config = config
        self.station_index_cache = {}

    @abstractmethod
    def fetch_file_list(self, start, end):
        pass

    @abstractmethod
    def process_files(self, file_list):
        pass

    def write_partitioned_parquet(self, df, s3_uri, partition_cols):
        try:
            df["year"] = df["valid_time"].dt.year
            df["month"] = df["valid_time"].dt.month
            s3_path = s3_uri.replace("s3://", "")
            bucket, *key_parts = s3_path.split("/")
            key_prefix = "/".join(key_parts).rstrip("/")
            s3 = pafs.S3FileSystem(region="us-east-2")
            full_path = f"{bucket}/{key_prefix}" if key_prefix else bucket
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(table, root_path=full_path, partition_cols=partition_cols, filesystem=s3)
            print(f"\u2705 Successfully wrote partitioned parquet to s3://{full_path}")
        except Exception as e:
            print(f"\u274C Failed to write partitioned parquet: {e}")

    def write_to_s3(self, df, s3_path):
        #print(df.head())
        try:
            fs = fsspec.filesystem("s3", profile="default", client_kwargs={"region_name": "us-east-2"})
            with fs.open(s3_path, "wb") as f:
                df.to_parquet(f, index=False)
            print(f"\u2705 Successfully wrote to {s3_path}")
        except Exception as e:
            print(f"\u274C Failed to write to S3: {e}")

    def append_to_parquet_s3(self, df_new, s3_path, unique_keys):
        try:
            fs = fsspec.filesystem("s3", profile="default", client_kwargs={"region_name": "us-east-2"})
            if fs.exists(s3_path):
                with fs.open(s3_path, "rb") as f:
                    df_existing = pd.read_parquet(f)
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                df_combined = df_combined.drop_duplicates(subset=unique_keys)
            else:
                df_combined = df_new
            with fs.open(s3_path, "wb") as f:
                df_combined.to_parquet(f, index=False)
            print(f"\u2705 Successfully wrote combined data to {s3_path}")
        except Exception as e:
            print(f"\u274C Failed to append Parquet on S3: {e}")

    def ensure_metadata(self):
        pass

    def download_data(self, model, dates, stations):
        """Optionally implemented by subclasses that require on-the-fly downloading"""
        pass

    def get_forecast_cycle_dates(self, model: str) -> pd.DatetimeIndex:
        from utils import generate_model_date_range
        return generate_model_date_range(model, self.config)
