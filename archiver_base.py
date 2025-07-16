from abc import ABC, abstractmethod
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.fs as pafs
import fsspec
from pathlib import Path
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



    def write_to_s3(self, df, s3_path, profile="default", region="us-east-2"):
        try:
            fs = fsspec.filesystem("s3", profile=profile, client_kwargs={"region_name": region})
            
            if fs.exists(s3_path):
                print(f"ℹ️ File exists at {s3_path}, appending to it...")
                with fs.open(s3_path, "rb") as f:
                    existing_df = pd.read_parquet(f)

                # Concatenate and drop duplicates if needed (optional)
                combined_df = pd.concat([existing_df, df], ignore_index=True).drop_duplicates()

                with fs.open(s3_path, "wb") as f:
                    combined_df.to_parquet(f, index=False)
            else:
                print(f"ℹ️ File does not exist at {s3_path}, creating new file...")
                with fs.open(s3_path, "wb") as f:
                    df.to_parquet(f, index=False)

            print(f"✅ Successfully wrote to {s3_path}")
            
        except Exception as e:
            print(f"❌ Failed to write to S3: {e}")


    def write_local_output(self, df, local_path, dedup_columns=None):
        """
        Save DataFrame locally to a Parquet file. If the file exists, append and de-duplicate.
        
        Parameters:
            df (pd.DataFrame): DataFrame to write
            local_path (str or Path): Path to local Parquet file
            dedup_columns (list or None): Columns to use for de-duplication. If None, all columns used.
        """
        try:
            local_path = Path(local_path)
            local_path.parent.mkdir(parents=True, exist_ok=True)

            if local_path.exists():
                print(f"ℹ️ File exists at {local_path}, appending and de-duplicating...")
                existing_df = pd.read_parquet(local_path)
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                if dedup_columns:
                    combined_df = combined_df.drop_duplicates(subset=dedup_columns)
                else:
                    combined_df = combined_df.drop_duplicates()
            else:
                print(f"ℹ️ Creating new file at {local_path}...")
                combined_df = df

            combined_df.to_parquet(local_path, index=False)
            print(f"📁 Saved locally: {local_path}")
            
        except Exception as e:
            print(f"❌ Failed to write local file: {local_path} — {e}")

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

