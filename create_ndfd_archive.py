import os
import io
import shutil
import requests
import fsspec
import xarray as xr
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.fs as pafs
from collections import defaultdict
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import wind_config as config

# setting temp storage
os.environ["TMPDIR"] = config.TMP

#Initializing our station iy, ix values from ndfd
station_index_cache = {}

def ensure_dir(directory):
    """Ensure a directory exists. If not, create it."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Creating {directory} directory")
    else:
        print(f"{directory} already exists...skipping creation step.")

def ll_to_index(loclat, loclon, datalats, datalons):
    # index, loclat, loclon = loclatlon
    abslat = np.abs(datalats-loclat)
    abslon = np.abs(datalons-loclon)
    c = np.maximum(abslon, abslat)
    latlon_idx_flat = np.argmin(c)
    latlon_idx = np.unravel_index(latlon_idx_flat, datalons.shape)
    return latlon_idx

def create_wind_metadata(url, token, state, networks, vars, obrange):
    # setting up synoptic params
    # Parameters for the API request
    params = {
        "token": token,
        "vars": vars,  # Variables to retrieve
        "obrange": obrange,
        "network": networks,
        "state": state,
        "output": "json"           # Output format
    }

    # Make the API request
    response = requests.get(url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
    return data

def parse_metadata(data):
    stn_dict = {"stid": [], "name": [], "latitude": [], "longitude": [], "elevation": []}
    for stn in data["STATION"]:
        stn_dict['stid'].append(stn['STID'])
        stn_dict['name'].append(stn['NAME'])
        stn_dict['latitude'].append(stn['LATITUDE'])
        stn_dict['longitude'].append(stn['LONGITUDE'])
        stn_dict['elevation'].append(stn['ELEVATION'])
    meta_df = pd.DataFrame(stn_dict)
    return meta_df

def extract_timestamp(filename):
    time_str = os.path.basename(filename).split("_")[-1]
    return datetime.strptime(time_str, "%Y%m%d%H%M")


def get_ndfd_file_list(start, end, element_dict, element_type="Wind"):
    global config
    """
    Return filtered S3 GRIB file paths for both Speed and Direction wind forecasts from NDFD.
    """
    # Ensure temp cache dir exists
    tmp = config.TMP
    ensure_dir(tmp)

    # Construct date range for forecast run times
    start = pd.to_datetime(start, format="%Y%m%d%H%M") - pd.Timedelta(days=3)
    end = pd.to_datetime(end, format="%Y%m%d%H%M") - pd.Timedelta(days=0)
    date_range = pd.date_range(start=start, end=end, freq="D")

    # S3 setup
    base_s3 = "s3://noaa-ndfd-pds/wmo"
    fs = fsspec.filesystem("s3", anon=True)
    filtered_files = {"wspd": [], "wdir": []}

    for component in ["wspd", "wdir"]:
        prefixes = element_dict[element_type][component]
        print(prefixes)
        for tdate in date_range:
            for prefix in prefixes:
                pattern = f"{base_s3}/{component}/{tdate:%Y}/{tdate:%m}/{tdate:%d}/{prefix}_*"
                try:
                    matched_files = fs.glob(pattern)
                    for file in matched_files:
                        filename = os.path.basename(file)
                        try:
                            ftime = datetime.strptime(filename.split("_")[-1], "%Y%m%d%H%M")
                            if ftime.hour in [11, 23]:  # 12Z or 00Z cycles
                                filtered_files[component].append(file)
                        except ValueError:
                            continue
                except Exception as e:
                    print(f"⚠️ Could not fetch files for {pattern}: {e}")
    
    return filtered_files


def process_file_pair(speed_file, dir_file, station_df, tmp_dir, element_keys):
    records = []
    try:
        speed_url = f'simplecache::s3://{speed_file}'
        #speed_url = f'simplecache::s3://{speed_file}?cache_storage={tmp_dir}'
        dir_url = f'simplecache::s3://{dir_file}' if dir_file else None
        #dir_url = f'simplecache::s3://{dir_file}?cache_storage={tmp_dir}' if dir_file else None

        with fsspec.open(speed_url, s3={"anon": True}, filecache={"cache_storage": tmp_dir}) as f_speed:
            ds_speed = xr.open_dataset(f_speed.name, engine='cfgrib', backend_kwargs={'indexpath': ''}, decode_timedelta=True)

        if dir_url:
            with fsspec.open(dir_url, s3={"anon": True}, filecache={"cache_storage": tmp_dir}) as f_dir:
                ds_dir = xr.open_dataset(f_dir.name, engine='cfgrib', backend_kwargs={'indexpath': ''}, decode_timedelta=True)
        else:
            ds_dir = None

        lats = ds_speed.latitude.values
        lons = ds_speed.longitude.values - 360
        steps = pd.to_timedelta(ds_speed.step.values)
        valid_times = pd.to_datetime(ds_speed.valid_time.values)

        if len(element_keys) > 1:
            spd_key, dir_key, gust_key = element_keys
            speed_array = ds_speed[spd_key].values
            dir_array = ds_dir[dir_key].values if ds_dir else None
        else:
            spd_key = element_keys[0]
            speed_array = ds_speed[spd_key].values
            dir_array = None

        for _, row in station_df.iterrows():
            stid = row["stid"]
            lat = row["latitude"]
            lon = row["longitude"]

            if stid in station_index_cache:
                iy, ix = station_index_cache[stid]
            else:
                iy, ix = ll_to_index(lat, lon, lats, lons)
                station_index_cache[stid] = (iy, ix)
            
            spd_values = speed_array[:, iy, ix]
            dir_values = dir_array[:, iy, ix] if dir_array is not None else [None] * len(spd_values)

            for step, valid_time, spd, direc in zip(steps, valid_times, spd_values, dir_values):
                step_hr = int(step.total_seconds() / 3600)
                record = {
                    "station_id": stid,
                    "valid_time": valid_time,
                    "forecast_hour": step_hr,
                }
                if config.ELEMENT == "Wind":
                    record["wind_speed_kt"] = round(float(spd * 1.94384), 2)
                    if direc is not None:
                        record["wind_dir_deg"] = round(float(direc), 0)
                elif config.ELEMENT == "Temperature":
                    record["temp_f"] = round(float(spd), 1)
                else:
                    record[spd_key] = float(spd)

                records.append(record)

    except Exception as e:
        print(f"❌ Failed to process {speed_file} + {dir_file}: {e}")
    return pd.DataFrame.from_records(records)

def extract_ndfd_forecasts_parallel(speed_files, direction_files, station_df, tmp_dir=config.TMP):
    element_keys = config.NDFD_ELEMENT_STRINGS[config.ELEMENT]

    speed_with_time = sorted([(f, extract_timestamp(f)) for f in speed_files], key=lambda x: x[1])
    dir_with_time = sorted([(f, extract_timestamp(f)) for f in direction_files], key=lambda x: x[1])
    matched_pairs = []
    for speed_file, speed_time in speed_with_time:
        if len(element_keys) > 1:
            closest_match = None
            min_diff = pd.Timedelta("2 minutes")
            for dir_file, dir_time in dir_with_time:
                diff = abs(dir_time - speed_time)
                if diff <= min_diff:
                    closest_match = dir_file
                    min_diff = diff
            matched_pairs.append((speed_file, closest_match))
        else:
            matched_pairs.append((speed_file, None))

    print(f"🔄 Matched {len(matched_pairs)} file pairs.")
    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(process_file_pair, s, d, station_df, tmp_dir, element_keys) for s, d in matched_pairs]
        for i, future in enumerate(as_completed(futures), 1):
            results.append(future.result())
            print(f"✅ Completed {i}/{len(matched_pairs)} file pairs.")
    df_combined = pd.concat(results, ignore_index=True)
    return df_combined



def write_partitioned_parquet(df, s3_uri, partition_cols):
    try:
        # Add partition columns
        df["year"] = df["valid_time"].dt.year
        df["month"] = df["valid_time"].dt.month

        # Parse bucket and key prefix from s3_uri
        if not s3_uri.startswith("s3://"):
            raise ValueError("s3_uri must start with 's3://'")

        # Split into bucket and prefix
        s3_path = s3_uri.replace("s3://", "")
        bucket, *key_parts = s3_path.split("/")
        key_prefix = "/".join(key_parts).rstrip("/")

        # Set up S3 filesystem with the correct bucket
        s3 = pafs.S3FileSystem(region="us-east-2")

        # Build full path within the bucket
        full_path = f"{bucket}/{key_prefix}" if key_prefix else bucket

        # Write Parquet to S3 in partitioned folders
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(
            table,
            root_path=full_path,
            partition_cols=partition_cols,
            filesystem=s3
        )

        print(f"✅ Successfully wrote partitioned parquet to s3://{full_path}")

    except Exception as e:
        print(f"❌ Failed to write partitioned parquet: {e}")

def write_to_s3(df, s3_parquet_path, region="us-east-2"):
    try:
        fs = fsspec.filesystem("s3", profile="default", client_kwargs={"region_name": region})
        # Write the combined DataFrame back to S3
        with fs.open(s3_parquet_path, "wb") as f:
            df.to_parquet(f, index=False)
        print(f"✅ Successfully wrote to {s3_parquet_path}")

    except Exception as e:
        print(f"❌ Failed to update parquet at {s3_parquet_path}: {e}")

def append_to_parquet_s3(
    df_new,
    s3_parquet_path,
    region="us-east-2",
    unique_keys=["station_id", "forecast_hour", "valid_time"]
):
    try:
        fs = fsspec.filesystem("s3", profile="default", client_kwargs={"region_name": region})

        # If the file exists, read it from S3
        if fs.exists(s3_parquet_path):
            print(f"📥 Reading existing Parquet from {s3_parquet_path}")
            with fs.open(s3_parquet_path, "rb") as f:
                df_existing = pd.read_parquet(f)
            print(f"📊 Existing records: {len(df_existing)}")

            # Combine and deduplicate
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=unique_keys)
            print(f"🧹 Combined and deduplicated to {len(df_combined)} records")
        else:
            print(f"🆕 No existing Parquet found. Creating new one at {s3_parquet_path}")
            df_combined = df_new

        # Write the combined DataFrame back to S3
        with fs.open(s3_parquet_path, "wb") as f:
            df_combined.to_parquet(f, index=False)
        print(f"✅ Successfully wrote to {s3_parquet_path}")

    except Exception as e:
        print(f"❌ Failed to update parquet at {s3_parquet_path}: {e}")

if __name__ == "__main__":
    # ensuring tmp storage
    os.makedirs(config.TMP, exist_ok=True)
    print(f"Temp cache is: {config.TMP}")
    if not os.path.exists(os.path.join(config.OBS, config.METADATA)):
        print(f"Couldn't find {config.METADATA} in {config.OBS}...will need to create the file")
        ensure_dir(config.OBS)
        print(f'Creating metadata file from {config.METADATA_URL}')
        meta_json = create_wind_metadata(config.METADATA_URL, config.API_KEY, config.STATE, config.NETWORK, config.WIND_VARS, config.OBS_START)
        meta_df = parse_metadata(meta_json)
        meta_df.to_csv(os.path.join(config.OBS, config.METADATA), index=False)
        print(f"All done creating metadata. Saved {config.METADATA} in {config.OBS}.")

    station_df = pd.read_csv(os.path.join(config.OBS, config.METADATA))
    speed_key, dir_key, gust_key = config.NDFD_FILE_STRINGS[config.ELEMENT]
    parquet_file = f"alaska_ndfd_{config.ELEMENT.lower()}_forecasts.parquet"
    #s3_output_path = f"{config.NDFD_S3_URL}{os.path.basename(parquet_file)}"

    # Handle in monthly chunks
    start = pd.to_datetime(config.OBS_START)
    end = pd.to_datetime(config.OBS_END)
    current = start
    sites = station_df["stid"].values.tolist()
    while current <= end:
        chunk_end = (current + relativedelta(months=1)) - pd.Timedelta(minutes=1)
        if chunk_end > end:
            chunk_end = end

        print(f"🗂️ Processing chunk: {current} to {chunk_end}")

        filtered_files = get_ndfd_file_list(current.strftime("%Y%m%d%H%M"), chunk_end.strftime("%Y%m%d%H%M"), config.NDFD_DICT)
        speed_files = filtered_files[speed_key]
        direction_files = filtered_files.get(dir_key, [])

        if not speed_files:
            print(f"⚠️ No data for chunk {current} to {chunk_end} — skipping.")
        else:
            df_ndfd = extract_ndfd_forecasts_parallel(speed_files, direction_files, station_df)
            print(df_ndfd.head())
            if config.USE_CLOUD_STORAGE:
                # Partitioned write (current logic)
                 #write_partitioned_parquet(df_ndfd, config.NDFD_S3_URL, partition_cols=["year", "month"])
                s3_url = f'{config.NDFD_S3_URL}{current.year}_{current.month:02d}_ndfd_{config.ELEMENT.lower()}_archive.parquet'
                write_to_s3(df_ndfd, s3_url)
            else:
                # looping through sites and saving .csv files locally
                for site in sites:
                    site_df = df_ndfd[df_ndfd["station_id"] == site]
                    #print(site_df.head())
                    site_file = os.path.join(os.path.join(config.MODEL_DIR,config.NDFD_DIR),f"{site}_ndfd_archive.csv")
                    if os.path.exists(site_file):
                        archive_df = pd.read_csv(site_file)
                        append_df = pd.concat([archive_df, site_df], ignore_index=True)
                        updated_df = append_df.drop_duplicates(subset=["valid_time", "forecast_hour"])
                        updated_df.to_csv(site_file, index=False)
                    else:
                        archive_df = site_df.reset_index(drop=True)
                        archive_df.to_csv(site_file, index=False)

        # 🔁 Clean up and recreate the cache dir
        shutil.rmtree(config.TMP, ignore_errors=True)
        os.makedirs(config.TMP)

        current += relativedelta(months=1)

## TODO Work on functionality for model archive (NBM) and observation archive