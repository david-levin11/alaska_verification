import os
import requests
import fsspec
import duckdb
import xarray as xr
import pandas as pd
import numpy as np
from collections import defaultdict
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import wind_config as config

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
    """
    Return filtered S3 GRIB file paths for both Speed and Direction wind forecasts from NDFD.
    """
    # Ensure temp cache dir exists
    tmp = "tmp"
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
        dir_url = f'simplecache::s3://{dir_file}' if dir_file else None

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

        if len(element_keys) == 2:
            spd_key, dir_key = element_keys
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
            iy, ix = ll_to_index(lat, lon, lats, lons)
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

def extract_ndfd_forecasts_parallel(speed_files, direction_files, station_df, tmp_dir="tmp"):
    ensure_dir(tmp_dir)
    element_keys = config.NDFD_ELEMENT_STRINGS[config.ELEMENT]

    dir_map = {extract_timestamp(f): f for f in direction_files}
    matched_pairs = []
    for speed_file in speed_files:
        stime = extract_timestamp(speed_file)
        if len(element_keys) == 2:
            match = min(direction_files, key=lambda f: abs(extract_timestamp(f) - stime))
            if abs(extract_timestamp(match) - stime) <= pd.Timedelta("1 minute"):
                matched_pairs.append((speed_file, match))
        else:
            matched_pairs.append((speed_file, None))

    print(f"🔄 Matched {len(matched_pairs)} file pairs.")

    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(process_file_pair, s, d, station_df, tmp_dir, element_keys) for s, d in matched_pairs]
        for future in as_completed(futures):
            results.append(future.result())

    df_combined = pd.concat(results, ignore_index=True)
    return df_combined

def extract_ndfd_forecasts(speed_files, direction_files, station_df, element, paired=True, tmp_dir="tmp"):
    global config
    ensure_dir(tmp_dir)
    records = []
    speed_var = config.NDFD_ELEMENT_STRINGS[element][0]
    dir_var = config.NDFD_ELEMENT_STRINGS[element][1] if paired else None
    dir_files_with_time = [(f, extract_timestamp(f)) for f in direction_files] if paired else []

    for speed_file in speed_files:
        speed_time = extract_timestamp(speed_file)
        best_match = None
        smallest_diff = pd.Timedelta("2 minutes")

        if paired:
            for dir_file, dir_time in dir_files_with_time:
                time_diff = abs(speed_time - dir_time)
                if time_diff <= pd.Timedelta("1 minute") and time_diff < smallest_diff:
                    smallest_diff = time_diff
                    best_match = dir_file
            if not best_match:
                print(f"⚠️ No direction match within 1 minute for {speed_file}")
                continue

        speed_url = f'simplecache::s3://{speed_file}'
        dir_url = f'simplecache::s3://{best_match}' if paired else None
        print(f"📦 Processing speed: {speed_url}")
        if paired:
            print(f"📦 Processing direction: {dir_url}")

        try:
            with fsspec.open(speed_url, s3={"anon": True}, filecache={"cache_storage": tmp_dir}) as f_speed:
                ds_speed = xr.open_dataset(f_speed.name, engine='cfgrib', backend_kwargs={'indexpath': ''}, decode_timedelta=True)

            if paired:
                with fsspec.open(dir_url, s3={"anon": True}, filecache={"cache_storage": tmp_dir}) as f_dir:
                    ds_dir = xr.open_dataset(f_dir.name, engine='cfgrib', backend_kwargs={'indexpath': ''}, decode_timedelta=True)
            init_time = pd.to_datetime(ds_speed.time.values)
            #print(f"Initial time is: {init_time}")
            lats = ds_speed.latitude.values
            lons = ds_speed.longitude.values - 360
            steps = pd.to_timedelta(ds_speed.step.values)
            valid_times = pd.to_datetime(ds_speed.valid_time.values)
            speed_array = ds_speed[speed_var].values
            dir_array = ds_dir[dir_var].values if paired else None

            for _, row in station_df.iterrows():
                stid = row["stid"]
                lat = row["latitude"]
                lon = row["longitude"]
                iy, ix = ll_to_index(lat, lon, lats, lons)
                spd_values = speed_array[:, iy, ix]
                dir_values = dir_array[:, iy, ix] if paired else [None] * len(spd_values)

                for step, valid_time, spd, direc in zip(steps, valid_times, spd_values, dir_values):
                    step_hr = int(step.total_seconds() / 3600)
                    record = {
                        "station_id": stid,
                        "init_time": init_time,
                        "valid_time": valid_time,
                        "forecast_hour": step_hr
                    }
                    # add additional logic here for other elements
                    if element == "Wind":
                        record["wind_speed_kt"] = round(float(spd * 1.94384), 2)
                    if paired:
                        record["wind_dir_deg"] = round(float(direc), 0)
                    records.append(record)

        except Exception as e:
            print(f"❌ Failed to process speed file: {speed_url}\nError: {e}")

    return pd.DataFrame.from_records(records)

def append_to_parquet_duckdb(df_new, parquet_path, unique_keys=["station_id", "init_time", "forecast_hour", "valid_time"]):
    import duckdb

    if not os.path.exists(parquet_path):
        df_new.to_parquet(parquet_path, index=False)
        print(f"🆕 Created new parquet file at {parquet_path}")
    else:
        con = duckdb.connect()
        con.execute("INSTALL parquet; LOAD parquet;")
        
        # Load existing data
        df_existing = con.execute(f"SELECT * FROM read_parquet('{parquet_path}')").df()

        # Combine and deduplicate
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=unique_keys)

        # Save updated dataset
        df_combined.to_parquet(parquet_path, index=False)
        print(f"✅ Appended data to {parquet_path} (deduplicated on {unique_keys})")


if __name__ == "__main__":

    if not os.path.exists(os.path.join(config.OBS, config.METADATA)):
        print(f"Couldn't find {config.METADATA} in {config.OBS}...will need to create the file")
        ensure_dir(config.OBS)
        print(f'Creating metadata file from {config.METADATA_URL}')
        meta_json = create_wind_metadata(config.METADATA_URL, config.API_KEY, config.STATE, config.NETWORK, config.WIND_VARS, config.OBS_START)
        meta_df = parse_metadata(meta_json)
        meta_df.to_csv(os.path.join(config.OBS, config.METADATA), index=False)
        print(f"All done creating metadata. Saved {config.METADATA} in {config.OBS}.")

    station_df = pd.read_csv(os.path.join(config.OBS, config.METADATA))

    # Get list of speed and direction files
    filtered_files = get_ndfd_file_list(config.OBS_START, config.OBS_END, config.NDFD_DICT)

    speed_key, dir_key, gust_key = config.NDFD_FILE_STRINGS[config.ELEMENT]
    speed_files = filtered_files[speed_key]
    direction_files = filtered_files.get(dir_key, [])  # Safe fallback for scalar elements

    # Use the parallel extraction function
    df_ndfd = extract_ndfd_forecasts_parallel(speed_files, direction_files, station_df)

    # Save to Parquet with deduplication on init + valid time
    parquet_dir = os.path.join(config.MODEL_DIR, config.NDFD_DIR)
    ensure_dir(parquet_dir)
    parquet_file = os.path.join(parquet_dir, f"alaska_ndfd_{config.ELEMENT.lower()}_forecasts.parquet")
    append_to_parquet_duckdb(df_ndfd, parquet_file)
