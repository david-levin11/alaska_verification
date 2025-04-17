import os
import numpy as np
import pandas as pd
import requests
import fsspec
import xarray as xr
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import archiver_config as config  # Update 'your_module' with actual config import path

station_index_cache = {}

def ll_to_index(loclat, loclon, datalats, datalons):
    abslat = np.abs(datalats - loclat)
    abslon = np.abs(datalons - loclon)
    c = np.maximum(abslon, abslat)
    latlon_idx_flat = np.argmin(c)
    latlon_idx = np.unravel_index(latlon_idx_flat, datalons.shape)
    return latlon_idx

def create_wind_metadata(url, token, state, networks, vars, obrange):
    params = {
        "token": token,
        "vars": vars,
        "obrange": obrange,
        "network": networks,
        "state": state,
        "output": "json"
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch metadata: {response.status_code}")

def parse_metadata(data):
    stn_dict = {"stid": [], "name": [], "latitude": [], "longitude": [], "elevation": []}
    for stn in data["STATION"]:
        stn_dict['stid'].append(stn['STID'])
        stn_dict['name'].append(stn['NAME'])
        stn_dict['latitude'].append(stn['LATITUDE'])
        stn_dict['longitude'].append(stn['LONGITUDE'])
        stn_dict['elevation'].append(stn['ELEVATION'])
    return pd.DataFrame(stn_dict)

def extract_timestamp(filename):
    time_str = os.path.basename(filename).split("_")[-1]
    return datetime.strptime(time_str, "%Y%m%d%H%M")

def get_ndfd_file_list(start, end, element_dict, element_type=config.ELEMENT):
    start = pd.to_datetime(start, format="%Y%m%d%H%M") - pd.Timedelta(days=3)
    end = pd.to_datetime(end, format="%Y%m%d%H%M")
    date_range = pd.date_range(start=start, end=end, freq="D")

    base_s3 = "s3://noaa-ndfd-pds/wmo"
    fs = fsspec.filesystem("s3", anon=True)
    if element_type == "Wind":
        filtered_files = {"wspd": [], "wdir": []}
        components = ["wspd", "wdir"]
    elif element_type == "Gust":
        filtered_files = {"wgust": []}
        components = ["wgust"]

    for component in components:
        prefixes = element_dict[element_type][component]
        for tdate in date_range:
            for prefix in prefixes:
                pattern = f"{base_s3}/{component}/{tdate:%Y}/{tdate:%m}/{tdate:%d}/{prefix}_*"
                try:
                    matched_files = fs.glob(pattern)
                    for file in matched_files:
                        filename = os.path.basename(file)
                        try:
                            ftime = datetime.strptime(filename.split("_")[-1], "%Y%m%d%H%M")
                            if ftime.hour in [11, 23]:
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

        ds_dir = None
        if dir_url:
            with fsspec.open(dir_url, s3={"anon": True}, filecache={"cache_storage": tmp_dir}) as f_dir:
                ds_dir = xr.open_dataset(f_dir.name, engine='cfgrib', backend_kwargs={'indexpath': ''}, decode_timedelta=True)

        lats = ds_speed.latitude.values
        lons = ds_speed.longitude.values - 360
        steps = pd.to_timedelta(ds_speed.step.values)
        valid_times = pd.to_datetime(ds_speed.valid_time.values)

        spd_key = element_keys[0]
        speed_array = ds_speed[spd_key].values
        dir_array = ds_dir[element_keys[1]].values if ds_dir and len(element_keys) > 1 else None

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
                elif config.ELEMENT == "Gust":
                    record["wind_gust_kt"] = round(float(spd * 1.94384), 2)
                else:
                    record[spd_key] = float(spd)

                records.append(record)

    except Exception as e:
        print(f"❌ Failed to process {speed_file} + {dir_file}: {e}")
    return pd.DataFrame.from_records(records)

def extract_ndfd_forecasts_parallel(speed_files, direction_files, station_df, tmp_dir):
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

    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(process_file_pair, s, d, station_df, tmp_dir, element_keys) for s, d in matched_pairs]
        for i, future in enumerate(as_completed(futures), 1):
            results.append(future.result())
            print(f"✅ Completed {i}/{len(matched_pairs)} file pairs.")
    
    return pd.concat(results, ignore_index=True)


## TODO What happens when we don't have direction files?  I.E pulling gusts or temps in process_file_pair or extract_ndfd_forecasts parallel