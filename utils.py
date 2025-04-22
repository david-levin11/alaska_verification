import os
import sys
import re
import numpy as np
import pandas as pd
import requests
import fsspec
import xarray as xr
from datetime import datetime
from pathlib import Path
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

def get_ndfd_file_list(start, end, element_dict, element_type):
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
    print(f"TMP dir is: {tmp_dir}")
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
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_file_pair, s, d, station_df, tmp_dir, element_keys) for s, d in matched_pairs]
        for i, future in enumerate(as_completed(futures), 1):
            results.append(future.result())
            print(f"✅ Completed {i}/{len(matched_pairs)} file pairs.")
    
    return pd.concat(results, ignore_index=True)

def generate_model_date_range(model, config):
    cycle = config.HERBIE_CYCLES[model]
    start = pd.Timestamp(config.OBS_START)
    end = pd.Timestamp(config.OBS_END)
    return pd.date_range(start=start, end=end, freq=cycle)

def generate_chunked_date_range(model, chunk_start, chunk_end, config):
    cycle = config.HERBIE_CYCLES[model]
    return pd.date_range(start=chunk_start, end=chunk_end, freq=cycle)

def get_model_file_list(start, end, fcst_hours, cycle, base_url, model="nbm", domain="ak"):
    """
    Generate available NBM HTTPS URLs by checking if the index file (.idx) exists.

    Returns:
    - list[str] — HTTPS URLs to GRIB2 files
    """
    #base_url = "https://noaa-nbm-grib2-pds.s3.amazonaws.com"
    init_times = pd.date_range(start=start, end=end, freq=cycle)
    if model == "nbm":
        designator = "blend"
    else:
        print(f"url formatting for {base_url} for {model} not implemented. Check file name on AWS such as 'blend.t12z.f024.ak.grib2'.")
        raise NotImplementedError
        sys.exit()
    file_urls = []
    for init in init_times:
        init_date = init.strftime("%Y%m%d")
        init_hour = init.strftime("%H")
        for fh in fcst_hours:
            fxx = f"f{fh:03d}"
            relative_path = f"{designator}.{init_date}/{init_hour}/core/{designator}.t{init_hour}z.core.{fxx}.{domain}.grib2"
            full_url = f"{base_url}/{relative_path}"
            idx_url = full_url + ".idx"

            try:
                r = requests.head(idx_url, timeout=5)
                if r.ok:
                    file_urls.append(full_url)
                else:
                    print(f"⚠️ Missing: {idx_url} — {r.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"⚠️ Error accessing {idx_url}: {e}")
    #print(f"File urls are: {file_urls}")
    return file_urls

def download_subset(remote_url, remote_file, local_filename, search_strings):
    """
    Download subset of GRIB2 file based on .idx entries matching search_strings.

    Parameters:
    - remote_url: full HTTPS URL to GRIB2 file
    - remote_file: name of file (for naming output file)
    - local_filename: local file name to save subset
    - search_strings: list of strings to match in .idx lines (e.g., [":WIND:10 m above", ":WDIR:10 m above"])
    """
    print("  > Downloading a subset of NBM gribs")
    #print("🧪 Search strings received:", search_strings)
    #print(search_string)
    #sys.exit()
    local_file = os.path.join("nbm", local_filename)
    os.makedirs(os.path.dirname(local_file), exist_ok=True)

    idx_url = remote_url + ".idx"
    r = requests.get(idx_url)
    if not r.ok:
        print(f'     ❌ SORRY! Could not get index file: {idx_url} ({r.status_code} {r.reason})')
        return None

    lines = r.text.strip().split('\n')
    expr = re.compile("|".join(re.escape(s) for s in search_strings))

    byte_ranges = {}
    for n, line in enumerate(lines, start=1):
        if "ens std dev" in line:
            continue  # skip ensemble standard deviation entries

        if expr.search(line):
            parts = line.split(':')
            rangestart = int(parts[1])

            # Use next line's byte offset as range end
            if n < len(lines):
                parts_next = lines[n].split(':')
                rangeend = int(parts_next[1])
            else:
                rangeend = ''  # last entry, read to end

            byte_ranges[f'{rangestart}-{rangeend}'] = line

    if not byte_ranges:
        print(f'      ❌ Unsuccessful! No matches for {search_strings}')
        return None

    for i, (byteRange, line) in enumerate(byte_ranges.items()):
        if i == 0:
            curl = f'curl -s --range {byteRange} {remote_url} > {local_file}'
        else:
            curl = f'curl -s --range {byteRange} {remote_url} >> {local_file}'
        os.system(curl)

    if os.path.exists(local_file):
        print(f'      ✅ Success! Matched [{len(byte_ranges)}] fields from {remote_file} → {local_file}')
        return local_file
    else:
        print(f'      ❌ Failed to save subset to {local_file}')
        return None


def extract_model_subset_parallel(
    file_urls, station_df, search_strings, element, model, config
):
    rename_map = config.HERBIE_RENAME_MAP[element][model]
    conversion_map = config.HERBIE_UNIT_CONVERSIONS[element].get(model, {})
    #print(search_strings)
    #sys.exit()
    def process_file(remote_url):
        records = []
        try:
            remote_file = os.path.basename(remote_url)
            local_file = download_subset(
                remote_url=remote_url,
                remote_file=remote_file,
                local_filename=remote_file,
                search_strings=search_strings
            )

            if not os.path.exists(local_file):
                return pd.DataFrame()

            ds = xr.open_dataset(
                local_file,
                engine="cfgrib",
                backend_kwargs={"indexpath": ""},
                decode_timedelta=True
            )

            lats = ds.latitude.values
            lons = ds.longitude.values - 360
            valid_time = pd.to_datetime(ds.valid_time.values)
            forecast_hour = int(re.search(r"\.f(\d{3})\.", remote_file).group(1))

            for _, row in station_df.iterrows():
                stid = row["stid"]
                lat, lon = row["latitude"], row["longitude"]

                if stid in station_index_cache:
                    iy, ix = station_index_cache[stid]
                else:
                    iy, ix = ll_to_index(lat, lon, lats, lons)
                    station_index_cache[stid] = (iy, ix)

                record = {
                    "station_id": stid,
                    "init_time": valid_time - pd.to_timedelta(forecast_hour, unit="h"),
                    "valid_time": valid_time,
                    "forecast_hour": forecast_hour,
                }

                for grib_var, renamed_var in rename_map.items():
                    if grib_var not in ds:
                        continue
                    val = ds[grib_var].values[iy, ix]
                    factor = conversion_map.get(renamed_var, 1.0)
                    if pd.notnull(val):
                        if "deg" in renamed_var:
                            record[renamed_var] = round(float(val), 0)
                        else:
                            record[renamed_var] = round(float(val * factor), 2)

                records.append(record)

            Path(local_file).unlink(missing_ok=True)
        except Exception as e:
            print(f"❌ Failed to process {remote_url}: {e}")
        return pd.DataFrame.from_records(records)

    results = []
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_file, url) for url in file_urls]
        for i, future in enumerate(as_completed(futures), 1):
            results.append(future.result())
            print(f"✅ Completed {i}/{len(file_urls)} files.")

    return pd.concat(results, ignore_index=True)

## TODO ADD hrrrak, urma, rrfs
## TODO ADD temp, precip vars