import os
import sys
import re
import tempfile
import shutil
import pygrib
import numpy as np
import pandas as pd
import requests
import fsspec
import xarray as xr
from datetime import datetime
from pathlib import Path
from scipy.spatial import cKDTree
from concurrent.futures import ThreadPoolExecutor, as_completed
import archiver_config as config  # Update 'your_module' with actual config import path

station_index_cache = {}

def K_to_F(kelvin):
  fahrenheit = 1.8*(kelvin-273)+32.
  return fahrenheit

def MS_to_KTS(ms):
    return ms*1.94384

def MS_to_MPH(ms):
    return ms*2.23694

def build_kdtree(lats, lons):
    """
    Build a cKDTree from 2D lat/lon arrays.
    Returns:
        tree: cKDTree object
        shape: original shape of the lat/lon grids
    """
    latlon_points = np.column_stack((lats.ravel(), lons.ravel()))
    tree = cKDTree(latlon_points)
    return tree, lats.shape

def query_kdtree(tree, shape, station_lat, station_lon):
    """
    Query cKDTree and return 2D grid indices (iy, ix)
    """
    dist, idx = tree.query([station_lat, station_lon])
    iy, ix = np.unravel_index(idx, shape)
    return iy, ix


def normalize_lons_to_minus180_180(lons):
    """Shift lons from [0, 360] to [-180, 180] only if needed."""
    if np.nanmin(lons) >= 0:
        #print("Found test case!")
        lons = ((lons + 180) % 360) - 180
    return lons

def ll_to_index(loclat, loclon, datalats, datalons):
    datalons = normalize_lons_to_minus180_180(datalons)

    abslat = np.abs(datalats - loclat)
    abslon = np.abs(datalons - loclon)
    c = np.maximum(abslon, abslat)
    latlon_idx_flat = np.argmin(c)
    latlon_idx = np.unravel_index(latlon_idx_flat, datalons.shape)
    return latlon_idx

def create_wind_metadata(url, token, state, networks, vars, obrange, precip=0):
    if precip==0:
        params = {
            "token": token,
            "vars": vars,
            "obrange": obrange,
            "network": networks,
            "state": state,
            "output": "json"
        }
    else:
        params = {
            "token": token,
            "precip": precip,
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

def create_precip_metadata(url, token, state, networks, obrange):
    params = {
        "token": token,
        "precip": "1",
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

def get_model_file_list(start, end, fcst_hours, cycle, base_url, element, model="nbm", domain="ak"):
    """
    Generate available NBM HTTPS URLs by checking if the index file (.idx) exists.

    Returns:
    - list[str] — HTTPS URLs to GRIB2 files
    """
    if domain == "ak":
        full_domain = "alaska"
    elif domain == "co":
        full_domain = "conus"
    elif domain == "hi":
        full_domain = "hawaii"
    #base_url = "https://noaa-nbm-grib2-pds.s3.amazonaws.com"
    init_times = pd.date_range(start=start, end=end, freq=cycle)
    if model == "nbm":
        designator = "blend"
        suite = "core"
    elif model == 'hrrr':
        designator = 'hrrr'
    elif model == 'urma':
        designator = f'{domain}urma'
    elif model == 'nbmqmd':
        designator = "blend"
        suite = "qmd"
    elif model == 'nbmqmd_exp':
        designator = "blend"
        suite = "qmd"
    else:
        print(f"url formatting for {base_url} for {model} not implemented. Check file name on AWS such as 'blend.t12z.f024.ak.grib2'.")
        raise NotImplementedError
        sys.exit()
    file_urls = []
    for init in init_times:
        init_date = init.strftime("%Y%m%d")
        init_hour = init.strftime("%H")
        # skipping forecast hours if urma
        if model == 'urma':
            relative_path = f"{designator}.{init_date}/{designator}.t{int(init_hour):02d}z.2dvaranl_ndfd_3p0.grb2"
            full_url = f"{base_url}/{relative_path}"
            try:
                r = requests.head(full_url, timeout=5)
                if r.ok:
                    file_urls.append(full_url)
                else:
                    print(f"⚠️ Missing: {full_url} — {r.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"⚠️ Error accessing {idx_url}: {e}")
        else:
            for fh in fcst_hours:
                if model == 'nbm':
                    fxx = f"f{fh:03d}"
                    relative_path = f"{designator}.{init_date}/{init_hour}/{suite}/{designator}.t{init_hour}z.{suite}.{fxx}.{domain}.grib2"
                elif model == 'nbmqmd':
                    fxx = f"f{fh:03d}"
                    relative_path = f"{designator}.{init_date}/{init_hour}/{suite}/{designator}.t{init_hour}z.{suite}.{fxx}.{domain}.grib2"
                elif model == 'nbmqmd_exp':
                    fxx = f"f{fh:03d}"
                    relative_path = f"{designator}.{init_date}/{init_hour}/{suite}/{designator}.t{init_hour}z.{suite}.{fxx}.{domain}.grib2"
                elif model == 'hrrr':
                    fxx = f"f{fh:02d}"
                    relative_path = f"{designator}.{init_date}/{full_domain}/{designator}.t{init_hour}z.wrf{config.HERBIE_PRODUCTS[config.MODEL]}{fxx}.{domain}.grib2"
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


def download_subset(remote_url, local_filename, search_strings, model, element,
                    require_all_matches=True,
                    required_phrases=None,
                    exclude_phrases=None):
    """
    Download a subset of a GRIB2 file based on .idx entries matching search_strings.

    If model == "nbmqpd", apply special logic to match 24-hr APCP percentiles.
    """
    print(f"  > Downloading subset for {os.path.basename(remote_url)}")
    os.makedirs(os.path.dirname(local_filename), exist_ok=True)

    # Download .idx file
    idx_url = remote_url + ".idx"
    r = requests.get(idx_url)
    if not r.ok:
        print(f'     ❌ Could not get index file: {idx_url} ({r.status_code} {r.reason})')
        return None

    lines = r.text.strip().split('\n')
    matched_ranges = {}

    # Special handling for NBM QPF percentiles
    if model == "nbmqmd" or model == 'nbmqmd_exp':
        # Extract forecast hour from filename (e.g., f060)
        base = os.path.basename(remote_url)
        fcst_match = re.search(r"f(\d{3})", base)
        if not fcst_match:
            print("     ❌ Could not determine forecast hour from filename.")
            return None
        fcst_hour = int(fcst_match.group(1))
        tr_end = fcst_hour
        if element == 'precip24hr':
            tr_start = fcst_hour - 24
            accum_str = f"{tr_start}-{tr_end} hour acc fcst"
        elif element == "maxt":
            tr_start = fcst_hour - 18
            accum_str = f"{tr_start}-{tr_end} hour max fcst"
        elif element == "mint":
            tr_start = fcst_hour - 18
            accum_str = f"{tr_start}-{tr_end} hour min fcst"
        elif element == "Wind":
            tr_start = tr_end
            accum_str = f"{tr_end} hour fcst"
        elif element == "Gust":
            tr_start = tr_end
            accum_str = f"{tr_end} hour fcst"
        else:
            raise NotImplementedError(f"Adjust your time step for {element} and {model} in download_subset in utils.py")
        # Target percentiles
        # With this:
        target_perc_values = {5, 10, 25, 50, 75, 90, 95}
        # Compile search patterns
        search_exprs = [re.escape(s) for s in search_strings]
        search_pattern = re.compile("|".join(search_exprs))
        for n, line in enumerate(lines, start=1):
            if exclude_phrases and any(phrase in line for phrase in exclude_phrases):
                continue
            if not search_pattern.search(line):
                continue
            if accum_str not in line:
                continue
            #if not any(level in line for level in target_levels):
            #    continue
            last_token = line.split(":")[-1].strip()
            match = re.match(r"(\d+)% level", last_token)
            if not match or int(match.group(1)) not in target_perc_values:
                continue
            parts = line.split(':')
            rangestart = int(parts[1])

            if n < len(lines):
                parts_next = lines[n].split(':')
                rangeend = int(parts_next[1]) - 1
            else:
                rangeend = ''

            byte_range = f'{rangestart}-{rangeend}' if rangeend else f'{rangestart}-'
            matched_ranges[byte_range] = line

    else:
        # Generic logic for other models: just match search strings
        exprs = {s: re.compile(re.escape(s)) for s in search_strings}
        matched_vars = set()

        for n, line in enumerate(lines, start=1):
            if exclude_phrases and any(phrase in line for phrase in exclude_phrases):
                continue

            for search_str, expr in exprs.items():
                if expr.search(line):
                    matched_vars.add(search_str)
                    parts = line.split(':')
                    rangestart = int(parts[1])
                    if n < len(lines):
                        parts_next = lines[n].split(':')
                        rangeend = int(parts_next[1]) - 1
                    else:
                        rangeend = ''
                    byte_range = f'{rangestart}-{rangeend}' if rangeend else f'{rangestart}-'
                    matched_ranges[byte_range] = line

        if require_all_matches and len(matched_vars) != len(search_strings):
            print(f'      ⚠️ Not all variables matched! Found: {matched_vars}. Skipping {remote_url}.')
            return None

    # Check if anything was found
    if not matched_ranges:
        print(f'      ❌ No matches found for {search_strings}')
        return None

    # Download GRIB subset
    with open(local_filename, 'wb') as f_out:
        for byteRange in matched_ranges.keys():
            r = requests.get(remote_url, headers={'Range': f'bytes=' + byteRange})
            if r.status_code in (200, 206):
                f_out.write(r.content)
            else:
                print(f"      ❌ Failed to download byte range {byteRange}")
                return None

    print(f'      ✅ Downloaded [{len(matched_ranges)}] fields from {os.path.basename(remote_url)} → {local_filename}')
    return local_filename if os.path.exists(local_filename) else None

def parse_date_and_time_from_url(remote_url, model):
    url_parts = remote_url.split('/')
    if model == 'nbm':
        return url_parts[-4], url_parts[-3]
    elif model == 'nbmqmd': 
        return url_parts[-4], url_parts[-3]
    elif model == 'nbmqmd_exp': 
        return url_parts[-4], url_parts[-3]
    elif model == 'hrrr':
        return url_parts[-3].split('.')[-1], url_parts[-1].split('.')[1].replace('t', '').replace('z', '')
    elif model == 'urma':
        return url_parts[-2].split('.')[-1], url_parts[-1].split('.')[1].replace('t', '').replace('z', '')
    else:
        raise ValueError(f"Unsupported date/time header parsing for model: {model}")

def extract_model_subset_parallel(file_urls, station_df, search_strings, element, model, config):
    rename_map = config.HERBIE_RENAME_MAP[element][model]
    conversion_map = config.HERBIE_UNIT_CONVERSIONS[element].get(model, {})
    print(f"Conversion map is: {conversion_map}")
    # Stage 1: Download all files in parallel
    local_files = []
    download_results = {}
    temp_download_dir = tempfile.mkdtemp(prefix="model_downloads_")
    print(f"📁 Using temp folder: {temp_download_dir}")

    def download_file(remote_url):
        remote_file = os.path.basename(remote_url)
        date_tag, time_tag = parse_date_and_time_from_url(remote_url, model)
        #print(f"Date tag is: {date_tag} and time tag is {time_tag}")
        local_file = os.path.join(temp_download_dir, f"{date_tag}_{time_tag}_{remote_file}")  # or whatever your directory is
        if model == 'urma':
            try:
                r = requests.get(remote_url)
                if r.status_code in (200, 206):
                    with open(local_file, 'wb') as f:
                        f.write(r.content)
                    return (remote_url, local_file)
                else:
                    print(f"❌ Failed to download URMA file: {remote_url}")
                    return (remote_url, None)
            except Exception as e:
                print(f"❌ Exception downloading URMA file: {e}")
                return (remote_url, None)
        elif model == 'nbmqmd':
            downloaded_file = download_subset(
                remote_url=remote_url,
                local_filename=local_file,
                search_strings=search_strings,
                model=model,
                element=element,
                require_all_matches=True,
                #required_phrases=config.HERBIE_REQUIRED_PHRASES[element][model],
                #exclude_phrases=config.HERBIE_EXCLUDE_PHRASES[element][model],
            )
            return (remote_url, downloaded_file)
        elif model == 'nbmqmd_exp':
            downloaded_file = download_subset(
                remote_url=remote_url,
                local_filename=local_file,
                search_strings=search_strings,
                model=model,
                element=element,
                require_all_matches=True,
                #required_phrases=config.HERBIE_REQUIRED_PHRASES[element][model],
                #exclude_phrases=config.HERBIE_EXCLUDE_PHRASES[element][model],
            )
            return (remote_url, downloaded_file)
        else:          
            downloaded_file = download_subset(
                remote_url=remote_url,
                local_filename=local_file,
                search_strings=search_strings,
                model=model,
                element=element,
                require_all_matches=True,
                required_phrases=config.HERBIE_REQUIRED_PHRASES[element][model],
                exclude_phrases=config.HERBIE_EXCLUDE_PHRASES[element][model],
            )
            return (remote_url, downloaded_file)

    print("📥 Starting parallel downloads...")
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(download_file, url) for url in file_urls]
        downloaded_files = []
        for i, future in enumerate(as_completed(futures), 1):
            remote_url, local_file = future.result()
            if local_file:
                downloaded_files.append(local_file)
            print(f"✅ Downloaded {i}/{len(file_urls)} files.")

    print(f"📂 {len(downloaded_files)} files downloaded. Now starting data extraction...")

    # Stage 2: Process each file (could also be parallel if needed, but safe to do serially)
    station_index_cache = {}  # move it here so it's scoped properly
    all_records = []
    # probabilistic data is processed differently due to issues with cfgrib
    if model not in  ['nbmqmd', 'nbmqmd_exp']:
        for i, local_file in enumerate(downloaded_files):
            print(f"Now processing {local_file}...")
            try:
                if model == "nbm":
                    ds = xr.open_dataset(
                        local_file,
                        engine="cfgrib",
                        backend_kwargs={
                            "indexpath": "",
                            "errors": "ignore"
                            },
                        decode_timedelta=True,
                    )
                elif model == "hrrr":
                    ds = xr.open_dataset(
                        local_file,
                        engine="cfgrib",
                        backend_kwargs={
                            "indexpath": "",
                            "errors": "ignore"
                            },
                        decode_timedelta=True,
                    )
                else:
                    ds = xr.open_dataset(
                        local_file,
                        engine="cfgrib",
                        backend_kwargs={
                            "filter_by_keys": {
                                "typeOfLevel": "heightAboveGround",
                                "stepType": "instant",
                                "level": 10
                            },
                            "indexpath": "",
                            "errors": "ignore"
                        },
                        decode_timedelta=True
                    )
                lats = ds.latitude.values
                lons = ds.longitude.values  # wrap longitude
                #print(f"We are looking at other lons...")
                #print(f"Lons are: {lons[150,150]}")
                #tree, grid_shape = build_kdtree(lats, lons)
                valid_time = pd.to_datetime(ds.valid_time.values)
                if model == 'nbm':
                    forecast_hour = int(re.search(r"\.f(\d{3})\.", os.path.basename(local_file)).group(1))
                elif model == 'nbmqmd':
                    forecast_hour = int(re.search(r"\.f(\d{3})\.", os.path.basename(local_file)).group(1))
                elif model == 'hrrr':
                    match = re.search(r"f(\d{2,3})", os.path.basename(local_file))
                    if match:
                        forecast_hour = int(match.group(1))
                    else:
                        raise ValueError(f"Could not extract forecast hour from {local_file}")
                elif model == 'urma':
                    forecast_hour = 0
                else:
                    print(f'File pattern matching not yet set up for {model}')
                    raise NotImplementedError

                for _, row in station_df.iterrows():
                    stid = row["stid"]
                    lat, lon = row["latitude"], row["longitude"]

                    if stid in station_index_cache:
                            iy, ix = station_index_cache[stid]
                    else:
                        #iy, ix = query_kdtree(tree, grid_shape, lat, lon)
                        iy, ix = ll_to_index(lat, lon, lats, lons)
                        station_index_cache[stid] = (iy, ix)

                    record = {
                        "station_id": stid,
                        "init_time": valid_time - pd.to_timedelta(forecast_hour, unit="h"),
                        "valid_time": valid_time,
                        "forecast_hour": forecast_hour,
                    }
                    if model == 'nbm' or model == 'urma':
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

                        all_records.append(record)

                    elif model == 'hrrr':
                        u = v = None  # Default to None in case either component is missing

                        for grib_var, renamed_var in rename_map.items():
                            if grib_var not in ds:
                                continue
                            val = ds[grib_var].values[iy, ix]
                            factor = conversion_map.get(renamed_var, 1.0)
                            val = val * factor if pd.notnull(val) else None

                            if renamed_var == "u_wind":
                                u = val
                            elif renamed_var == "v_wind":
                                v = val
                            else:
                                if val is not None:
                                    record[renamed_var] = round(float(val), 2)

                        # If both u and v exist, compute speed and direction
                        if u is not None and v is not None:
                            speed = np.sqrt(u**2 + v**2)
                            direction = (270 - np.degrees(np.arctan2(v, u))) % 360
                            record["wind_dir_deg"] = round(float(direction), 0)
                            record["wind_speed_kt"] = round(float(speed), 2)

                        all_records.append(record)         
            except Exception as e:
                print(f"❌ Failed to process {local_file}: {e}")
    # using pygrib to process nbmqmd files
    else:
        for local_file in downloaded_files:
            print(f"Now processing {local_file}...")

            try:
                if model == "nbmqmd" or model == "nbmqmd_exp":
                    # Parse once
                    grbs = list(pygrib.open(local_file))
                    forecast_hour = int(re.search(r"\.f(\d{3})\.", os.path.basename(local_file)).group(1))
                    valid_time = pd.to_datetime(grbs[1].validDate)
                    lats, lons = grbs[0].latlons()
                    #lons = lons - 360
                    #print(f"Model is nbmqmd")
                    #print(f"Lons are: {lons[150,150]}")
                    #tree, grid_shape = build_kdtree(lats, lons)
                    # Cache all GRIB values by percentile
                    grib_fields = {}
                    for g in grbs:
                        if hasattr(g, "percentileValue"):
                            #print(g)
                            grib_fields[int(g.percentileValue)] = g.values

                    # Process all stations
                    for _, row in station_df.iterrows():
                        stid = row["stid"]
                        lat, lon = row["latitude"], row["longitude"]

                        if stid in station_index_cache:
                            iy, ix = station_index_cache[stid]
                        else:
                            #iy, ix = query_kdtree(tree, grid_shape, lat, lon)
                            #station_index_cache[stid] = (iy, ix)
                            iy, ix = ll_to_index(lat, lon, lats, lons)
                            station_index_cache[stid] = (iy, ix)
                        record = {
                            "station_id": stid,
                            "init_time": valid_time - pd.to_timedelta(forecast_hour, unit="h"),
                            "valid_time": valid_time,
                            "forecast_hour": forecast_hour,
                        }

                        for perc, values in grib_fields.items():
                            if element == "precip24hr":
                                record[f"qpf_p{perc}"] = round(float(values[iy, ix] * conversion_map[element]), 2)
                            elif element == "maxt":
                                record[f"maxt_p{perc}"] = round(float(K_to_F(values[iy, ix])), 2),
                            elif element == "mint":
                                record[f"mint_p{perc}"] = round(float(K_to_F(values[iy, ix])), 2)
                            elif element == "Wind":
                                record[f"wind_p{perc}"] = round(float(MS_to_KTS(values[iy,ix])),2)
                            elif element == "Gust":
                                record[f"gust_p{perc}"] = round(float(MS_to_KTS(values[iy,ix])),2)
                                #
                            else:
                                raise NotImplementedError(f"Unit conversions not set up for {element} in {model}.  Check HERBIE_UNIT_CONVERSIONS in archiver_config.py")
                        all_records.append(record)
                else:
                    raise NotImplementedError(f"Probabilistic file processing not yet set up for {model}")
            except Exception as e:
                print(f"❌ Failed to process {local_file}: {e}")
    # cleaning up
    for local_file in downloaded_files:
        Path(local_file).unlink(missing_ok=True)

    shutil.rmtree(temp_download_dir)
    return pd.DataFrame.from_records(all_records)


## TODO ADD hrrrak, urma, rrfs
## TODO ADD temp, precip vars