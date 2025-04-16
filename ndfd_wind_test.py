import xarray as xr
import fsspec
import pandas as pd
import numpy as np
import requests
from collections import defaultdict
from datetime import datetime, timedelta
import os
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

def extract_ndfd_forecasts(speed_files, direction_files, station_df, tmp_dir="tmp"):
    global config
    ensure_dir(tmp_dir)
    records = []

    # Create list of tuples with direction file and parsed timestamp
    dir_files_with_time = [(f, extract_timestamp(f)) for f in direction_files]

    for speed_file in speed_files:
        speed_time = extract_timestamp(speed_file)

        # Find closest direction file within 1 minute
        best_match = None
        smallest_diff = pd.Timedelta("2 minutes")

        for dir_file, dir_time in dir_files_with_time:
            time_diff = abs(speed_time - dir_time)
            if time_diff <= pd.Timedelta("1 minute") and time_diff < smallest_diff:
                smallest_diff = time_diff
                best_match = dir_file

        if not best_match:
            print(f"⚠️ No direction match within 1 minute for {speed_file}")
            continue

        speed_url = f'simplecache::s3://{speed_file}'
        dir_url = f'simplecache::s3://{best_match}'
        print(f"📦 Processing speed: {speed_url}\n📦 Processing direction: {dir_url}")

        try:
            with fsspec.open(speed_url, s3={"anon": True}, filecache={"cache_storage": tmp_dir}) as f_speed:
                ds_speed = xr.open_dataset(f_speed.name, engine='cfgrib', backend_kwargs={'indexpath': ''}, decode_timedelta=True)
            with fsspec.open(dir_url, s3={"anon": True}, filecache={"cache_storage": tmp_dir}) as f_dir:
                ds_dir = xr.open_dataset(f_dir.name, engine='cfgrib', backend_kwargs={'indexpath': ''}, decode_timedelta=True)

            lats = ds_speed.latitude.values
            lons = ds_speed.longitude.values - 360
            steps = pd.to_timedelta(ds_speed.step.values)
            valid_times = pd.to_datetime(ds_speed.valid_time.values)
            speed_array = ds_speed[config.NDFD_ELEMENT_STRINGS[config.ELEMENT][0]].values
            dir_array = ds_dir[config.NDFD_ELEMENT_STRINGS[config.ELEMENT][1]].values

            for _, row in station_df.iterrows():
                stid = row["stid"]
                lat = row["latitude"]
                lon = row["longitude"]

                iy, ix = ll_to_index(lat, lon, lats, lons)
                spd_values = speed_array[:, iy, ix]
                dir_values = dir_array[:, iy, ix]

                for step, valid_time, spd, direc in zip(steps, valid_times, spd_values, dir_values):
                    step_hr = int(step.total_seconds() / 3600)
                    records.append({
                        "station_id": stid,
                        "valid_time": valid_time,
                        "forecast_hour": step_hr,
                        "wind_speed_kt": round(float(spd * 1.94384), 2),
                        "wind_dir_deg": round(float(direc), 0)
                    })

        except Exception as e:
            print(f"❌ Failed to process pair:\n  Speed: {speed_url}\n  Dir: {dir_url}\n  Error: {e}")

    df_long = pd.DataFrame.from_records(records)
    return df_long


# Example usage:
if __name__ == "__main__":

    if not os.path.exists(os.path.join(config.OBS, config.METADATA)):
        print(f"Couldn't find {config.METADATA} in {config.OBS}...will need to create the file")
        ensure_dir(config.OBS)
        # getting our metadata if we don't have it
        print(f'Creating metadata file from {config.METADATA_URL}')
        meta_json = create_wind_metadata(config.METADATA_URL, config.API_KEY, config.STATE, config.NETWORK, config.WIND_VARS, config.OBS_START)
        meta_df = parse_metadata(meta_json)
        meta_df.to_csv(os.path.join(config.OBS, config.METADATA), index=False)
        print(f"All done creating metadata.  Saved {config.METADATA} in {config.OBS}.")

    station_df = pd.read_csv(os.path.join(config.OBS, config.METADATA))
    filtered_files = get_ndfd_file_list(config.OBS_START, config.OBS_END, config.NDFD_DICT)

    # Access them like this:
    speed_files = filtered_files["wspd"]
    direction_files = filtered_files["wdir"]

    print("Speed:", speed_files[:3])
    print("Direction:", direction_files[:3])
    df_ndfd = extract_ndfd_forecasts(filtered_files["wspd"], filtered_files["wdir"], station_df)
    print(df_ndfd.head())
    