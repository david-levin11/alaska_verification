import xarray as xr
import fsspec
import pandas as pd
import numpy as np
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


if __name__ == "__main__":
    stids = pd.read_csv(os.path.join(config.OBS, config.METADATA))
    station_df = stids[['stid', 'latitude','longitude']]

    # Set up variables need to use start and end dates for obs keeping in mind these would be model run dates
    theDate = pd.Timestamp.now()
    rdate = [theDate - pd.Timedelta(n, units='D') for n in range(1)]

    tmp = 'tmp'
    ensure_dir(tmp)
    # Pattern list of S3 URLs
    url_patterns = [
        f'simplecache::s3://noaa-ndfd-pds/wmo/wspd/{tdate:%Y}/{tdate:%m}/{tdate:%d}/YCRZ9[89]*'
        for tdate in rdate
    ]
    print(f"URL patterns are: {url_patterns}")
    # Expand wildcards using fsspec
    fs = fsspec.filesystem("s3", anon=True)
    all_files = []
    for pattern in url_patterns:
        try:
            matched = fs.glob(pattern.replace("simplecache::", ""))
            all_files.extend(matched)
        except Exception as e:
            print(f"Failed to list files for pattern {pattern}: {e}")
    # filtering only to 00 and 12 UTC initial times
    filtered_files = []

    for ndfd_file in all_files:
        # Extract the HH (hour) portion from the filename
        # Assuming format: '.../YCRZ98_KWBN_YYYYMMDDHH47'
        filename = os.path.basename(ndfd_file)
        file_date = datetime.strptime(filename.split('_')[-1],"%Y%m%d%H%M")
        if file_date.hour == 11 or file_date.hour == 23:
            filtered_files.append(ndfd_file)
    print(filtered_files)

    records = []

    for grib_file in filtered_files:
        s3_url = f'simplecache::s3://{grib_file}'
        print(f"📦 Processing {s3_url}")

        try:
            with fsspec.open(s3_url, s3={"anon": True}, filecache={"cache_storage": tmp}) as f:
                ds = xr.open_dataset(f.name, engine='cfgrib', backend_kwargs={'indexpath': ''})
                lats = ds.latitude.values
                lons = ds.longitude.values-360
                init_time = pd.to_datetime(ds.time.values)
                steps = pd.to_timedelta(ds.step.values)
                valid_times = pd.to_datetime(ds.valid_time.values)
                wind_array = ds["si10"].values  # shape: (step, y, x)

            for _, row in station_df.iterrows():
                stid = row["stid"]
                lat = row["latitude"]
                lon = row["longitude"]
                if stid == "PAJN":
                    iy, ix = ll_to_index(lat, lon, lats, lons)
                    values = wind_array[:, iy, ix]
                    for step, valid_time, value in zip(steps, valid_times, values):
                        step_hr = int(step.total_seconds() / 3600)
                        records.append({
                            "station_id": stid,
                            "valid_time": valid_time,
                            "forecast_hour": step_hr,
                            "wind_speed_kt": round(float(value * 1.94384), 2)
                        })

        except Exception as e:
            print(f"❌ Failed to process {s3_url}: {e}")
    df_wide = pd.DataFrame.from_records(records)
    print(df_wide)


    ## TODO How do we pull in days 4-7 as well?
    ## TODO How about directions and gusts?