#!/usr/bin/env python
# coding: utf-8

"""

YCRZ97-* AK wind  day 4-7
YCRZ98-* AK wind  day 1-3

YBRZ97-* AK wind dir day 4-7
YBRZ98-* AK wind dir day 1-3

YWRZ98-* AK gust day 1-3 only

https://noaa-ndfd-pds.s3.amazonaws.com/wmo/{param}/{year}/{mon}/{day}/

YWRZ98_KWBN_{2025}{02}{18}{00}47  00 - 23 hourly

wdir
wgust
wspd

"""

#AWS file load examples

import xarray as xr
from datetime import datetime, timedelta
import fsspec
import requests
import pandas as pd
import os, sys
import wind_config as config

theDate=pd.Timestamp.now()-pd.Timedelta(3,units='D')

# ### Use cached storage for grib files (decoded with cfgrib)
# ### Multiple GRiB files, cached storage

# wspd wgust wdir

def ensure_dir(directory):
    """Ensure a directory exists. If not, create it."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Creating {directory} directory")
    else:
        print(f"{directory} already exists...skipping creation step.")

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
    stations = list(zip(station_df['latitude'], station_df['longitude']))


    theDate = pd.Timestamp.now()-pd.Timedelta(3, unit='D')
    rdates = [theDate - pd.Timedelta(n, unit='D') for n in range(7)]

    base_path = "s3://noaa-ndfd-pds/wmo/wspd"
    out_data = []

    for tdate in rdates:
        day_url = f"{base_path}/{tdate:%Y}/{tdate:%m}/{tdate:%d}/YCRZ9[89]*"

        # List matching files using s3fs
        fs = fsspec.filesystem("s3", anon=True)
        try:
            files = fs.glob(day_url)
        except Exception as e:
            print(f"⚠️ Failed to list files for {tdate.date()}: {e}")
            continue

        for grib_url in files:
            s3_url = f"s3://{grib_url}"  # convert to full S3 path
            print(f"Opening {s3_url}")
            #print(f"📦 Processing: {grib_url}")

            # Open using fsspec + cfgrib (streaming)
            #try:
            with fsspec.open(f'simplecache::{s3_url}',
                                s3={'anon': True},
                                filecache={'cache_storage': './tmp'}) as f:
                ds = xr.open_dataset(f, engine='cfgrib',
                        backend_kwargs={'indexpath': ''},  # Optional: avoid writing .idx index files
                        cache=False)
            for lat, lon in stations:
                point_ds = ds.sel(latitude=lat, longitude=lon, method='nearest')

                record = {
                    "timestamp": pd.to_datetime(ds.time.values),
                    "lat": float(lat),
                    "lon": float(lon),
                    "si10": float(point_ds['unknown'].values),  # you may need to confirm var name
                }
                out_data.append(record)

        #except Exception as e:
        #    print(f"❌ Failed to open/process {grib_url}: {e}")
        #    continue

    # Convert to DataFrame
    df = pd.DataFrame(out_data)
    print(df.head())
