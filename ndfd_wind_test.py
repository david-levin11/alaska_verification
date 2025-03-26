import xarray as xr
import fsspec
import pandas as pd

from datetime import datetime, timedelta
import os


def ensure_dir(directory):
    """Ensure a directory exists. If not, create it."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Creating {directory} directory")
    else:
        print(f"{directory} already exists...skipping creation step.")

# Set up variables
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
# Loop through each file individually
for i, s3_file in enumerate(filtered_files):
    if i == 0:
        s3_url = f'simplecache::s3://{s3_file}'
        print(f"📦 Processing {s3_url}")

        try:
            # Use fsspec to stream + cache
            with fsspec.open(s3_url, s3={"anon": True}, filecache={"cache_storage": tmp}) as f:
                ds = xr.open_dataset(f.name, engine='cfgrib', backend_kwargs={'indexpath': ''})

            # --- EXTRACT LOGIC HERE ---
            # For example, print metadata or extract nearest point
            print(ds)
            print(f"Init time is: {ds.time}")
            print(f"Steps are: {pd.TimedeltaIndex(ds.step)}")

            # You could grab nearest grid point to a location
            # point_ds = ds.sel(latitude=60.0, longitude=-150.0, method='nearest')
            # print(point_ds)

        except Exception as e:
            print(f"❌ Failed to open/process {s3_file}: {e}")
## TODO Need to check to see if the file name is the initial time?
## TODO Need to pull out point data at various lead times for each site
## TODO How do we pull in days 4-7 as well?
## TODO How about directions and gusts?