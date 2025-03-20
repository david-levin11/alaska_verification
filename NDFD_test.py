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
import fsspec
import pandas as pd
import os, sys

theDate=pd.Timestamp.now()

# ### Use cached storage for grib files (decoded with cfgrib)
# ### Multiple GRiB files, cached storage

# wspd wgust wdir

rdate=[theDate-pd.Timedelta(n,units='D') for n in range(7)]
url=[]
for tdate in rdate:
    url.append(f'simplecache::s3://noaa-ndfd-pds/wmo/wspd/{tdate:%Y}/{tdate:%m}/{tdate:%d}/YCRZ9[89]*')

tmp='/export/opc_home/todd.spindler/data/tmp'

fs = fsspec.open_local(url,
	s3={'anon':True},
    filecache={'cache_storage':tmp})
    
ts = xr.open_mfdataset(
    fs,
    engine="cfgrib",
    combine="nested",
    concat_dim="time",
    parallel=True
    )

