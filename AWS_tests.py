#!/usr/bin/env python
# coding: utf-8

# # AWS file load examples

# ### Single AWS file load test

# In[1]:


import xarray as xr
import fsspec
import pandas as pd
from time import process_time
import os, sys


# In[2]:


#help(fsspec.open_files)


# In[3]:


theDate=pd.Timestamp.now()


# ### Use cached storage for grib files (decoded with cfgrib)

# In[4]:


t = process_time()


# In[5]:


uri = f'simplecache::s3://noaa-gfs-bdp-pds/gfs.{theDate:%Y%m%d}/00/wave/gridded/gfswave.t00z.global.0p16.f000.grib2'
tmp='/home/tspindler/Leaflet/tmp'

infile=fsspec.open_local(uri,s3={'anon':True},filecache={'cache_storage':tmp}) 
ds=xr.open_dataset(infile, engine="cfgrib") 

print(process_time() - t)

# ### Multiple GRiB files, cached storage

t = process_time()


paths = [
    f'simplecache::s3://noaa-gfs-bdp-pds/gfs.{theDate-pd.Timedelta("2D"):%Y%m%d}/00/wave/gridded/gfswave.t00z.global.0p16.f001.grib2',
    f'simplecache::s3://noaa-gfs-bdp-pds/gfs.{theDate-pd.Timedelta("1D"):%Y%m%d}/00/wave/gridded/gfswave.t00z.global.0p16.f001.grib2',
    f'simplecache::s3://noaa-gfs-bdp-pds/gfs.{theDate:%Y%m%d}/00/wave/gridded/gfswave.t00z.global.0p16.f001.grib2',
]
tmp='/export/opc_home/todd.spindler/Leaflet/tmp'

fs = fsspec.open_local(paths,s3={'anon':True},filecache={'cache_storage':tmp})
ts = xr.open_mfdataset(
    fs,
    engine="cfgrib",
    combine="nested",
    concat_dim="t",
    parallel=True
    )

print(process_time() - t)


# In[15]:


ts


# ### Single AWS netcdf load

# In[16]:


path=f's3://noaa-nws-rtofs-pds/rtofs.{theDate:%Y%m%d}/rtofs_glo_2ds_n010_diag.nc'
fs = fsspec.filesystem('s3', anon=True)


# In[17]:


ds = xr.open_dataset(fs.open(path),engine='h5netcdf')


# In[18]:


ds


# ### Multiple AWS netcdf load

# In[19]:


paths = [
    's3://noaa-goes16/ABI-L2-LSTC/2022/185/03/OR_ABI-L2-LSTC-M6_G16_s20221850301180_e20221850303553_c20221850305091.nc',
    's3://noaa-goes16/ABI-L2-LSTC/2022/185/02/OR_ABI-L2-LSTC-M6_G16_s20221850201180_e20221850203553_c20221850205142.nc'
    ]


# ### Multiple netcdf datasets, parallel=False, single xarray call

# In[20]:


fs = fsspec.filesystem('s3', anon=True)
flist = [fs.open(path, mode="rb") for path in paths]


# In[21]:


ts = xr.open_mfdataset(
    flist,
    engine="h5netcdf",
    combine="nested",
    concat_dim="t",
    parallel=False
    )
ts


# ### multiple netcdf files, multiple xarray calls, concatenate list, no parallel 

# In[22]:


ts = xr.concat([xr.open_dataset(fs.open(path,  mode="rb"), engine="h5netcdf") for path in paths],dim='t')
ts


# ### Multiple netcdf files, Single xarray call, parallel version

# In[23]:


ts=xr.open_mfdataset([fs.open(path, mode="rb") for path in paths],
    engine="h5netcdf",
    combine="nested",
    concat_dim="t",
    parallel=True
)
ts


# In[ ]:




