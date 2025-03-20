from herbie import Herbie
import polars as pl
import pandas as pd
import xarray as xr

models=['GFS','HRRRAK','NBM','RAP','RTMA_AK','URMA_AK']

params={ 'obs':{'air_temp','wind','relative_humidity','pressure','dew_point_temperature'},
		 'nbm':{'TMP'     ,'WIND','RH'               ,'None',    'DPT'},
		 'gfs':{'TMP'     ,'WIND','RH'               ,'PRES',    'DPT'},   # product=pgrb2.0p25
		 'rap':{'TMP'     ,'WIND','RH'               ,'PRES',    'DPT'},   # product=awp242
		 'hrrrak':{},
		 'rtma':{},
		 'urma':{},
		}

		
		params['nbm']['air_temp']='TMP'
