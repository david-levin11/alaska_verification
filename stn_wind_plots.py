#!/usr/bin/env python

import matplotlib as mpl
mpl.use('agg')
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from herbie import FastHerbie, Herbie
import synoptic
import polars as pl
import pandas as pd
import xarray as xr
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import concurrent.futures as cf
import subprocess
import warnings
import os, sys

WANT_POOL=False

imageDir='/home/tspindler/ATPG/Verification/images'

allSites=['PAAQ','PANC','PAFR','ARCA2','PHMA2','AYSA2','PATO','NDBCPPXA2',
'NDBCWIXA2','NDBCPOTA2','RHRA2','MCPA2','NDBCAMAA2','ACRA2','PABI','AFTA2',
'SRFA2','PAYA','PAGY','NDBCCSPA2','NDBCRIXA2','NDBCSISA2','NDBDPRTA2',
'NDBCLIXA2','NDBCERXA2','NDBCMRNA2','NDBCPRBA2','NDBCMIXA2','TRMA2','JMLA2',
'PAJN','NDBCPXGA2','PAMD','NDBCLCNA2','NDBCCDXA2','NDBCSPXA2','NDBCCDXA2',
'PASI','PAOM','PADU','PASN','PACD']

#---------------------------------------------------------
def get_stations(start,end):
	print('Getting observations from Synoptic')
	
	df = synoptic.TimeSeries(
		#stid=allSites.values(),
		stid=allSites,
		#recent=pd.Timedelta(24,'h'),
		start=start,
		end=end,
		vars="wind_speed,wind_direction,wind_gust",
		units='metric',
		verbose=False,
		).df()
	
	obs=df.to_pandas()
	obs.set_index('date_time',inplace=True)
	stn=df['stid','latitude','longitude'].unique().to_pandas()
	return stn, obs

#---------------------------------------------------------
def FasterHerbie(dates,model,fxx,product):
	
	# nice way to get the latest date at 00Z
	recent = pd.Timestamp("now").floor("12h") - pd.Timedelta("12h")
	
	store = []
	for var, lev in zip(["UGRD", "VGRD"], ["TGL_10","TGL_10"]):
		_ds = Herbie(
			dates,
			model=model,
			fxx=fxx,
			product=product,
			variable=var,
			level=lev,
			).xarray()
		store.append(_ds)
			
	ds = xr.merge(store)
	
	return ds
#---------------------------------------------------------
def get_model(model,dates,stns):

	products={'nbm':'ak',
			'gfs':'pgrb2.0p25',
			'hrrrak':'sfc',
			'rtma_ak':'ges',
			'urma_ak':'ges'
			}
			
	fcsts={
		'nbm':[24,48,72,96],
		'gfs':[24,48,72,96],
		'hrrrak':[12,24,36],
		'rtma_ak':[0],  # hourly run, no fcsts, just analysis
		'urma_ak':[0],  # same as rtma, no fcsts, just analysis
		}
			
	print(f'getting {model} data with Herbie')
	all_dates=[]
	for fcst in fcsts[model]:
		rdates=dates-pd.Timedelta(fcst,unit='hours')
		if model in ['rtma_ak','urma_ak']:
			H=FastHerbie(rdates,model=model,product=products[model],
				priority=['aws'])
			H.download()
		else:
			H=FastHerbie(rdates,model=model,fxx=[fcst],
				product=products[model],priority=['aws'])
		if model=='nbm':
			ds1=H.xarray(':WIND:10 m above',remove_grib=False)
			ds2=H.xarray(':WDIR:10 m above',remove_grib=False)
			ds3=H.xarray(':GUST:',remove_grib=False)
			ds=xr.merge([ds1,ds2,ds3])
		else:
			ds1=H.xarray(':[UV]GRD:10 m above',remove_grib=False).herbie.with_wind()
			ds2=H.xarray(':GUST:',remove_grib=False)
			ds=xr.merge([ds1,ds2])
			ds=ds.drop_vars(['u10','v10'])
		pts = ds.herbie.pick_points(stns,method='weighted',tree_name=f'{model}_tree',use_cached_tree=True)	
		if 'k' in pts.dims:
			pts=pts.drop_dims('k')
		all_dates.append(pts)

	all_dates=xr.combine_nested(all_dates,concat_dim='time')

	return all_dates

#---------------------------------------------------------
def plot_station(station,obs,pts,cycle,model):
	
	params=dict(wind_speed=[':WIND:','si10'],
		wind_direction=[':WDIR:','wdir10'],
		wind_gust=[':GUST:','gust'])
		
	obs1=obs[obs.stid==station]
	pts1=pts.where(pts.point_stid==station,drop=True).squeeze().to_dataframe()

	for param in obs1.variable.unique().tolist():
		obs2=obs1[obs1.variable==param]
		obs2=obs2.value.resample(cycle).mean()
		if obs2.dropna().size==0:
			continue
		
		# inconsistent names
		if param=='wind_gust':
			if model=='hrrrak' or model=='gfs':
				newparam='gust'
			else:
				newparam='i10fg'
		else:
			newparam=params[param][1]
						
		fig=plt.figure(dpi=150)
		ax=obs2.plot(color='black',linewidth=1,linestyle='--',label='obs')

		if model in ['urma_ak','rtma_ak']:
			pts1.plot(ax=ax,label=model,y=newparam,linewidth=1)
		else:
			pts2=pts1.groupby('step')
			for grp in pts2:
				grp[1].reset_index(inplace=True)
				grp[1]['vtime']=grp[1]['time']+grp[0]
				grp[1].plot(ax=ax,label=f'{grp[0].total_seconds()/60/60:02n} H fcst',x='vtime',y=newparam,linewidth=1)
				
		plt.grid(which='both',axis='both')
		ax.set_xlim(right=end)
		ax.legend()
		plt.title(f"{station} {model.upper()} {param.upper().replace('_',' ')}")
		ax.set_ylabel(obs.units[obs.variable==param].unique().tolist()[0])
		plt.savefig(f'{imageDir}/{station}_{param}_{model}.png')
		plt.close()
	return
		
#---------------------------------------------------------
if __name__=='__main__':

	end = pd.Timestamp("now").floor("12h") - pd.Timedelta("24h")
	start=end-pd.Timedelta('7d')
		
	# get obs data from Synoptic
	stns,obs=get_stations(start,end)
		
	for model in ['hrrrak','nbm','urma_ak','rtma_ak','gfs']:
		
		# GFS on AWS is 6h cycle time, NBM is 1 hour, all of the others are 3h, 
		# even though rtma/urma and hrrr are hourly on nomads.
		if model=='gfs':
			cycle='6h'
		elif model in ['nbm','rtma_ak','urma_ak']:
			cycle='3h'
		else:
			cycle='3h'
			
		dates=pd.date_range(start,end,freq=cycle)
			
		pts=get_model(model,dates,stns)	

		with warnings.catch_warnings():
			warnings.simplefilter("ignore")
			if WANT_POOL:
				with ProcessPoolExecutor(max_workers=4) as executor:
					for station in stns.stid:
						executor.submit(plot_station,station,obs,pts,cycle,model)
			else:
				for station in tqdm(stns.stid):
					plot_station(station,obs,pts,cycle,model)
	

	if not os.path.exists(imageDir):
		os.mkdir(imageDir)
	subprocess.run(f'rm /var/www/html/verification/images/*',shell=True)
	subprocess.run(f'rsync -av {imageDir} /var/www/html/verification/images/.',shell=True)
	subprocess.run(f'rm -rf {imageDir}/*',shell=True)
