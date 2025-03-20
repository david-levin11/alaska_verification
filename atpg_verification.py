#!/bin/env python
"""
ATPG Verification Package
Version 0.01
23 Jan 2025

Prototype verification package to extract obs from Synoptic API, model data from AWS

Initial work to get station data, interp model to station location for timeseries,
then compare obs to model and perform descriptive statistics on them

Initial params include
air temp
wind speed
precip type

"""
#import matplotlib as mpl
#mpl.use('agg')
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import fsspec
import synoptic
import polars as pl
import pandas as pd
import xarray as xr
from herbie import Herbie
import seaborn as sns
import cartopy.crs as ccrs
from time import process_time
import os, sys

# define AK regions and stations
allSites=dict(
	JuneauLand=['PAGY','PAHN','PAGS','PAJN','PAOH','SDIA2','PASI','PAGN','PAPG',
                'PAWG','PAKT','PAKW','PAHY','PAMM','PAFE'],
    JuneauMarine=['EROA2','LIXA2','NKXA2','SCXA2','RIXA2','CSPA2', 'FFIA2',
                  'PGXA2','CDEA2','LCNA2','GIXA2','MXXA2','PBPA2','GLIA2','KEXA2',
                  'TKEA2'],
    JuneauMarineHRRR=['EROA2','LIXA2','NKXA2','SCXA2','RIXA2','CSPA2', 'FFIA2',
                      'PGXA2','CDXA2','LCNA2','GIXA2','JLXA2','PBPA2','GLIA2','KECA2',
                      'TKEA2'],
    AnchorageLand=['PANC','PAAQ','PAWS','PABV','SBPA2','PATO','WHMA2','PAWD',
                   'MCPA2','PAHO','PAEN','PASX','RUFA2','SHDA2','PAVD','RHVA2','RSCA2',
                   'PAGK','CXCA2','PXKA2','PLCA2','PATK','WOWA2','MMRA2','COVA2','PAII',
                   'PADL','PAPH','PADU','PAVC','PASD','UNLA2','NKLA2','PADK','PADQ',
                   'ALIA2','PANI','PAIG','PAIL','PASL','PABE','PAPM','PATG','PAKI',
                   'PAVA','PACV','ARCA2','SMCA2','PGPA2','RBTA2','GAHA2','RHRA2'],
    AnchorageMarine=['PAMD','46076','46061','46060','SELA2','PILA2','AMAA2','46080',
                     '46001','46078','SKJA2','46075','PAPB','PASN','BLIA2','AUGA2'],
    FairbanksLand=['ACRA2','BKCA2','TKRA2','CKNA2','SGNA2','PABI','FCPA2','TRDA2',
                   'TKLA2','PAIN','SMPA2','RXBA2','NHPA2','PAFA','TEXA2',
                   'PAEI','SLRA2','BCHA2','BREA2','PAFB','D6992','CPKA2','WICA2',
                   'PATA','PAEG','PFYU','PAGA','PABT','CHMA2','RAMA2','PAKP','IMYA2',
                   'PABA','PAAD','PASC','PAKU','PALP','PAQT','UMTA2','PAOR',
                   'PABR','PAWI','SSIA2','ASIA2','PAWN','PAOT','MNOA2','PAGH','SAGA2',
                   'PASK','PADE','HDOA2','PFSH','PFEL','PAKK','PAUN','PANV','PAOM',
                   'PATE','PATC','PAIW','PALU','PAPO','PAMK','PACZ','PASM','PADM',
                   'PATL','PAKV','PRDA2','PPIZ'])

#----------------------------------------------------------------------------
def get_synoptic_data(stid=None,vars='air_temp'):
	# get Synoptic observation data
	# extraction can be by station id, all stations (default), or by region
	if stid==None:
		stid=allSites.values()
	elif stid in allSites.keys():
		stid=allSites[stid]
		
	df = synoptic.TimeSeries(
    	stid=stid,
		recent=timedelta(hours=24),
		vars=vars,
		units='metric',
	).df()
	
	return df

#----------------------------------------------------------------------------
def get_aws_data(model,theDate,cycle,fcst=None):
	# extract model data from AWS
	# supported models are expected to include
	# GFS HRRR-AK RAP NBM RTMA URMA 

	if model=='RTMA' or model=='URMA':
		H = Herbie(
		    f"{theDate:%Y%m%d} {cycle:02n}:00",
			model=model,
			product="ges",
			)
	
	# GFS (pgrb2, pgrb2b)
	awsPattern=dict(
    	GFS=[f'simplecache::s3://noaa-gfs-bdp-pds/gfs.{theDate:%Y%m%d}/{cycle:02n}/atmos/gfs.t00z.pgrb2.0p25.f{fcst:03n}',
    		 f'simplecache::s3://noaa-gfs-bdp-pds/gfs.{theDate:%Y%m%d}/{cycle:02n}/atmos/gfs.t00z.pgrb2b.0p25.f{fcst:03n}'],
    	# HRRR-AK (wrfnat, wrfprs, wrfsfc, wrfsubh)  wrfprs are pressure surfaces, wrfsfc for surface values
    	HRRR=[#f'simplecache::s3://noaa-hrrr-bdp-pds/hrrr.{theDate:%Y%m%d}/alaska/hrrr.t{cycle:02n}z.wrfnatf{fcst:02n}.ak.grib2',
    		  #f'simplecache::s3://noaa-hrrr-bdp-pds/hrrr.{theDate:%Y%m%d}/alaska/hrrr.t{cycle:02n}z.wrfprsf{fcst:02n}.ak.grib2',
    		  f'simplecache::s3://noaa-hrrr-bdp-pds/hrrr.{theDate:%Y%m%d}/alaska/hrrr.t{cycle:02n}z.wrfsfcf{fcst:02n}.ak.grib2',
    		  #f'simplecache::s3://noaa-hrrr-bdp-pds/hrrr.{theDate:%Y%m%d}/alaska/hrrr.t{cycle:02n}z.wrfsubhf{fcst:02n}.ak.grib2'
			 ],
    	# RAP
    	RAP=[f'simplecache::s3://noaa-rap-pds/rap.{theDate:%Y%m%d}/rap.t{cycle:02n}z.awp242f{fcst:02n}.grib2'],
    	# NBM - note that fcst starts at 1
    	NBM=[f'simplecache::s3://noaa-nbm-grib2-pds/blend.{theDate:%Y%m%d}/{cycle:02n}/core/blend.t{cycle:02n}z.core.f{fcst:03n}.ak.grib2'],
    	# RTMA needs Herbie or have to extract each variable separately
    	#RTMA=[f'simplecache::s3://noaa-rtma-pds/akrtma.{theDate:%Y%m%d}/akrtma.t{cycle:02n}z.2dvarges_ndfd_3p0.grb2'],
		RTMA=[f'simplecache::s3://noaa-rtma-pds/rtma2p5.{theDate:%Y%m%d}/rtma2p5.t{cycle:02n}z.2dvarges_ndfd.grb2_wexp'],
    	# URMA
        URMA=[f'simplecache::s3://noaa-urma-pds/akurma.{theDate:%Y%m%d}/akurma.t{cycle:02n}z.2dvarges_ndfd_3p0.grb2']
	)
		
	gribOpts=dict(
		GFS={'filter_by_keys':{'typeOfLevel': 'surface'}},
		HRRR={'filter_by_keys':{'stepType': 'instant', 'typeOfLevel': 'surface'}},
		RAP={'filter_by_keys':{'stepType': 'instant', 'typeOfLevel': 'surface'}},
		NBM={'filter_by_keys':{'stepType': 'instant', 'typeOfLevel': 'surface'}},
		RTMA=None,
		URMA=None)
			
	paths = awsPattern[model]

	fs = fsspec.open_local(paths,s3={'anon':True},filecache={'cache_storage':'/tmp'})
	ds = xr.open_mfdataset(
		fs,
		engine="cfgrib",
		combine="by_coords",
		parallel=True,
		backend_kwargs=gribOpts[model]
		)
		
	return ds	

#----------------------------------------------------------------------------
def plot_ts(df):
	# simple line plot for timeseries data
	plt.figure(figsize=(10, 5))
	ax = sns.lineplot(
		df,
		x="date_time",
		y="value",
		hue="variable",
		palette=["tab:red", "tab:green"],
		)
	ax.set_title(f"{df['stid'][0]}: {df['name'][0]}")
	ax.set_ylabel("Temperature (°F)")
	ax.set_xlabel("")
	ax.grid(alpha=0.5, zorder=0, lw=0.5, ls="--")
	ax.legend(title="")
	plt.savefig(f"ts_{df['stid'][0]}_{df['variable'][0]}.png")
	plt.close()
	return

#----------------------------------------------------------------------------
def station_map(df):
	# simple station location map
	stn=df['stid','latitude','longitude'].unique().to_pandas()
	ax=plt.axes(projection=ccrs.Miller())
	stn.plot(ax=ax,x='longitude',y='latitude',marker='x',markersize=18,color='red',linestyle='',legend=False,transform=ccrs.PlateCarree())
	ax.coastlines()
	ax.gridlines(draw_labels=True)
	ax.set_title(f"{df['name'][0]}")
	plt.savefig(f"location_{df['stid'][0]}.png")
	plt.close()
	return

#----------------------------------------------------------------------------
if __name__=='__main__':
		
	data=get_synoptic_data('PADL','air_temp')	
	plot_ts(data)
	station_map(data)
	
