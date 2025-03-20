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

imageDir='/export/opc_home/todd.spindler/ATPG/Verification/images'

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

params=dict(air_temp=[':TMP:2 m above ground:','t2m'],
	#wind=[':WIND:10 m above ground:','max_10si'],  # no WIND in GFS
	relative_humidity=[':RH:2 m above ground:','r2'],
	pressure=[':PRES:surface:','sp'],
	dew_point_temperature=[':DPT:2 m above ground:','d2m'])
		
#---------------------------------------------------------
def get_stations(start,end):
	print('Getting observations from Synoptic')
	
	df = synoptic.TimeSeries(
		#stid=allSites.values(),
		stid=allSites.values(),
		#recent=pd.Timedelta(24,'h'),
		start=start,
		end=end,
		vars="air_temp,relative_humidity,pressure,dew_point_temperature",
		units='metric',
		verbose=False,
		).df()
	
	obs=df.to_pandas()
	obs.set_index('date_time',inplace=True)
	stn=df['stid','latitude','longitude'].unique().to_pandas()
	return stn, obs
				
#---------------------------------------------------------
def get_model(model,dates,stn,fcsts):

	products={'nbm':'ak',
			'gfs':'pgrb2.0p25',
			'hrrrak':'sfc',
			'rtma_ak':'ges',
			'urma_ak':'ges',
			'hiresw':['fv3_2p5km','ak']}
			
	fcsts={
		'nbm':[24,48,72,96],
		'gfs':[24,48,72,96],
		'hrrrak':[12,24,36],
		'rtma_ak':[0],  # hourly run, no fcsts, just analysis
		'urma_ak':'ges',# same as rtma, no fcsts, just analysis
		'hiresw':[]
		}  # 06 and 18Z runs, 0-60 hr fcsts

			
	print(f'getting {model} data with Herbie')
	all_dates=[]
	for fcst in fcsts:
		rdates=dates-pd.Timedelta(fcst,unit='hours')
		H=FastHerbie(rdates,model=model,fxx=[fcst],product=products[model],
			max_threads=10,priority=['aws','google','etc.'])
		all_pts=xr.Dataset()		
		for pname,[param,xparam] in params.items():
			print('processing',pname)
			if pname=='wind' and model=='gfs':
				ds = H.xarray(":[UV]GRD:10 m above ground",remove_grib=False)
			else:
				ds = H.xarray(param,remove_grib=False)
			pts = ds.herbie.pick_points(stn,method='weighted')
	
			if xparam=='t2m' or xparam=='d2m':
				if abs(pts[xparam].max())>100: 
					pts[xparam]=pts[xparam]-273.15
			if 'k' in pts.dims:
				pts=pts.drop_dims('k')
			all_pts=all_pts.merge(pts,compat='override')
			
		all_dates.append(all_pts)

	return xr.combine_nested(all_dates,concat_dim='time')

#---------------------------------------------------------
if __name__=='__main__':
	model='hrrrak'
	cycle=0
	end=pd.Timestamp(f'{pd.Timestamp.now():%Y%m%d}')
	start=end-pd.Timedelta('7d')	
	dates=pd.date_range(start,end,freq='6h')
	
	stns,obs=get_stations(start,end)
	pts=get_model(model,dates,stns)	

	for station in stns.stid:
		print('processing station',station)
		obs1=obs[obs.stid==station]

		pts1=pts.where(pts.point_stid==station,drop=True).squeeze().to_dataframe()
		for param in obs1.variable.unique():
			obs2=obs1[obs1.variable==param]
			obs2=obs2.value.resample('6h').mean()

			if obs2.dropna().size==0:
				continue
			fig=plt.figure(dpi=150)
			ax=obs2.plot(color='black',linewidth=2,label='obs')
						
			pts2=pts1.groupby('step')
			for grp in pts2:
				grp[1].reset_index(inplace=True)
				grp[1]['vtime']=grp[1]['time']+grp[0]
				grp[1].plot(ax=ax,label=f'{grp[0].days*24} H fcst',x='vtime',y=params[param][1])
			plt.grid(which='both',axis='both')
			ax.set_xlim(right=end)
			ax.legend()
			plt.title(f'{station} : {param.upper().replace('_',' ')}')
			ax.set_ylabel(obs.units[obs.variable==param].unique().tolist()[0])
			plt.savefig(f'{imageDir}/stn_{station}_{params[param][1]}_{model}.png')
			plt.close()
