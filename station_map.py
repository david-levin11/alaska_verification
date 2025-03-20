import synoptic
import xarray as xr
import pandas as pd
import leafmap.foliumap as leafmap
from folium import IFrame

allSites=['PAAQ','PANC','PAFR','ARCA2','PHMA2','AYSA2','PATO','NDBCPPXA2',
'NDBCWIXA2','NDBCPOTA2','RHRA2','MCPA2','NDBCAMAA2','ACRA2','PABI','AFTA2',
'SRFA2','PAYA','PAGY','NDBCCSPA2','NDBCRIXA2','NDBCSISA2','NDBDPRTA2',
'NDBCLIXA2','NDBCERXA2','NDBCMRNA2','NDBCPRBA2','NDBCMIXA2','TRMA2','JMLA2',
'PAJN','NDBCPXGA2','PAMD','NDBCLCNA2','NDBCCDXA2','NDBCSPXA2','NDBCCDXA2',
'PASI','PAOM','PADU','PASN','PACD']

end=pd.Timestamp(f'{pd.Timestamp.now():%Y%m%d}')
start=end-pd.Timedelta('7d')

df = synoptic.TimeSeries(
	stid=allSites,
	start=start,
	end=end,
	vars="wind_speed,wind_direction,wind_gust",
	units='metric',
	verbose=False,
	).df()
	
obs=df.to_pandas()
obs.set_index('date_time',inplace=True)
stn=df['stid','latitude','longitude'].unique().to_pandas()

m = leafmap.Map(center=(65, -150), zoom=4)
m.add_basemap("OpenTopoMap")
m.add_circle_markers_from_xy(stn,  x="longitude", y="latitude",
	popup=["stid"], layer_name="Stations")

m.to_html('/var/www/html/verification/station_map.html')
	
