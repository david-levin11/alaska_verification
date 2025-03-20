import synoptic
import xarray as xr
import pandas as pd
import folium
from folium import IFrame
import base64

'''
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
'''

m = folium.Map(center=(65, -150))
Filename ='test_pic.png'
encoded = base64.b64encode(open(Filename, 'rb').read())
html='<img src="data:image/png;base64,{}">'.format
resolution, width, height = 75, 50, 25
iframe = IFrame(html(encoded.decode('UTF-8')), width=(width*resolution)+20, height=(height*resolution)+20)
popup = folium.Popup(iframe, max_width=100)
icon = folium.Icon(color="red", icon="ok")
marker = folium.Marker(location=[37.426, -122.085], popup=popup, icon=icon)
marker.add_to(m)


m.save('/var/www/html/verification/test.html')
	
