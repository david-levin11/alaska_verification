import os

######################### Directories #################################

HOME = os.path.abspath(os.path.dirname(__file__))

OBS = os.path.join(HOME, 'obs')

TMP = os.path.join(HOME, 'tmp')

######################## File Names #################################

METADATA = "alaska_obs_metadata.csv"

WIND_OBS_FILE = "alaska_wind_obs.csv"

WIND_OBS_FILE_COMPRESSED = "alaska_wind_obs.parquet"


###################### Synoptic Params ##########################
API_KEY = "c6c8a66a96094960aabf1fed7d07ccf0" # link to get an API key can be found at https://docs.google.com/document/d/1YuMUYog4J7DpFoEszMmFir4Ehqk9Q0GHG_QhSdrgV9M/edit?usp=sharing

TIMESERIES_URL = "https://api.synopticdata.com/v2/stations/timeseries"

METADATA_URL = "https://api.synopticdata.com/v2/stations/metadata"

STATE = "ak"

WIND_VARS = "wind_direction,wind_speed,wind_gust"

NETWORK = "1,107,90,179,200,286,3004"

OBS_START = "202501021800"

OBS_END = "202501031800"

################### Model Params ###################################

HERBIE_MODELS = ['hrrrak','nbm','urma_ak','rtma_ak','gfs']

HERBIE_PRODUCTS = {'nbm':'ak',
			'gfs':'pgrb2.0p25',
			'hrrrak':'sfc',
			'rtma_ak':'ges',
			'urma_ak':'ges'
			}

HERBIE_FORECASTS = {
		'nbm':[24,48,72,96],
		'gfs':[24,48,72,96],
		'hrrrak':[12,24,36],
		'rtma_ak':[0],  # hourly run, no fcsts, just analysis
		'urma_ak':[0],  # same as rtma, no fcsts, just analysis
		}

HERBIE_CYCLES = {"nbm": "3h", "hrrrak": "3h", "urma_ak": "3h", "gfs": "6h", "rtma_ak": "3h"}


