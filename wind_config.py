import os


USE_CLOUD_STORAGE = True # Set to true to append to S3 bucket database.  False saves site level .csv files locally
######################### Wx Elements ################################
ELEMENT = 'Wind'

######################### Directories #################################

HOME = os.path.abspath(os.path.dirname(__file__))

OBS = os.path.join(HOME, 'obs')

MODEL_DIR = os.path.join(HOME, 'model')

TMP = os.path.join(HOME, 'tmp_cache')


######################## File Names #################################

METADATA = "alaska_obs_metadata.csv"

WIND_OBS_FILE = f"alaska_{ELEMENT.lower()}_obs.csv"

WIND_OBS_FILE_COMPRESSED = f"alaska_{ELEMENT.lower()}_obs.parquet"


###################### Synoptic Params ##########################
API_KEY = "c6c8a66a96094960aabf1fed7d07ccf0" # link to get an API key can be found at https://docs.google.com/document/d/1YuMUYog4J7DpFoEszMmFir4Ehqk9Q0GHG_QhSdrgV9M/edit?usp=sharing

TIMESERIES_URL = "https://api.synopticdata.com/v2/stations/timeseries"

METADATA_URL = "https://api.synopticdata.com/v2/stations/metadata"

STATE = "ak"

if ELEMENT == "Wind":
	WIND_VARS = "wind_direction,wind_speed,wind_gust"

NETWORK = "1,107,90,179,200,286,3004"

OBS_START = "202101010000"

OBS_END = "202103010000"
# Start with 1 second and back off
INITIAL_WAIT = 1
# Number of retry attempts
MAX_RETRIES = 5

################### Model Params ###################################
MODEL = 'nbm'

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

ELEMENT_DICT = {'Wind': {'nbm': ['si10', 'wdir10']}}

HERBIE_XARRAY_STRINGS = {'Wind': {'nbm': [':WIND:10 m above', ':WDIR:10 m above', ':GUST:'],
								   'hrrrak': [':[UV]GRD:10 m above',':GUST:']}}

########################## NDFD Params #################################
NDFD_DIR = 'ndfd'

NDFD_DICT = {"Wind": {"wspd": ["YCRZ98", "YCRZ97"], "wdir": ["YBRZ98", "YBRZ97"]}}

NDFD_SPEED_STRING = "YCRZ98"

NDFD_DIR_STRING = "YBRZ98"

NDFD_SPEED_STRING_EXT = "YCRZ97"

NDFD_DIR_STRING_EXT = "YBRZ97"

NDFD_FILE_STRINGS = {"Wind": ["wspd", "wdir", "wgust"]}

NDFD_ELEMENT_STRINGS = {"Wind": ["si10", "wdir10", "ifg10"]}

NDFD_S3_URL = "s3://alaska-verification/ndfd/"


##################### AWS Params #################################

