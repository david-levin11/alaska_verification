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

HFMETAR = "0"

OBS_VARS = {"Wind": ["wind_direction", "wind_speed", "wind_gust"]}

OBS_PARSE_VARS = {"Wind": ["wind_direction_set_1", "wind_speed_set_1", "wind_gust_set_1"]}

OBS_RENAME_MAP = {
    "Wind": {
        "wind_speed_set_1": "obs_wind_speed_kts",
        "wind_direction_set_1": "obs_wind_dir_deg",
        "wind_gust_set_1": "obs_wind_gust_kts"
    }
}

NETWORK = "1,107,90,179,200,286,3004"

OBS_START = "202201010000"

OBS_END = "202202010000"
# Start with 1 second and back off
INITIAL_WAIT = 1
# Number of retry attempts
MAX_RETRIES = 5

################### Model Params ###################################
MODEL = 'nbm'

HERBIE_MODELS = ['hrrr','nbm','urma','rtma','gfs']

HERBIE_PRODUCTS = {'nbm':'ak',
			'gfs':'pgrb2.0p25',
			'hrrr':'sfc',
			'rtma_ak':'ges',
			'urma_ak':'ges'
			}

HERBIE_FORECASTS = {
		'nbm':[5,11,17,23,29,35,41,47,53,59,65,71,83,95,107,119,131,143,155,167],
		'gfs':[24,48,72,96],
		'hrrr':[12,18,24,30,36,42,48],
		'rtma':[0],  # hourly run, no fcsts, just analysis
		'urma':[0],  # same as rtma, no fcsts, just analysis
		}

HERBIE_CYCLES = {"nbm": "6h", "hrrr": "6h", "urma": "3h", "gfs": "6h", "rtma_ak": "3h"}

ELEMENT_DICT = {'Wind': {'nbm': ['si10', 'wdir10', 'i10fg']}}

HERBIE_XARRAY_STRINGS = {'Wind': {'nbm': [':WIND:10 m above', ':WDIR:10 m above', ':GUST:10 m above'],
								   'hrrr': [':UGRD:10 m above',':VGRD:10 m above',':GUST:surface'],
                                   'urma': []}}

HERBIE_REQUIRED_PHRASES = {'Wind': {'nbm': ['10 m above ground'], 'hrrr': ['10 m above ground']}}

HERBIE_EXCLUDE_PHRASES = {'Wind': {'nbm': ['ens std dev'], 'hrrr': ['ens std dev']}}

HERBIE_RENAME_MAP = {
    "Wind": {
        "nbm": {
            "wdir10": "wind_dir_deg",
            "si10": "wind_speed_kt",
            "i10fg": "wind_gust_kt"
        },
        "urma": {
            "wdir10": "wind_dir_deg",
            "si10": "wind_speed_kt",
            "i10fg": "wind_gust_kt"
        },
        "hrrr": {
            "u10": "u_wind",
            "v10": "v_wind",
            "gust": "wind_gust_kt"
        }
    }
}

HERBIE_UNIT_CONVERSIONS = {
    "Wind": {
        "nbm": {
            "wind_speed_kt": 1.94384,
            "wind_gust_kt": 1.94384
        },
        "urma": {
            "wind_speed_kt": 1.94384,
            "wind_gust_kt": 1.94384
        },
        "hrrr": {
            "u_wind": 1.94384,
            "v_wind": 1.94384,
            "wind_gust_kt": 1.9484
        }
    }
}

HERBIE_OPEN_INSTRUCTIONS = {
    "Wind": {
        "nbm": {
            "variables": ["si10", "wdir10", "i10fg"],
            "drop_vars": [],
            "with_wind": False
        },
        "urma_ak": {
            "variables": ["u10", "v10"],
            "drop_vars": ["u10", "v10"],
            "with_wind": True
        }
    }
}

HERBIE_OUTPUT_COLUMNS = {
    "Wind": {
        "nbm": ["wind_speed_kt", "wind_dir_deg", "wind_gust_kt"]
    },
    "Temperature": {
        "nbm": ["temp_f"]
    },
    "Precipitation": {
        "nbm": ["precip_in"]
    }
}

HERBIE_DOMAIN = "ak"


########################## NDFD Params #################################
NDFD_DIR = 'ndfd'

NDFD_DICT = {"Wind": {"wspd": ["YCRZ98", "YCRZ97"], "wdir": ["YBRZ98", "YBRZ97"]}, "Gust": {"wgust": ["YWRZ98"]}}

NDFD_SPEED_STRING = "YCRZ98"

NDFD_DIR_STRING = "YBRZ98"

NDFD_GUST_STRING = "YWRZ98"

NDFD_SPEED_STRING_EXT = "YCRZ97"

NDFD_DIR_STRING_EXT = "YBRZ97"

NDFD_FILE_STRINGS = {"Wind": ["wspd", "wdir"], "Gust": ["wgust"]}

NDFD_ELEMENT_STRINGS = {"Wind": ["si10", "wdir10"], "Gust": ["i10fg"]}

##################### AWS Params #################################

NDFD_S3_URL = "s3://alaska-verification/ndfd/"

NBM_S3_URL = "https://noaa-nbm-grib2-pds.s3.amazonaws.com/"

S3_URLS = {"ndfd": "s3://alaska-verification/ndfd/",
            "nbm": "s3://alaska-verification/nbm/",
              "obs": "s3://alaska-verification/obs/",
                'hrrr': "s3://alaska-verification/hrrr/",
                'urma': "s3://alaska-verification/urma/"

              }

MODEL_URLS = {'nbm': "https://noaa-nbm-grib2-pds.s3.amazonaws.com",
               'hrrr':'https://noaa-hrrr-bdp-pds.s3.amazonaws.com',
                 'urma': 'https://noaa-urma-pds.s3.amazonaws.com'
                 }


#################### Processing Params ########################
# for process pool operations
MAX_WORKERS = 8