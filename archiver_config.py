import os


USE_CLOUD_STORAGE = True # Set to true to append to S3 bucket database.  False saves site level .csv files locally
######################### Wx Elements ################################
ELEMENT = 'Gust'

######################### Directories #################################

HOME = os.path.abspath(os.path.dirname(__file__))

OBS = os.path.join(HOME, 'obs')

MODEL_DIR = os.path.join(HOME, 'model')

TMP = os.path.join(HOME, 'tmp_cache')

for directory in [OBS, MODEL_DIR, TMP]:
    os.makedirs(directory, exist_ok=True)
######################## File Names #################################

WIND_OBS_FILE = f"alaska_{ELEMENT.lower()}_obs.csv"

WIND_OBS_FILE_COMPRESSED = f"alaska_{ELEMENT.lower()}_obs.parquet"


###################### Synoptic Params ##########################
API_KEY = "c6c8a66a96094960aabf1fed7d07ccf0" # link to get an API key can be found at https://docs.google.com/document/d/1YuMUYog4J7DpFoEszMmFir4Ehqk9Q0GHG_QhSdrgV9M/edit?usp=sharing

TIMESERIES_URL = "https://api.synopticdata.com/v2/stations/timeseries"
# This will need to be changed after the new Synoptic statistics API is released.
STATISTICS_URL = "https://api.synopticdata.com/v2/stations/legacystats"

METADATA_URL = "https://api.synopticdata.com/v2/stations/metadata"

STATE = "ak"

HFMETAR = "0"

OBS_VARS = {"Wind": ["wind_direction", "wind_speed", "wind_gust"],
            "precip24hr": ["precip_intervals", "precip_accum"],
            "precip6hr": ["precip_intervals", "precip_accum"],
            "maxt": ["air_temp"],
            "mint": ['air_temp']}
# Need to set this up for precip24hr and maxt mint
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
MODEL = 'nbmqmd_exp'

HERBIE_MODELS = ['hrrr','nbm','nbmqmd','nbmqmd_exp','urma','rtma','gfs']

HERBIE_PRODUCTS = {'nbm':'ak',
            'nbmqmd': 'ak',
            'nbmqmd_exp': 'ak',
			'gfs':'pgrb2.0p25',
			'hrrr':'sfc',
			'rtma_ak':'ges',
			'urma_ak':'ges'
			}

HERBIE_FORECASTS = {
		'nbm':{
            'Wind':[5,11,17,23,29,35,41,47,53,59,65,71,83,95,107,119,131,143,155,167]
        },
        'nbmqmd': {
            'precip24hr': [24,30,36,48,60,72,84,96,108,120,132,144,156,168],
            'precip6hr': [6,12,18,24,30,36,42,48,54,60,66,72,78,84,90,96,102,108,114,120],
            'maxt': [18, 30, 42, 54, 66, 78, 90, 102, 114, 126, 138, 150, 162, 174],
            'mint': [18, 30, 42, 54, 66, 78, 90, 102, 114, 126, 138, 150, 162, 174]
        },
        'nbmqmd_exp': {
            'precip24hr': [24,30,36,48,60,72,84,96,108,120,132,144,156,168],
            'precip6hr': [6,12,18,24,30,36,42,48,54,60,66,72,78,84,90,96,102,108,114,120],
            'maxt': [18, 30, 42, 54, 66, 78, 90, 102, 114, 126, 138, 150, 162, 174],
            'mint': [18, 30, 42, 54, 66, 78, 90, 102, 114, 126, 138, 150, 162, 174],
            'Wind': [12,18,24,30,36,42,48,54,60,66,72,84,96,108,120,132,144,156,168],
            'Gust': [12,18,24,30,36,42,48,54,60,66,72,84,96,108,120,132,144,156,168],
        },
		'gfs':{
            'Wind': [24,48,72,96]
        },
		'hrrr':{
            'Wind': [12,18,24,30,36,42,48]
        },
		'rtma':{
            'Wind':[0]
          },  # hourly run, no fcsts, just analysis
		'urma':{
            'Wind':[0]
          }  # same as rtma, no fcsts, just analysis
		}


AVAILABLE_FIELDS = {'nbm': ['Wind'], 'nbmqmd': ['precip24hr', 'precip6hr', "maxt", 'mint'], 'nbmqmd_exp': ['precip24hr', 'precip6hr', "maxt", 'mint', 'Wind', "Gust"], 'hrrr': ['Wind'], 'urma': ['Wind']}

HERBIE_CYCLES = {"nbm": "6h","nbmqmd": "12h", "nbmqmd_exp": "12h", "hrrr": "6h", "urma": "3h", "gfs": "6h", "rtma_ak": "3h"}

HERBIE_XARRAY_STRINGS = {'Wind': {'nbm': [':WIND:10 m above', ':WDIR:10 m above', ':GUST:10 m above'],
                                  'nbmqmd_exp': [':WIND:10 m above'],
								   'hrrr': [':UGRD:10 m above',':VGRD:10 m above',':GUST:surface'],
                                   'urma': []},
                        'precip24hr': {'nbmqmd': [':APCP:surface:'],'nbmqmd_exp': [':APCP:surface:']},
                        'precip6hr': {'nbmqmd': [':APCP:surface:'],'nbmqmd_exp': [':APCP:surface:']},
                        'maxt': {'nbmqmd': [':TMP:2 m above ground:'], 'nbmqmd_exp': [':TMP:2 m above ground:']},
                        'mint': {'nbmqmd': [':TMP:2 m above ground:'],'nbmqmd_exp': [':TMP:2 m above ground:']},
                        'Gust': {'nbmqmd_exp': [':GUST:10 m above']}
                        }

QMD_CYCLES = {
    'precip24hr': {
    'nbmqmd': 24,
    'nbmqmd_exp': 24
    },
    'precip6hr': {
        'nbmqmd': 6,
        'nbmqmd_exp': 6
    },
    'maxt': {
        'nbmqmd': 18,
        'nbmqmd_exp': 18
    },
    'mint': {
        'nbmqmd': 18,
        'nbmqmd_exp': 18
    }
}

HERBIE_REQUIRED_PHRASES = {'Wind': {'nbm': ['10 m above ground'], 'hrrr': ['10 m above ground']},
                           'precip24hr': {'nbmqmd': ['APCP:surface']},
                           'maxt': {'nbmqmd': [':TMP:2 m above ground:']},
                           'mint': {'nbmqmd': [':TMP:2 m above ground:']}}

HERBIE_EXCLUDE_PHRASES = {'Wind': {'nbm': ['ens std dev'], 'hrrr': ['ens std dev']},
                          'precip24hr': {'nbmqmd': ['ens std dev']},
                          'maxt': {'nbmqmd': ['ens std dev']},
                          'mint': {'nbmqmd': [':TMP:2 m above ground:']}}

HERBIE_RENAME_MAP = {
    "Wind": {
        "nbm": {
            "wdir10": "wind_dir_deg",
            "si10": "wind_speed_kt",
            "i10fg": "wind_gust_kt"
        },
        "nbmqmd_exp": {
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
    },
    "Gust": {
        "nbmqmd_exp": {
            "i10fg": "wind_gust_kt"
        }
    },
    "precip24hr": {
        "nbmqmd": {
            "apcp": "precip_accum_24hr"
        },
        "nbmqmd_exp": {
            "apcp": "precip_accum_24hr"
        }
    },
    "precip6hr": {
        "nbmqmd": {
            "apcp": "precip_accum_6hr"
        },
        "nbmqmd_exp": {
            "apcp": "precip_accum_6hr"
        }
    },
    "maxt": {
        "nbmqmd": {
            "t2m": "max_temp"
        },
        "nbmqmd_exp": {
            "t2m": "max_temp"
        }
    },
    "mint": {
        "nbmqmd": {
            "t2m": "min_temp"
        },
        "nbmqmd_exp": {
            "t2m": "min_temp"
        }
    
    }
}

HERBIE_UNIT_CONVERSIONS = {
    "Wind": {
        "nbm": {
            "wind_speed_kt": 1.94384,
            "wind_gust_kt": 1.94384
        },
        "nbmqmd_exp": {
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
        },
        "nbmqmd_exp":  {"wind_speed_kt": 1.94384
        }
    },
    "Gust": {
        "nbmqmd_exp": {
            "wind_gust_kt": 1.9484
        }
    },
    "precip24hr": {
        "nbmqmd":  {"precip24hr": 0.0393701
        },
        "nbmqmd_exp":  {"precip24hr": 0.0393701
        }
    },
    "precip6hr": {
        "nbmqmd":  {"precip6hr": 0.0393701
        },
        "nbmqmd_exp":  {"precip6hr": 0.0393701
        }
    },
    "maxt": {
        "nbmqmd":  {"maxt": 1.8
        },
        "nbmqmd_exp":  {"maxt": 1.8
        }
    },
    "mint": {
        "nbmqmd":  {"mint": 1.8
        },
        "nbmqmd_exp":  {"mint": 1.8
        }
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
                'urma': "s3://alaska-verification/urma/",
                "nbmqmd": "s3://alaska-verification/nbmqmd/",
                "nbmqmd_exp": "s3://alaska-verification/nbmqmd_exp/"
              }

MODEL_URLS = {'nbm': "https://noaa-nbm-grib2-pds.s3.amazonaws.com",
              'nbmqmd': "https://noaa-nbm-grib2-pds.s3.amazonaws.com",
              'nbmqmd_exp': "https://noaa-nbm-para-pds.s3.amazonaws.com",
               'hrrr':'https://noaa-hrrr-bdp-pds.s3.amazonaws.com',
                 'urma': 'https://noaa-urma-pds.s3.amazonaws.com'
                 }


#################### Processing Params ########################
# for process pool operations
MAX_WORKERS = 8