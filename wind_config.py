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

NETWORK = 

OBS_START = "202501010000"

OBS_END = "202501020000"






