import requests
import pandas as pd
#import archiver_config as config
from time import sleep
#from datetime import datetime
from archiver_base import Archiver

class ObsArchiver(Archiver):
    def __init__(self, config):
        super().__init__(config)
        self.api_token = config.API_KEY
        self.obs_fields = config.OBS_VARS[config.ELEMENT]  # e.g., ['wind_speed', 'wind_direction']
        self.obs_parse = config.OBS_PARSE_VARS[config.ELEMENT]
        self.network = config.NETWORK
        self.hfmetar = config.HFMETAR
        self.state = config.STATE
        self.url = config.TIMESERIES_URL
        self.metadata_url = config.METADATA_URL
        self.initial_wait = config.INITIAL_WAIT
        self.max_retries = config.MAX_RETRIES

    def get_station_metadata(self):
        params = {
            "state": self.state,
            "network": self.network,
            "hfmetar": self.hfmetar,
            "token": self.api_token,
            "status": "active",
            "units": "english",
            "complete": "1",
            "format": "json"
        }
        response = requests.get(self.metadata_url, params=params)
        response.raise_for_status()
        metadata = response.json()
        stations = metadata.get("STATION", [])
        
        self.station_metadata = {
            s["STID"]: {
                "zone": s.get("NWSZONE"),
                "cwa": s.get("CWA")
            }
            for s in stations
        }
        return list(self.station_metadata.keys())

    def fetch_observations(self, station_ids, start_time, end_time):
        all_obs = []
        for chunk in self._chunk_station_ids(station_ids):
            attempt = 0
            wait = self.initial_wait
            while attempt < self.max_retries:
                try:
                    params = {
                        "stid": ",".join(chunk),
                        "start": start_time,
                        "end": end_time,
                        "vars": ",".join(self.obs_fields),
                        "hfmetars": self.hfmetar,
                        "units": "english",
                        "token": self.api_token,
                        "obtimezone": "utc",
                        "output": "json"
                    }
                    r = requests.get(self.url, params=params)
                    r.raise_for_status()
                    obs_json = r.json()
                    df = self.process_obs_data(obs_json["STATION"])
                    if isinstance(df, pd.DataFrame):
                        all_obs.append(df)
                    else:
                        print(f"⚠️ Unexpected return type from process_obs_data: {type(df)}")
                    break
                except Exception as e:
                    print(f"Retry {attempt+1}/{self.max_retries} failed: {e}")
                    attempt += 1
                    sleep(wait)
                    wait *= 2
        return pd.concat(all_obs, ignore_index=True)

    def process_obs_data(self, raw_obs_json):
        all_records = []

        for station in raw_obs_json:
            stid = station.get("STID")
            obs_data = station.get("OBSERVATIONS", {})
            times = obs_data.get("date_time", [])
            if not times:
                continue  # skip stations with no data
            
            zone_info = self.station_metadata.get(stid, {})
            for t, timestamp in enumerate(times):
                record = {
                    "stid": station.get("STID"),
                    "name": station.get("NAME"),
                    "lat": station.get("LATITUDE"),
                    "lon": station.get("LONGITUDE"),
                    "elev": station.get("ELEVATION"),
                    "valid_time": pd.to_datetime(timestamp),
                    "NWSZONE": zone_info.get("zone"),
                    "NWSCWA": zone_info.get("cwa")
                }
                for var in self.obs_parse:
                    values = obs_data.get(var, [])
                    record[var] = values[t] if t < len(values) else None

                all_records.append(record)
        df = pd.DataFrame(all_records)

        # Drop unnecessary columns
        df.drop(columns=["name"], inplace=True, errors="ignore")

        # Rename columns using config
        rename_map = self.config.OBS_RENAME_MAP[self.config.ELEMENT]
        df.rename(columns=rename_map, inplace=True)

        return df
    
    def _chunk_station_ids(self, station_ids, chunk_size=50):
        for i in range(0, len(station_ids), chunk_size):
            yield station_ids[i:i + chunk_size]

    def fetch_file_list(self, start, end):
        """Stub: required by base class but not used in Synoptic context"""
        return []

    def process_files(self, file_list):
        """Stub: observations don't use a file-based fetch"""
        return None

##TODO Will need to update to grab precipitation and max/min temps
#  Can use statistics api for max/min temps: https://api.synopticlabs.org/v2/stations/legacystats?&token=c6c8a66a96094960aabf1fed7d07ccf0&vars=air_temp&start=202507081200&end=202507090600&type=maximum&units=temp%7Cf 
#  Can use precip api for precip: https://api.synopticdata.com/v2/stations/precipitation?&token=c6c8a66a96094960aabf1fed7d07ccf0&bbox=-154.5,63.0,-141.0,66.0&pmode=totals&start=202507071200&end=202507081200&units=precip|in
#
# if __name__ == "__main__":
#     obs_archiver = ObsArchiver(config)
#     stations = obs_archiver.get_station_metadata()
#     df_obs = obs_archiver.fetch_observations(stations, config.OBS_START, config.OBS_END)
#     print(df_obs[df_obs['stid']=='PAJN'].head(10))