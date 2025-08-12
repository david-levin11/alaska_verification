import requests
import pandas as pd
import archiver_config as config
from time import sleep
from datetime import datetime, timedelta
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
    
    @staticmethod
    def _fmt_time(t):
        """Accept 'YYYYmmddHHMM' (str/int) or datetime -> Synoptic format string."""
        if isinstance(t, datetime):
            return t.strftime("%Y%m%d%H%M")
        s = str(t)
        # tolerate 'YYYYmmddHH' by appending minutes
        return s if len(s) == 12 else (s + "00" if len(s) == 10 else s)

    def fetch_precip_rolling(
        self,
        station_ids,
        start_time,
        end_time,
        *,
        accum_hours,            # e.g., 6 or 24
        step_hours,             # e.g., 6 (for 6h) or 12 (for 24h rolling every 12)
        units="english",        # "english" (in) or "metric" (mm)
        interval_window="0.5,0.5",
    ):
        """
        Rolling precipitation accumulations using Synoptic's precipitation service.

        Strategy:
        1) Request pmode=intervals with interval=step_hours across [start,end].
        2) For each station, sort intervals by end_time.
        3) Rolling sum over k = accum_hours // step_hours intervals.

        Returns DataFrame with:
        stid, lat, lon, elev, accum_hours, step_hours,
        start_time, end_time, precip_total, precip_units, NWSZONE, NWSCWA
        """
        if accum_hours % step_hours != 0:
            raise ValueError("accum_hours must be an integer multiple of step_hours")
        k = accum_hours // step_hours

        base_url = "https://api.synopticdata.com/v2/stations/precip"
        units_param = "precip|in" if units.lower() == "english" else "precip|mm"

        parts = []
        for chunk in self._chunk_station_ids(station_ids):
            attempt, wait = 0, self.initial_wait
            while attempt < self.max_retries:
                try:
                    params = {
                        "token": self.api_token,
                        "stid": ",".join(chunk),
                        "start": self._fmt_time(start_time),
                        "end": self._fmt_time(end_time),
                        "pmode": "intervals",
                        "interval": str(int(step_hours)),   # step cadence the API will return
                        "obtimezone": "utc",
                        "interval_window": interval_window,
                        "units": units_param,
                        "output": "json",
                    }
                    r = requests.get(base_url, params=params, timeout=60)
                    r.raise_for_status()
                    js = r.json()
                    df_int = self._process_precip_json_for_rolling(js)  # helper below
                    if not df_int.empty:
                        parts.append(df_int)
                    break
                except Exception as e:
                    print(f"Retry {attempt+1}/{self.max_retries} (stations {chunk[:3]}...): {e}")
                    attempt += 1
                    sleep(wait)
                    wait *= 2

        if not parts:
            return pd.DataFrame(
                columns=["stid","lat","lon","elev","accum_hours","step_hours",
                         "start_time","end_time","precip_total","precip_units","NWSZONE","NWSCWA"]
            )


        df = pd.concat(parts, ignore_index=True)
        df = df.sort_values(["stid", "end_time"]).reset_index(drop=True)
        df["interval_precip"] = pd.to_numeric(df["interval_precip"], errors="coerce")

        k = accum_hours // step_hours
        df["precip_total"] = (
            df.groupby("stid", sort=False)["interval_precip"]
            .transform(lambda s: s.rolling(window=k, min_periods=k).sum())
        )

        out = df.loc[df["precip_total"].notna()].copy()
        out["accum_hours"] = accum_hours
        out["step_hours"] = step_hours
        out["start_time"] = out["end_time"] - pd.to_timedelta(accum_hours, unit="h")

        # if you DON'T want to keep the interval values, drop them here:
        # out = out.drop(columns=["interval_precip"])

        cols = ["stid","lat","lon","elev","accum_hours","step_hours",
                "start_time","end_time","precip_total","precip_units","NWSZONE","NWSCWA"]
        out = out.loc[:, cols].reset_index(drop=True)
        return out


    def _process_precip_json_for_rolling(self, raw_json):
        stations = raw_json.get("STATION", []) or []
        units = (raw_json.get("UNITS", {}) or {}).get("precipitation", None)
        rows = []
        for st in stations:
            stid = st.get("STID")
            obs = ((st.get("OBSERVATIONS") or {}).get("precipitation") or [])
            zone_info = getattr(self, "station_metadata", {}).get(stid, {})
            for rec in obs:
                rows.append({
                    "stid": stid,
                    "lat": st.get("LATITUDE"),
                    "lon": st.get("LONGITUDE"),
                    "elev": st.get("ELEVATION"),
                    "end_time": pd.to_datetime(rec.get("last_report")),
                    "interval_precip": rec.get("total"),     # <-- renamed
                    "precip_units": units,
                    "NWSZONE": zone_info.get("zone"),
                    "NWSCWA": zone_info.get("cwa"),
                })
        return pd.DataFrame(rows)


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
if __name__ == "__main__":
    obs_archiver = ObsArchiver(config)
    stations = obs_archiver.get_station_metadata()
    print(stations)
    if config.ELEMENT == "precip24hr":
        df_obs = obs_archiver.fetch_precip_rolling(stations, config.OBS_START, config.OBS_END, accum_hours=24, step_hours=12)
    elif config.ELEMENT == "precip6hr":
        df_obs = obs_archiver.fetch_precip_rolling(stations, config.OBS_START, config.OBS_END, accum_hours=6, step_hours=6)
    elif config.ELEMENT == "Wind":
        df_obs = obs_archiver.fetch_observations(stations, config.OBS_START, config.OBS_END)
    #df_obs = obs_archiver.fetch_observations(stations, config.OBS_START, config.OBS_END)
    print(df_obs[df_obs['stid']=='PAJN'].head(10))