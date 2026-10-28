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
        """Discover regional stations, including listings without Alaska STATE tags."""
        # Current station status is not a reliable indicator of observation
        # availability, including for historical backfills. Include all statuses.
        params = {
            "hfmetar": self.hfmetar,
            "token": self.api_token,
            "units": "english",
            "complete": "1",
            "format": "json"
        }
        # Live API probes return out-of-box stations with exactly +/-180 as an
        # endpoint. Split the requests just inside the dateline: the same probes
        # with +/-179.999999 returned no out-of-box stations.
        # These regional bounds include offshore and neighboring stations; they
        # are deliberately not a strict Alaska land boundary.
        selectors = (
            [{"bbox": "-179.999999,50,-129,73"}, {"bbox": "170,50,179.999999,73"}]
            if self.state.lower() == "ak" else [{"state": self.state}]
        )
        # Keep validation bounds at the true dateline, independently of the
        # request workaround. The endpoint inset is less than 0.1 m here.
        alaska_bounds = ([(-180, 50, -129, 73), (170, 50, 180, 73)]
                         if self.state.lower() == "ak" else [])
        stations = {}
        rejected = set()
        for selector in selectors:
            response = requests.get(self.metadata_url, params={**params, **selector}, timeout=60)
            response.raise_for_status()
            metadata = response.json()
            code = str(metadata.get("SUMMARY", {}).get("RESPONSE_CODE", ""))
            if code == "2":
                continue
            if code != "1" or not isinstance(metadata.get("STATION"), list):
                raise ValueError(f"Unexpected Synoptic metadata response (code={code or 'missing'})")
            for station in metadata["STATION"]:
                # Never trust the remote bbox alone: dateline-edge requests can
                # return coordinates beyond the intended region. Validate the
                # union locally before any observation downloads are requested.
                if alaska_bounds:
                    try:
                        lat = float(station["LATITUDE"])
                        lon = float(station["LONGITUDE"])
                    except (KeyError, TypeError, ValueError):
                        rejected.add(station.get("STID"))
                        continue
                    if not any(west <= lon <= east and south <= lat <= north
                               for west, south, east, north in alaska_bounds):
                        rejected.add(station.get("STID"))
                        continue
                stations[station["STID"]] = station

        self.station_metadata = {
            s["STID"]: {
                "zone": s.get("NWSZONE"),
                "cwa": s.get("CWA")
            }
            for s in stations.values()
        }
        selection = "Alaska geographic bounds" if self.state.lower() == "ak" else f"state={self.state}"
        print(f"OBS metadata: {len(stations):,} stations selected using {selection}; all statuses included")
        if rejected:
            print(f"OBS metadata: rejected {len(rejected):,} listings outside geographic bounds or with invalid coordinates")
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
        accum_hours,
        step_hours,
        units="english",
        interval_window="0.5,0.5",
    ):
        """Fetch direct accumulation totals at UTC endpoints every step_hours.

        For 24-hour totals every six hours, four offset interval=24 requests
        cover the requested range. Synoptic computes every total independently;
        the archiver never sums shorter precipitation intervals. Each request
        starts one accumulation before its first requested endpoint.
        """
        if (accum_hours <= 0 or step_hours <= 0 or accum_hours % step_hours != 0
                or int(accum_hours) != accum_hours or int(step_hours) != step_hours):
            raise ValueError("accum_hours must be a positive integer multiple of step_hours")
        requested_start = pd.to_datetime(self._fmt_time(start_time), format="%Y%m%d%H%M", utc=True)
        requested_end = pd.to_datetime(self._fmt_time(end_time), format="%Y%m%d%H%M", utc=True)
        if requested_start > requested_end:
            raise ValueError("start_time must not be after end_time")
        duration = pd.Timedelta(hours=accum_hours)
        first_endpoint = requested_start.ceil(f"{step_hours}h")
        base_url = "https://api.synopticdata.com/v2/stations/precip"
        units_param = "precip|in" if units.lower() == "english" else "precip|mm"
        parts = []
        for offset in range(int(accum_hours // step_hours)):
            first = first_endpoint + pd.Timedelta(hours=offset * step_hours)
            if first > requested_end:
                break
            query_start = first - duration
            query_end = first + ((requested_end - first) // duration) * duration
            for chunk in self._chunk_station_ids(station_ids):
                attempt, wait = 0, self.initial_wait
                while attempt < self.max_retries:
                    try:
                        params = {
                            "token": self.api_token,
                            "stid": ",".join(chunk),
                            "start": self._fmt_time(query_start),
                            "end": self._fmt_time(query_end),
                            "pmode": "intervals",
                            "interval": str(int(accum_hours)),
                            "obtimezone": "utc",
                            "interval_window": interval_window,
                            "units": units_param,
                            "output": "json",
                        }
                        r = requests.get(base_url, params=params, timeout=60)
                        r.raise_for_status()
                        js = r.json()
                        code = str(js.get("SUMMARY", {}).get("RESPONSE_CODE", ""))
                        if code == "2":
                            break
                        if code != "1" or not isinstance(js.get("STATION"), list):
                            raise ValueError(f"Unexpected Synoptic precipitation response (code={code or 'missing'})")
                        df_int = self._process_precip_json_for_rolling(
                            js, query_start=query_start, accum_hours=accum_hours)
                        if not df_int.empty:
                            parts.append(df_int)
                        break
                    except Exception as e:
                        # Do not print request URLs/exceptions that can contain
                        # the API token in their query string.
                        print(f"Retry {attempt+1}/{self.max_retries} for precipitation "
                              f"(stations {chunk[:3]}..., endpoint hour {first.hour:02}Z; {type(e).__name__})")
                        attempt += 1
                        if attempt >= self.max_retries:
                            raise RuntimeError("Synoptic precipitation request exhausted retries") from e
                        sleep(wait)
                        wait *= 2

        from precip_windows import validate_precip_windows
        intervals = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        return validate_precip_windows(intervals, requested_start, requested_end,
            accum_hours=accum_hours, step_hours=step_hours,
            interval_window=interval_window)

    def _process_precip_json_for_rolling(self, raw_json, *, query_start, accum_hours):
        stations = raw_json.get("STATION", []) or []
        units = (raw_json.get("UNITS", {}) or {}).get("precipitation", None)
        rows = []
        for st in stations:
            stid = st.get("STID")
            obs = ((st.get("OBSERVATIONS") or {}).get("precipitation") or [])
            zone_info = getattr(self, "station_metadata", {}).get(stid, {})
            for rec in obs:
                # Synoptic's interval is the one-based position in this
                # request, including missing slots. Do not infer the requested
                # endpoint by rounding a potentially incomplete last_report.
                try:
                    index = float(rec.get("interval"))
                    nominal_end = (query_start + pd.Timedelta(hours=int(index)*accum_hours)
                                   if index.is_integer() and index > 0 else pd.NaT)
                except (TypeError, ValueError, OverflowError):
                    nominal_end = pd.NaT
                rows.append({
                    "stid": stid,
                    "lat": st.get("LATITUDE"),
                    "lon": st.get("LONGITUDE"),
                    "elev": st.get("ELEVATION"),
                    "first_report": pd.to_datetime(rec.get("first_report"), utc=True, errors="coerce"),
                    "last_report": pd.to_datetime(rec.get("last_report"), utc=True, errors="coerce"),
                    "nominal_end_time": nominal_end,
                    "report_type": rec.get("report_type"),
                    "interval_precip": rec.get("total"),
                    "precip_units": units,
                    "NWSZONE": zone_info.get("zone"),
                    "NWSCWA": zone_info.get("cwa"),
                })
        frame = pd.DataFrame(rows)
        if not frame.empty:
            for column in ("first_report", "last_report", "nominal_end_time"):
                frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
            frame["interval_precip"] = pd.to_numeric(frame["interval_precip"], errors="coerce")
        return frame

    @staticmethod
    def _to_utc_timestamp(val):
        """Accept 'YYYYmmdd' or 'YYYYmmddHHMM' or datetime → pandas UTC Timestamp @ 00Z for YYYYmmdd."""
        if isinstance(val, datetime):
            return pd.Timestamp(val, tz="UTC").floor("D")
        s = str(val)
        if len(s) == 8:
            return pd.to_datetime(s + "0000", format="%Y%m%d%H%M", utc=True).floor("D")
        # assume YYYYmmddHHMM
        return pd.to_datetime(s, format="%Y%m%d%H%M", utc=True).floor("D")

    def fetch_tmax_12to06_timeseries(self, station_ids, start_date, end_date, *, units="english"):
        """
        Max temperature over 12Z→06Z-next-day windows for each station and day.

        Parameters
        ----------
        station_ids : list[str]
        start_date  : 'YYYYmmdd' or 'YYYYmmddHHMM' or datetime  (anchor-day start, UTC)
        end_date    : 'YYYYmmdd' or 'YYYYmmddHHMM' or datetime  (anchor-day end, UTC)
        units       : "english" (°F) or "metric" (°C)

        Returns DataFrame with columns:
        stid, lat, lon, elev, date (anchor day UTC),
        window_start, window_end, tmax, temp_units, NWSZONE, NWSCWA
        """
        # --- build fetch window: first 12Z through last 06Z after end_date
        start_day = self._to_utc_timestamp(start_date)
        end_day   = self._to_utc_timestamp(end_date)
        if end_day < start_day:
            raise ValueError("end_date must be >= start_date")

        fetch_start = start_day + pd.Timedelta(hours=12)     # first 12Z
        fetch_end   = end_day + pd.Timedelta(days=1, hours=6)  # last 06Z after end_day

        units_param = "english" if units.lower() == "english" else "metric"

        parts = []
        for chunk in self._chunk_station_ids(station_ids):
            attempt, wait = 0, self.initial_wait
            while attempt < self.max_retries:
                try:
                    params = {
                        "token": self.api_token,
                        "stid": ",".join(chunk),
                        "start": fetch_start.strftime("%Y%m%d%H%M"),
                        "end":   fetch_end.strftime("%Y%m%d%H%M"),
                        "vars":  "air_temp",
                        "obtimezone": "utc",
                        "units": units_param,     # °F or °C
                        "output": "json",
                        "hfmetars": self.hfmetar,
                    }
                    
                    r = requests.get(self.url, params=params, timeout=60)
                    r.raise_for_status()
                    js = r.json()
                    
                    # --- flatten minimal time series
                    rows = []
                    for st in js.get("STATION", []):
                        stid = st.get("STID")
                        obs = st.get("OBSERVATIONS", {}) or {}
                        times = obs.get("date_time", []) or []
                        # pick temperature key (e.g., 'air_temp_set_1')
                        tkey = "air_temp_set_1"
                        if tkey not in obs:
                            # fallback to any air_temp* key
                            cand = [k for k in obs if k.startswith("air_temp")]
                            if cand:
                                tkey = cand[0]
                            else:
                                continue
                        temps = obs.get(tkey, []) or []

                        zone_info = self.station_metadata.get(stid, {}) if hasattr(self, "station_metadata") else {}

                        for i, ts in enumerate(times):
                            rows.append({
                                "stid": stid,
                                "lat": st.get("LATITUDE"),
                                "lon": st.get("LONGITUDE"),
                                "elev": st.get("ELEVATION"),
                                "valid_time": pd.to_datetime(ts, utc=True),
                                "temp": pd.to_numeric(temps[i], errors="coerce") if i < len(temps) else pd.NA,
                                "NWSZONE": zone_info.get("zone"),
                                "NWSCWA": zone_info.get("cwa"),
                            })
                    if rows:
                        parts.append(pd.DataFrame(rows))
                    break
                except Exception as e:
                    print(f"Retry {attempt+1}/{self.max_retries} (stations {chunk[:3]}...): {e}")
                    attempt += 1
                    sleep(wait); wait *= 2

        if not parts:
            return pd.DataFrame(columns=[
                "stid","lat","lon","elev","date","window_start","window_end",
                "tmax","temp_units","NWSZONE","NWSCWA"
            ])

        df = pd.concat(parts, ignore_index=True)
        df = df.sort_values(["stid","valid_time"]).reset_index(drop=True)

        # --- keep only times inside the 12Z–24Z OR 00Z–06Z windows
        hr = df["valid_time"].dt.hour
        df = df[(hr >= 12) | (hr < 6)].copy()

        # --- anchor each sample to its 12Z-start day: floor((t-12h) to day)
        df["anchor_day"] = (df["valid_time"] - pd.Timedelta(hours=12)).dt.floor("D")
        # keep only requested anchor-day range
        df = df[(df["anchor_day"] >= start_day) & (df["anchor_day"] <= end_day)]

        # --- compute Tmax per station & anchor_day
        grp = (df.groupby(["stid","anchor_day"], as_index=False)
                .agg(tmax=("temp", "max"),
                    lat=("lat","first"),
                    lon=("lon","first"),
                    elev=("elev","first"),
                    NWSZONE=("NWSZONE","first"),
                    NWSCWA=("NWSCWA","first")))

        # --- add window bounds + units
        grp["window_start"] = grp["anchor_day"] + pd.Timedelta(hours=12)
        grp["window_end"]   = grp["anchor_day"] + pd.Timedelta(days=1, hours=6)
        grp.rename(columns={"anchor_day": "date"}, inplace=True)
        grp["temp_units"] = "F" if units_param == "english" else "C"

        cols = ["stid","lat","lon","elev","date","window_start","window_end",
                "tmax","temp_units","NWSZONE","NWSCWA"]
        return grp.loc[:, cols].sort_values(["stid","date"]).reset_index(drop=True)

    def fetch_tmin_00to18_timeseries(self, station_ids, start_date, end_date, *, units="english"):
        """
        Min temperature over the 18-hour window: 00Z → 18Z (same day) for each station.

        Parameters
        ----------
        station_ids : list[str]
        start_date  : 'YYYYmmdd' or 'YYYYmmddHHMM' or datetime  (anchor-day start, UTC)
        end_date    : 'YYYYmmdd' or 'YYYYmmddHHMM' or datetime  (anchor-day end, UTC)
        units       : "english" (°F) or "metric" (°C)

        Returns DataFrame with columns:
        stid, lat, lon, elev, date (anchor day UTC),
        window_start, window_end, tmin, temp_units, NWSZONE, NWSCWA
        """
        # Helper from your tmax function
        def _to_utc_timestamp(val):
            if isinstance(val, datetime):
                return pd.Timestamp(val, tz="UTC").floor("D")
            s = str(val)
            if len(s) == 8:
                return pd.to_datetime(s + "0000", format="%Y%m%d%H%M", utc=True).floor("D")
            return pd.to_datetime(s, format="%Y%m%d%H%M", utc=True).floor("D")

        # Anchor-day range
        start_day = _to_utc_timestamp(start_date)
        end_day   = _to_utc_timestamp(end_date)
        if end_day < start_day:
            raise ValueError("end_date must be >= start_date")

        # Fetch just what we need: from first 00Z to last 18Z (same day window)
        fetch_start = start_day + pd.Timedelta(hours=0)
        fetch_end   = end_day   + pd.Timedelta(hours=18)

        units_param = "english" if units.lower() == "english" else "metric"

        parts = []
        for chunk in self._chunk_station_ids(station_ids):
            attempt, wait = 0, self.initial_wait
            while attempt < self.max_retries:
                try:
                    params = {
                        "token": self.api_token,
                        "stid": ",".join(chunk),
                        "start": fetch_start.strftime("%Y%m%d%H%M"),
                        "end":   fetch_end.strftime("%Y%m%d%H%M"),
                        "vars":  "air_temp",
                        "obtimezone": "utc",
                        "units": units_param,     # °F or °C
                        "output": "json",
                        "hfmetars": self.hfmetar,
                    }
                    r = requests.get(self.url, params=params, timeout=60)
                    r.raise_for_status()
                    js = r.json()

                    rows = []
                    for st in js.get("STATION", []):
                        stid = st.get("STID")
                        obs = st.get("OBSERVATIONS", {}) or {}
                        times = obs.get("date_time", []) or []
                        # temperature key (e.g., air_temp_set_1)
                        tkey = "air_temp_set_1"
                        if tkey not in obs:
                            cand = [k for k in obs if k.startswith("air_temp")]
                            if not cand:
                                continue
                            tkey = cand[0]
                        vals = obs.get(tkey, []) or []

                        zone_info = getattr(self, "station_metadata", {}).get(stid, {})
                        for i, ts in enumerate(times):
                            rows.append({
                                "stid": stid,
                                "lat":  st.get("LATITUDE"),
                                "lon":  st.get("LONGITUDE"),
                                "elev": st.get("ELEVATION"),
                                "valid_time": pd.to_datetime(ts, utc=True),
                                "temp": pd.to_numeric(vals[i], errors="coerce") if i < len(vals) else pd.NA,
                                "NWSZONE": zone_info.get("zone"),
                                "NWSCWA": zone_info.get("cwa"),
                            })
                    if rows:
                        parts.append(pd.DataFrame(rows))
                    break
                except Exception as e:
                    print(f"Retry {attempt+1}/{self.max_retries} (stations {chunk[:3]}...): {e}")
                    attempt += 1
                    sleep(wait); wait *= 2

        if not parts:
            return pd.DataFrame(columns=[
                "stid","lat","lon","elev","date","window_start","window_end",
                "tmin","temp_units","NWSZONE","NWSCWA"
            ])

        df = pd.concat(parts, ignore_index=True)
        df = df.sort_values(["stid","valid_time"]).reset_index(drop=True)

        # Keep only samples inside [00Z, 18Z) for each day
        hr = df["valid_time"].dt.hour
        df = df[(hr >= 0) & (hr < 18)].copy()

        # Anchor day is simply the UTC calendar day (no offset needed)
        df["anchor_day"] = df["valid_time"].dt.floor("D")
        df = df[(df["anchor_day"] >= start_day) & (df["anchor_day"] <= end_day)]

        # Tmin per station & anchor_day
        grp = (df.groupby(["stid","anchor_day"], as_index=False)
                .agg(tmin=("temp", "min"),
                    lat=("lat","first"),
                    lon=("lon","first"),
                    elev=("elev","first"),
                    NWSZONE=("NWSZONE","first"),
                    NWSCWA=("NWSCWA","first")))

        # Window bounds + units
        grp["window_start"] = grp["anchor_day"] + pd.Timedelta(hours=0)
        grp["window_end"]   = grp["anchor_day"] + pd.Timedelta(hours=18)
        grp.rename(columns={"anchor_day": "date"}, inplace=True)
        grp["temp_units"] = "F" if units_param == "english" else "C"

        cols = ["stid","lat","lon","elev","date","window_start","window_end",
                "tmin","temp_units","NWSZONE","NWSCWA"]
        return grp.loc[:, cols].sort_values(["stid","date"]).reset_index(drop=True)

    def fetch_observations(self, station_ids, start_time, end_time):
        all_obs = []
        requested_count = returned_count = 0
        for chunk in self._chunk_station_ids(station_ids):
            requested_count += len(chunk)
            wait = self.initial_wait
            for attempt in range(self.max_retries):
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
                    r = requests.get(self.url, params=params, timeout=90)
                    r.raise_for_status()
                    obs_json = r.json()
                    code = str(obs_json.get("SUMMARY", {}).get("RESPONSE_CODE", ""))
                    if code == "2":
                        print("WARNING: No matching stations or access unavailable; "
                              f"stations={','.join(chunk)}; requested={len(chunk)}, returned=0")
                        break
                    # Missing STATION must not silently swallow other API errors.
                    if code not in ("", "1") or not isinstance(obs_json.get("STATION"), list):
                        raise ValueError(f"Unexpected Synoptic response (code={code or 'missing'})")
                    stations = obs_json["STATION"]
                    df = self.process_obs_data(stations) if stations else pd.DataFrame()
                    if not isinstance(df, pd.DataFrame):
                        raise TypeError("process_obs_data must return a DataFrame")
                    all_obs.append(df)
                    returned_count += len(stations)
                    print(f"OBS batch: requested={len(chunk)}, returned={len(stations)}")
                    break
                except Exception as e:
                    # Exception text can include URLs containing the API token.
                    print(f"Attempt {attempt+1}/{self.max_retries} failed: {type(e).__name__}")
                    if attempt + 1 == self.max_retries:
                        raise RuntimeError(
                            f"Observation batch failed after {self.max_retries} attempts; "
                            f"stations={','.join(chunk)}"
                        ) from None
                    sleep(wait)
                    wait *= 2
        print(f"OBS station counts: requested={requested_count}, returned={returned_count}")
        return pd.concat(all_obs, ignore_index=True) if all_obs else pd.DataFrame()

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
#  Can use statistics api for max/min temps: https://api.synopticlabs.org/v2/stations/legacystats?&token=YOUR_API_KEY&vars=air_temp&start=202507081200&end=202507090600&type=maximum&units=temp%7Cf
#  Can use precip api for precip: https://api.synopticdata.com/v2/stations/precipitation?&token=YOUR_API_KEY&bbox=-154.5,63.0,-141.0,66.0&pmode=totals&start=202507071200&end=202507081200&units=precip|in
#
if __name__ == "__main__":
    obs_archiver = ObsArchiver(config)
    stations = obs_archiver.get_station_metadata()
    print(stations)
    if config.ELEMENT == "precip24hr":
        df_obs = obs_archiver.fetch_precip_rolling(stations, config.OBS_START, config.OBS_END, accum_hours=24, step_hours=6)
    elif config.ELEMENT == "precip6hr":
        df_obs = obs_archiver.fetch_precip_rolling(stations, config.OBS_START, config.OBS_END, accum_hours=6, step_hours=6)
    elif config.ELEMENT == "Wind":
        df_obs = obs_archiver.fetch_observations(stations, config.OBS_START, config.OBS_END)
    elif config.ELEMENT == "rh":
        df_obs = obs_archiver.fetch_observations(stations, config.OBS_START, config.OBS_END)
    elif config.ELEMENT == "maxt":
        df_obs = obs_archiver.fetch_tmax_12to06_timeseries(
        stations,
        config.OBS_START,   # UTC anchor-day start (YYYYmmdd is fine)
        config.OBS_END
        )
    elif config.ELEMENT == "mint":
        df_obs = obs_archiver.fetch_tmin_00to18_timeseries(stations, config.OBS_START, config.OBS_END)
    #df_obs = obs_archiver.fetch_observations(stations, config.OBS_START, config.OBS_END)
    print(df_obs[df_obs['stid']=='PAJN'].head(10))
