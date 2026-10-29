from archiver_base import Archiver
import os
import sys
import pandas as pd
import requests
from pathlib import Path
from utils import get_ndfd_file_list, extract_ndfd_forecasts_parallel, parse_metadata

class NDFDArchiver(Archiver):
    def __init__(self, config, start=None, wxelement=None):
        super().__init__(config)
        self.start = start or config.OBS_START  # fallback to config if not passed
        self.wxelement = wxelement or config.ELEMENT
        self.station_df = self.ensure_metadata()

    def ensure_metadata(self):
        """Use the full fresh regional inventory for every NDFD element.

        Historical observations can exist at inactive stations or listings
        without STATE tags. Legacy per-element OBS caches are never consulted.
        Forecast extraction must not depend on a station's sensor inventory.
        """
        from regional_stations import discover_regional_stations
        if not self.config.API_KEY:
            raise ValueError("Set SYNOPTIC_API_KEY before requesting NDFD station metadata")
        stations = discover_regional_stations(self.config.METADATA_URL,
            self.config.API_KEY, self.config.STATE, getattr(self.config, "HFMETAR", "0"),
            get=requests.get)
        frame = parse_metadata({"STATION": stations})
        if frame.empty:
            raise ValueError("No stations with valid coordinates returned for NDFD extraction")
        print(f"NDFD {self.wxelement}: {len(frame):,} extraction stations; fresh regional discovery")
        # Diagnostic snapshot only. A locked CSV must not stop extraction, and
        # this file is never read back as a station-selection cache.
        snapshot = Path(self.config.OBS) / f"ndfd_{self.wxelement.lower()}_model_sites.csv"
        try:
            snapshot.parent.mkdir(parents=True, exist_ok=True)
            frame.to_csv(snapshot, index=False)
        except OSError as error:
            print(f"WARNING: Could not write station-list snapshot {snapshot}: {error}")
        return frame

    def ensure_metadata_precip(self):
        """Compatibility entry point; precipitation uses the same full inventory."""
        return self.ensure_metadata()

    def fetch_file_list(self, start, end):
        return get_ndfd_file_list(start, end, self.config.NDFD_DICT, self.config.ELEMENT)

    def process_files(self, file_list):
        if self.config.ELEMENT == "Wind":
            speed_key, dir_key = self.config.NDFD_FILE_STRINGS[self.config.ELEMENT]
        elif self.config.ELEMENT == "Gust":
            speed_key = self.config.NDFD_FILE_STRINGS[self.config.ELEMENT][0]
            dir_key = None
        elif self.config.ELEMENT == "precip6hr":
            speed_key = self.config.NDFD_FILE_STRINGS[self.config.ELEMENT][0]
            dir_key = None
        elif self.config.ELEMENT == "maxt":
            speed_key = self.config.NDFD_FILE_STRINGS[self.config.ELEMENT][0]
            dir_key = None
        elif self.config.ELEMENT == "mint":
            speed_key = self.config.NDFD_FILE_STRINGS[self.config.ELEMENT][0]
            dir_key = None
        elif self.config.ELEMENT == "snow6hr":
            speed_key = self.config.NDFD_FILE_STRINGS[self.config.ELEMENT][0]
            dir_key = None
        elif self.config.ELEMENT == "rh":
            speed_key = self.config.NDFD_FILE_STRINGS[self.config.ELEMENT][0]
            dir_key = None
        else:
            print(f"process_files is not set up yet for {self.config.ELEMENT}.  Add to ndfd_archiver.py and archiver_config")
            sys.exit()
        speed_files = file_list[speed_key]
        dir_files = file_list.get(dir_key, [])
        return extract_ndfd_forecasts_parallel(speed_files, dir_files, self.station_df, tmp_dir=self.config.TMP)

