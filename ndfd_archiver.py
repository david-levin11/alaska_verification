from archiver_base import Archiver
import os
import sys
import pandas as pd
from pathlib import Path
from utils import get_ndfd_file_list, extract_ndfd_forecasts_parallel, create_wind_metadata, parse_metadata, create_precip_metadata

class NDFDArchiver(Archiver):
    def __init__(self, config, start=None, wxelement=None):
        super().__init__(config)
        self.start = start or config.OBS_START  # fallback to config if not passed
        self.wxelement = wxelement or config.ELEMENT
        if self.wxelement in ["precip24hr", "precip6hr", "snow6hr"]:
            self.station_df = self.ensure_metadata_precip()
        else:
            self.station_df = self.ensure_metadata()

    def ensure_metadata(self):
        print(f"Creating metadata for {self.wxelement}")
        metadata = f'alaska_{self.wxelement}_obs_metadata.csv'
        meta_path = Path(self.config.OBS) / metadata
        if self.wxelement == "Gust":
            meta_element = self.config.OBS_VARS['Wind']
        else:
            meta_element = self.config.OBS_VARS[self.wxelement]
        if not meta_path.exists():
            print(f"Creating metadata from {self.config.METADATA_URL}")
            meta_json = create_wind_metadata(
                self.config.METADATA_URL,
                self.config.API_KEY,
                self.config.STATE,
                self.config.NETWORK,
                meta_element,
                self.start  # ✅ Use dynamic start date
            )
            meta_df = parse_metadata(meta_json)
            meta_df.to_csv(meta_path, index=False)
        else:
            meta_df = pd.read_csv(meta_path)
        return meta_df

    def ensure_metadata_precip(self):
        print(f"Creating metadata for {self.wxelement}")
        metadata = f'alaska_{self.wxelement}_obs_metadata.csv'
        meta_path = Path(self.config.OBS) / metadata
        if not meta_path.exists():
            print(f"Creating metadata from {self.config.METADATA_URL}")
            meta_json = create_precip_metadata(
                self.config.METADATA_URL,
                self.config.API_KEY,
                self.config.STATE,
                self.config.NETWORK,
                self.start,  # ✅ Use dynamic start date
            )
            meta_df = parse_metadata(meta_json)
            meta_df.to_csv(meta_path, index=False)
        else:
            meta_df = pd.read_csv(meta_path)
        return meta_df

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
        else:
            print(f"process_files is not set up yet for {self.config.ELEMENT}.  Add to ndfd_archiver.py and archiver_config")
            sys.exit()
        speed_files = file_list[speed_key]
        dir_files = file_list.get(dir_key, [])
        return extract_ndfd_forecasts_parallel(speed_files, dir_files, self.station_df, tmp_dir=self.config.TMP)

