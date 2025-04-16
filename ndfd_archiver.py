from archiver_base import Archiver
import os
import pandas as pd
from utils import get_ndfd_file_list, extract_ndfd_forecasts_parallel, create_wind_metadata, parse_metadata

class NDFDArchiver(Archiver):
    def __init__(self, config):
        super().__init__(config)
        self.station_df = self.ensure_metadata()

    def ensure_metadata(self):
        meta_path = os.path.join(self.config.OBS, self.config.METADATA)
        if not os.path.exists(meta_path):
            print(f"Creating metadata from {self.config.METADATA_URL}")
            os.makedirs(self.config.OBS, exist_ok=True)
            meta_json = create_wind_metadata(
                self.config.METADATA_URL,
                self.config.API_KEY,
                self.config.STATE,
                self.config.NETWORK,
                self.config.WIND_VARS,
                self.config.OBS_START
            )
            meta_df = parse_metadata(meta_json)
            meta_df.to_csv(meta_path, index=False)
            return meta_df
        else:
            return pd.read_csv(meta_path)

    def fetch_file_list(self, start, end):
        return get_ndfd_file_list(start, end, self.config.NDFD_DICT)

    def process_files(self, file_list):
        speed_key, dir_key, _ = self.config.NDFD_FILE_STRINGS[self.config.ELEMENT]
        speed_files = file_list[speed_key]
        dir_files = file_list.get(dir_key, [])
        return extract_ndfd_forecasts_parallel(speed_files, dir_files, self.station_df, tmp_dir=self.config.TMP)