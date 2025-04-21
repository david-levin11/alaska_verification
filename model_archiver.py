from archiver_base import Archiver
from utils import create_wind_metadata, parse_metadata, get_model_file_list, extract_model_subset_parallel
from pathlib import Path
import pandas as pd
import archiver_config as config

class ModelArchiver(Archiver):
    def __init__(self, config, start=None):
        super().__init__(config)
        self.start = start or config.OBS_START  # default fallback
        self.station_df = self.ensure_metadata()

    def ensure_metadata(self):
        meta_path = Path(self.config.OBS) / self.config.METADATA
        if not meta_path.exists():
            print(f"Creating metadata from {self.config.METADATA_URL}")
            meta_json = create_wind_metadata(
                self.config.METADATA_URL,
                self.config.API_KEY,
                self.config.STATE,
                self.config.NETWORK,
                self.config.WIND_VARS,
                self.start  # ✅ Use dynamic start date
            )
            meta_df = parse_metadata(meta_json)
            meta_df.to_csv(meta_path, index=False)
        else:
            meta_df = pd.read_csv(meta_path)
        return meta_df

    def fetch_file_list(self, start, end):
        return get_model_file_list(
            start=start,
            end=end,
            fcst_hours=self.config.HERBIE_FORECASTS[self.config.MODEL],
            cycle=self.config.HERBIE_CYCLES[self.config.MODEL],
            base_url=self.config.MODEL_URLS[self.config.MODEL],
            model=self.config.MODEL,
            domain=self.config.HERBIE_DOMAIN
        )

    def process_files(self, file_urls):
        return extract_model_subset_parallel(
            file_urls=file_urls,
            station_df=self.station_df,
            search_strings=self.config.HERBIE_XARRAY_STRINGS[self.config.ELEMENT][self.config.MODEL],
            element=self.config.ELEMENT,
            model=self.config.MODEL,
            config=self.config
        )


if __name__ == "__main__":
    archiver = ModelArchiver(config)
    files = archiver.fetch_file_list("2022-01-01 01:00:00", "2022-01-03 01:00:00")
    #print(files)
    df = archiver.process_files(files)
    print(df.head())