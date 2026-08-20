from archiver_base import Archiver
from utils import create_wind_metadata, create_precip_metadata, parse_metadata, get_model_file_list, extract_model_subset_parallel
import pandas as pd
import archiver_config as config

class ModelArchiver(Archiver):
    def __init__(self, config, start=None, wxelement=None):
        super().__init__(config)
        self.start = start or config.OBS_START  # default fallback
        self.wxelement = wxelement or config.ELEMENT

        # Model extraction should use all active AK Synoptic stations,
        # regardless of whether they report the requested observed variable.
        self.station_df = self.ensure_model_metadata()

        self.station_df.to_csv(
            f"{self.config.MODEL}_{self.wxelement}_model_sites.csv",
            index=False,
        )

    def ensure_model_metadata(self):
        """
        Create/load all-active AK Synoptic station metadata for model extraction.

        This metadata is intentionally not element-specific. The model archive
        should contain point forecasts for the full station universe. The obs
        archiver can later restrict to stations that actually report each
        observed element.
        """

        metadata = "alaska_all_active_synoptic_station_metadata.csv"
        meta_path = Path(self.config.OBS) / metadata

        if not meta_path.exists():
            print(f"Creating all-active AK Synoptic metadata from {self.config.METADATA_URL}")

            meta_json = create_all_station_metadata(
                url=self.config.METADATA_URL,
                token=self.config.API_KEY,
                state=self.config.STATE,
                status="active",
                networks=None,
            )

            meta_df = parse_metadata(meta_json)
            meta_df.to_csv(meta_path, index=False)

        else:
            print(f"Loading all-active AK Synoptic metadata from {meta_path}")
            meta_df = pd.read_csv(meta_path)

        self._print_station_metadata_summary(meta_df)

        return meta_df

    def _print_station_metadata_summary(self, meta_df):
        station_col = None

        for candidate in ["stid", "station_id", "STID"]:
            if candidate in meta_df.columns:
                station_col = candidate
                break

        print(f"Model metadata rows: {len(meta_df):,}")
        print(f"Model metadata columns: {meta_df.columns.tolist()}")

        if station_col is None:
            print("WARNING: Could not find a station ID column in model metadata.")
            return

        station_ids = meta_df[station_col].astype(str).str.strip()

        print(f"Unique model extraction stations: {station_ids.nunique():,}")

        check_stations = [
            "ABYA2",
            "NDBCABYA2",
            "KTNA2",
            "COOPKTNA2",
            "PAJN",
        ]

        print("Check stations in model extraction metadata:")
        for stid in check_stations:
            print(f"  {stid}: {(station_ids == stid).any()}")

    def fetch_file_list(self, start, end):
        return get_model_file_list(
            start=start,
            end=end,
            fcst_hours=self.config.HERBIE_FORECASTS[self.config.MODEL][self.wxelement],
            cycle=self.config.HERBIE_CYCLES[self.config.MODEL],
            base_url=self.config.MODEL_URLS[self.config.MODEL],
            element=self.wxelement,
            model=self.config.MODEL,
            domain=self.config.HERBIE_DOMAIN,
        )

    def process_files(self, file_urls):
        return extract_model_subset_parallel(
            file_urls=file_urls,
            station_df=self.station_df,
            search_strings=self.config.HERBIE_XARRAY_STRINGS[self.wxelement][self.config.MODEL],
            element=self.wxelement,
            model=self.config.MODEL,
            config=self.config,
        )

if __name__ == "__main__":
    archiver = ModelArchiver(config)
    files = archiver.fetch_file_list("2025-01-30 01:00:00", "2025-01-31 01:00:00")
    print(files)
    df = archiver.process_files(files)
    print(f"Dataframe is: {df[df['station_id']=='PAJN'].head(50)}")
    df.to_csv("test.csv")