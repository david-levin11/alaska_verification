from archiver_base import Archiver
import os
import gc
import sys
import requests
from datetime import datetime
import numpy as np
import xarray as xr
import pandas as pd
from herbie import FastHerbie, Herbie
from glob import glob
import archiver_config as config
from utils import generate_model_date_range, create_wind_metadata, parse_metadata
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

class NBMArchiver(Archiver):
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
            return meta_df[["stid", "latitude", "longitude"]].dropna()
        else:
            output = pd.read_csv(meta_path)
            return output[["stid", "latitude", "longitude"]].dropna()
    
    # def download_data(self, stations, model="nbm"):
    #     products = self.config.HERBIE_PRODUCTS
    #     fcsts = self.config.HERBIE_FORECASTS[model]
    #     element = self.config.ELEMENT
    #     dates = self.get_forecast_cycle_dates(model)
    #     all_dates = []
    #     for fcst in fcsts:
    #         rdate = dates - pd.Timedelta(fcst, unit='hours')
    #         H = FastHerbie(rdate, model=model, fxx=[fcst], product=products[model], priority=['aws'])

    #         if element == "Wind":
    #             varlist = self.config.HERBIE_XARRAY_STRINGS[element][model]
    #             if model == 'nbm':
    #                 ds1 = H.xarray(varlist[0], remove_grib=True)
    #                 ds2 = H.xarray(varlist[1], remove_grib=True)
    #                 ds3 = H.xarray(varlist[2], remove_grib=True)
    #                 ds = xr.merge([ds1, ds2, ds3])
    #             else:
    #                 ds1 = H.xarray(varlist[0], remove_grib=True).herbie.with_wind()
    #                 ds2 = H.xarray(varlist[1], remove_grib=True)
    #                 ds = xr.merge([ds1, ds2]).drop_vars(['u10', 'v10'])
    #         else:
    #             raise NotImplementedError(f"Support for element {element} not yet implemented")

    #         pts = ds.herbie.pick_points(stations, method='weighted', tree_name=f'{model}_tree', use_cached_tree=True)
    #         if 'k' in pts.dims:
    #             pts = pts.drop_dims('k')
    #         all_dates.append(pts)

    #     return xr.combine_nested(all_dates, concat_dim='time')
    #     #return xr.concat(all_dates, dim="time")
    def download_data(self, stations, model="nbm"):
        products = self.config.HERBIE_PRODUCTS
        fcsts = self.config.HERBIE_FORECASTS[model]
        element = self.config.ELEMENT
        dates = self.get_forecast_cycle_dates(model)
        print(dates)
        #sys.exit()
        dfs = []

        for fcst in fcsts:
            rdate = dates - pd.Timedelta(fcst, unit='hours')
            print(f"Forecast hour is: {fcst} out of {fcsts}")
            print(f"Retroactive dates are: {rdate}")
            H = FastHerbie(rdate, model=model, fxx=[fcst], product=products[model], priority=['aws'])

            if element == "Wind":
                varlist = self.config.HERBIE_XARRAY_STRINGS[element][model]
                if model == 'nbm':
                    ds1 = H.xarray(varlist[0], remove_grib=False)
                    ds2 = H.xarray(varlist[1], remove_grib=False)
                    ds3 = H.xarray(varlist[2], remove_grib=False)
                    ds = xr.merge([ds1, ds2, ds3])
                else:
                    ds1 = H.xarray(varlist[0], remove_grib=False).herbie.with_wind()
                    ds2 = H.xarray(varlist[1], remove_grib=False)
                    ds = xr.merge([ds1, ds2]).drop_vars(['u10', 'v10'])
            else:
                raise NotImplementedError(f"Support for element {element} not yet implemented")

            pts = ds.herbie.pick_points(
                stations,
                method='weighted',
                tree_name=f'{model}_tree',
                use_cached_tree=True
            )
            if 'k' in pts.dims:
                pts = pts.drop_dims('k')

            df = self.process_files(pts)
            # Clean up xarray memory
            for var in ["ds", "ds1", "ds2", "ds3"]:
                if var in locals():
                    del locals()[var]
            gc.collect()
            # Log memory usage
            mem_mb = df.memory_usage(deep=True).sum() / 1_048_576  # bytes → MB
            print(f"📊 Processed {rdate} f{fcst}: {len(df)} rows, approx. {mem_mb:.2f} MB")
            dfs.append(df)

        return pd.concat(dfs, ignore_index=True)

    def fetch_file_list(self, start, end):
        return None  # Not needed for NBM — uses Herbie downloads

    def process_files(self, ds: xr.Dataset) -> pd.DataFrame:
        element = self.config.ELEMENT
        df = ds.to_dataframe().reset_index()
        df["valid_time"] = pd.to_datetime(df["valid_time"])
        df["time"] = pd.to_datetime(df["time"])
        rename_map = self.config.HERBIE_RENAME_MAP[element][self.config.MODEL]
        df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

        conversion_map = self.config.HERBIE_UNIT_CONVERSIONS[element][self.config.MODEL]
        for var, factor in conversion_map.items():
            if var in df.columns:
                df[var] = round((df[var] * factor),2)


        df["forecast_hour"] = (df["valid_time"] - df["time"]).dt.total_seconds() // 3600
        df["forecast_hour"] = df["forecast_hour"].astype(int)

        df = df.rename(columns={"point_stid": "station_id"})

        expected_cols = ["station_id", "valid_time", "forecast_hour"]
        for col in ["wind_speed_kt", "wind_dir_deg", "wind_gust_kt"]:
            if col in df.columns:
                expected_cols.append(col)

        return df[expected_cols]
    
    

if __name__ == "__main__":
    archiver = NBMArchiver(config)
    metadata = archiver.ensure_metadata()
    #print(metadata.head())
    ds = archiver.download_data(metadata)
    print(ds.head())
    #df = archiver.process_files(ds)
    #print(df.head())
    #print(df["valid_time"].dt.strftime("%Y-%m-%d %H:%M:%S").head(10))

## TODO Need to generate my list of rundates outside and pass it in...otherwise it regenerates different run times each time
## TODO may need to explore just abandoning Herbie and using fsspec and download_subset to accomplish what we did with NDFD