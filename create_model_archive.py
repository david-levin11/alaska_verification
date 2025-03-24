import os
import requests
from datetime import datetime
import numpy as np
import xarray as xr
import pandas as pd
from herbie import FastHerbie, Herbie
import wind_config as config

"""
Latest version of Herbie has issues with an Unbound Local Error when defining the CRS
for the NBM.  Refer to my comment on: https://github.com/blaylockbk/Herbie/issues/416 for a fix.
Will need to add logic to crs.py in Herbie to allow for the radius of the earth in the NBM to be 6371200
"""

def ensure_dir(directory):
    """Ensure a directory exists. If not, create it."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Creating {directory} directory")
    else:
        print(f"{directory} already exists...skipping creation step.")


def create_wind_metadata(url, token, state, networks, vars, obrange):
    # setting up synoptic params
    # Parameters for the API request
    params = {
        "token": token,
        "vars": vars,  # Variables to retrieve
        "obrange": obrange,
        "network": networks,
        "state": state,
        "output": "json"           # Output format
    }

    # Make the API request
    response = requests.get(url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
    return data

def parse_metadata(data):
    stn_dict = {"stid": [], "name": [], "latitude": [], "longitude": [], "elevation": []}
    for stn in data["STATION"]:
        stn_dict['stid'].append(stn['STID'])
        stn_dict['name'].append(stn['NAME'])
        stn_dict['latitude'].append(stn['LATITUDE'])
        stn_dict['longitude'].append(stn['LONGITUDE'])
        stn_dict['elevation'].append(stn['ELEVATION'])
    meta_df = pd.DataFrame(stn_dict)
    return meta_df


def get_model(model,dates,stns):
    global config
    products = config.HERBIE_PRODUCTS
    fcsts = config.HERBIE_FORECASTS
			
    print(f'getting {model} data with Herbie')
    all_dates=[]
    for fcst in fcsts[model]:
        rdates=dates-pd.Timedelta(fcst,unit='hours')
        if model in ['rtma_ak','urma_ak']:
            H=FastHerbie(rdates,model=model,product=products[model],
                priority=['aws'])
            H.download()
        else:
            H=FastHerbie(rdates,model=model,fxx=[fcst],
                product=products[model],priority=['aws'])
        if config.ELEMENT == "Wind":
            varlist = config.HERBIE_XARRAY_STRINGS[config.ELEMENT][model]
            if model=='nbm':
                ds1=H.xarray(varlist[0],remove_grib=False)
                ds2=H.xarray(varlist[1],remove_grib=False)
                ds3=H.xarray(varlist[2],remove_grib=False)
                ds=xr.merge([ds1,ds2,ds3])
            else:
                ds1=H.xarray(varlist[0],remove_grib=False).herbie.with_wind()
                ds2=H.xarray(varlist[1],remove_grib=False)
                ds=xr.merge([ds1,ds2])
                ds=ds.drop_vars(['u10','v10'])
        pts = ds.herbie.pick_points(stns,method='weighted',tree_name=f'{model}_tree',use_cached_tree=True)	
        if 'k' in pts.dims:
            pts=pts.drop_dims('k')
        all_dates.append(pts)

    all_dates=xr.combine_nested(all_dates,concat_dim='time')
    return all_dates


def append_to_netcdf(new_ds, output_path, time_dim="time"):
    """
    Appends new data along the time dimension to an existing NetCDF file.
    Avoids duplicate time steps using precise timestamp matching.
    """
    if os.path.exists(output_path):
        print(f"Existing NetCDF found: {output_path}. Merging new data...")

        existing_ds = xr.open_dataset(output_path, decode_timedelta=True)

        # Normalize time values for robust comparison
        existing_times = pd.to_datetime(existing_ds[time_dim].values).astype(str)
        #print(f"Existing times are: {existing_times}")
        new_times = pd.to_datetime(new_ds[time_dim].values).astype(str)
        #print(f"New times are: {new_times}")
        # Find times in new_ds that are NOT in existing_ds
        mask = ~pd.Series(new_times).isin(existing_times)
        new_unique_times = new_ds[time_dim].values[mask.values]
        #print(f"New unique times are: {new_unique_times}")
        if len(new_unique_times) == 0:
            print("No new time steps to append.")
            return

        filtered_new_ds = new_ds.sel({time_dim: new_unique_times})
        combined_ds = xr.concat([existing_ds, filtered_new_ds], dim=time_dim)
        #combined_ds = combined_ds.sortby(time_dim)

        # Save updated dataset
        combined_ds.to_netcdf(output_path, mode="w")
        print(f"Appended {len(new_unique_times)} new time steps to {output_path}")
    else:
        print(f"Saving new dataset to {output_path}")
        new_ds.to_netcdf(output_path)

def create_dataframe_fm_netcdf(ncfile, outputdir):
    global config
    with xr.open_dataset(ncfile, decode_timedelta=True) as ds:
        #ds = ds.sortby("time")
        # Loop through each point
        stid_list = ds.point_stid.values
        print(ds)
        for i, stid in enumerate(stid_list):
            if stid == 'RIXA2':
                # Extract weather element series for this point across time
                # Code for extracting variable is contingent upon model and variable and should be 
                # defined in the config file
                if config.MODEL == "nbm" and config.ELEMENT == 'Wind':
                    spd_var = config.ELEMENT_DICT[config.ELEMENT][config.MODEL][0]
                    dir_var = config.ELEMENT_DICT[config.ELEMENT][config.MODEL][1]
                    spd_series = ds[spd_var][:, i].to_pandas()
                    dir_series = ds[dir_var][:, i].to_pandas()
                    # convert to kts
                    spd_element = spd_series.values*1.94384
                    dir_element = round(dir_series,0)
                    # Add step and valid_time for each row (aligned by time)
                    df = pd.DataFrame({
                        spd_var: spd_element,
                        dir_var: dir_element,
                        'valid_time': ds.valid_time.values,
                        'step': ds.step.values
                    })
                    print(df)
                    df['step_hr'] = df['step'].dt.total_seconds() // 3600  # convert timedelta to hours
                    pivot = df.pivot(index='valid_time', columns='step_hr', values=[spd_var,dir_var])
                    # Flatten and rename columns
                    pivot.columns = [
                        f"{int(step)}hr {'Speed' if var == spd_var else 'Direction'} Forecast"
                        for var, step in pivot.columns
                    ]

                    pivot = pivot.reset_index()
                else:
                    print(f"Haven't set up config for extracting {config.ELEMENT} from {config.MODEL}.")
                    continue
                
                print(pivot)
                # saving as .csv
                outfile = f'{stid.lower()}_{config.MODEL}_forecasts.csv'
                pivot.to_csv(os.path.join(outputdir, outfile), index=False)
                print(f'Saved {outfile} in {outputdir}')
            # TO-DO:  Save these dataframes in appropriate model directory and add update/append functionality
        


model = config.MODEL

if __name__ == "__main__":
    if not os.path.exists(os.path.join(config.OBS, config.METADATA)):
        print(f"Couldn't find {config.METADATA} in {config.OBS}...will need to create the file")
        ensure_dir(config.OBS)
        # getting our metadata if we don't have it
        print(f'Creating metadata file from {config.METADATA_URL}')
        meta_json = create_wind_metadata(config.METADATA_URL, config.API_KEY, config.STATE, config.NETWORK, config.WIND_VARS, config.OBS_START)
        meta_df = parse_metadata(meta_json)
        meta_df.to_csv(os.path.join(config.OBS, config.METADATA), index=False)
        print(f"All done creating metadata.  Saved {config.METADATA} in {config.OBS}.")
    
    # grabbing wind archive at our synoptic metadata sites
    # Load station list from CSV
    df_sites = pd.read_csv(os.path.join(config.OBS, config.METADATA))  
    station_points = df_sites[["stid", "latitude", "longitude"]].dropna()
    print(station_points.head(5))
    cycle=config.HERBIE_CYCLES[model]
    #end = pd.Timestamp('now').floor("12h") - pd.Timedelta("24h")
    end = pd.Timestamp(config.OBS_END)
    print(f'End time is: {end}')
    #start=end-pd.Timedelta('7d')
    start = pd.Timestamp(config.OBS_START)
    print(f'Start time is: {start}')
    dates=pd.date_range(start,end,freq=cycle)
    print(f'Date range is: {dates}')

    # getting our archive by model
    model_data = get_model(model, dates, station_points)
    #making sure we have a model directory
    ensure_dir(config.MODEL_DIR)
    # creating a directory for our particular model if we haven't already
    ensure_dir(os.path.join(config.MODEL_DIR, model))
    raw_output_file = f"{model}_archive_latest.nc"
    raw_output_loc = os.path.join(os.path.join(config.MODEL_DIR, model), raw_output_file)
    append_to_netcdf(model_data, raw_output_loc)
    # # saving our model data as netcdf
    # model_data.to_netcdf(os.path.join(os.path.join(config.MODEL_DIR, model), raw_output_file))
    print(f"Saved raw {model} data to {os.path.join(os.path.join(config.MODEL_DIR, model), raw_output_file)}")
    
    # Now creating our dataframes for archive purposes
    create_dataframe_fm_netcdf(raw_output_loc, os.path.join(os.path.join(config.MODEL_DIR, model)))
