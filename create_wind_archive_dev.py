import os 
import time
import requests
import pandas as pd
import aiohttp
import asyncio
import wind_config as config

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

def fetch_wind_obs(base_url, stid, token, vars, start, end, outputdir):
    print(f"Fetching data for station: {stid}...")
    # API request parameters
    params = {
        "token": token,
        "stid": stid,
        "vars": vars,
        "start": start,
        "end": end,
        "obtimezone": "UTC",
        "units": "english",
        "output": "json",
    }

    # Make the API request
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()

        if "STATION" in data and len(data["STATION"]) > 0:
            station_data = data["STATION"][0]

            # Extract timestamps, wind speed, and wind direction
            timestamps = station_data["OBSERVATIONS"]["date_time"]
            wind_directions = station_data["OBSERVATIONS"].get("wind_direction_set_1", [None]*len(timestamps))
            wind_speeds = station_data["OBSERVATIONS"].get("wind_speed_set_1", [None]*len(timestamps))
            wind_gusts = station_data["OBSERVATIONS"].get("wind_gust_set_1", [None]*len(timestamps))

            # Create a DataFrame for the station
            df_station = pd.DataFrame({
                "timestamp": timestamps,
                "wind_direction": wind_directions,
                "wind_speed": wind_speeds,
                "wind_gust": wind_gusts
            })
            # converting timestamps
            df_station["timestamp"] = pd.to_datetime(df_station["timestamp"])
            # save dataframe
            outfile = f"{stid}_WindObs.csv"
            #Check to see if we already have a file and if so, append the data
            if not os.path.exists(os.path.join(outputdir, outfile)):
                # we don't have a file so create and save dataframe
                df_station.to_csv(os.path.join(outputdir, outfile), index=False)
            else:
                #open existing file and append
                archive = pd.read_csv(os.path.join(outputdir, outfile))
                update = pd.concat([archive, df_station])
                # dropping duplicate times
                final_update = update.drop_duplicates(subset=["timestamp"])
                #saving our updated file
                final_update.to_csv(os.path.join(outputdir, outfile), index=False)

            print(f"Successfully saved {outfile} to {outputdir}!")
        else:
            print(f"Failed to fetch data for station {stid} (Status Code: {response.status_code})")

        # Respect API rate limits
        time.sleep(1)

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

async def fetch_wind_obs(session, base_url, stid, token, vars, start, end, outputdir):
    """Fetch wind observations asynchronously and save to CSV."""
    print(f"Fetching data for station: {stid}...")

    params = {
        "token": token,
        "stid": stid,
        "vars": vars,
        "start": start,
        "end": end,
        "obtimezone": "UTC",
        "units": "english",
        "output": "json",
    }

    async with session.get(base_url, params=params) as response:
        if response.status == 200:
            data = await response.json()

            if "STATION" in data and len(data["STATION"]) > 0:
                station_data = data["STATION"][0]

                # Extract timestamps, wind speed, and wind direction
                timestamps = station_data["OBSERVATIONS"]["date_time"]
                wind_directions = station_data["OBSERVATIONS"].get("wind_direction_set_1", [None] * len(timestamps))
                wind_speeds = station_data["OBSERVATIONS"].get("wind_speed_set_1", [None] * len(timestamps))
                wind_gusts = station_data["OBSERVATIONS"].get("wind_gust_set_1", [None] * len(timestamps))

                # Create DataFrame
                df_station = pd.DataFrame({
                    "timestamp": timestamps,
                    "wind_direction": wind_directions,
                    "wind_speed": wind_speeds,
                    "wind_gust": wind_gusts
                })

                # Convert timestamps
                df_station["timestamp"] = pd.to_datetime(df_station["timestamp"])

                # Save DataFrame
                outfile = f"{stid}_WindObs.csv"
                file_path = os.path.join(outputdir, outfile)

                # Check if archive exists before reading
                if os.path.exists(file_path):
                    archive = pd.read_csv(file_path)
                    
                    # Ensure archive has the same columns
                    if archive.empty:
                        update = df_station
                    else:
                        update = pd.concat([archive, df_station], ignore_index=True)  # Safe concat
                    
                    # Remove duplicates properly
                    update.drop_duplicates(subset=["timestamp"], inplace=True)
                else:
                    update = df_station  # If no previous file, just use new data

                # Save the updated DataFrame
                update.to_csv(file_path, index=False)
                print(f"Successfully saved {outfile} to {outputdir}!")

async def main():
    """Main function to run all API calls asynchronously."""
    start_time = time.time()

    # Read station IDs from metadata file
    df_sites = pd.read_csv(os.path.join(config.OBS, config.METADATA))
    station_ids = df_sites["stid"].dropna().tolist()

    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_wind_obs(
                session, config.TIMESERIES_URL, stid, config.API_KEY, 
                config.WIND_VARS, config.OBS_START, config.OBS_END, config.OBS
            ) for stid in station_ids
        ]
        await asyncio.gather(*tasks)  # Run all API calls concurrently

    print(f"Data collection complete in {time.time() - start_time:.2f} seconds!")

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
        
    # grabbing data at our synoptic metadata sites
    asyncio.run(main())


    # # grabbing wind archive at our synoptic metadata sites
    # # Load station list from CSV
    # df_sites = pd.read_csv(os.path.join(config.OBS, config.METADATA))  
    # station_ids = df_sites["stid"].dropna().tolist()
    
    # # fetching obs
    # for stid in station_ids:
    #     if stid == "PANC":
    #         fetch_wind_obs(config.TIMESERIES_URL, stid, config.API_KEY, config.WIND_VARS, config.OBS_START, config.OBS_END, config.OBS)
    # print("Data collection complete!")