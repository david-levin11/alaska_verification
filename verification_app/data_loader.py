import duckdb
import pandas as pd
from datetime import datetime

def generate_monthly_paths(prefix, model, start_date, end_date):
    """Generates a list of S3 parquet paths based on the date range."""
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    paths = []
    
    current = start.replace(day=1)
    while current <= end:
        year_month = current.strftime("%Y_%m")
        path = f"{prefix}/{year_month}_{model}_wind_archive.parquet"
        paths.append(path)
        # Move to first of next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return paths

def fetch_data(aws_key, aws_secret, analysis_mode, model, obs, start_date, end_date, 
               station_list, forecast_hours, storm_init_time, percentile_col_dict, percentile):
    """Connects to DuckDB, queries S3, and returns the raw model and observation dataframes."""
    
    # 1. Optimize query parameters based on mode
    query_start = start_date
    query_end = end_date
    query_stations = station_list

    con = duckdb.connect()
    con.execute("SET s3_region='us-east-2'")
    con.execute(f"SET s3_access_key_id = '{aws_key}'")
    con.execute(f"SET s3_secret_access_key = '{aws_secret}'")

    modelfiles = generate_monthly_paths(f"s3://alaska-verification/{model}", model, query_start, query_end)
    obfiles = generate_monthly_paths(f"s3://alaska-verification/{obs}", obs, query_start, query_end)

    station_placeholders = ", ".join(["?"] * len(query_stations))

    # 2. Dynamic SQL for Models
    if analysis_mode == "Storm Specific Zoom":
        if model == "ndfd":
            # NDFD doesn't have init_time natively
            modelquery = f"""
            SELECT * FROM read_parquet({modelfiles})
            WHERE station_id IN ({station_placeholders})
              AND valid_time BETWEEN ? AND ?
            """
            model_params = query_stations + [query_start, query_end]
        else:
            modelquery = f"""
            SELECT * FROM read_parquet({modelfiles})
            WHERE station_id IN ({station_placeholders})
              AND init_time = ? AND valid_time BETWEEN ? AND ?
            """
            model_params = query_stations + [storm_init_time, query_start, query_end]
    else:
        hour_str = ", ".join(str(h) for h in forecast_hours)
        modelquery = f"""
        SELECT * FROM read_parquet({modelfiles})
        WHERE station_id IN ({station_placeholders})
          AND forecast_hour IN ({hour_str}) AND valid_time BETWEEN ? AND ?
        """
        model_params = query_stations + [query_start, query_end]

    try:
        modeldf = con.execute(modelquery, model_params).df()
    except Exception as e:
        error_msg = str(e)
        if "404" in error_msg:
            return pd.DataFrame(), pd.DataFrame(), f"Data Missing: Could not find {model.upper()} data in your S3 bucket for the selected dates."
        return pd.DataFrame(), pd.DataFrame(), f"Database error: {error_msg}"

    if modeldf.empty:
        return pd.DataFrame(), pd.DataFrame(), "No model data found for these parameters."

    # --- Reconstruct init_time for NDFD ---
    if model == "ndfd":
        modeldf['init_time'] = modeldf['valid_time'] - pd.to_timedelta(modeldf['forecast_hour'], unit='h')
        
        if analysis_mode == "Storm Specific Zoom":
            modeldf = modeldf[modeldf['init_time'] == pd.to_datetime(storm_init_time)]
            if modeldf.empty:
                 return pd.DataFrame(), pd.DataFrame(), f"No NDFD data matched the init time: {storm_init_time}."

    # Rename column if we are running Aggregate stats
    if model in ["nbmqmd_exp", "nbmqmd"] and analysis_mode == "Aggregate Verification":
        modeldf = modeldf.rename(columns={percentile_col_dict[model][percentile]:"wind_speed_kt"})

    # 3. Query Observations
    station_select = "station_id" if obs == "urma" else "stid"
    obquery = f"""
    SELECT * FROM read_parquet({obfiles})
    WHERE ({station_select}) IN ({station_placeholders})
      AND valid_time BETWEEN ? AND ?
    """
    ob_params = query_stations + [query_start, query_end]
    
    try:
        obdf = con.execute(obquery, ob_params).df()
    except Exception as e:
        return modeldf, pd.DataFrame(), f"Observation Database error: {e}"

    # 4. Standardize datetimes to be tz-naive
    for col in ['valid_time', 'init_time']:
        if col in modeldf.columns:
            modeldf[col] = pd.to_datetime(modeldf[col]).dt.tz_localize(None)
            
    if 'valid_time' in obdf.columns:
        obdf['valid_time'] = pd.to_datetime(obdf['valid_time']).dt.tz_localize(None)

    return modeldf, obdf, None