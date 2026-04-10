import streamlit as st
import pandas as pd
import numpy as np
from data_loader import fetch_data
# 1. Import ALL your plotting functions
from plots import (
    plot_probabilistic_timeseries,
    plot_forecast_bias_bar_chart,
    plot_confusion_matrix,
    plot_threshold_reliability,
    plot_quantile_rank_histogram
    )

# Streamlit Page Configuration
st.set_page_config(page_title="Weather Verification", layout="wide")

# --- Configuration Dictionaries ---
fcst_hrs_dict = {
    'nbmqmd_exp': {"Day1": [6,12,18,24], "Day2": [30,36,42,48], "Day3": [52,58,66,72], "Day4": [84,96,108,120]}
}
percentile_col_dict = {'nbmqmd_exp': {"5": 'wind_p5', "50": 'wind_p50', "75": 'wind_p75', "95": 'wind_p95'}}

beaufort_bins = [-0.1, 1, 3, 6, 10, 16, 21, 27, 33, 40, 47, 55, 63, np.inf]
beaufort_labels = {0: "Calm", 1: "Light Air", 2: "Light Breeze", 3: "Gentle Breeze", 4: "Moderate Breeze", 
                   5: "Fresh Breeze", 6: "Strong Breeze", 7: "Near Gale", 8: "Gale", 9: "Strong Gale", 
                   10: "Storm", 11: "Violent Storm", 12: "Hurricane"}
marine_criteria_bins = [-0.1, 25, 33, 48, 63]
marine_criteria_labels = {0: "None", 1: "SCA", 2: "Gale", 3: "Storm"}

category_labels_dict = {
    "Marine Category": [marine_criteria_bins, marine_criteria_labels, list(marine_criteria_labels.keys()), "marine_cat_model", "marine_cat_obs"],
    "Beaufort Category": [beaufort_bins, beaufort_labels, list(beaufort_labels.keys()), "beaufort_cat_model", "beaufort_cat_obs"]
}

# --- Sidebar User Interface ---
st.sidebar.title("Configuration")

aws_key = st.sidebar.text_input("AWS Access Key", type="password")
aws_secret = st.sidebar.text_input("AWS Secret Key", type="password")

st.sidebar.markdown("---")
analysis_mode = st.sidebar.selectbox("Analysis Mode", ["Aggregate Verification", "Storm Specific Zoom"])

model = st.sidebar.selectbox("Model", ["nbmqmd_exp", "nbm", "hrrr", "ndfd"])
percentile = st.sidebar.selectbox("Verification Percentile (for NBM)", ["5", "10", "25", "50", "75", "90", "95"], index=4)
obs = st.sidebar.selectbox("Verification Source", ["obs", "urma"])

group_name = st.sidebar.text_input("Group Name", "Lynn Canal")
station_ids_input = st.sidebar.text_input("Station IDs (comma separated)", "EROWC, LIXA2, NKXA2, RIXA2")
station_list = [s.strip() for s in station_ids_input.split(',')]

if analysis_mode == "Aggregate Verification":
    forecast_projection = st.sidebar.selectbox("Projection", ["Day1", "Day2", "Day3", "Day4", "Day5", "Day6", "Day7"])
    criteria = st.sidebar.selectbox("Verification Criteria", ["Marine Category", "Beaufort Category"])
    start_date = st.sidebar.date_input("Start Date", pd.to_datetime("2025-10-01"))
    end_date = st.sidebar.date_input("End Date", pd.to_datetime("2026-03-17"))
    wind_threshold = st.sidebar.number_input("Reliability Wind Threshold (kts)", value=25)
    
    # --- Define the variable here so it always exists for fetch_data ---
    storm_init_time = None 
    
else:
    storm_station = st.sidebar.text_input("Storm Station", "EROWC")
    storm_init_time = st.sidebar.text_input("Storm Init Time", "2026-02-21 00:00:00")
    start_date = st.sidebar.date_input("Storm Start Date", pd.to_datetime("2026-02-21"))
    end_date = st.sidebar.date_input("Storm End Date", pd.to_datetime("2026-02-23"))
    
    # Provide placeholders for aggregate variables so they don't break either
    forecast_projection = "Day1" 
    criteria = "Beaufort Category"
    wind_threshold = 25

# --- Main App Execution ---
st.title("Alaska Wind Verification Dashboard")

if st.sidebar.button("Run Verification"):
    if not aws_key or not aws_secret:
        st.error("Please enter your AWS credentials.")
    else:
        with st.spinner("Fetching data from database..."):
            forecast_hours = fcst_hrs_dict.get(model, {}).get(forecast_projection, [])
            
            modeldf, obdf, error_msg = fetch_data(
                aws_key, aws_secret, analysis_mode, model, obs, start_date, end_date, 
                station_list, forecast_hours, storm_init_time, percentile_col_dict, percentile
            )
            
        if error_msg:
            st.error(error_msg)
        else:
            st.success("Data fetched successfully!")
            
            with st.spinner("Generating visuals..."):
                if analysis_mode == "Storm Specific Zoom":
                    if model in ["nbmqmd", "nbmqmd_exp"]:
                        modeldf = modeldf.rename(columns={"wind_speed_kt":percentile_col_dict[model][percentile]})
                        fig = plot_probabilistic_timeseries(
                            model_df=modeldf, obs_df=obdf, target_station=storm_station,
                            target_init_time=storm_init_time, plot_start=start_date, 
                            plot_end=end_date, model=model
                        )
                        st.pyplot(fig)
                    else:
                        st.warning("Plume charts require a probabilistic model.")
                
                else:
                    # --- Aggregate Verification Logic ---
                    raw_obs_col = "obs_wind_speed_kts" if "obs_wind_speed_kts" in obdf.columns else "wind_speed_kt"
                    
                    # 1. Bin the categories
                    modeldf[category_labels_dict[criteria][3]] = pd.cut(modeldf["wind_speed_kt"], bins=category_labels_dict[criteria][0], labels=category_labels_dict[criteria][2])
                    obdf[category_labels_dict[criteria][4]] = pd.cut(obdf[raw_obs_col], bins=category_labels_dict[criteria][0], labels=category_labels_dict[criteria][2])
                    
                    # 2. Merge Dataframes
                    if obs == "obs":
                        obdf.rename(columns={'stid': 'station_id'}, inplace=True, errors='ignore')
                        
                        # --- FIX: Force both timelines to nanosecond resolution ---
                        modeldf['valid_time'] = modeldf['valid_time'].astype('datetime64[ns]')
                        obdf['valid_time'] = obdf['valid_time'].astype('datetime64[ns]')
                        
                        modeldf = modeldf.sort_values(['valid_time', 'station_id', 'forecast_hour'])
                        obdf = obdf.sort_values(['valid_time', 'station_id'])
                        
                        merged = pd.merge_asof(
                                                modeldf,
                                                obdf,
                                                on='valid_time',
                                                by='station_id',
                                                direction='nearest',
                                                tolerance=pd.Timedelta("1h")  # Optional, adjust as needed
                                            )
                    else:
                        merged = pd.merge(
                            modeldf, obdf[["valid_time", "wind_speed_kt", category_labels_dict[criteria][4]]],
                            on="valid_time", suffixes=("_model", "_obs")
                        )

                    merged[category_labels_dict[criteria][4]] = pd.to_numeric(merged[category_labels_dict[criteria][4]])
                    merged[category_labels_dict[criteria][3]] = pd.to_numeric(merged[category_labels_dict[criteria][3]])

                    # 3. Generate and display plots using columns for a clean layout
                    st.subheader("Categorical Verification")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if model in ["nbmqmd", "nbmqmd_exp"]:
                            fig_bias = plot_forecast_bias_bar_chart(
                            merged_df=merged, category_col_obs=category_labels_dict[criteria][4],
                            category_col_model=category_labels_dict[criteria][3], model=model, obs=obs,
                            station_id=group_name, start_date=start_date, end_date=end_date,
                            forecast_projection=forecast_projection, category_labels=category_labels_dict[criteria][1],
                            percentile=percentile,
                            varname=percentile_col_dict[model][percentile]
                            )
                        else:
                            fig_bias = plot_forecast_bias_bar_chart(
                            merged_df=merged, category_col_obs=category_labels_dict[criteria][4],
                            category_col_model=category_labels_dict[criteria][3], model=model, obs=obs,
                            station_id=group_name, start_date=start_date, end_date=end_date,
                            forecast_projection=forecast_projection, category_labels=category_labels_dict[criteria][1]
                            )
                        st.pyplot(fig_bias)

                    with col2:
                        if model in ["nbmqmd", "nbmqmd_exp"]:
                            fig_conf = plot_confusion_matrix(
                            merged_df=merged, category_col_obs=category_labels_dict[criteria][4],
                            category_col_model=category_labels_dict[criteria][3], model=model, obs=obs,
                            station_id=group_name, start_date=start_date, end_date=end_date,
                            forecast_projection=forecast_projection, category_labels=category_labels_dict[criteria][1],
                            percentile=percentile,
                            varname=percentile_col_dict[model][percentile],
                            criteria=criteria
                            )
                        else:
                            fig_conf = plot_confusion_matrix(
                                merged_df=merged, category_col_obs=category_labels_dict[criteria][4],
                                category_col_model=category_labels_dict[criteria][3], model=model, obs=obs,
                                station_id=group_name, start_date=start_date, end_date=end_date,
                                forecast_projection=forecast_projection, category_labels=category_labels_dict[criteria][1]
                            )
                        st.pyplot(fig_conf)

                    # 4. Probabilistic Plots
                    if model in ["nbmqmd", "nbmqmd_exp"]:
                        st.markdown("---")
                        st.subheader("Probabilistic Verification")
                        col3, col4 = st.columns(2)
                        
                        if "wind_speed_kt" in merged.columns and percentile_col_dict[model][percentile] not in merged.columns:
                            merged_rel = merged.rename(columns={"wind_speed_kt":percentile_col_dict[model][percentile]})
                        else:
                            merged_rel = merged.copy()
                            
                        with col3:
                            fig_rel = plot_threshold_reliability(
                                merged_df=merged_rel, threshold=wind_threshold, model=model, obs=obs,
                                station_id=group_name, start_date=start_date, end_date=end_date
                            )
                            st.pyplot(fig_rel)
                            
                        with col4:
                            fig_rank = plot_quantile_rank_histogram(
                                merged_df=merged_rel, model=model, station_id=group_name,
                                start_date=start_date, end_date=end_date
                            )
                            st.pyplot(fig_rank)