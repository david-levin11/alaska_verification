import streamlit as st
import pandas as pd
import numpy as np
from data_loader import fetch_data
from plots import (
    plot_storm_timeseries,  
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

# ==========================================
# SIDEBAR: Global Settings
# ==========================================
st.sidebar.title("Global Settings")
aws_key = st.sidebar.text_input("AWS Access Key", type="password")
aws_secret = st.sidebar.text_input("AWS Secret Key", type="password")
st.sidebar.markdown("---")

model = st.sidebar.selectbox("Model", ["nbmqmd_exp", "nbm", "hrrr", "ndfd"])
percentile = st.sidebar.selectbox("Verification Percentile", ["5", "10", "25", "50", "75", "90", "95"], index=4)
obs = st.sidebar.selectbox("Verification Source", ["obs", "urma"])

group_name = st.sidebar.text_input("Group Name", "Lynn Canal")
station_ids_input = st.sidebar.text_input("Station IDs (comma separated)", "EROWC, LIXA2, RIXA2, NKXA2")
station_list = [s.strip() for s in station_ids_input.split(',')]

# ==========================================
# MAIN APP AREA & TABS
# ==========================================
st.title("Weather Model Verification Dashboard")

# Create the two tabs
tab_agg, tab_storm = st.tabs(["📊 Aggregate Verification", "🌪️ Storm Specific Zoom"])

# ---------------------------------------------------------
# TAB 1: Aggregate Verification
# ---------------------------------------------------------
with tab_agg:
    st.header("Aggregate Statistics")
    
    # Layout inputs using columns
    c1, c2, c3 = st.columns(3)
    forecast_projection = c1.selectbox("Forecast Projection", ["Day1", "Day2", "Day3", "Day4", "Day5", "Day6", "Day7"])
    agg_start_date = c2.date_input("Start Date", pd.to_datetime("2025-10-01"))
    agg_end_date = c3.date_input("End Date", pd.to_datetime("2026-03-17"))
    
    c4, c5 = st.columns(2)
    criteria = c4.selectbox("Verification Criteria", ["Marine Category", "Beaufort Category"])
    wind_threshold = c5.number_input("Reliability Wind Threshold (kts)", value=25)
    
    st.markdown("##### Wind Direction Filter")
    use_dir_filter = st.checkbox("Filter by Observed Wind Direction?")
    if use_dir_filter:
        d1, d2 = st.columns(2)
        min_dir = d1.number_input("Minimum Direction (Degrees)", min_value=0, max_value=360, value=315)
        max_dir = d2.number_input("Maximum Direction (Degrees)", min_value=0, max_value=360, value=45)
        st.caption("Tip: If Min > Max (e.g., 315 to 45), it filters across North (360°).")
    
    # Button for Tab 1
    if st.button("Run Aggregate Verification", type="primary"):
        if not aws_key or not aws_secret:
            st.error("Please enter your AWS credentials in the sidebar.")
        else:
            with st.spinner("Fetching and processing aggregate data..."):
                forecast_hours = fcst_hrs_dict.get(model, {}).get(forecast_projection, [])
                
                modeldf, obdf, error_msg = fetch_data(
                    aws_key, aws_secret, "Aggregate Verification", model, obs, agg_start_date, agg_end_date, 
                    station_list, forecast_hours, None, percentile_col_dict, percentile
                )
                
            if error_msg:
                st.error(error_msg)
            else:
                raw_obs_col = "obs_wind_speed_kts" if "obs_wind_speed_kts" in obdf.columns else "wind_speed_kt"
                
                # 1. Bin categories
                modeldf[category_labels_dict[criteria][3]] = pd.cut(modeldf["wind_speed_kt"], bins=category_labels_dict[criteria][0], labels=category_labels_dict[criteria][2])
                obdf[category_labels_dict[criteria][4]] = pd.cut(obdf[raw_obs_col], bins=category_labels_dict[criteria][0], labels=category_labels_dict[criteria][2])
                
                # 2. Merge Dataframes
                if obs == "obs":
                    obdf.rename(columns={'stid': 'station_id'}, inplace=True, errors='ignore')
                    modeldf['valid_time'] = modeldf['valid_time'].astype('datetime64[ns]')
                    obdf['valid_time'] = obdf['valid_time'].astype('datetime64[ns]')
                    modeldf = modeldf.sort_values(['valid_time', 'station_id', 'forecast_hour'])
                    obdf = obdf.sort_values(['valid_time', 'station_id'])
                    
                    merged = pd.merge_asof(
                        modeldf, obdf, on='valid_time', by='station_id',
                        direction='nearest', tolerance=pd.Timedelta("1H")
                    )
                else:
                    merged = pd.merge(
                        modeldf, obdf[["valid_time", "wind_speed_kt", category_labels_dict[criteria][4]]],
                        on="valid_time", suffixes=("_model", "_obs")
                    )

                merged[category_labels_dict[criteria][4]] = pd.to_numeric(merged[category_labels_dict[criteria][4]])
                merged[category_labels_dict[criteria][3]] = pd.to_numeric(merged[category_labels_dict[criteria][3]])

                # 3. Apply Wind Direction Filter
                if use_dir_filter:
                    dir_col_name = "wind_dir" if "wind_dir" in merged.columns else "obs_wind_dir_deg" 
                    if dir_col_name in merged.columns:
                        if min_dir <= max_dir:
                            merged = merged[(merged[dir_col_name] >= min_dir) & (merged[dir_col_name] <= max_dir)]
                        else:
                            merged = merged[(merged[dir_col_name] >= min_dir) | (merged[dir_col_name] <= max_dir)]
                        st.info(f"Filtered to wind directions between {min_dir}° and {max_dir}°. ({len(merged)} valid pairs remain).")
                        if merged.empty:
                            st.error("No data points fell within that wind direction range!")
                            st.stop()
                    else:
                        st.warning("Could not find a wind direction column. Proceeding with all data.")

                # 4. Draw Plots
                st.subheader("Categorical Verification")
                col1, col2 = st.columns(2)
                with col1:
                    fig_bias = plot_forecast_bias_bar_chart(merged, category_labels_dict[criteria][4], category_labels_dict[criteria][3], model, obs, group_name, agg_start_date, agg_end_date, forecast_projection, category_labels_dict[criteria][1], varname=percentile_col_dict[model][percentile])
                    st.pyplot(fig_bias)
                with col2:
                    fig_conf = plot_confusion_matrix(merged, category_labels_dict[criteria][4], category_labels_dict[criteria][3], model, obs, group_name, agg_start_date, agg_end_date, forecast_projection, category_labels_dict[criteria][1],varname=percentile_col_dict[model][percentile])
                    st.pyplot(fig_conf)

                if model in ["nbmqmd", "nbmqmd_exp"]:
                    st.markdown("---")
                    st.subheader("Probabilistic Verification")
                    col3, col4 = st.columns(2)
                    merged_rel = merged.rename(columns={"wind_speed_kt":percentile_col_dict[model][percentile]}) if "wind_speed_kt" in merged.columns and percentile_col_dict[model][percentile] not in merged.columns else merged.copy()
                        
                    with col3:
                        fig_rel = plot_threshold_reliability(merged_rel, wind_threshold, model, obs, group_name, agg_start_date, agg_end_date)
                        st.pyplot(fig_rel)
                    with col4:
                        fig_rank = plot_quantile_rank_histogram(merged_rel, model, group_name, agg_start_date, agg_end_date)
                        st.pyplot(fig_rank)


# ---------------------------------------------------------
# TAB 2: Storm Specific Zoom
# ---------------------------------------------------------
with tab_storm:
    st.header("Plume Chart Generation")
    
    s1, s2 = st.columns(2)
    storm_station = s1.text_input("Target Station", "EROWC")
    storm_init_time = s2.text_input("Model Init Time (YYYY-MM-DD HH:MM:SS)", "2026-02-21 00:00:00")
    
    s3, s4 = st.columns(2)
    storm_start = s3.date_input("Plot Start Date", pd.to_datetime("2026-02-21"))
    storm_end = s4.date_input("Plot End Date", pd.to_datetime("2026-02-24"))
    
    # Button for Tab 2
    if st.button("Generate Storm Timeseries", type="primary"):
        if not aws_key or not aws_secret:
            st.error("Please enter your AWS credentials in the sidebar.")
        else:
            with st.spinner(f"Fetching data for {storm_station}..."):
                modeldf, obdf, error_msg = fetch_data(
                    aws_key, aws_secret, "Storm Specific Zoom", model, obs, storm_start, storm_end, 
                    [storm_station.strip()], [], storm_init_time, percentile_col_dict, percentile
                )
                
            if error_msg:
                st.error(error_msg)
            else:
                # Call our updated function name
                fig = plot_storm_timeseries(
                    model_df=modeldf, obs_df=obdf, target_station=storm_station.strip(),
                    target_init_time=storm_init_time, plot_start=storm_start, 
                    plot_end=storm_end, model=model
                )
                
                # Check if fig is None (which happens if model_plot_df was empty)
                if fig:
                    st.pyplot(fig)
                else:
                    st.warning("Could not generate chart due to missing data.")