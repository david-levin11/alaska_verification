import os
import yaml
import re
from datetime import datetime, date
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
import streamlit as st

import fsspec
import s3fs
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow as pa
import plotly.express as px


import os
import yaml

CONFIG_PATHS = [
    os.environ.get("STREAMLIT_APP_CONFIG", ""),       # optional env override
    ".streamlit/app_config.yaml",                     # project local
    "/etc/verification_app/config.yaml",             # system-wide (optional)
]

def load_config() -> dict:
    for p in CONFIG_PATHS:
        if p and os.path.exists(p):
            with open(p, "r") as f:
                return yaml.safe_load(f)
    # Fallback minimal default (so app still runs if YAML missing)
    return

CFG = load_config()

# -----------------------------
# Helpers
# -----------------------------
FILENAME_REGEX = re.compile(
    r"(?P<year>\d{4})_(?P<month>\d{2})_(?P<model>[^_]+)_(?P<element>[^_]+)_archive\.parquet$"
)

def resolve_dataset_io(dataset_key: str, role: str) -> tuple:
    """
    role: 'forecast' or 'obs' (for backend_default selection)
    Returns (fs_protocol, root, s3_anon, aws_profile)
    """
    backend = CFG["backend_default"][role].strip().lower()
    ds = CFG["storage"][dataset_key]
    if backend == "s3":
        return "s3", ds["s3_root"].rstrip("/"), bool(ds.get("s3_anon", False)), ds.get("aws_profile") or None
    else:
        return "file", ds["local_root"].rstrip("/"), False, None

def tokenize(model_token: str) -> str:
    case = (CFG["filename"].get("case") or "lower").lower()
    if case == "lower":
        return model_token.lower()
    if case == "upper":
        return model_token.upper()
    return model_token  # asis

def get_schema(element: str, dataset_key: str) -> dict:
    try:
        return CFG["schema"][element][dataset_key]
    except KeyError:
        return {}

def filename_for(yyyy: int, mm: int, model_token: str, element: str) -> str:
    pat = CFG["filename"]["pattern"]
    return pat.replace("{YYYY}", f"{yyyy:04d}") \
              .replace("{MM}",   f"{mm:02d}")    \
              .replace("{model}", tokenize(model_token)) \
              .replace("{element}", tokenize(element))


def month_range(start: date, end: date) -> List[Tuple[int, int]]:
    # Inclusive list of (YYYY, MM)
    months = []
    y, m = start.year, start.month
    while (y < end.year) or (y == end.year and m <= end.month):
        months.append((y, m))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return months

def schema_block(element: str, dataset_key: str) -> dict:
    return CFG.get("schema", {}).get(element, {}).get(dataset_key, {}) or {}

def schema_value_keys(element: str, dataset_key: str) -> list[str]:
    """
    Return all value keys for this element/dataset from YAML.
    Example (wind): ['speed_value','dir_value','gust_value']
    Fallback: ['value'] if present.
    """
    s = schema_block(element, dataset_key)
    keys = [k for k in s.keys() if k.endswith("_value")]
    if not keys and "value" in s:
        keys = ["value"]
    return keys

def metric_name_from_key(value_key: str) -> str:
    # 'speed_value' -> 'speed', 'dir_value' -> 'dir', 'gust_value' -> 'gust', 'value' -> 'value'
    return value_key[:-6] if value_key.endswith("_value") else value_key

def needed_cols_from_schema(s: dict, value_keys: list[str]) -> list[str]:
    cols = [s["time"], s["station"]]
    for vk in value_keys:
        if vk in s:
            cols.append(s[vk])
    return list(dict.fromkeys(cols))  # unique, preserve order

def rename_to_internal(df: pd.DataFrame, s: dict, value_keys: list[str], role: str) -> pd.DataFrame:
    """
    role: 'fcst' or 'obs'
    Produces names like: __time_model__, __station__, __fcst_speed__, __obs_dir__, etc.
    """
    rename_map = {
        s["time"]:    "__time_model__" if role == "fcst" else "__time_obs__",
        s["station"]: "__station__",
    }
    for vk in value_keys:
        if vk in s:
            src = s[vk]
            metric = metric_name_from_key(vk)
            rename_map[src] = f"__{role}_{metric}__"
    return df.rename(columns=rename_map, errors="ignore")


@st.cache_data(show_spinner=False)
def get_files(
    root: str,
    use_s3: bool,
    years_months: List[Tuple[int, int]],
    model: str,
    element: str,
    s3_anon: bool,
    aws_profile: Optional[str],
) -> Tuple[List[str], str]:
    """
    Returns a list of parquet file paths under root matching the naming template:
    YYYY_MM_{model}_{element}_archive.parquet
    Also returns the fsspec protocol ("file" or "s3").
    """
    if use_s3:
        # Configure S3 filesystem
        session_kwargs = {}
        if aws_profile:
            session_kwargs["profile"] = aws_profile
        fs = s3fs.S3FileSystem(anon=s3_anon, **({"session_kwargs": session_kwargs} if session_kwargs else {}))
        protocol = "s3"
    else:
        fs = fsspec.filesystem("file")
        protocol = "file"

    matches = []
    for (yy, mm) in years_months:
        fname = filename_for(yy, mm, model, element)  # model here should already be the model_token
        path = f"{root.rstrip('/')}/{fname}"
        if fs.exists(path):
            matches.append(path)
    return matches, protocol

def _infer_time_col(cols: List[str]) -> Optional[str]:
    for c in cols:
        lc = c.lower()
        if "valid_time" in lc or "valid_datetime" in lc or lc in ("valid", "time", "datetime"):
            return c
    return None

def _infer_station_col(cols: List[str]) -> Optional[str]:
    for c in cols:
        lc = c.lower()
        if lc in ("station_id", "stid", "station", "id"):
            return c
    return None

@st.cache_data(show_spinner=True)
def read_dataset(
    files: List[str],
    protocol: str,
    columns: Optional[List[str]] = None,
    filters: Optional[List[Tuple[str, str, object]]] = None,
    s3_anon: bool = False,
    aws_profile: Optional[str] = None,
) -> pd.DataFrame:
    """
    Read multiple parquet files into a single DataFrame via pyarrow.dataset
    using fsspec filesystem (S3 or local).
    Optional 'filters' apply row-level filtering if columns exist (pyarrow-level predicate pushdown).
    """
    if not files:
        return pd.DataFrame()

    if protocol == "s3":
        session_kwargs = {}
        if aws_profile:
            session_kwargs["profile"] = aws_profile
        fs = s3fs.S3FileSystem(anon=s3_anon, **({"session_kwargs": session_kwargs} if session_kwargs else {}))
    else:
        fs = fsspec.filesystem("file")

    dset = ds.dataset(files, filesystem=fs, format="parquet")
    # If user-specified columns include fields not present, pyarrow will error; so we guard a bit.
    cols = columns or None
    if cols:
        present = [c for c in cols if c in dset.schema.names]
        if present:
            table = dset.to_table(columns=present, filter=_build_arrow_filter(filters, dset.schema))
        else:
            table = dset.to_table(filter=_build_arrow_filter(filters, dset.schema))
    else:
        table = dset.to_table(filter=_build_arrow_filter(filters, dset.schema))

    return table.to_pandas()  # let pandas choose native dtypes

def _build_arrow_filter(filters, schema) -> Optional[ds.Expression]:
    if not filters:
        return None
    expr = None
    for col, op, val in filters:
        if col not in schema.names:
            # Skip unknown columns
            continue
        sub = None
        if op == "==":
            sub = ds.field(col) == val
        elif op == ">=":
            sub = ds.field(col) >= val
        elif op == "<=":
            sub = ds.field(col) <= val
        elif op == ">":
            sub = ds.field(col) > val
        elif op == "<":
            sub = ds.field(col) < val
        elif op == "in":
            sub = ds.field(col).isin(val)
        if sub is not None:
            expr = sub if expr is None else (expr & sub)
    return expr


def read_parquet_bundle(
    root, use_s3, years_months, model, element, s3_anon=False, aws_profile=None,
    columns=None, filters=None
) -> pd.DataFrame:
    files, protocol = get_files(
        root=root, use_s3=use_s3, years_months=years_months,
        model=model, element=element, s3_anon=s3_anon, aws_profile=aws_profile
    )
    if not files:
        return pd.DataFrame(), protocol, files
    df = read_dataset(
        files=files, protocol=protocol, columns=columns, filters=filters,
        s3_anon=s3_anon, aws_profile=aws_profile
    )
    return df, protocol, files

def align_obs_to_model_asof(
    model_df: pd.DataFrame,
    obs_df: pd.DataFrame,
    *,
    station_col_model: str,
    station_col_obs: str,
    time_col_model: str,
    time_col_obs: str,
    tolerance_minutes: int = 30
) -> pd.DataFrame:
    """Align obs to model using nearest time per-station with a tolerance."""
    m = model_df.copy()
    o = obs_df.copy()

    # ensure UTC-aware datetimes
    m[time_col_model] = pd.to_datetime(m[time_col_model], errors="coerce", utc=True)
    o[time_col_obs]   = pd.to_datetime(o[time_col_obs], errors="coerce", utc=True)
    m = m.dropna(subset=[time_col_model, station_col_model])
    o = o.dropna(subset=[time_col_obs,   station_col_obs])

    # sort for merge_asof
    m = m.sort_values([station_col_model, time_col_model])
    o = o.sort_values([station_col_obs,   time_col_obs])

    # rename obs station/time so we can ‘by’ on the same name
    o = o.rename(columns={station_col_obs: "_station_obs_by", time_col_obs: "_time_obs_key"})
    m = m.rename(columns={station_col_model: "_station_obs_by", time_col_model: "_time_model_key"})

    aligned = pd.merge_asof(
        m, o,
        left_on="_time_model_key",
        right_on="_time_obs_key",
        by="_station_obs_by",
        direction="nearest",
        tolerance=pd.Timedelta(minutes=tolerance_minutes)
    )
    return aligned


def safe_to_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", utc=True)

def compute_basic_metrics(df: pd.DataFrame, fcst_col: str, obs_col: str) -> dict:
    sub = df[[fcst_col, obs_col]].dropna()
    if sub.empty:
        return {"count": 0, "bias": np.nan, "mae": np.nan, "rmse": np.nan}
    err = sub[fcst_col] - sub[obs_col]
    bias = float(err.mean())
    mae = float(err.abs().mean())
    rmse = float(np.sqrt((err**2).mean()))
    return {"count": int(len(sub)), "bias": bias, "mae": mae, "rmse": rmse}


# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="Point Forecast Verification Viewer", layout="wide")

st.title("Point Forecast Verification Viewer")
st.caption("Works with monthly Parquet archives stored locally or on S3, using your YYYY_MM_model_element naming.")

with st.sidebar:
    st.header("Selection")
    # Models available come from CFG['storage'] keys
    available_models = sorted([k for k in CFG["storage"].keys() if k not in ("urma","obs")]) or ["nbm"]
    model = st.selectbox("Forecast model", available_models, index=available_models.index("nbm") if "nbm" in available_models else 0)

    # Elements come from CFG['schema'] keys
    available_elements = sorted(CFG["schema"].keys())
    element = st.selectbox("Element", available_elements, index=(available_elements.index("wind") if "wind" in available_elements else 0))

    # Pick obs based on mapping (allow override if you want)
    default_obs_key = CFG["obs_by_model"].get(model, "urma")
    obs_key = default_obs_key  # or add a selectbox to override

    station_id = st.text_input("Station ID", "PAJN")

    start_date = st.date_input("Start date (UTC)", value=date(2025,1,1))
    end_date   = st.date_input("End date (UTC)",   value=date(2025,1,31))

    go = st.button("Run Query", type="primary")


# -----------------------------
# Main logic
# -----------------------------
if go:
    if start_date > end_date:
        st.error("Start date must be <= End date.")
        st.stop()

    ym = month_range(start_date, end_date)
    start_ts = pd.Timestamp(start_date, tz="UTC")
    end_ts   = pd.Timestamp(end_date,   tz="UTC") + pd.Timedelta(days=1)

    tol_min = int(CFG["alignment"].get("tolerance_minutes", 30))

    # ---- Resolve IO for forecast and obs from config ----
    fcst_protocol, fcst_root, fcst_s3_anon, fcst_profile = resolve_dataset_io(model, role="forecast")
    obs_protocol,  obs_root,  obs_s3_anon,  obs_profile  = resolve_dataset_io(obs_key, role="obs")

    # ---- Forecast schema & columns ----
    # Try model-specific schema first, then generic 'forecast'
    fcst_schema = schema_block(element, model) or schema_block(element, "forecast")
    if not fcst_schema:
        st.error(f"No forecast schema in config for element='{element}' (model '{model}' or 'forecast').")
        st.stop()

    fcst_val_keys = schema_value_keys(element, model) or schema_value_keys(element, "forecast")
    if not fcst_val_keys:
        st.error(f"No value keys found in schema for element='{element}' forecast side.")
        st.stop()

    needed_fcst = needed_cols_from_schema(fcst_schema, fcst_val_keys)

    # ---- Read forecast files ----
    fcst_model_token = CFG["storage"][model]["model_token"]
    files_fcst, _ = get_files(fcst_root, fcst_protocol == "s3", ym, fcst_model_token, element, fcst_s3_anon, fcst_profile)
    if not files_fcst:
        st.warning("No forecast Parquet files found for selection.")
        st.stop()

    arrow_filters_fcst = [(fcst_schema["station"], "==", station_id)]
    df_fcst = read_dataset(files_fcst, fcst_protocol, columns=needed_fcst, filters=arrow_filters_fcst,
                        s3_anon=fcst_s3_anon, aws_profile=fcst_profile)
    if df_fcst.empty:
        st.warning("Forecast dataset returned no rows for station.")
        st.stop()

    # ---- Standardize forecast names & time window ----
    df_fcst = rename_to_internal(df_fcst, fcst_schema, fcst_val_keys, role="fcst")
    df_fcst["__time_model__"] = pd.to_datetime(df_fcst["__time_model__"], errors="coerce", utc=True)
    df_fcst = df_fcst.dropna(subset=["__time_model__", "__station__"])
    df_fcst = df_fcst[(df_fcst["__time_model__"] >= start_ts) & (df_fcst["__time_model__"] < end_ts)]
    df_fcst = df_fcst.sort_values(["__station__", "__time_model__"])

    # ---- Read obs bundle ----
    # ---- Obs schema & columns ----
    obs_schema = schema_block(element, obs_key)
    if not obs_schema:
        st.error(f"No obs schema in config for element='{element}', dataset='{obs_key}'.")
        st.stop()

    obs_val_keys = schema_value_keys(element, obs_key)
    if not obs_val_keys:
        st.error(f"No value keys found in schema for element='{element}' obs side '{obs_key}'.")
        st.stop()

    needed_obs = needed_cols_from_schema(obs_schema, obs_val_keys)

    # ---- Read obs files ----
    obs_model_token = CFG["storage"][obs_key]["model_token"]
    files_obs, _ = get_files(obs_root, obs_protocol == "s3", ym, obs_model_token, element, obs_s3_anon, obs_profile)
    if not files_obs:
        st.warning("No observation Parquet files found for selection.")
        st.stop()

    arrow_filters_obs = [(obs_schema["station"], "==", station_id)]
    df_obs = read_dataset(files_obs, obs_protocol, columns=needed_obs, filters=arrow_filters_obs,
                        s3_anon=obs_s3_anon, aws_profile=obs_profile)
    if df_obs.empty:
        st.warning("Obs dataset returned no rows for station.")
        st.stop()

    # ---- Standardize obs names & time window ----
    df_obs = rename_to_internal(df_obs, obs_schema, obs_val_keys, role="obs")
    df_obs["__time_obs__"] = pd.to_datetime(df_obs["__time_obs__"], errors="coerce", utc=True)
    df_obs = df_obs.dropna(subset=["__time_obs__", "__station__"])
    df_obs = df_obs[(df_obs["__time_obs__"] >= start_ts) & (df_obs["__time_obs__"] < end_ts)]
    df_obs = df_obs.sort_values(["__station__", "__time_obs__"])


    # ---- Align obs → model (per-station nearest) ----
    aligned = pd.merge_asof(
        df_fcst, df_obs,
        left_on="__time_model__",
        right_on="__time_obs__",
        by="__station__",
        direction="nearest",
        tolerance=pd.Timedelta(minutes=int(CFG["alignment"].get("tolerance_minutes", 30))),
    )

    # ---- Optional units conversion (speed/gust only), if configured ----
    units_cfg = CFG.get("units", {}).get(element, {})
    if units_cfg and units_cfg.get("convert_obs_to_forecast", False):
        obs_u  = (units_cfg.get("obs", "") or "").lower()
        fcst_u = (units_cfg.get("forecast", "") or "").lower()
        if obs_u in ("mps", "m/s") and fcst_u == "kt":
            for col in aligned.columns:
                if col.startswith("__obs_") and (col.endswith("_speed__") or col.endswith("_gust__")):
                    aligned[col] = aligned[col] * 1.94384

    # ---- Produce a single wide DF with all available metrics ----
    rename_map = {
        "__time_model__": "valid_time",
        "__station__": "station_id",
    }
    # Bring across every __fcst_*__ and __obs_*__
    for c in aligned.columns:
        if c.startswith("__fcst_") and c.endswith("__"):
            metric = c[len("__fcst_"):-len("__")]
            rename_map[c] = f"forecast_{metric}"
        elif c.startswith("__obs_") and c.endswith("__"):
            metric = c[len("__obs_"):-len("__")]
            rename_map[c] = f"observed_{metric}"

    df = aligned.rename(columns=rename_map, errors="ignore")

    # Keep only columns we care about (valid_time, station_id, and all forecast_/observed_*)
    base_cols = ["valid_time", "station_id"]
    value_cols = [c for c in df.columns if c.startswith("forecast_") or c.startswith("observed_")]
    df = df[base_cols + value_cols].dropna(subset=["valid_time"]).sort_values("valid_time").reset_index(drop=True)
    print(df.head())
    # Optionally show what we have
    st.caption(f"Available metrics: {sorted(set([c.split('_',1)[1] for c in value_cols]))}")


    # ---- Metrics & plot ----
    # ---- figure out which metrics are available on BOTH sides ----
    fc_metrics  = {c[len("forecast_"):] for c in df.columns if c.startswith("forecast_")}
    obs_metrics  = {c[len("observed_"):] for c in df.columns if c.startswith("observed_")}
    common_metrics = sorted(fc_metrics & obs_metrics)

    if not common_metrics:
        st.error(f"No matching forecast/obs metric pairs found. "
                f"Forecast has: {sorted(fc_metrics)} | Obs has: {sorted(obs_metrics)}")
        st.stop()

    # default from YAML if present, otherwise first common
    default_metric = (CFG.get("schema", {})
                        .get(element, {})
                        .get("default_metric", None))
    if default_metric not in common_metrics:
        default_metric = common_metrics[0]

    # Optional selector (comment this out if you want zero UI)
    metric_choice = st.selectbox("Metric", common_metrics,
                                index=common_metrics.index(default_metric))

    fc_col = f"forecast_{metric_choice}"
    ob_col = f"observed_{metric_choice}"

    # ensure numeric where applicable (direction OK as float)
    for c in (fc_col, ob_col):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # ---- metrics (direction uses circular error) ----
    # if element == "wind" and metric_choice == "dir":
    #     metrics = compute_dir_metrics(df, fc_col, ob_col)
    # else:
    metrics = compute_basic_metrics(df, fc_col, ob_col)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Pairs", f"{metrics['count']:,}")
    c2.metric("Bias (fcst-obs)", f"{metrics['bias']:.2f}")
    c3.metric("MAE", f"{metrics['mae']:.2f}")
    c4.metric("RMSE", f"{metrics['rmse']:.2f}")

    # ---- plot the chosen metric ----
    st.subheader(f"Time Series — {metric_choice}")
    plot_df = (
        df[["valid_time", fc_col, ob_col]]
        .rename(columns={"valid_time": "t", fc_col: "forecast", ob_col: "observed"})
        .melt("t", var_name="series", value_name="value")
    )
    fig = px.line(plot_df, x="t", y="value", color="series", render_mode="webgl")
    fig.update_layout(margin=dict(l=10, r=10, t=40, b=10),
                    title=f"{model.upper()} vs {obs_key.upper()} {element.upper()} — {station_id}")
    st.plotly_chart(fig, use_container_width=True)

    
    with st.expander("Show aligned sample"):
        st.dataframe(df.head(200), use_container_width=True)
