"""Station-level REFS assembly and statistics, independent of GRIB/download code."""
from refs_archive_io import read_source
import numpy as np
import pandas as pd

PERTURBED_MEMBERS = tuple(f"m{i:03d}" for i in range(1, 6))
# Explicit analysis recipe, not a claim of reproducing every official REFS product.
# Override for a different feed/version or a fixed-membership experiment.
DEFAULT_MAX_HOURS = {"rrfs": 60, "rrfsens": 60, "hrrr": 48}


def add_interval_precip_from_total(df, *, total_col="precip_accum",
                                   out_col="precip_6h", hours=6,
                                   group_cols=("station_id", "init_time"),
                                   clip_negative_to_zero=True):
    """Difference cumulative totals within a station, source run, and member.

    The f006 total is already a 0-6 h accumulation and needs no f000 record.
    Earlier leads and missing endpoints remain NaN. Inputs must be totals since
    initialization. Negative differences are clipped only when requested.
    """
    result = df.copy()
    if total_col not in result:
        raise KeyError(total_col)
    groups = list(group_cols)
    if "member_id" in result and "member_id" not in groups:
        groups.append("member_id")
    result["valid_time"] = pd.to_datetime(result["valid_time"])
    result["init_time"] = pd.to_datetime(result["init_time"])
    keys = groups + ["valid_time"]
    if result.duplicated(keys).any():
        raise ValueError("Duplicate cumulative records within a source run/member")
    previous = result[keys + [total_col]].copy()
    previous["valid_time"] += pd.Timedelta(hours=hours)
    previous = previous.rename(columns={total_col: "_previous_total"})
    result = result.merge(previous, on=keys, how="left", validate="one_to_one")
    result[out_col] = result[total_col] - result.pop("_previous_total")
    lead = (result["valid_time"] - result["init_time"]) / pd.Timedelta(hours=1)
    result.loc[lead == hours, out_col] = result.loc[lead == hours, total_col]
    result.loc[lead < hours, out_col] = np.nan
    if clip_negative_to_zero:
        result[out_col] = result[out_col].clip(lower=0)  # preserves NaN
    result[out_col] = result[out_col].round(2)
    return result


def _utc(value):
    return pd.to_datetime(value, utc=True)


def assemble_refs(archive_root, element, ensemble_init_time, valid_time, *,
                  station_ids=None, max_forecast_hours=None, source_cache=None, aws_profile=None):
    """Read source-month Parquets and return a row for every expected member.

    archive_root is the directory containing hrrr/, rrfs/, and rrfsens/.
    Actual init_time/forecast_hour are retained; ensemble_* fields describe the
    requested cycle. Missing files/rows yield NaN member values, not zeros.
    Pass station_ids to also report stations missing from every source file.
    Source availability limits are configurable and are applied before reading.
    """
    cycle, valid = _utc(ensemble_init_time), _utc(valid_time)
    lead = (valid - cycle) / pd.Timedelta(hours=1)
    if cycle.hour % 6 or cycle.minute or cycle.second or cycle.microsecond:
        raise ValueError("REFS cycle must be 00/06/12/18 UTC")
    if not 0 <= lead <= 60 or lead != int(lead):
        raise ValueError("REFS forecast hour must be an integer from 0 through 60")
    limits = dict(DEFAULT_MAX_HOURS)
    if max_forecast_hours is not None:
        limits.update(max_forecast_hours)
    recipe = []
    for model, members in (("rrfs", ("control",)),
                           ("rrfsens", PERTURBED_MEMBERS), ("hrrr", ("control",))):
        for lag in (0, 6):
            if lead + lag <= limits[model]:
                for member in members:
                    recipe.append((model, member, lag, cycle - pd.Timedelta(hours=lag)))
    cache = {} if source_cache is None else source_cache
    selected = []
    stations = set(str(s) for s in station_ids) if station_ids is not None else set()
    for model, member, lag, source_init in recipe:
        path = (str(archive_root), model, element.lower(), source_init.strftime("%Y_%m"), aws_profile)
        if path not in cache:
            frame = read_source(archive_root, model, element, source_init.strftime("%Y_%m"), aws_profile)
            if not frame.empty:
                for col in ("init_time", "valid_time"):
                    frame[col] = pd.to_datetime(frame[col], utc=True)
                frame["station_id"] = frame["station_id"].astype(str)
                if model == "rrfsens" and "member_id" not in frame:
                    raise ValueError(f"Missing member_id in {path}")
                # Also support older cumulative-only archives without modifying them.
                if element.lower() in ("precip6hr", "snow6hr"):
                    total, interval = (("precip_accum", "precip_6h") if element.lower() == "precip6hr"
                                       else ("snow_accum", "snow_6h"))
                    if total in frame:
                        frame = add_interval_precip_from_total(frame, total_col=total, out_col=interval)
            cache[path] = frame
        frame = cache[path]
        if not frame.empty:
            mask = (frame.init_time == source_init) & (frame.valid_time == valid)
            if model == "rrfsens":
                mask &= frame.member_id == member
            frame = frame.loc[mask].copy()
            if station_ids is not None:
                frame = frame[frame.station_id.isin(stations)]
            else:
                stations.update(frame.station_id)
            if frame.duplicated("station_id").any():
                raise ValueError(f"Duplicate source records in {path} for {source_init}, {member}")
            if not frame.empty and not (frame.forecast_hour == lead + lag).all():
                raise ValueError(f"Inconsistent source forecast_hour in {path}")
        selected.append((model, member, lag, source_init, frame))
    if not stations:
        return pd.DataFrame()
    output = []
    for model, member, lag, source_init, frame in selected:
        rows = pd.DataFrame({"station_id": sorted(stations)})
        if not frame.empty:
            rows = rows.merge(frame, on="station_id", how="left", validate="one_to_one", indicator=True)
            rows["source_available"] = rows.pop("_merge").eq("both")
        else:
            rows["source_available"] = False
        rows["source_model"] = model
        rows["member_id"] = member
        rows["init_time"] = source_init
        rows["forecast_hour"] = int(lead + lag)
        rows["valid_time"] = valid
        rows["ensemble_init_time"] = cycle
        rows["ensemble_forecast_hour"] = int(lead)
        rows["lag_hours"] = lag
        rows["ensemble_member_id"] = f"{model}:{member}:lag{lag:02d}"
        output.append(rows)
    return pd.concat(output, ignore_index=True) if output else pd.DataFrame()


def summarize_refs(members, value_column, *, percentiles=(5, 10, 25, 50, 75, 90, 95),
                   thresholds=(), require_complete=True):
    """Equal-weight member statistics; probabilities use strict >, on a 0-1 scale.

    Default: suppress statistics if any expected value is missing. Set
    require_complete=False explicitly to use available members. Percentiles use
    NumPy's linear method and spread is population standard deviation (ddof=0).
    Counts are variable-specific; missing precipitation endpoints are unavailable.
    """
    if members.empty:
        return pd.DataFrame()
    if value_column not in members and members.source_available.any():
        raise ValueError(f"Value column {value_column!r} is absent from the available source data")
    if any(not 0 <= p <= 100 for p in percentiles):
        raise ValueError("Percentiles must be between 0 and 100")
    keys = ["station_id", "ensemble_init_time", "valid_time", "ensemble_forecast_hour"]
    if members.duplicated(keys + ["ensemble_member_id"]).any():
        raise ValueError("Duplicate ensemble members")
    output = []
    for group, frame in members.groupby(keys, dropna=False):
        values = pd.to_numeric(frame.get(value_column, pd.Series(np.nan, index=frame.index)), errors="coerce")
        values = values[frame.source_available & np.isfinite(values)].to_numpy()
        n, expected = len(values), len(frame)
        usable = n > 0 and (not require_complete or n == expected)
        row = dict(zip(keys, group))
        row.update(value_column=value_column, n_expected=expected, n_available=n,
                   complete=n == expected, quantile_method="linear", weighting="equal",
                   probability_operator=">", require_complete=require_complete)
        row["mean"] = float(np.mean(values)) if usable else np.nan
        row["spread"] = float(np.std(values)) if usable else np.nan
        for p in percentiles:
            row[f"p{p:g}"] = float(np.percentile(values, p, method="linear")) if usable else np.nan
        for threshold in thresholds:
            row[f"prob_gt_{threshold:g}"] = float(np.mean(values > threshold)) if usable else np.nan
        output.append(row)
    return pd.DataFrame(output)
