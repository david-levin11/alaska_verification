"""Validate direct accumulation totals returned by Synoptic."""
import pandas as pd


COLUMNS = ["stid", "lat", "lon", "elev", "accum_hours", "step_hours",
           "start_time", "end_time", "precip_total", "precip_units", "NWSZONE", "NWSCWA",
           "first_report", "last_report", "nominal_start_time", "nominal_end_time", "report_type"]


def validate_precip_windows(intervals, start, end, *, accum_hours, step_hours,
                            interval_window="0.5,0.5"):
    """Retain independently returned totals only when actual bounds fit.

    The requested window comes from the API's interval position and request
    start. Both actual boundaries and the actual duration must agree with that
    window within the configured interval tolerance (30 minutes by default).
    Identical repeated rows are deduplicated; conflicting totals for one
    station/nominal endpoint are excluded rather than arbitrarily selected.
    """
    if intervals.empty:
        return pd.DataFrame(columns=COLUMNS)
    short, *long = [float(v) for v in str(interval_window).split(",")]
    lower = pd.Timedelta(hours=short)
    upper = pd.Timedelta(hours=long[0] if long else short)
    duration = pd.Timedelta(hours=accum_hours)
    df = intervals.copy()
    for column in ("first_report", "last_report", "nominal_end_time"):
        df[column] = pd.to_datetime(df[column], utc=True, errors="coerce")
    df["nominal_start_time"] = df["nominal_end_time"] - duration
    df["precip_total"] = pd.to_numeric(df["interval_precip"], errors="coerce")
    df = df.loc[df.nominal_end_time.between(start, end)].copy()
    df = df.drop_duplicates(subset=["stid", "first_report", "last_report",
                                   "nominal_end_time", "precip_total", "report_type", "precip_units"])
    ambiguous = df.duplicated(["stid", "nominal_end_time"], keep=False)
    valid = (~ambiguous & df.precip_total.ge(0) & df.precip_total.lt(float("inf"))
             & (df.last_report-df.first_report).between(duration-lower, duration+upper)
             & (df.first_report-df.nominal_start_time).between(-lower, upper)
             & (df.last_report-df.nominal_end_time).between(-lower, upper))
    rejected = int((~valid).sum())
    if rejected:
        print(f"Precipitation validation: skipped {rejected} incomplete/inconsistent or conflicting totals.")
    out = df.loc[valid].copy()
    out["accum_hours"] = accum_hours
    out["step_hours"] = step_hours
    out["start_time"] = out.first_report
    out["end_time"] = out.last_report
    return out.reindex(columns=COLUMNS).sort_values(["stid", "nominal_end_time"]).reset_index(drop=True)
