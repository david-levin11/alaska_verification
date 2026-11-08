"""Observed RH minima for daily 18Z–06Z UTC forecast windows."""
import re
import numpy as np
import pandas as pd

COLUMNS = ['stid','lat','lon','elev','date','window_start','window_end','valid_time',
           'minrh','rh_units','n_obs','first_report','last_report','max_gap_minutes',
           'coverage_ok','rh_sensor','NWSZONE','NWSCWA']


def utc_timestamp(value):
    text = str(value)
    if text.isdigit() and len(text) in (8,10,12):
        return pd.to_datetime(text.ljust(12,'0'),format='%Y%m%d%H%M',utc=True)
    stamp = pd.Timestamp(value)
    return stamp.tz_localize('UTC') if stamp.tzinfo is None else stamp.tz_convert('UTC')


def minrh_endpoints(start,end):
    """Inclusive endpoint range, unlike the temperature anchor-day interface."""
    start,end = utc_timestamp(start),utc_timestamp(end)
    if start > end:
        raise ValueError('start_time must not be after end_time')
    ends = pd.date_range(start.normalize()+pd.Timedelta(hours=6),
        end.normalize()+pd.Timedelta(hours=6),freq='D')
    return ends[(ends >= start) & (ends <= end)]


def aggregate_minrh(stations,endpoints,metadata,*,max_gap_minutes=90):
    """Min of available reports in [18Z,06Z); retain coverage diagnostics.

    Select one sensor per station/window: set_1, then set_1d, then remaining
    sensor numbers. A preferred sensor must have valid in-window reports.
    Never mix sensors, fill missing hours, or infer missing RH values.
    """
    if not np.isfinite(max_gap_minutes) or max_gap_minutes <= 0:
        raise ValueError('max_gap_minutes must be finite and positive')
    rows = []
    for station in stations:
        stid = station.get('STID')
        obs = station.get('OBSERVATIONS') or {}
        times = pd.Series(pd.to_datetime(obs.get('date_time',[]),utc=True,errors='coerce'))
        if not stid or times.empty:
            continue
        ends = (times+pd.Timedelta(hours=6)).dt.floor('D')+pd.Timedelta(hours=6)
        in_window = ((times.dt.hour >= 18) | (times.dt.hour < 6)) & ends.isin(endpoints)
        keys = []
        for name in obs:
            match = re.fullmatch(r'relative_humidity_set_(\d+)(d?)',name)
            if match:
                keys.append((int(match[1]),bool(match[2]),name))
        selected = {}
        for _,_,name in sorted(keys):
            raw = pd.Series(obs[name],dtype=object).reindex(times.index)
            # QC markers such as False are missing, not 0% RH.
            values = pd.to_numeric(raw.map(lambda v:None if isinstance(v,(bool,np.bool_)) else v),errors='coerce').astype(float)
            valid = in_window & np.isfinite(values) & values.between(0,100)
            if valid.any():
                frame = pd.DataFrame({'time':times[valid],'value':values[valid],'end':ends[valid]})
                frame = frame.drop_duplicates('time',keep='last').sort_values('time')
                for endpoint,group in frame.groupby('end',sort=True):
                    if endpoint not in selected:
                        selected[endpoint] = (name,group)
        if not selected:
            continue
        zone = metadata.get(stid,{})
        for endpoint,(sensor,group) in sorted(selected.items()):
            start = endpoint-pd.Timedelta(hours=12)
            reports = group.time.tolist()
            gap = pd.Series([start,*reports,endpoint]).diff().dt.total_seconds().max()/60
            rows.append(dict(stid=stid,lat=station.get('LATITUDE'),lon=station.get('LONGITUDE'),
                elev=station.get('ELEVATION'),date=endpoint.normalize(),window_start=start,
                window_end=endpoint,valid_time=endpoint,minrh=float(group.value.min()),rh_units='%',
                n_obs=len(group),first_report=reports[0],last_report=reports[-1],
                max_gap_minutes=float(gap),coverage_ok=bool(gap <= max_gap_minutes),
                rh_sensor=sensor,NWSZONE=zone.get('zone'),NWSCWA=zone.get('cwa')))
    return pd.DataFrame(rows,columns=COLUMNS).sort_values(['stid','window_end']).reset_index(drop=True)
