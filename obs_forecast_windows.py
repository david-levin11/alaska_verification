"""Observed extrema for explicit forecast bounds; no change to legacy OBS."""
import re
import numpy as np
import pandas as pd

from obs_minrh import utc_timestamp

KEYS = ['stid','window_start','window_end']
SPECS = {'maxt':('air_temp','tmax',max),'mint':('air_temp','tmin',min),
         'minrh':('relative_humidity','minrh',min)}


def validate_periods(periods):
    periods = periods[['station_id','window_start','window_end']].copy()
    if periods.station_id.isna().any():
        raise ValueError('Missing forecast station ID')
    periods['station_id'] = periods.station_id.astype(str)
    for column in ['window_start','window_end']:
        periods[column] = pd.to_datetime(periods[column],utc=True,errors='raise')
    hours = (periods.window_end-periods.window_start).dt.total_seconds()/3600
    if periods.isna().any().any() or not hours.between(0,48,inclusive='right').all():
        raise ValueError('Invalid forecast window bounds')
    return periods.drop_duplicates().sort_values(['station_id','window_end','window_start']).reset_index(drop=True)


def native_periods(root,element,start,end):
    paths = sorted((root/(element+'_native')).glob('*_archive.parquet'))
    if not paths:
        raise ValueError(f'No {element}_native archives under {root}; import/migrate forecasts first')
    # Initialization and valid months differ. Scan all monthly files and filter
    # by endpoint, never assume that an endpoint is in its initialization file.
    periods = validate_periods(pd.concat([pd.read_parquet(p,columns=
        ['station_id','window_start','window_end']) for p in paths],ignore_index=True))
    return periods.loc[periods.window_end.between(utc_timestamp(start),utc_timestamp(end))].reset_index(drop=True)


def standard_periods(stations,element,start,end):
    start,end = utc_timestamp(start),utc_timestamp(end)
    endpoint_hour = {'maxt':0,'mint':12,'minrh':6}[element]
    endpoints = pd.date_range(start.normalize()+pd.Timedelta(hours=endpoint_hour),
        end.normalize()+pd.Timedelta(hours=endpoint_hour),freq='D')
    endpoints = endpoints[(endpoints>=start)&(endpoints<=end)]
    return validate_periods(pd.DataFrame([
        dict(station_id=s,window_start=t-pd.Timedelta(hours=12),window_end=t)
        for s in stations for t in endpoints],columns=['station_id','window_start','window_end']))


def aggregate_windows(stations,periods,element,metadata,*,max_gap_minutes=90):
    """Use actual reports within [start,end), one sensor per station/window.

    Sparse minima/maxima remain available with coverage_ok=False. Never combine
    sensors or manufacture missing observations. Window-end reports are excluded.
    """
    if not np.isfinite(max_gap_minutes) or max_gap_minutes<=0:
        raise ValueError('max_gap_minutes must be positive and finite')
    periods = validate_periods(periods)
    variable,value_column,reduction = SPECS[element]
    rows=[]
    by_station = {s:p for s,p in periods.groupby('station_id')}
    for station in stations:
        stid = station.get('STID')
        if stid not in by_station:
            continue
        obs = station.get('OBSERVATIONS') or {}
        times = pd.Series(pd.to_datetime(obs.get('date_time',[]),utc=True,errors='coerce'))
        sensors=[]
        for name,raw in obs.items():
            match = re.fullmatch(re.escape(variable)+r'_set_(\d+)(d?)',name)
            if not match:
                continue
            values = pd.Series(raw,dtype=object).reindex(times.index)
            values = pd.to_numeric(values.map(lambda v:None if isinstance(v,(bool,np.bool_)) else v),errors='coerce').astype(float)
            valid = times.notna() & np.isfinite(values)
            if element=='minrh':
                valid &= values.between(0,100)
            frame = pd.DataFrame({'time':times[valid],'value':values[valid]})
            frame = frame.drop_duplicates('time',keep='last').sort_values('time')
            sensors.append((int(match[1]),bool(match[2]),name,frame))
        sensors.sort(key=lambda item:item[:2])
        for period in by_station[stid].itertuples(index=False):
            begin,endpoint = period.window_start,period.window_end
            for _,_,sensor,frame in sensors:
                selected = frame.loc[(frame.time>=begin)&(frame.time<endpoint)]
                if selected.empty:
                    continue
                reports = selected.time.tolist()
                gap = pd.Series([begin,*reports,endpoint]).diff().dt.total_seconds().max()/60
                zone = metadata.get(stid,{})
                value = float(reduction(selected.value))
                row = dict(stid=stid,lat=station.get('LATITUDE'),lon=station.get('LONGITUDE'),
                    elev=station.get('ELEVATION'),date=endpoint.normalize(),valid_time=endpoint,
                    window_start=begin,window_end=endpoint,window_hours=(endpoint-begin).total_seconds()/3600,
                    n_obs=len(selected),first_report=reports[0],last_report=reports[-1],
                    max_gap_minutes=float(gap),coverage_ok=bool(gap<=max_gap_minutes),
                    sensor=sensor,NWSZONE=zone.get('zone'),NWSCWA=zone.get('cwa'))
                row[value_column] = value
                row[element] = value
                row['rh_units' if element=='minrh' else 'temp_units'] = '%' if element=='minrh' else 'F'
                rows.append(row)
                break
    return pd.DataFrame(rows)


def fetch_windows(archiver,periods,element,*,max_gap_minutes=90):
    """Fetch complete explicit bounds, retry all batches, fail on exhaustion."""
    import requests
    from time import sleep
    periods = validate_periods(periods)
    if periods.empty:
        return pd.DataFrame()
    variable = SPECS[element][0]
    parts=[]
    for chunk in archiver._chunk_station_ids(periods.station_id.unique().tolist()):
        selected = periods.loc[periods.station_id.isin(chunk)]
        wait = archiver.initial_wait
        for attempt in range(archiver.max_retries):
            try:
                params = dict(token=archiver.api_token,stid=','.join(chunk),
                    start=selected.window_start.min().strftime('%Y%m%d%H%M'),
                    end=selected.window_end.max().strftime('%Y%m%d%H%M'),
                    vars=variable,obtimezone='utc',units='english',output='json',hfmetars=archiver.hfmetar)
                response = requests.get(archiver.url,params=params,timeout=90)
                response.raise_for_status()
                payload = response.json()
                code = str(payload.get('SUMMARY',{}).get('RESPONSE_CODE',''))
                if code=='2':
                    print(f'OBS {element}: no data/access for batch of {len(chunk)} stations')
                    break
                if code!='1' or not isinstance(payload.get('STATION'),list):
                    raise ValueError('Unexpected Synoptic response')
                units = str(payload.get('UNITS',{}).get(variable,'')).lower()
                allowed = ('%','percent','percentage') if element=='minrh' else ('f','fahrenheit','degrees f','degf')
                if units not in allowed:
                    raise ValueError('Unexpected observation units')
                rows = [s for s in payload['STATION'] if s.get('STID') in chunk]
                parts.append(aggregate_windows(rows,selected,element,archiver.station_metadata,
                    max_gap_minutes=max_gap_minutes))
                break
            except Exception as error:
                print(f'OBS {element} attempt {attempt+1}/{archiver.max_retries} failed: {type(error).__name__}')
                if attempt+1==archiver.max_retries:
                    raise RuntimeError(f'OBS {element} batch failed; no partial batch will be saved') from None
                sleep(wait)
                wait *= 2
    return pd.concat(parts,ignore_index=True) if parts else pd.DataFrame()


def observation_plan(data,root,model,element,*,replace_conflicts=False):
    plan=[]
    if data.empty:
        return plan
    if data.duplicated(KEYS).any():
        raise ValueError('Duplicate observation window keys')
    for month,new in data.groupby(data.window_end.dt.strftime('%Y_%m')):
        destination = root/model/element/f'{month}_archive.parquet'
        old = pd.read_parquet(destination) if destination.exists() else pd.DataFrame()
        if not old.empty:
            if old.duplicated(KEYS).any():
                raise ValueError(f'Existing duplicate observations: {destination}')
            a,b = old.set_index(KEYS),new.set_index(KEYS)
            shared = a.index.intersection(b.index)
            left,right = a.reindex(index=shared,columns=b.columns),b.loc[shared]
            equal = (left.eq(right)|(left.isna()&right.isna())).all(axis=1)
            if (~equal).any() and not replace_conflicts:
                raise ValueError(f'Changed observations in {destination}; use --replace-conflicts after review')
            new = new.loc[~pd.MultiIndex.from_frame(new[KEYS]).isin(shared[equal.to_numpy()])]
        if new.empty:
            continue
        combined = pd.concat([old,new],ignore_index=True).drop_duplicates(KEYS,keep='last').reset_index(drop=True)
        plan.append((destination,combined,dict(new_or_replaced_rows=len(new))))
    return plan
