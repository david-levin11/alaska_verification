"""Serialize NDFD native GRIB reads while allowing concurrent downloads."""
from threading import Lock


_DECODE_LOCK = Lock()


def load_ndfd_dataset(path, *, source=None):
    """Open, fully load and close a cfgrib dataset under one shared lock.

    Loading must happen inside the lock: xarray arrays can otherwise defer
    native decoding until .values is accessed later in a worker thread.
    Returned arrays are in memory and no longer depend on an open GRIB file.
    """
    with _DECODE_LOCK:
        import xarray as xr
        print(f"Decoding NDFD GRIB: {source or path}", flush=True)
        dataset = xr.open_dataset(path, engine="cfgrib",
            backend_kwargs={"indexpath": ""}, decode_timedelta=True)
        try:
            return dataset.load()
        finally:
            dataset.close()


def read_ndfd_minrh_windows(path):
    """Identify NDFD MinRH by GRIB codes and retain each statistical interval.

    NWS centre 8 can decode as shortName/units=unknown in ecCodes. NOAA's
    parameter table defines discipline/category/number 0/1/198 as MinRH (%).
    Never assume an unknown variable is MinRH without inspecting its messages.
    Use the shared native-decoder lock, including this metadata-only GRIB read.
    """
    import math
    import pandas as pd
    import pygrib
    from datetime import datetime
    windows = []
    with _DECODE_LOCK:
        with pygrib.open(str(path)) as messages:
            for message in messages:
                codes = tuple(int(message[k]) for k in
                    ('discipline', 'parameterCategory', 'parameterNumber'))
                if codes != (0, 1, 198) or int(message['typeOfStatisticalProcessing']) != 3:
                    raise ValueError(f'Not a minimum-relative-humidity message: {codes}')
                if int(message['stepUnits']) != 1:
                    raise ValueError('MinRH forecast steps must be expressed in hours')
                start, end = float(message['startStep']), float(message['endStep'])
                # Statistical interval units: minute/hour/day/3h/6h/12h/second.
                unit_hours = {0: 1/60, 1: 1, 2: 24, 10: 3, 11: 6, 12: 12, 13: 1/3600}
                unit = int(message['indicatorOfUnitForTimeRange'])
                duration = float(message['lengthOfTimeRange']) * unit_hours.get(unit, float('nan'))
                if not math.isfinite(duration) or not (end > start >= 0) or abs((end-start)-duration) > 1e-8:
                    raise ValueError('Invalid or inconsistent MinRH statistical interval')
                if not end.is_integer():
                    raise ValueError('MinRH forecast endpoint must be a whole hour')
                reference = pd.Timestamp(datetime.strptime(
                    f"{int(message['dataDate']):08d}{int(message['dataTime']):04d}", '%Y%m%d%H%M'))
                endpoint = pd.Timestamp(datetime(*(int(message[prefix+'OfEndOfOverallTimeInterval'])
                    for prefix in ('year', 'month', 'day', 'hour', 'minute', 'second'))))
                window_start = reference + pd.Timedelta(hours=start)
                actual_hours = (endpoint-window_start).total_seconds()/3600
                lead = (endpoint-reference).total_seconds()/3600
                mismatch = abs(actual_hours-duration) > 1e-8
                # Some 00Z first periods encode start=0 and duration=12 while
                # their explicit endpoint is 06Z. Preserve those inconsistent
                # source fields rather than move the endpoint to 12Z or infer
                # a pre-initialization start. Only allow a shortened first
                # interval; arbitrary later mismatches still fail validation.
                if (not lead.is_integer() or actual_hours <= 0 or
                        (mismatch and not (start == 0 and actual_hours < duration))):
                    raise ValueError('Invalid or inconsistent MinRH explicit endpoint')
                if mismatch:
                    print(f'WARNING: MinRH interval starts {window_start} and ends {endpoint} '
                          f'({actual_hours:g}h), but source declares {duration:g}h; '
                          'preserving bounds and flagging window_duration_mismatch.', flush=True)
                windows.append(dict(init_time=reference, window_start=window_start,
                    window_end=endpoint, window_hours=actual_hours,
                    source_window_hours=duration, window_duration_mismatch=mismatch,
                    encoded_forecast_hour=int(end)))
    if not windows:
        raise ValueError('No MinRH GRIB messages found')
    return windows
