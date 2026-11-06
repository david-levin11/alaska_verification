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
    from grib_timestamps import percentile_times
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
                reference, endpoint = percentile_times(message, int(end))
                windows.append(dict(init_time=pd.Timestamp(reference),
                    window_start=pd.Timestamp(reference)+pd.Timedelta(hours=start),
                    window_end=pd.Timestamp(endpoint), window_hours=duration))
    if not windows:
        raise ValueError('No MinRH GRIB messages found')
    return windows
