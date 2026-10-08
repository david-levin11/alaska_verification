"""Authoritative timestamps for GRIB percentile archive records (naive UTC)."""
from datetime import datetime, timedelta


def _key(message, name):
    try:
        return message[name]
    except (KeyError, ValueError, RuntimeError):
        return None


def percentile_times(message, forecast_hour):
    """Return GRIB reference and endpoint times, rejecting inconsistent leads.

    pygrib.validDate can denote the interval start for statistical fields.
    Never infer the reference time by subtracting a lead from that property.
    """
    date, time = _key(message, 'dataDate'), _key(message, 'dataTime')
    if date is None or time is None:
        raise ValueError('Missing GRIB reference date/time')
    reference = datetime.strptime(f'{int(date):08d}{int(time):04d}', '%Y%m%d%H%M')
    names = [x+'OfEndOfOverallTimeInterval' for x in ('year','month','day','hour','minute','second')]
    parts = [_key(message, n) for n in names]
    if all(p is not None for p in parts):
        endpoint = datetime(*map(int, parts))
    elif any(p is not None for p in parts):
        raise ValueError('Incomplete GRIB interval endpoint')
    elif _key(message, 'stepType') == 'instant':
        date, time = _key(message, 'validityDate'), _key(message, 'validityTime')
        if date is None or time is None:
            raise ValueError('Missing instantaneous GRIB validity date/time')
        endpoint = datetime.strptime(f'{int(date):08d}{int(time):04d}', '%Y%m%d%H%M')
    else:
        raise ValueError('Missing explicit GRIB statistical interval endpoint')
    if endpoint-reference != timedelta(hours=forecast_hour):
        raise ValueError(f'GRIB endpoint {endpoint} disagrees with reference {reference} and f{forecast_hour:03d}')
    return reference, endpoint
