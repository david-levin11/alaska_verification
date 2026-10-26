"""Deterministic interval forecasts sourced from QMD but archived as NBM."""
from grib_timestamps import percentile_times


ELEMENTS = ("precip6hr", "precip24hr", "maxt", "mint")
INTERVALS = {"precip6hr": 6, "precip24hr": 24, "maxt": 18, "mint": 18}


def source_model(model, element):
    return "nbmqmd" if model == "nbm" and element in ELEMENTS else model


def deterministic_ranges(lines, element, forecast_hour):
    """Select exactly one unannotated field with the requested interval.

    Percentiles, exceedance probabilities, ensemble means/stddev, and other
    accumulation lengths can coexist in one QMD file. None are substitutes.
    """
    interval = INTERVALS[element]
    if forecast_hour < interval:
        return {}
    start = forecast_hour - interval
    operation = {"maxt": "max", "mint": "min"}.get(element, "acc")
    variable, level = ("APCP", "surface") if element.startswith("precip") else ("TMP", "2 m above ground")
    labels = {f"{start}-{forecast_hour} hour {operation} fcst"}
    if start % 24 == 0 and forecast_hour % 24 == 0:
        labels.add(f"{start // 24}-{forecast_hour // 24} day {operation} fcst")
    matches = {}
    for i, line in enumerate(lines):
        fields = [part.strip() for part in line.split(":")]
        if len(fields) < 6 or fields[3:5] != [variable, level] or fields[5] not in labels:
            continue
        if any(fields[6:]):
            continue
        first = int(fields[1])
        last = str(int(lines[i + 1].split(":")[1]) - 1) if i + 1 < len(lines) else ""
        matches[f"{first}-{last}"] = line
    if len(matches) > 1:
        raise ValueError(f"Multiple deterministic {element} fields at f{forecast_hour:03d}")
    return matches


def decode_field(message, element, forecast_hour):
    """Validate a deterministic GRIB interval and return UTC times and values."""
    import numpy as np

    if int(message["productDefinitionTemplateNumber"]) != 8:
        raise ValueError("Expected a deterministic statistical-interval GRIB field (PDT 8)")
    operation = {"maxt": "max", "mint": "min"}.get(element, "accum")
    if message["stepType"] != operation:
        raise ValueError(f"Wrong GRIB statistic for {element}")
    if int(message['stepUnits']) != 1:
        raise ValueError('Expected GRIB step units in hours')
    if int(message["endStep"]) != forecast_hour or int(message["startStep"]) != forecast_hour - INTERVALS[element]:
        raise ValueError(f"Wrong GRIB interval for {element} f{forecast_hour:03d}")
    # Numeric parameter IDs are stable across ecCodes short-name versions
    # (e.g. max_2t versus tmax for the same 2-m maximum temperature).
    precip = element.startswith('precip')
    parameter = tuple(int(message[key]) for key in ('discipline', 'parameterCategory', 'parameterNumber'))
    if parameter != ((0, 1, 8) if precip else (0, 0, 0)):
        raise ValueError(f'Wrong GRIB parameter for {element}')
    if message['typeOfLevel'] != ('surface' if precip else 'heightAboveGround') or (not precip and message['level'] != 2):
        raise ValueError(f'Wrong GRIB level for {element}')
    if message['units'] not in (('kg m**-2', 'mm') if precip else ('K',)):
        raise ValueError(f'Unexpected GRIB units for {element}')
    reference, endpoint = percentile_times(message, forecast_hour)
    values = np.ma.asarray(message.values, dtype=float).filled(np.nan)
    if precip:
        values = values * 0.0393701  # kg m-2 / mm to inches
    else:
        values = (values - 273.15) * 1.8 + 32
    return reference, endpoint, np.round(values, 2)
