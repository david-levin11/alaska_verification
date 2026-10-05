"""Load variable-specific probability recipes in the source archive's units."""
import json
import math
from pathlib import Path

DEFAULT_CONFIG = Path(__file__).with_name('refs_thresholds.json')
UNITS = {'wind_speed_kt': 'kt', 'wind_gust_kt': 'kt', 'precip_6h': 'in',
         'snow_6h': 'in', 'rh': '%'}


def validate_recipe(variable, recipe):
    if variable not in UNITS:
        raise ValueError(f'Unknown threshold variable: {variable}')
    if not isinstance(recipe, dict) or set(recipe) != {'units', 'operator', 'thresholds'}:
        raise ValueError(f'{variable}: specify units, operator, and thresholds')
    if recipe['units'] != UNITS[variable]:
        raise ValueError(f'{variable}: units must be {UNITS[variable]}')
    if recipe['operator'] not in ('>', '<'):
        raise ValueError(f'{variable}: operator must be > or <')
    values = recipe['thresholds']
    if not isinstance(values, list) or any(
        isinstance(v, bool) or not isinstance(v, (int, float)) or
        not math.isfinite(v) or v < 0 or (variable == 'rh' and v > 100)
        for v in values
    ):
        raise ValueError(f'{variable}: thresholds must be finite nonnegative numbers (RH 0–100)')
    return dict(units=recipe['units'], operator=recipe['operator'],
                thresholds=sorted(set(float(v) for v in values)))


def load_threshold_config(path=None):
    with Path(path or DEFAULT_CONFIG).open() as stream:
        config = json.load(stream)
    if not isinstance(config, dict) or set(config) != set(UNITS):
        raise ValueError(f'Threshold config must contain exactly: {", ".join(UNITS)}')
    return {variable: validate_recipe(variable, recipe) for variable, recipe in config.items()}


def resolve_threshold_config(path=None, thresholds=None, value_column=None):
    config = load_threshold_config(path)
    if thresholds is not None:
        if value_column not in UNITS:
            raise ValueError('--thresholds requires a supported --value-column; use --threshold-config for multiple variables')
        # Legacy CLI overrides retain strict exceedance semantics, including RH.
        config[value_column] = validate_recipe(value_column, dict(
            units=UNITS[value_column], operator='>', thresholds=list(thresholds)))
    return config
