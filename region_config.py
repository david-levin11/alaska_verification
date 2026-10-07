"""Region policy shared by source archivers, REFS, and the daily wrapper."""
from pathlib import PurePosixPath

REGIONS = ('alaska', 'hawaii')


def normalize_region(region):
    value = str(region).lower()
    value = {'ak': 'alaska', 'hi': 'hawaii'}.get(value, value)
    if value not in REGIONS:
        raise ValueError(f'Unsupported region {region!r}; choose alaska or hawaii')
    return value


def default_root(region, kind):
    region = normalize_region(region)
    return f'hawaii/{kind}' if region == 'hawaii' else kind


def region_root(root, region, kind):
    """Explicit Hawaii roots must include a hawaii namespace to prevent mixing."""
    region = normalize_region(region)
    root = str(root) if root is not None else default_root(region, kind)
    if region == 'hawaii' and 'hawaii' not in PurePosixPath(root).parts:
        raise ValueError(f'Hawaii {kind} root must contain a hawaii directory/prefix: {root}')
    return root


def refs_limits(region='alaska', overrides=None):
    region = normalize_region(region)
    limits = {'rrfs': 60, 'rrfsens': 60, 'hrrr': 48}
    if overrides:
        limits.update(overrides)
    if region == 'hawaii':
        limits['hrrr'] = -1  # Neither current nor lagged HRRR belongs to Hawaii.
    return limits
