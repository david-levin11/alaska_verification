"""Use decoded grid coordinates, including Hawaii Mercator grids, for point lookup."""
import hashlib
import numpy as np


def grid_coordinates(lats, lons, *, paired=True):
    lat = np.asarray(lats, dtype=float)
    lon = (np.asarray(lons, dtype=float) + 180) % 360 - 180
    if lat.ndim == lon.ndim == 1:
        if paired and lat.shape == lon.shape:
            # cfgrib uses a single "values" dimension for Mercator grids.
            lat, lon = lat.reshape(1, -1), lon.reshape(1, -1)
        else:
            lon, lat = np.meshgrid(lon, lat)
    if lat.ndim != 2 or lat.shape != lon.shape:
        raise ValueError('Expected matching 2-D latitude/longitude grids')
    fingerprint = hashlib.sha256()
    fingerprint.update(str(lat.shape).encode())
    fingerprint.update(lat.tobytes())
    fingerprint.update(lon.tobytes())
    return lat, lon, fingerprint.digest()


def outside_hawaii_grid(lat, lon, lats, lons, iy, ix):
    # Do not assign remote/Northwestern Hawaiian stations an edge-cell forecast.
    # 0.1 degree comfortably exceeds the diagonal of the 2.5-km grids.
    dlon = abs((float(lon) - lons[iy, ix] + 180) % 360 - 180)
    return max(abs(float(lat) - lats[iy, ix]), dlon) > 0.1
