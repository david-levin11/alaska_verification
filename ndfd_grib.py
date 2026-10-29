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
