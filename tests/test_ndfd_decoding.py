"""Concurrent NDFD downloads must never overlap native GRIB decoding."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier, Lock
from types import SimpleNamespace
import time

import numpy as np
import pandas as pd
import pytest
import xarray as xr

import archiver_config as config
import utils
from ndfd_grib import load_ndfd_dataset


@pytest.mark.parametrize('element', ['precip6hr', 'maxt', 'Wind'])
def test_downloads_overlap_but_open_load_and_close_are_serial(element, monkeypatch):
    config.configure_region('alaska')
    monkeypatch.setattr(config, 'ELEMENT', element)
    monkeypatch.setattr(config, 'MAX_WORKERS', 4)
    keys = config.NDFD_ELEMENT_STRINGS[element]
    barrier = Barrier(4)
    guard = Lock()
    state = dict(downloads=0, max_downloads=0, decoders=0, max_decoders=0)
    datasets = []

    @contextmanager
    def downloaded(url, **kwargs):
        if 'primary' in url:
            with guard:
                state['downloads'] += 1
                state['max_downloads'] = max(state['max_downloads'], state['downloads'])
            barrier.wait(timeout=10)
            with guard:
                state['downloads'] -= 1
        yield SimpleNamespace(name=url)

    class Dataset:
        def __init__(self, path):
            self.loaded = self.closed = False
            field = keys[1] if 'direction' in path else keys[0]
            self.data = {field: np.full((2, 2, 2), 90. if 'direction' in path else 25.4)}
            self.data.update(latitude=np.array([[58.,58.],[59.,59.]]),
                longitude=np.array([[-135.,-134.],[-135.,-134.]]),
                step=np.array([np.timedelta64(6,'h'),np.timedelta64(12,'h')]),
                valid_time=np.array(['2026-07-02T06:00','2026-07-02T12:00'], dtype='datetime64[m]'))
        def load(self):
            assert state['decoders'] == 1
            time.sleep(0.01)  # Release the GIL while other workers contend.
            self.loaded = True
            return self
        def close(self):
            assert self.loaded and state['decoders'] == 1
            self.closed = True
            with guard:
                state['decoders'] -= 1
        def __getitem__(self, key):
            assert self.loaded and self.closed  # No lazy read outside the lock.
            return SimpleNamespace(values=self.data[key])
        def __getattr__(self, key):
            return self[key]

    def opened(path, **kwargs):
        assert kwargs == dict(engine='cfgrib', backend_kwargs={'indexpath': ''}, decode_timedelta=True)
        with guard:
            state['decoders'] += 1
            state['max_decoders'] = max(state['max_decoders'], state['decoders'])
        dataset = Dataset(path)
        datasets.append(dataset)
        return dataset

    monkeypatch.setattr(utils.fsspec, 'open', downloaded)
    monkeypatch.setattr(xr, 'open_dataset', opened)
    files = [f'bucket/primary_2026070{day}0000' for day in range(1,5)]
    directions = [f'bucket/direction_2026070{day}0000' for day in range(1,5)] if len(keys) > 1 else []
    stations = pd.DataFrame(dict(stid=['PAJN','TEST'],latitude=[58.,59.],longitude=[-135.,-134.]))
    result = utils.extract_ndfd_forecasts_parallel(files, directions, stations, '/tmp')
    assert len(result) == 16
    assert set(result.station_id) == {'PAJN','TEST'}
    assert set(result.forecast_hour) == {6,12}
    assert state['max_downloads'] == 4
    assert state['max_decoders'] == 1
    assert state['decoders'] == 0
    assert len(datasets) == (8 if element == 'Wind' else 4)
    assert all(ds.loaded and ds.closed for ds in datasets)
    if element == 'precip6hr':
        assert result.precip6hr.tolist() == [1.]*16


def test_failed_load_closes_file_and_releases_lock(monkeypatch):
    events = []
    class Dataset:
        def load(self):
            events.append('load')
            raise ValueError('bad GRIB')
        def close(self): events.append('close')
    monkeypatch.setattr(xr, 'open_dataset', lambda *a, **k: Dataset())
    with pytest.raises(ValueError, match='bad GRIB'):
        load_ndfd_dataset('bad.grib2')
    assert events == ['load','close']
    # A later worker can acquire the lock after a failure.
    class Good(Dataset):
        def load(self): return self
    monkeypatch.setattr(xr, 'open_dataset', lambda *a, **k: Good())
    with ThreadPoolExecutor(max_workers=1) as pool:
        assert isinstance(pool.submit(load_ndfd_dataset, 'good.grib2').result(timeout=5), Good)


def test_processing_failure_is_not_saved_as_partial_results(monkeypatch):
    @contextmanager
    def broken(*args, **kwargs):
        raise OSError('download failed')
        yield
    monkeypatch.setattr(utils.fsspec, 'open', broken)
    with pytest.raises(OSError, match='download failed'):
        utils.process_file_pair('bucket/file', None, pd.DataFrame(), '/tmp', ['tmax'])
