"""MinRH routing, native-message validation and unchanged percent extraction."""
from contextlib import contextmanager
from types import SimpleNamespace
import numpy as np
import pandas as pd
import pytest
import xarray as xr
import archiver_config as config
import utils
from ndfd_grib import read_ndfd_minrh_windows


@pytest.fixture(autouse=True)
def region():
    config.configure_region('alaska')
    yield
    config.configure_region('alaska')


def message(**overrides):
    result = dict(discipline=0, parameterCategory=1, parameterNumber=198,
        typeOfStatisticalProcessing=3, stepUnits=1, startStep=6, endStep=18,
        indicatorOfUnitForTimeRange=1, lengthOfTimeRange=12,
        dataDate=20261104, dataTime=1200,
        yearOfEndOfOverallTimeInterval=2026, monthOfEndOfOverallTimeInterval=11,
        dayOfEndOfOverallTimeInterval=5, hourOfEndOfOverallTimeInterval=6,
        minuteOfEndOfOverallTimeInterval=0, secondOfEndOfOverallTimeInterval=0)
    return {**result, **overrides}


def mock_grib(monkeypatch, messages):
    import pygrib
    @contextmanager
    def opened(path): yield iter(messages)
    monkeypatch.setattr(pygrib, 'open', opened)


@pytest.mark.parametrize('region,prefix', [('alaska','ZRRZ'),('hawaii','ZRSZ')])
def test_listing_uses_minrh_folder_and_both_ranges(region, prefix, monkeypatch):
    config.configure_region(region)
    seen = []
    def glob(pattern):
        seen.append(pattern)
        stem=pattern[:-1]
        return [stem+'KWBN_202611041146', stem+'KWBN_202611042333', stem+'KWBN_202611041000']
    monkeypatch.setattr(utils.fsspec, 'filesystem', lambda *a, **k: SimpleNamespace(glob=glob))
    files=utils.get_ndfd_file_list('202611040000','202611042359',config.NDFD_DICT,'minrh')
    assert len(seen) == 8  # Existing three-day lookback plus requested day.
    assert all('/wmo/minrh/' in p and f'/{prefix}' in p for p in seen)
    assert any(prefix+'97_' in p for p in seen)
    assert any(prefix+'98_' in p for p in seen)
    assert len(files['minrh']) == 16
    assert all(p.split('_')[-1][8:10] in ('11','23') for p in files['minrh'])


def test_actual_message_window(monkeypatch):
    mock_grib(monkeypatch,[message()])
    window=read_ndfd_minrh_windows('sample')[0]
    assert window == dict(init_time=pd.Timestamp('2026-11-04 12:00'),
        window_start=pd.Timestamp('2026-11-04 18:00'),
        window_end=pd.Timestamp('2026-11-05 06:00'),window_hours=12.)


@pytest.mark.parametrize('override', [dict(parameterNumber=1),
    dict(typeOfStatisticalProcessing=2),dict(stepUnits=0),
    dict(lengthOfTimeRange=18),dict(indicatorOfUnitForTimeRange=255),
    dict(hourOfEndOfOverallTimeInterval=12)])
def test_reject_wrong_parameter_or_period(monkeypatch,override):
    mock_grib(monkeypatch,[message(**override)])
    with pytest.raises(ValueError): read_ndfd_minrh_windows('wrong')


@pytest.mark.parametrize('field', ['unknown','minrh'])
def test_extract_uses_validated_field_and_preserves_interval(monkeypatch,tmp_path,field):
    monkeypatch.setattr(config,'ELEMENT','minrh')
    mock_grib(monkeypatch,[message()])
    ds=xr.Dataset({field:(('step','y','x'),np.array([[[42.25]]]))},coords={
        'latitude':(('y','x'),[[58.]]),'longitude':(('y','x'),[[-135.]]),
        'step':[np.timedelta64(18,'h')], 'time':np.datetime64('2026-11-04T12:00'),
        'valid_time':('step',np.array(['2026-11-05T06:00'],dtype='datetime64[ns]'))})
    @contextmanager
    def opened(*a,**k):yield SimpleNamespace(name='sample')
    monkeypatch.setattr(utils.fsspec,'open',opened)
    import ndfd_grib
    monkeypatch.setattr(ndfd_grib,'load_ndfd_dataset',lambda *a,**k:ds)
    stations=pd.DataFrame([dict(stid='TEST',latitude=58.,longitude=-135.)])
    result=utils.process_file_pair('test',None,stations,str(tmp_path),['unknown'])
    assert result.minrh.tolist()==[42.2]  # Existing RH rounding, no unit conversion.
    assert result.forecast_hour.tolist()==[18]
    assert result.window_hours.tolist()==[12.]
    assert result.valid_time.eq(result.window_end).all()
    assert result.window_start.iloc[0]==pd.Timestamp('2026-11-04 18:00')
    path=tmp_path/'minrh.parquet'
    result.to_parquet(path,index=False)
    pd.testing.assert_frame_equal(result,pd.read_parquet(path))
