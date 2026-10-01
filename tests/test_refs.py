from pathlib import Path
from types import SimpleNamespace
import numpy as np
import pandas as pd
import pytest
from ensemble_processing import add_interval_precip_from_total, assemble_refs, summarize_refs


def test_intervals_first_period_missing_endpoint_and_member_identity():
    rows = []
    init = pd.Timestamp('2026-09-30 18:00')
    for member, totals in [('m001', [0.1, 0.2, 0.7, 1.0]), ('m002', [1., 2., 7., 10.])]:
        for fh, total in zip([3, 6, 12, 21], totals):
            rows.append(dict(station_id='PAJN', member_id=member, init_time=init,
                             valid_time=init + pd.Timedelta(hours=fh), precip_accum=total))
    out = add_interval_precip_from_total(pd.DataFrame(rows))
    np.testing.assert_allclose(out.precip_6h, [np.nan, .2, .5, np.nan, np.nan, 2., 5., np.nan], equal_nan=True)


def write_sources(root, cycle, lead=24):
    valid = cycle + pd.Timedelta(hours=lead)
    for model, member_ids in [('rrfs', ['control']), ('hrrr', ['control']),
                              ('rrfsens', [f'm{i:03d}' for i in range(1,6)])]:
        rows = []
        for lag in [0,6]:
            for member in member_ids:
                rows.append(dict(station_id='PAJN', init_time=cycle-pd.Timedelta(hours=lag),
                                 valid_time=valid, forecast_hour=lead+lag, member_id=member,
                                 wind_speed_kt=40. if lag else 20.))
        frame = pd.DataFrame(rows)
        for month, part in frame.groupby(frame.init_time.dt.strftime('%Y_%m')):
            path = root / model / 'wind' / f'{month}_archive.parquet'
            path.parent.mkdir(parents=True, exist_ok=True)
            part.to_parquet(path, index=False)
    return valid


def test_cross_month_alignment_and_probabilities(tmp_path):
    cycle = pd.Timestamp('2026-10-01 00:00')
    valid = write_sources(tmp_path, cycle)
    out = assemble_refs(tmp_path, 'Wind', cycle, valid)
    assert len(out) == 14
    assert set(out.forecast_hour) == {24,30}
    assert set(out.ensemble_forecast_hour) == {24}
    assert set(out.loc[out.lag_hours == 6, 'init_time'].dt.day) == {30}
    summary = summarize_refs(out, 'wind_speed_kt', thresholds=[30]).iloc[0]
    assert summary.n_available == summary.n_expected == 14
    assert summary.p50 == 30
    assert summary.prob_gt_30 == .5


def test_missing_members_suppress_statistics_unless_opted_in(tmp_path):
    cycle = pd.Timestamp('2026-10-01')
    valid = write_sources(tmp_path, cycle)
    (tmp_path/'rrfsens'/'wind'/'2026_09_archive.parquet').unlink()
    out = assemble_refs(tmp_path, 'wind', cycle, valid, station_ids=['PAJN','MISSING'])
    summary = summarize_refs(out, 'wind_speed_kt').set_index('station_id')
    assert summary.loc['PAJN', 'n_available'] == 9
    assert pd.isna(summary.loc['PAJN','p50'])
    assert summary.loc['MISSING','n_available'] == 0
    partial = summarize_refs(out, 'wind_speed_kt', require_complete=False).set_index('station_id')
    assert np.isfinite(partial.loc['PAJN','mean'])


def test_scheduled_horizon_is_distinct_from_missing_data(tmp_path):
    out = assemble_refs(tmp_path, 'wind', '2026-10-01', '2026-10-03 12:00', station_ids=['PAJN'])
    # Default recipe: current control plus five current perturbations, no HRRR.
    assert len(out) == 6
    assert not out.source_available.any()
    out = assemble_refs(tmp_path, 'wind', '2026-10-01', '2026-10-03 12:00',
                        station_ids=['PAJN'], max_forecast_hours={'rrfs':84})
    assert len(out) == 7


def test_duplicate_members_rejected(tmp_path):
    cycle = pd.Timestamp('2026-10-01')
    valid = write_sources(tmp_path, cycle)
    out = assemble_refs(tmp_path,'wind',cycle,valid)
    with pytest.raises(ValueError, match='Duplicate ensemble'):
        summarize_refs(pd.concat([out,out]),'wind_speed_kt')


def test_member_urls_and_date_parsing(monkeypatch):
    import utils
    monkeypatch.setattr(utils.requests, 'head', lambda *a, **kw: SimpleNamespace(ok=True))
    urls = utils.get_model_file_list(pd.Timestamp('2026-09-30'),pd.Timestamp('2026-09-30'),
                                    [6], '6h', 'https://example.test', 'Wind',
                                    model='rrfs', domain='ak', member_id='m001')
    assert urls == ['https://example.test/rrfsens.20260930/00/m001/rrfs.t00z.m001.2dfldnomads.3km.f006.ak.grib2']
    assert utils.parse_date_and_time_from_url(urls[0], 'rrfs') == ('20260930','00')


def test_local_writer_retains_members_and_replaces_rerun(tmp_path):
    from archiver_base import Archiver
    class Writer(Archiver):
        def fetch_file_list(self, *a): pass
        def process_files(self, *a): pass
    writer = Writer(None)
    path = tmp_path / 'archive.parquet'
    frame = pd.DataFrame({'station_id':['PAJN','PAJN'],'member_id':['m001','m002'],'value':[1.,2.]})
    writer.write_local_output(frame,path,dedup_columns=['station_id','member_id'])
    writer.write_local_output(frame.assign(value=[3.,4.]),path,dedup_columns=['station_id','member_id'])
    result = pd.read_parquet(path)
    assert len(result) == 2
    assert list(result.value) == [3.,4.]


def test_archiver_extracts_members_separately(monkeypatch):
    import model_archiver
    calls = []
    def extract(**kwargs):
        calls.append(kwargs)
        return pd.DataFrame({'station_id':['PAJN'],'precip_6h':[.1]})
    monkeypatch.setattr(model_archiver, 'extract_model_subset_parallel', extract)
    archiver = object.__new__(model_archiver.ModelArchiver)
    archiver.config = SimpleNamespace(MODEL='rrfsens', HERBIE_XARRAY_STRINGS={'precip6hr':{'rrfs':['APCP']}})
    archiver.wxelement = 'precip6hr'
    archiver.station_df = pd.DataFrame()
    result = archiver.process_files({'m001':['url1'],'m002':['url2'],'m003':[]})
    assert list(result.member_id) == ['m001','m002']
    assert all(call['model']=='rrfs' for call in calls)


def test_month_chunks_preserve_cycles_and_file_month(monkeypatch,tmp_path):
    import run_model_archiver as runner
    windows, outputs = [], []
    class FakeArchiver:
        def __init__(self,*a,**kw): pass
        def fetch_file_list(self,start,end):
            windows.append((start,end))
            return ['mock']
        def process_files(self,files):
            start,end = windows[-1]
            cycles = pd.date_range(start,end,freq='6h')
            return pd.DataFrame({'station_id':'PAJN','init_time':cycles,
                                 'valid_time':cycles+pd.Timedelta(hours=6),
                                 'forecast_hour':6,'member_id':'m001'})
        def write_local_output(self,df,path,dedup_columns):
            outputs.append((df,path,dedup_columns))
    monkeypatch.setattr(runner,'ModelArchiver',FakeArchiver)
    monkeypatch.setattr(runner.config,'TMP',str(tmp_path/'cache'))
    monkeypatch.setattr(runner.config,'MODEL_DIR',str(tmp_path/'model'))
    # Restore global config altered by the CLI function after this test.
    for name in ['MODEL','ELEMENT','USE_CLOUD_STORAGE']:
        monkeypatch.setattr(runner.config,name,getattr(runner.config,name))
    runner.run_monthly_archiving(pd.Timestamp('2026-09-30 18:00'),pd.Timestamp('2026-10-01 06:00'),
                                 'rrfsens','Wind',True)
    assert [list(df.init_time.dt.hour) for df,_,_ in outputs] == [[18],[0,6]]
    assert [Path(path).name for _,path,_ in outputs] == ['2026_09_archive.parquet','2026_10_archive.parquet']
    assert all('member_id' in keys for _,_,keys in outputs)


def test_s3_writer_uses_member_key(monkeypatch):
    import archiver_base
    import fsspec
    fs = fsspec.filesystem('memory')
    monkeypatch.setattr(archiver_base.fsspec,'filesystem',lambda *a,**kw:fs)
    class Writer(archiver_base.Archiver):
        def fetch_file_list(self,*a): pass
        def process_files(self,*a): pass
    writer=Writer(None)
    frame=pd.DataFrame({'station_id':['PAJN','PAJN'],'member_id':['m001','m002'],'value':[1.,2.]})
    path='/refs-test.parquet'
    writer.write_to_s3(frame,path,dedup_columns=['station_id','member_id'])
    writer.write_to_s3(frame.assign(value=[3.,4.]),path,dedup_columns=['station_id','member_id'])
    with fs.open(path,'rb') as stream:
        result=pd.read_parquet(stream)
    assert len(result)==2 and list(result.value)==[3.,4.]
