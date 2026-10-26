"""Deterministic QMD selection, interval decoding, and NBM archive identity."""
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
import ast
import os
import shutil
import tempfile

import numpy as np
import pandas as pd
import pytest

import archiver_config as config
from archiver_base import Archiver
from model_archiver import ModelArchiver
from nbm_deterministic import ELEMENTS, INTERVALS, deterministic_ranges, decode_field, source_model
import utils


@pytest.fixture(autouse=True)
def restore_config():
    config.configure_region('alaska')
    yield
    config.configure_region('alaska')


@pytest.mark.parametrize('element,variable,level,period', [
    ('precip6hr', 'APCP', 'surface', '30-36 hour acc fcst'),
    ('precip24hr', 'APCP', 'surface', '12-36 hour acc fcst'),
    ('maxt', 'TMP', '2 m above ground', '18-36 hour max fcst'),
    ('mint', 'TMP', '2 m above ground', '18-36 hour min fcst'),
])
def test_index_selects_only_exact_deterministic_interval(element, variable, level, period):
    records = [
        f'{variable}:{level}:{period}:5% level',
        f'{variable}:{level}:{period}:prob >2.54:prob fcst 255/255',
        f'{variable}:{level}:{period}:ens mean',
        f'{variable}:{level}:{period}:ens std dev',
        f'{variable}:{level}:24-36 hour acc fcst:',
        f'{variable}:{level}:{period}:',
        f'{variable}:{level}:{period}:50% level',
    ]
    lines = [f'{i+1}:{i*100}:d=2026102500:{r}' for i, r in enumerate(records)]
    assert list(deterministic_ranges(lines, element, 36)) == ['500-599']
    assert not deterministic_ranges(lines[:5], element, 36)


def test_day_labels_and_missing_or_ambiguous_fields():
    lines = ['1:0:d=2026102500:APCP:surface:0-1 day acc fcst:',
             '2:100:d=2026102500:APCP:surface:0-24 hour acc fcst:50% level']
    assert list(deterministic_ranges(lines, 'precip24hr', 24)) == ['0-99']
    assert not deterministic_ranges(lines, 'precip24hr', 6)
    assert not deterministic_ranges(lines, 'precip6hr', 24)
    with pytest.raises(ValueError, match='Multiple deterministic'):
        deterministic_ranges(lines + ['3:200:d=2026102500:APCP:surface:0-24 hour acc fcst:'], 'precip24hr', 24)


class Message(dict):
    def __init__(self, element, lead):
        precip = element.startswith('precip')
        reference = datetime(2026, 10, 25)
        endpoint = reference + timedelta(hours=lead)
        super().__init__(dataDate=20261025, dataTime=0, productDefinitionTemplateNumber=8,
            stepType={'maxt':'max','mint':'min'}.get(element,'accum'), stepUnits=1,
            startStep=lead-INTERVALS[element], endStep=lead,
            discipline=0, parameterCategory=1 if precip else 0,
            parameterNumber=8 if precip else 0, typeOfLevel='surface' if precip else 'heightAboveGround',
            level=0 if precip else 2, units='kg m**-2' if precip else 'K')
        for name, value in zip(('year','month','day','hour','minute','second'),
                               (endpoint.year,endpoint.month,endpoint.day,endpoint.hour,0,0)):
            self[name+'OfEndOfOverallTimeInterval'] = value
        self.values = np.ma.array([[25.4, 50.8], [0, 100]], mask=[[False, True], [False, False]]) if precip else np.array([[273.15,283.15],[263.15,293.15]])
        # An intentionally misleading validDate must never shift initialization.
        self.validDate = reference + timedelta(hours=lead-INTERVALS[element])

    def latlons(self):
        return np.array([[58,58],[59,59]]), np.array([[-135,-134],[-135,-134]])


@pytest.mark.parametrize('element', ELEMENTS)
def test_decoder_uses_grib_reference_and_endpoint_and_correct_units(element):
    g = Message(element, 36)
    reference, endpoint, values = decode_field(g, element, 36)
    assert reference == datetime(2026,10,25)
    assert endpoint == datetime(2026,10,26,12)
    assert values[0,0] == (1 if element.startswith('precip') else 32)
    if element.startswith('precip'):
        assert np.isnan(values[0,1])
    else:
        assert values[1,0] == 14
    g['productDefinitionTemplateNumber'] = 10
    with pytest.raises(ValueError, match='deterministic'):
        decode_field(g, element, 36)
    g['productDefinitionTemplateNumber'] = 8
    g['startStep'] -= 6
    with pytest.raises(ValueError, match='interval'):
        decode_field(g, element, 36)


@pytest.mark.parametrize('key,value', [
    ('parameterNumber', 0), ('typeOfLevel', 'heightAboveGround'),
    ('units', 'm'), ('stepUnits', 2), ('stepType', 'avg'),
])
def test_decoder_rejects_wrong_quantity_units_and_statistic(key, value):
    g=Message('precip6hr',36)
    g[key]=value
    with pytest.raises(ValueError):
        decode_field(g,'precip6hr',36)


@pytest.mark.parametrize('region', ['alaska','hawaii'])
def test_source_routing_schedules_and_classification(region, tmp_path, monkeypatch):
    config.configure_region(region)
    for element in ELEMENTS:
        assert config.HERBIE_FORECASTS['nbm'][element] == config.HERBIE_FORECASTS['nbmqmd'][element]
        assert element in config.AVAILABLE_FIELDS['nbm']
        assert element not in config.PROBABILISTIC_ELEMENTS['nbm']
        assert element in config.PROBABILISTIC_ELEMENTS['nbmqmd']
        assert source_model('nbm', element) == 'nbmqmd'
    assert source_model('nbm', 'Wind') == 'nbm'
    assert config.HERBIE_FORECASTS['nbm']['Wind'][0] == 2
    config.MODEL='nbm'
    monkeypatch.setattr(ModelArchiver, 'ensure_model_metadata', lambda self: pd.DataFrame({'stid':['PAJN']}))
    monkeypatch.setattr(config, 'OBS', str(tmp_path))
    monkeypatch.setattr(utils.requests, 'head', lambda *a, **k: SimpleNamespace(ok=True))
    archiver=ModelArchiver(config, wxelement='precip24hr')
    urls=archiver.fetch_file_list(pd.Timestamp('2026-10-25'), pd.Timestamp('2026-10-25 12:00'))
    assert len(urls) == 2 * len(config.HERBIE_FORECASTS['nbmqmd']['precip24hr'])
    assert all('/qmd/blend.t' in u for u in urls)
    assert any('/00/qmd/' in u for u in urls) and any('/12/qmd/' in u for u in urls)
    assert all(u.endswith(f'.{config.HERBIE_DOMAIN}.grib2') for u in urls)


@pytest.mark.parametrize('element', ELEMENTS)
@pytest.mark.parametrize('locked_download', [False, True])
def test_real_extractor_outputs_deterministic_columns(element, locked_download, monkeypatch, tmp_path, capsys):
    config.MODEL='nbm'
    config.ELEMENT=element
    message=Message(element,36)
    class Messages:
        def __enter__(self): return iter([message])
        def __exit__(self,*a): return False
    monkeypatch.setattr(utils.pygrib,'open',lambda path: Messages())
    def download(**kwargs):
        p=Path(kwargs['local_filename']);p.write_bytes(b'fake-grib');return str(p)
    monkeypatch.setattr(utils,'download_subset',download)
    monkeypatch.setattr(tempfile,'tempdir',str(tmp_path))
    if locked_download:
        original_unlink = os.unlink
        def unlink(path, *args, **kwargs):
            if str(path).endswith('.grib2'):
                raise PermissionError(13, 'File is being used by another process', str(path))
            return original_unlink(path, *args, **kwargs)
        monkeypatch.setattr(os, 'unlink', unlink)
    result=utils.extract_model_subset_parallel(
        ['https://example.test/blend.20261025/00/qmd/blend.t00z.qmd.f036.ak.grib2'],
        pd.DataFrame({'stid':['NKXA2'],'latitude':[58.0],'longitude':[-135.0]}),
        config.HERBIE_XARRAY_STRINGS[element]['nbm'], element,'nbm',config)
    assert result.columns.tolist()==['station_id','init_time','valid_time','forecast_hour',element]
    assert result[element].tolist()==[1 if element.startswith('precip') else 32]
    assert result.init_time.iloc[0]==pd.Timestamp('2026-10-25')
    assert result.valid_time.iloc[0]==pd.Timestamp('2026-10-26 12:00')
    if locked_download:
        assert list(tmp_path.glob('model_downloads_*/*.grib2'))
        assert 'Continuing with extracted forecasts' in capsys.readouterr().out
        # Exercise the real archive writer after cleanup encounters the lock.
        output = tmp_path / 'model' / 'nbm' / element / '2026_10_archive.parquet'
        Archiver.write_local_output(SimpleNamespace(), result, str(output),
            dedup_columns=['station_id', 'init_time', 'valid_time', 'forecast_hour'])
        pd.testing.assert_frame_equal(pd.read_parquet(output), result)
    else:
        assert not list(tmp_path.glob('model_downloads_*'))


def test_download_subset_preserves_nbmqmd_percentiles(monkeypatch, tmp_path):
    lines='\n'.join([
        '1:0:d=2026102500:APCP:surface:30-36 hour acc fcst:5% level',
        '2:100:d=2026102500:APCP:surface:30-36 hour acc fcst:',
        '3:200:d=2026102500:APCP:surface:30-36 hour acc fcst:ens mean'])
    seen=[]
    def get(url,**kwargs):
        if url.endswith('.idx'):
            return SimpleNamespace(ok=True,text=lines,close=lambda:None)
        seen.append(kwargs['headers']['Range'])
        return SimpleNamespace(status_code=206,content=b'GRIB',close=lambda:None)
    monkeypatch.setattr(utils,'get_with_retry',get)
    for model in ('nbm','nbmqmd'):
        utils.download_subset('https://example.test/blend.f036.ak.grib2',str(tmp_path/f'{model}.grib2'),
            [':APCP:surface:'],model,'precip6hr')
    assert seen==['bytes=100-199','bytes=0-99']


@pytest.mark.parametrize('element', ELEMENTS)
def test_runner_writes_nbm_months_with_qmd_cycle_alignment(element,tmp_path):
    config_dict=dict(TMP=str(tmp_path/'tmp'),MODEL_DIR=str(tmp_path/'model'),
        configure_region=lambda r:None,HERBIE_MODELS=['nbm'],AVAILABLE_FIELDS={'nbm':[element]},
        NBM_START_HOURS={'nbm':[1,7,13,19],'nbmqmd':[0,6,12,18]},
        HERBIE_CYCLES={'nbm':'6h','nbmqmd':'12h'})
    cfg=SimpleNamespace(**config_dict)
    windows=[]
    def fetch(start,end): windows.append((start,end));return [start]
    def process(urls):
        return pd.DataFrame({'station_id':['NKXA2'],'init_time':urls,
            'valid_time':[urls[0]+pd.Timedelta(hours=24)],'forecast_hour':[24],element:[1.]})
    fake=SimpleNamespace(fetch_file_list=fetch,process_files=process,
        write_local_output=lambda df,path,**kw:Archiver.write_local_output(SimpleNamespace(),df,path,**kw))
    tree=ast.parse((Path(__file__).parents[1]/'run_model_archiver.py').read_text())
    method=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='run_monthly_archiving')
    ns=dict(config=cfg,tempfile=SimpleNamespace(),pd=pd,os=os,shutil=shutil,
            ModelArchiver=lambda *a,**k:fake)
    exec(compile(ast.Module(body=[method],type_ignores=[]),'runner','exec'),ns)
    ns[method.name](pd.Timestamp('2026-06-30'),pd.Timestamp('2026-07-01 12:00'),'nbm',element,True)
    assert [s for s,_ in windows]==[pd.Timestamp('2026-06-30'),pd.Timestamp('2026-07-01')]
    for month in ('2026_06','2026_07'):
        p=tmp_path/'model'/'nbm'/element/f'{month}_archive.parquet'
        assert pd.read_parquet(p)[element].tolist()==[1.]
    assert not (tmp_path/'model'/'nbmqmd').exists()
