"""Regional routing and Alaska compatibility, with no network or credentials."""
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace
import numpy as np
import pandas as pd
import pytest
import archiver_config as config
import utils
from model_archiver import ModelArchiver
from obs_archiver import ObsArchiver
from ensemble_processing import assemble_refs, summarize_refs
from run_refs_processing import process_range
from region_config import region_root
from grid_utils import grid_coordinates, outside_hawaii_grid


@pytest.fixture(autouse=True)
def restore_region():
    config.configure_region('alaska')
    yield
    config.configure_region('alaska')


def test_configuration_resets_without_changing_alaska_paths():
    before=(config.MODEL_DIR, config.OBS, config.TMP, config.NDFD_DIR, config.NDFD_DICT)
    config.configure_region('hawaii')
    assert config.STATE == config.HERBIE_DOMAIN == 'hi'
    assert 'hrrr' not in config.HERBIE_MODELS
    assert {'rrfs','rrfsens','urma','nbm','nbmqmd'} <= set(config.HERBIE_MODELS)
    assert not any(f.startswith('snow') for fields in config.AVAILABLE_FIELDS.values() for f in fields)
    assert config.NDFD_DICT['Wind']['wspd']==['YCSZ98','YCSZ97']
    assert config.NDFD_DICT['rh']['rh']==['YRSZ98','YRSZ97']
    assert Path(config.MODEL_DIR).parts[-2:]==('hawaii','model')
    assert Path(config.OBS).parts[-2:]==('hawaii','obs')
    assert config.S3_URLS['rrfs'].endswith('/hawaii/rrfs/')
    for model in ('rrfs','rrfsens'):
        assert 3 in config.HERBIE_FORECASTS[model]['precip6hr']
    config.configure_region('alaska')
    assert before==(config.MODEL_DIR, config.OBS, config.TMP, config.NDFD_DIR, config.NDFD_DICT)
    assert 'hrrr' in config.HERBIE_MODELS


@pytest.mark.parametrize('model,member,suffix',[
    ('rrfs',None,'rrfs.20261006/00/rrfs.t00z.2dfld.2p5km.f006.hi.grib2'),
    ('rrfs','m001','rrfsens.20261006/00/m001/rrfs.t00z.m001.2dfldnomads.2p5km.f006.hi.grib2'),
    ('urma',None,'hiurma.20261006/hiurma.t00z.2dvaranl_ndfd.grb2'),
    ('nbm',None,'blend.20261006/00/core/blend.t00z.core.f006.hi.grib2'),
    ('nbmqmd',None,'blend.20261006/00/qmd/blend.t00z.qmd.f006.hi.grib2'),
])
def test_hawaii_urls(monkeypatch,model,member,suffix):
    monkeypatch.setattr(utils.requests,'head',lambda *a,**k:SimpleNamespace(ok=True))
    urls=utils.get_model_file_list('2026-10-06','2026-10-06',[6],'6h','https://example.org','Wind',model,'hi',member)
    assert urls==['https://example.org/'+suffix]


def test_hrrr_hawaii_rejected_before_network():
    with pytest.raises(ValueError,match='HRRR'):
        utils.get_model_file_list('2026-10-06','2026-10-06',[6],'6h','https://example.org','Wind','hrrr','hi')


def test_station_metadata_queries_hawaii_and_uses_own_cache(tmp_path,monkeypatch):
    import model_archiver
    config.configure_region('hawaii')
    monkeypatch.setattr(config,'OBS',str(tmp_path))
    monkeypatch.setattr(config,'API_KEY','dummy')
    seen=[]
    def fetch(**kw):
        seen.append(kw)
        return {'STATION':[{'STID':'PHNL','LATITUDE':'21.32','LONGITUDE':'-157.92'}]}
    monkeypatch.setattr(model_archiver,'create_all_station_metadata',fetch)
    a=ModelArchiver(config,wxelement='Wind')
    assert a.station_df.stid.tolist()==['PHNL']
    assert seen[0]['state']=='hi'
    assert (tmp_path/'hawaii_all_active_synoptic_station_metadata.csv').exists()
    ModelArchiver(config,wxelement='Wind')
    assert len(seen)==1


def test_obs_state(monkeypatch):
    import obs_archiver
    config.configure_region('hawaii')
    monkeypatch.setattr(config,'ELEMENT','Wind')
    calls=[]
    def fetch(url,params):
        calls.append(params)
        return SimpleNamespace(raise_for_status=lambda:None,json=lambda:{'STATION':[{'STID':'PHNL'}]})
    monkeypatch.setattr(obs_archiver.requests,'get',fetch)
    assert ObsArchiver(config).get_station_metadata()==['PHNL']
    assert calls[0]['state']=='hi'


def test_decoded_mercator_coordinates_and_outside_stations():
    lat,lon,key=grid_coordinates([21.3,21.325],[202.075,202.1],paired=False)
    assert utils.ll_to_index(21.325,-157.9,lat,lon)==(1,1)
    assert not outside_hawaii_grid(21.325,-157.9,lat,lon,1,1)
    assert outside_hawaii_grid(28,-177,lat,lon,1,1)
    assert grid_coordinates(lat+1,lon)[2]!=key


@pytest.mark.parametrize('lead,count',[(3,12),(42,12),(48,12),(54,12),(57,6),(60,6)])
def test_hawaii_membership_without_hrrr(tmp_path,lead,count):
    root=tmp_path/'hawaii'/'model'
    cycle=pd.Timestamp('2026-10-01',tz='UTC')
    result=assemble_refs(root,'rh',cycle,cycle+pd.Timedelta(hours=lead),region='hawaii',station_ids=['PHNL'])
    assert len(result)==count
    assert set(result.source_model)=={'rrfs','rrfsens'}
    assert not summarize_refs(result.assign(rh=np.nan),'rh').complete.any()


def test_hawaii_batch_month_boundary_and_idempotency(tmp_path):
    root=tmp_path/'hawaii'/'model'; dest=tmp_path/'hawaii'/'derived'/'refs'
    cycle=pd.Timestamp('2026-10-01',tz='UTC'); valid=cycle+pd.Timedelta(hours=6)
    for model,members in [('rrfs',['control']),('rrfsens',config.RRFS_MEMBERS)]:
        for lag in (0,6):
            init=cycle-pd.Timedelta(hours=lag)
            path=root/model/'rh'/f'{init:%Y_%m}_archive.parquet'; path.parent.mkdir(parents=True,exist_ok=True)
            pd.DataFrame([dict(station_id='PHNL',member_id=m,init_time=init,valid_time=valid,forecast_hour=6+lag,rh=40+lag) for m in members]).to_parquet(path,index=False)
    kwargs=dict(archive_root=root,output_root=dest,elements=['rh'],forecast_hours=[6],region='hawaii')
    paths=process_range(cycle,cycle+pd.Timedelta(hours=6),**kwargs)
    first=pd.read_parquet(paths[0]); assert paths[0].endswith('2026_10_archive.parquet')
    assert first.n_expected.tolist()==[12] and first.n_available.tolist()==[12]
    assert first.complete.all() and first['mean'].tolist()==[43]
    process_range(cycle,cycle+pd.Timedelta(hours=6),**kwargs)
    pd.testing.assert_frame_equal(first,pd.read_parquet(paths[0]))
    assert not (tmp_path/'model').exists()


def test_hawaii_cannot_write_into_default_alaska_roots():
    for root in ('model','derived/refs','s3://alaska-verification/derived/refs'):
        with pytest.raises(ValueError,match='hawaii'):
            region_root(root,'hawaii','archive')


@pytest.mark.parametrize('region', ['alaska','hawaii'])
def test_daily_region_selection(tmp_path,region):
    env={**os.environ,'DRY_RUN':'1','LOG_DIR':str(tmp_path),'REGION':'alaska'}
    for key in ('MODELS','REFS_ELEMENTS','NDFD_ELEMENTS','OBS_ELEMENTS','REFS_ARCHIVE_ROOT','REFS_OUTPUT_ROOT'):
        env.pop(key,None)
    result=subprocess.run(['bash','run_daily_archives.sh','2026-10-06','--region',region],env=env,text=True,capture_output=True)
    assert result.returncode==0,result.stderr
    output=result.stdout
    assert f'--region {region}' in output
    assert '--model rrfsens' in output and '--model urma' in output
    if region=='hawaii':
        assert '--model hrrr' not in output and 'snow' not in output
        assert '--archive-root hawaii/model --output-root hawaii/derived/refs' in output
    else:
        assert '--model hrrr' in output and 'snow6hr' in output
        assert '--archive-root model --output-root derived/refs' in output


def test_unsupported_daily_model_fails_before_processing(tmp_path):
    env={**os.environ,'DRY_RUN':'1','LOG_DIR':str(tmp_path),'MODELS':'hrrr'}
    r=subprocess.run(['bash','run_daily_archives.sh','--region','hawaii'],env=env,capture_output=True,text=True)
    assert r.returncode==2
    assert 'DRY_RUN:' not in r.stdout


def test_flattened_mercator_grid_keeps_paired_coordinates():
    lat,lon,_=grid_coordinates([21.3,21.3,21.325,21.325],[202.075,202.1,202.075,202.1])
    assert lat.shape==lon.shape==(1,4)
    assert utils.ll_to_index(21.325,-157.9,lat,lon)==(0,3)
    assert np.array([1,2,3,4]).reshape(lat.shape)[0,3]==4


def test_extract_flattened_hawaii_rrfs(monkeypatch):
    config.configure_region('hawaii')
    class Dataset(dict):
        def __getattr__(self,key): return self[key]
    ds=Dataset({k:SimpleNamespace(values=np.array(v)) for k,v in {
        'latitude':[21.3,21.3,21.325,21.325],
        'longitude':[202.075,202.1,202.075,202.1],
        'valid_time':np.datetime64('2026-10-06T06:00'),
        'u10':[1.,2.,3.,3.], 'v10':[1.,2.,3.,4.], 'gust':[2.,3.,4.,6.]
    }.items()})
    monkeypatch.setattr(utils.xr,'open_dataset',lambda *a,**k:ds)
    monkeypatch.setattr(utils,'download_subset',lambda **kw:kw['local_filename'])
    stations=pd.DataFrame([dict(stid='PHNL',latitude=21.325,longitude=-157.9),
                           dict(stid='REMOTE',latitude=28.,longitude=-177.)])
    frame=utils.extract_model_subset_parallel(
        ['https://example.org/rrfs.20261006/00/rrfs.t00z.2dfld.2p5km.f006.hi.grib2'],
        stations,config.HERBIE_XARRAY_STRINGS['Wind']['rrfs'],'Wind','rrfs',config)
    assert frame.station_id.tolist()==['PHNL']
    assert frame.wind_speed_kt.tolist()==[9.72]
    assert frame.forecast_hour.tolist()==[6]


def test_ndfd_flattened_hawaii_grid(monkeypatch):
    from contextlib import contextmanager
    config.configure_region('hawaii')
    monkeypatch.setattr(config,'ELEMENT','Wind')
    class Dataset(dict):
        def __getattr__(self,key): return self[key]
    common={k:SimpleNamespace(values=np.array(v)) for k,v in {
        'latitude':[21.3,21.3,21.325,21.325],
        'longitude':[202.075,202.1,202.075,202.1],
        'step':[np.timedelta64(6,'h'),np.timedelta64(12,'h')],
        'valid_time':[np.datetime64('2026-10-06T06:00'),np.datetime64('2026-10-06T12:00')],
    }.items()}
    speed=Dataset(**common,si10=SimpleNamespace(values=np.array([[1.,2.,3.,4.],[2.,3.,4.,5.]])))
    direction=Dataset(wdir10=SimpleNamespace(values=np.array([[60.,60.,60.,60.],[90.,90.,90.,90.]])))
    @contextmanager
    def opened(url,**kw): yield SimpleNamespace(name=url)
    monkeypatch.setattr(utils.fsspec,'open',opened)
    monkeypatch.setattr(utils.xr,'open_dataset',lambda name,**kw:direction if 'wdir' in name else speed)
    stations=pd.DataFrame([dict(stid='PHNL',latitude=21.325,longitude=-157.9)])
    result=utils.process_file_pair('bucket/wspd','bucket/wdir',stations,'/tmp',['si10','wdir10'])
    assert result.wind_speed_kt.tolist()==[7.78,9.72]
    assert result.wind_dir_deg.tolist()==[60.,90.]
    assert result.forecast_hour.tolist()==[6,12]
