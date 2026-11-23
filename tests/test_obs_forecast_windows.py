"""Exact native bounds, 12h reference windows, API behavior and safe writes."""
from pathlib import Path
from types import SimpleNamespace
import pandas as pd
import pytest

from obs_forecast_windows import (validate_periods,native_periods,standard_periods,
    aggregate_windows,fetch_windows,observation_plan)
from experimental_ndfd import install_plan


def period(start,end,stid='A'):
    return dict(station_id=stid,window_start=start,window_end=end)


def station(times,values,**sensors):
    return dict(STID='A',LATITUDE='58',LONGITUDE='-135',ELEVATION='10',
        OBSERVATIONS=dict(date_time=[pd.Timestamp(t).isoformat() for t in times],
            air_temp_set_1=values,**sensors))


@pytest.mark.parametrize('element,hour',[('maxt',0),('mint',12),('minrh',6)])
def test_standard_bounds_endpoints_and_month_boundary(element,hour):
    d=standard_periods(['A'],element,'2026-06-01','2026-06-01 23:59')
    assert len(d)==1
    assert d.window_end.iloc[0]==pd.Timestamp('2026-06-01',tz='UTC')+pd.Timedelta(hours=hour)
    assert (d.window_end-d.window_start).dt.total_seconds().eq(12*3600).all()


def test_native_periods_scan_initialization_month_preserve_partial_and_deduplicate(tmp_path):
    directory=tmp_path/'maxt_native';directory.mkdir()
    d=pd.DataFrame([period('2026-05-31 14:00','2026-06-01 05:00'),
        period('2026-05-31 14:00','2026-06-01 05:00'),
        period('2026-06-01 00:00','2026-06-01 05:00')])
    d.to_parquet(directory/'2026_05_archive.parquet')
    out=native_periods(tmp_path,'maxt','2026-06-01','2026-06-01 23:59')
    assert len(out)==2
    assert set((out.window_end-out.window_start).dt.total_seconds()/3600)=={5,15}
    with pytest.raises(ValueError,match='No mint_native'):
        native_periods(tmp_path,'mint','2026-06-01','2026-06-02')


@pytest.mark.parametrize('element,begin,end,expected',[
    ('maxt','2026-11-09 14:00Z','2026-11-10 05:00Z',16),
    ('mint','2026-11-09 02:00Z','2026-11-09 20:00Z',2),
    ('maxt','2026-11-09 00:00Z','2026-11-09 05:00Z',4),
])
def test_aggregation_exact_bounds_and_end_exclusion(element,begin,end,expected):
    times=pd.date_range('2026-11-09 00:00Z','2026-11-10 06:00Z',freq='h')
    values=list(range(len(times)))
    # A huge endpoint value would change an incorrectly inclusive max/min.
    values[times.get_loc(pd.Timestamp(end))]=1000 if element=='maxt' else -1000
    p=pd.DataFrame([period(begin,end)])
    if element=='maxt' and begin.endswith('14:00Z'):
        expected=28
    d=aggregate_windows([station(times,values)],p,element,{'A':{'cwa':'AJK'}})
    assert d[element].iloc[0]==expected
    assert d.valid_time.iloc[0]==pd.Timestamp(end)
    assert d.last_report.iloc[0]<pd.Timestamp(end)
    assert d.coverage_ok.iloc[0]
    assert d.NWSCWA.iloc[0]=='AJK'


def test_sensor_qc_sparse_periods_and_fallback():
    times=pd.date_range('2026-11-09 00:00Z',periods=13,freq='h')
    source=station(times,[False,float('inf')]+[None]*11,air_temp_set_1d=list(range(13)))
    periods=pd.DataFrame([period('2026-11-09','2026-11-09 12:00')])
    d=aggregate_windows([source],periods,'mint',{})
    assert d.mint.tolist()==[0.]
    assert d.sensor.tolist()==['air_temp_set_1d']
    source['OBSERVATIONS']['air_temp_set_1']=[50]+[None]*12
    d=aggregate_windows([source],periods,'mint',{})
    assert d.mint.tolist()==[50.] and d.n_obs.tolist()==[1]
    assert d.coverage_ok.tolist()==[False]
    assert aggregate_windows([source],periods.assign(station_id='B'),'mint',{}).empty


def test_native_minrh_18h_and_partial_6h():
    times=pd.date_range('2026-11-09 00:00Z','2026-11-10 06:00Z',freq='h')
    source=station(times,[100]*len(times),relative_humidity_set_1=[-1,False]+[65]*(len(times)-2))
    periods=pd.DataFrame([period('2026-11-09 00:00','2026-11-09 06:00'),
        period('2026-11-09 12:00','2026-11-10 06:00')])
    d=aggregate_windows([source],periods,'minrh',{})
    assert d.window_hours.tolist()==[6,18]
    assert d.minrh.tolist()==[65.,65.]
    assert d.n_obs.tolist()==[4,18]


@pytest.fixture
def source(monkeypatch):
    import requests
    calls=[]
    def get(url,params,timeout):
        calls.append(params)
        times=pd.date_range(pd.to_datetime(params['start'],format='%Y%m%d%H%M',utc=True),
            pd.to_datetime(params['end'],format='%Y%m%d%H%M',utc=True),freq='h')
        return SimpleNamespace(raise_for_status=lambda:None,json=lambda:dict(
            SUMMARY={'RESPONSE_CODE':1},UNITS={'air_temp':'F'},
            STATION=[station(times,list(range(len(times))))]))
    monkeypatch.setattr(requests,'get',get)
    archiver=SimpleNamespace(url='https://example.test',api_token='DO_NOT_PRINT',hfmetar='0',
        initial_wait=0,max_retries=2,station_metadata={},_chunk_station_ids=lambda ids:[ids])
    return archiver,calls


def test_fetch_warmup_real_units_and_unchanged_rerun(source,tmp_path):
    archiver,calls=source
    p=standard_periods(['A'],'maxt','2026-06-01','2026-06-01 23:59')
    d=fetch_windows(archiver,p,'maxt')
    assert calls[0]['start']=='202605311200' and calls[0]['end']=='202606010000'
    assert calls[0]['vars']=='air_temp'
    assert d.maxt.tolist()==[11.]
    plan=observation_plan(d,tmp_path,'obs_12hr','maxt')
    assert plan[0][0]==tmp_path/'obs_12hr/maxt/2026_06_archive.parquet'
    manifest=install_plan(plan,{},tmp_path,model='obs_12hr')
    assert manifest.exists()
    assert not observation_plan(d,tmp_path,'obs_12hr','maxt')
    changed=d.assign(maxt=99,tmax=99)
    with pytest.raises(ValueError,match='Changed observations'):
        observation_plan(changed,tmp_path,'obs_12hr','maxt')
    install_plan(observation_plan(changed,tmp_path,'obs_12hr','maxt',replace_conflicts=True),{},tmp_path,model='obs_12hr')
    assert pd.read_parquet(plan[0][0]).maxt.tolist()==[99.]


@pytest.mark.parametrize('payload',[
    {'SUMMARY':{'RESPONSE_CODE':1},'STATION':[],'UNITS':{'air_temp':'C'}},
    {'SUMMARY':{'RESPONSE_CODE':5},'STATION':[]},
])
def test_api_failure_retries_without_exposing_key(source,monkeypatch,capsys,payload):
    import requests
    archiver,_=source
    calls=[]
    def get(*args,**kwargs):
        calls.append(1)
        return SimpleNamespace(raise_for_status=lambda:None,json=lambda:payload)
    monkeypatch.setattr(requests,'get',get)
    p=standard_periods(['A'],'maxt','2026-06-01','2026-06-01 23:59')
    with pytest.raises(RuntimeError,match='no partial batch'):
        fetch_windows(archiver,p,'maxt')
    assert len(calls)==2
    assert 'DO_NOT_PRINT' not in capsys.readouterr().out


def test_real_cli_routes_both_profiles_to_separate_archives(source,tmp_path,monkeypatch):
    import archiver_config as config
    import run_obs_forecast_windows as runner
    from obs_archiver import ObsArchiver
    archiver,calls=source
    monkeypatch.setattr(config,'API_KEY','TEST')
    monkeypatch.setattr(ObsArchiver,'get_station_metadata',lambda self:['A'])
    for key in ['url','api_token','hfmetar','initial_wait','max_retries','station_metadata']:
        monkeypatch.setattr(ObsArchiver,key,getattr(archiver,key),raising=False)
    original=ObsArchiver.__init__
    def init(self,cfg):
        original(self,cfg)
        self.station_metadata={}
        self.initial_wait=0
    monkeypatch.setattr(ObsArchiver,'__init__',init)
    directory=tmp_path/'ndfd_exp/maxt_native';directory.mkdir(parents=True)
    pd.DataFrame([period('2026-05-31 14:00','2026-06-01 05:00')]).to_parquet(directory/'2026_05_archive.parquet')
    common=['--archive-root',str(tmp_path),'--start','2026-06-01','--end','2026-06-01 23:59','--elements','maxt']
    assert runner.main(common)==0
    assert not (tmp_path/'obs_exp').exists()
    # The current metadata inventory may no longer list a historical ID.
    monkeypatch.setattr(ObsArchiver,'get_station_metadata',lambda self:[])
    assert runner.main(common+['--apply'])==0
    assert pd.read_parquet(tmp_path/'obs_exp/maxt/2026_06_archive.parquet').window_hours.tolist()==[15.]
    monkeypatch.setattr(ObsArchiver,'get_station_metadata',lambda self:['A'])
    assert runner.main(common+['--profile','12hr','--apply'])==0
    assert pd.read_parquet(tmp_path/'obs_12hr/maxt/2026_06_archive.parquet').window_hours.tolist()==[12.]


@pytest.mark.parametrize('begin,end',[
    ('2026-11-09','2026-11-09'),('2026-11-10','2026-11-09'),
    ('2026-11-09','2026-11-12'),(None,'2026-11-10'),
])
def test_invalid_periods_are_rejected(begin,end):
    with pytest.raises(ValueError,match='Invalid forecast'):
        validate_periods(pd.DataFrame([period(begin,end)]))


def test_no_data_response_and_all_missing_windows(source,monkeypatch):
    import requests
    archiver,_=source
    monkeypatch.setattr(requests,'get',lambda *a,**k:SimpleNamespace(
        raise_for_status=lambda:None,json=lambda:{'SUMMARY':{'RESPONSE_CODE':2}}))
    p=standard_periods(['A'],'mint','2026-06-01','2026-06-01 23:59')
    assert fetch_windows(archiver,p,'mint').empty
    times=pd.date_range('2026-06-01',periods=12,freq='h',tz='UTC')
    assert aggregate_windows([station(times,[None]*12)],p,'mint',{}).empty
