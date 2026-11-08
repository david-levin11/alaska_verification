"""Daily MinRH boundaries, sensor selection, API errors and archive routing."""
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace
import pandas as pd
import pytest
import obs_archiver
from obs_archiver import ObsArchiver
from obs_minrh import aggregate_minrh,minrh_endpoints


def station(times,values,stid='A',**sensors):
    return dict(STID=stid,LATITUDE='58.3',LONGITUDE='-134.5',ELEVATION='20',
        OBSERVATIONS=dict(date_time=[pd.Timestamp(t).isoformat() for t in times],
            relative_humidity_set_1=values,**sensors))


def response(payload):
    return SimpleNamespace(raise_for_status=lambda:None,json=lambda:payload)


@pytest.fixture
def source(monkeypatch):
    archiver=ObsArchiver.__new__(ObsArchiver)
    archiver.url='https://example.test/timeseries'
    archiver.api_token='TEST_SECRET'
    archiver.hfmetar='0'
    archiver.initial_wait=0
    archiver.max_retries=2
    archiver.station_metadata={'A':{'zone':'AKZ325','cwa':'AJK'}}
    calls=[]
    mutate=[lambda rows:rows]
    def get(url,params,timeout):
        calls.append(params)
        assert timeout==90
        times=pd.date_range(pd.to_datetime(params['start'],format='%Y%m%d%H%M',utc=True),
            pd.to_datetime(params['end'],format='%Y%m%d%H%M',utc=True),freq='h')
        # The lower daytime values must never enter the overnight minimum.
        values=[5 if 6 <= t.hour < 18 else 40+t.hour for t in times]
        rows=[station(times,values,stid) for stid in params['stid'].split(',')]
        return response(dict(SUMMARY={'RESPONSE_CODE':1},UNITS={'relative_humidity':'%'},STATION=mutate[0](rows)))
    monkeypatch.setattr(obs_archiver.requests,'get',get)
    monkeypatch.setattr(obs_archiver,'sleep',lambda wait:None)
    return archiver,calls,mutate


def test_endpoint_window_warmup_and_percent_minimum(source):
    a,calls,_=source
    df=a.fetch_minrh_18to06_timeseries(['A'],'202605010000','202605012359')
    assert len(calls)==1
    assert calls[0]['start']=='202604301800' and calls[0]['end']=='202605010600'
    assert calls[0]['vars']=='relative_humidity'
    assert not {'state','status','network'} & calls[0].keys()
    assert df.minrh.tolist()==[40.]
    assert df.rh_units.tolist()==['%'] and df.n_obs.tolist()==[12]
    assert df.coverage_ok.tolist()==[True] and df.max_gap_minutes.tolist()==[60.]
    assert df.window_start.iloc[0]==pd.Timestamp('2026-04-30 18:00Z')
    assert df.window_end.iloc[0]==df.valid_time.iloc[0]==pd.Timestamp('2026-05-01 06:00Z')
    assert df.last_report.iloc[0]==pd.Timestamp('2026-05-01 05:00Z')
    assert df.NWSCWA.tolist()==['AJK']


def test_monthly_and_daily_requests_agree(source):
    a,_,_=source
    whole=a.fetch_minrh_18to06_timeseries(['A'],'202605310000','202606022359')
    daily=pd.concat([a.fetch_minrh_18to06_timeseries(['A'],d+'0000',d+'2359')
        for d in ['20260531','20260601','20260602']],ignore_index=True)
    pd.testing.assert_frame_equal(whole,daily)
    assert whole.date.dt.strftime('%Y-%m-%d').tolist()==['2026-05-31','2026-06-01','2026-06-02']


def test_empty_or_non_aligned_endpoint_ranges(source):
    a,calls,_=source
    assert a.fetch_minrh_18to06_timeseries(['A'],'202605010601','202605012359').empty
    assert not calls
    assert minrh_endpoints('2026-05-01 06:01','2026-05-02 06:00').tolist()==[pd.Timestamp('2026-05-02 06:00Z')]
    with pytest.raises(ValueError):minrh_endpoints('20260502','20260501')


def test_sensor_fallback_invalid_values_and_real_zero():
    times=pd.date_range('2026-05-01 18:00Z',periods=12,freq='h')
    st=station(times,[None]*12,relative_humidity_set_1d=[None,-1,101,float('inf'),False,70,65,0,55,50,49,48])
    df=aggregate_minrh([st],minrh_endpoints('202605020000','202605022359'),{})
    assert df.minrh.tolist()==[0.] and df.n_obs.tolist()==[7]
    assert df.rh_sensor.tolist()==['relative_humidity_set_1d']
    assert df.coverage_ok.tolist()==[False]
    st['OBSERVATIONS']['relative_humidity_set_1']=[75]*12
    df=aggregate_minrh([st],minrh_endpoints('202605020000','202605022359'),{})
    assert df.minrh.tolist()==[75.]  # Do not combine sensors to reduce the min.


def test_sparse_window_does_not_discard_later_valid_windows(source):
    a,_,mutate=source
    def missing(rows):
        for s in rows:
            obs=s['OBSERVATIONS']
            obs['relative_humidity_set_1']=[v if pd.Timestamp(t).day!=1 or pd.Timestamp(t).hour!=0 else None
                for t,v in zip(obs['date_time'],obs['relative_humidity_set_1'])]
        return rows
    mutate[0]=missing
    df=a.fetch_minrh_18to06_timeseries(['A'],'202605010000','202605032359')
    assert len(df)==3 and df.n_obs.tolist()==[11,12,12]
    assert df.coverage_ok.tolist()==[False,True,True]


def test_sensor_fallback_is_per_window_and_independent_of_chunk_size():
    times=pd.date_range('2026-05-01 18:00Z','2026-05-03 06:00Z',freq='h')
    primary=[None if t < pd.Timestamp('2026-05-02 18:00Z') else 75 for t in times]
    st=station(times,primary,relative_humidity_set_1d=[60]*len(times))
    whole=aggregate_minrh([st],minrh_endpoints('202605020000','202605032359'),{})
    days=pd.concat([aggregate_minrh([st],minrh_endpoints(d+'0000',d+'2359'),{})
        for d in ['20260502','20260503']],ignore_index=True)
    pd.testing.assert_frame_equal(whole,days)
    assert whole.minrh.tolist()==[60.,75.]
    assert whole.rh_sensor.tolist()==['relative_humidity_set_1d','relative_humidity_set_1']


def test_station_batches_and_duplicate_input_ids(source):
    a,calls,_=source
    ids=['A']+[f'S{i}' for i in range(50)]
    df=a.fetch_minrh_18to06_timeseries(ids+['A'],'202605010000','202605012359')
    assert len(calls)==2 and len(df)==51
    assert set(df.stid)==set(ids)


@pytest.mark.parametrize('payload',[{'SUMMARY':{'RESPONSE_CODE':5}}, {'STATION':[]},
    {'SUMMARY':{'RESPONSE_CODE':1},'STATION':[],'UNITS':{'relative_humidity':'fraction'}}])
def test_api_failures_stop_chunk_without_exposing_token(source,monkeypatch,capsys,payload):
    a,_,_=source
    monkeypatch.setattr(obs_archiver.requests,'get',lambda *args,**kwargs:response(payload))
    with pytest.raises(RuntimeError,match='batch failed'):
        a.fetch_minrh_18to06_timeseries(['A'],'20260501','20260502')
    assert 'TEST_SECRET' not in capsys.readouterr().out


def test_api_no_data_is_empty(source,monkeypatch):
    a,_,_=source
    monkeypatch.setattr(obs_archiver.requests,'get',lambda *args,**kwargs:response({'SUMMARY':{'RESPONSE_CODE':2}}))
    assert a.fetch_minrh_18to06_timeseries(['A'],'20260501','20260502').empty


def test_later_batch_failure_never_returns_partial_station_set(source,monkeypatch,capsys):
    a,_,_=source
    original=obs_archiver.requests.get
    def get(url,params,timeout):
        if params['stid']=='LAST':
            raise RuntimeError('https://example.test?token=TEST_SECRET')
        return original(url,params=params,timeout=timeout)
    monkeypatch.setattr(obs_archiver.requests,'get',get)
    with pytest.raises(RuntimeError,match='stations=LAST'):
        a.fetch_minrh_18to06_timeseries([f'S{i}' for i in range(50)]+['LAST'],'20260501','20260502')
    assert 'TEST_SECRET' not in capsys.readouterr().out


@pytest.mark.parametrize('region',['alaska','hawaii'])
def test_daily_wrapper_includes_observed_minrh(region,tmp_path):
    env={**os.environ,'DRY_RUN':'1','MODELS':'nbm','RUN_REFS':'0',
        'RUN_NDFD':'0','RUN_OBS':'1','LOG_DIR':str(tmp_path)}
    script=Path(__file__).parents[1]/'run_daily_archives.sh'
    result=subprocess.run(['bash',str(script),'--region',region,'2026-11-05'],
        env=env,capture_output=True,text=True)
    assert result.returncode==0,result.stdout+result.stderr
    lines=[line for line in result.stdout.splitlines() if 'DRY_RUN:' in line
        and 'run_obs_archiver.py' in line and '--element minrh ' in line]
    assert len(lines)==1


@pytest.mark.parametrize('region',['alaska','hawaii'])
@pytest.mark.parametrize('local',[True,False])
def test_real_runner_month_boundaries_writers_and_repeat_dedup(source,monkeypatch,tmp_path,region,local):
    import archiver_config as config
    import run_obs_archiver as runner
    a,calls,_=source
    root=tmp_path/region
    root.mkdir()
    cfg=SimpleNamespace(**vars(config))
    cfg.configure_region=lambda r:None
    cfg.API_KEY='TEST_SECRET';cfg.OBS=str(root);cfg.MODEL_DIR=str(root/'model')
    cfg.S3_URLS={'obs':str(root)+'/'}
    monkeypatch.setattr(runner,'config',cfg)
    monkeypatch.setattr(ObsArchiver,'get_station_metadata',lambda self:['A'])
    # Route cloud writes to disk using the real S3 writer's append logic.
    import archiver_base
    monkeypatch.setattr(archiver_base.fsspec,'filesystem',lambda *args,**kwargs:
        SimpleNamespace(exists=lambda p:Path(p).exists(),open=lambda p,mode:open(p,mode)))
    for _ in range(2):
        runner.run_monthly_obs_archiving(pd.Timestamp('2026-05-15'),pd.Timestamp('2026-06-02 23:59'),'minrh',local,region)
    for month,count in [('2026_05',17),('2026_06',2)]:
        p=root/'model'/'obs'/'minrh'/f'{month}_archive.parquet' if local else root/f'{month}_obs_minrh_archive.parquet'
        df=pd.read_parquet(p)
        assert len(df)==count and df.minrh.eq(40).all()
        assert df.window_end.dt.strftime('%Y_%m').eq(month).all()
        assert not df.duplicated(['stid','date','window_start','window_end']).any()
    assert calls[0]['start']=='202605141800' and calls[1]['start']=='202605311800'
