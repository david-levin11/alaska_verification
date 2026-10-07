import os
from pathlib import Path
import subprocess
import pandas as pd
import pytest
import fsspec
import refs_archive_io as io
import ensemble_processing as ep
from run_refs_processing import process_range, utc_time


def make_sources(root, cycles):
    for model, ids in [('rrfs',['control']),('hrrr',['control']),('rrfsens',ep.PERTURBED_MEMBERS)]:
        records=[]
        for cycle in sorted(set(cycles)|{c-pd.Timedelta(hours=6) for c in cycles}):
            for lead in (6,12):
                for member in ids:
                    records.append(dict(station_id='PAOU',member_id=member,init_time=cycle,
                                        valid_time=cycle+pd.Timedelta(hours=lead),forecast_hour=lead,
                                        wind_speed_kt=20.,wind_gust_kt=30.))
        frame=pd.DataFrame(records)
        for month,part in frame.groupby(frame.init_time.dt.strftime('%Y_%m')):
            path=root/model/'wind'/f'{month}_archive.parquet'
            path.parent.mkdir(parents=True,exist_ok=True)
            part.to_parquet(path,index=False)


def test_batch_all_cycles_and_month_upsert(tmp_path,monkeypatch):
    cycles=list(pd.date_range('2026-09-30 18:00','2026-10-01 18:00',freq='6h',tz='UTC'))
    sources=tmp_path/'model'; out=tmp_path/'derived'
    make_sources(sources,cycles)
    calls=[]; original=ep.read_source
    def counted(*args):
        calls.append(args)
        return original(*args)
    monkeypatch.setattr(ep,'read_source',counted)
    kwargs=dict(archive_root=sources,output_root=out,elements=['Wind'],forecast_hours=[6])
    process_range('2026-09-30 18:00','2026-10-02',**kwargs)
    assert len(calls)==6  # one read per model/month, shared over cycles
    sept=pd.read_parquet(out/'wind/2026_09_archive.parquet')
    oct=pd.read_parquet(out/'wind/2026_10_archive.parquet')
    assert len(sept)==2 and len(oct)==8
    assert set(oct.ensemble_init_time.dt.hour)=={0,6,12,18}
    assert oct.complete.all() and (oct.n_available==14).all()
    # Rerunning a subset replaces records, preserving all other cycles.
    process_range('2026-10-01','2026-10-01 06:00',**kwargs)
    result=pd.read_parquet(out/'wind/2026_10_archive.parquet')
    assert len(result)==8 and not result.duplicated(['station_id','ensemble_init_time','valid_time','value_column']).any()


def test_no_downgrade_and_recipe_conflict(tmp_path):
    cycle=pd.Timestamp('2026-10-01',tz='UTC')
    make_sources(tmp_path/'model',[cycle])
    kw=dict(archive_root=tmp_path/'model',output_root=tmp_path/'out',elements=['Wind'],forecast_hours=[6])
    process_range(cycle,cycle+pd.Timedelta(hours=6),**kw)
    (tmp_path/'model/hrrr/wind/2026_10_archive.parquet').unlink()
    process_range(cycle,cycle+pd.Timedelta(hours=6),**kw)
    assert pd.read_parquet(tmp_path/'out/wind/2026_10_archive.parquet').complete.all()
    with pytest.raises(ValueError,match='recipe differs'):
        process_range(cycle,cycle+pd.Timedelta(hours=6),require_complete=False,**kw)


def test_mock_s3_reads_existing_flat_layout_and_updates(monkeypatch):
    fs=fsspec.filesystem('memory')
    monkeypatch.setattr(io,'filesystem',lambda path,profile=None:(fs,str(path).replace('s3://','/')))
    frame=pd.DataFrame({'station_id':['PAOU'],'wind_speed_kt':[5.]})
    io.write_parquet(frame,'s3://bucket/hrrr/2026_10_hrrr_wind_archive.parquet')
    pd.testing.assert_frame_equal(io.read_source('s3://bucket','hrrr','wind','2026_10'),frame)
    cycle=pd.Timestamp('2026-10-01',tz='UTC')
    summary=pd.DataFrame({'station_id':['PAOU'],'ensemble_init_time':[cycle],
                          'valid_time':[cycle+pd.Timedelta(hours=6)],'value_column':['wind_speed_kt'],
                          'complete':[False],'statistics_config':['test'],'n_available':[13]})
    io.update_monthly(summary,'s3://bucket/derived/refs','wind')
    io.update_monthly(summary.assign(complete=True,n_available=14),'s3://bucket/derived/refs','wind')
    with fs.open('/bucket/derived/refs/wind/2026_10_archive.parquet','rb') as stream:
        result=pd.read_parquet(stream)
    assert len(result)==1 and result.complete.all() and result.n_available.iloc[0]==14


def test_compact_dates_and_no_data_fail(tmp_path):
    assert utc_time('202610011200')==pd.Timestamp('2026-10-01 12:00',tz='UTC')
    with pytest.raises(ValueError,match='No statistics'):
        process_range('2026-10-01','2026-10-02',archive_root=tmp_path,output_root=tmp_path/'out',elements=['Wind'],forecast_hours=[6])


def test_shell_stage_order_lookback_and_failure(tmp_path):
    # Execute a copied wrapper with a fake python command: no downloads or uploads.
    script=Path(__file__).parents[1]/'run_daily_archives.sh'
    (tmp_path/script.name).write_text(script.read_text())
    fake=tmp_path/'python'
    fake.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$CALL_LOG"\nif [[ "${FAIL_SOURCE:-0}" == 1 && "$*" == *"--model rrfsens"* ]]; then exit 1; fi\n')
    fake.chmod(0o755)
    log=tmp_path/'calls'
    env=dict(os.environ,PATH=str(tmp_path)+':'+os.environ['PATH'],CALL_LOG=str(log),
             MODELS='hrrr rrfs rrfsens',RUN_NDFD='0',RUN_OBS='0',REFS_ELEMENTS='Wind',REFS_THRESHOLD_CONFIG='/tmp/custom_refs.json')
    result=subprocess.run(['bash',str(tmp_path/script.name),'2026-10-01'],env=env,capture_output=True,text=True)
    assert result.returncode==0,result.stderr
    calls=log.read_text().splitlines()
    assert calls[-1].startswith('run_refs_processing.py')
    assert '--lookback-days 2' in calls[-1]
    assert '--threshold-config /tmp/custom_refs.json' in calls[-1]
    assert '--start 202609291800' in calls[0] and '--end 202610011800' in calls[0]
    log.unlink()
    result=subprocess.run(['bash',str(tmp_path/script.name),'2026-10-01'],env=dict(env,FAIL_SOURCE='1'),capture_output=True,text=True)
    assert result.returncode==1
    assert 'run_refs_processing.py' not in log.read_text()


def test_default_leads_and_accumulation_start(tmp_path,monkeypatch):
    import run_refs_processing as runner
    seen=[]
    def assemble(root,element,cycle,valid,**kw):
        seen.append((element,cycle.hour,int((valid-cycle)/pd.Timedelta(hours=1))))
        return pd.DataFrame({'station_id':['PAOU'],'ensemble_init_time':[cycle],'valid_time':[valid],
                             'ensemble_forecast_hour':[seen[-1][2]],'ensemble_member_id':['test'],
                             'source_available':[True],'wind_speed_kt':[10.],'wind_gust_kt':[20.],
                             'precip_6h':[.2]})
    monkeypatch.setattr(runner,'assemble_refs',assemble)
    runner.process_range('2026-10-01','2026-10-02',output_root=tmp_path,elements=['Wind','precip6hr'])
    assert len([x for x in seen if x[0]=='wind'])==4*20
    assert len([x for x in seen if x[0]=='precip6hr'])==4*19
    assert min(x[2] for x in seen if x[0]=='precip6hr')==6
    assert len(pd.read_parquet(tmp_path/'wind/2026_10_archive.parquet'))==160
