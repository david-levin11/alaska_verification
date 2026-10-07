from argparse import Namespace
import pandas as pd
import pytest
from validate_refs_archive import run, independent_interval


def sources(root):
    for model,ids in [('hrrr',['control']),('rrfs',['control']),('rrfsens',[f'm{i:03}' for i in range(1,6)])]:
        for element,total,interval in [('precip6hr','precip_accum','precip_6h'),('snow6hr','snow_accum','snow_6h')]:
            rows=[]
            for cycle in pd.date_range('2026-09-30 18:00','2026-10-01 18:00',freq='6h',tz='UTC'):
                for lead in range(3,67,3):
                    for n,member in enumerate(ids):
                        rate=(n+1)*.01
                        rows.append(dict(station_id='PAJN',member_id=member,init_time=cycle,
                            valid_time=cycle+pd.Timedelta(hours=lead),forecast_hour=lead,
                            **{total:lead*rate,interval:round(6*rate,2) if lead>=6 else float('nan')}))
            frame=pd.DataFrame(rows)
            for month,part in frame.groupby(frame.init_time.dt.strftime('%Y_%m')):
                path=root/model/element/(month+'_archive.parquet');path.parent.mkdir(parents=True,exist_ok=True)
                part.to_parquet(path,index=False)


def args(tmp_path):
    return Namespace(date='2026-10-01',archive_root=str(tmp_path/'model'),test_output_root=str(tmp_path/'test'),
        elements=['precip6hr','snow6hr'],stations=['PAJN'],forecast_hours=[6,12,48,54,60],aws_profile=None,
        threshold_config=None,boundary_date='2026-10-01',report=None)


def test_full_validation_and_corrupt_interval(tmp_path):
    sources(tmp_path/'model'); options=args(tmp_path)
    assert run(options)==0
    path=tmp_path/'model/rrfsens/precip6hr/2026_10_archive.parquet'
    frame=pd.read_parquet(path);frame.loc[frame.forecast_hour==12,'precip_6h']=999
    frame.to_parquet(path,index=False)
    assert run(options)==1


def test_missing_boundary_not_pass(tmp_path):
    sources(tmp_path/'model');options=args(tmp_path);options.boundary_date=None
    assert run(options)==2


def test_negative_totals_detected():
    init=pd.Timestamp('2026-10-01',tz='UTC')
    frame=pd.DataFrame(dict(station_id=['PAJN']*2,init_time=[init]*2,
        valid_time=[init+pd.Timedelta(hours=h) for h in [6,12]],precip_accum=[2.,1.]))
    with pytest.raises(ValueError,match='Negative cumulative'):
        independent_interval(frame,'PAJN','control',init,init+pd.Timedelta(hours=12),'precip_accum','precip_6h')


def test_s3_destination(tmp_path,monkeypatch):
    import fsspec
    import refs_archive_io as io
    import validate_refs_archive as validator
    original=io.filesystem
    memory=fsspec.filesystem('memory')
    def fs(path,profile=None):
        return (memory,str(path).replace('s3://','/')) if str(path).startswith('s3://') else original(path,profile)
    monkeypatch.setattr(io,'filesystem',fs)
    monkeypatch.setattr(validator,'filesystem',fs)
    sources(tmp_path/'model');options=args(tmp_path)
    options.test_output_root='s3://validation-test/refs'
    assert run(options)==0
