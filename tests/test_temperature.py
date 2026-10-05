"""Exercise real extraction logic with synthetic decoded GRIBs (no network)."""
import ast
import os
import re
import tempfile
import shutil
from pathlib import Path
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import pandas as pd
import pytest
import archiver_config as config
from run_refs_processing import process_range
from refs_threshold_config import load_threshold_config, validate_recipe

ROOT = Path(__file__).parents[1]


def extraction_function():
    # Isolate the production function from optional pygrib/scipy imports.
    tree=ast.parse((ROOT/'utils.py').read_text())
    node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='extract_model_subset_parallel')
    ns=dict(os=os,re=re,tempfile=tempfile,shutil=shutil,Path=Path,np=np,pd=pd,
            ThreadPoolExecutor=ThreadPoolExecutor,as_completed=as_completed)
    exec(compile(ast.Module(body=[node],type_ignores=[]),'utils.py','exec'),ns)
    return ns


@pytest.mark.parametrize('model',['hrrr','rrfs'])
def test_extract_temperature_kelvin_to_fahrenheit(model):
    ns=extraction_function()
    data={k:SimpleNamespace(values=np.array(v)) for k,v in {
        'latitude':[[60,60,60]], 'longitude':[[-150,-149,-148]],
        'valid_time':np.datetime64('2026-10-04T06:00'),
        't2m':[[273.15,233.15,float('nan')]]}.items()}
    class Dataset(dict):
        def __getattr__(self,k): return self[k]
    ns.update(xr=SimpleNamespace(open_dataset=lambda *a,**kw:Dataset(data)),
              ll_to_index=lambda lat,lon,*a:(0,int(lon+150)),
              parse_date_and_time_from_url=lambda *a:('20261004','00'),
              download_subset=lambda **kw:kw['local_filename'])
    stations=pd.DataFrame(dict(stid=['A','B','C'],latitude=[60]*3,longitude=[-150,-149,-148]))
    result=ns['extract_model_subset_parallel'](['test.f006.grib2'],stations,
        config.HERBIE_XARRAY_STRINGS['temp2m'][model],'temp2m',model,config)
    assert result.temp_2m_f.iloc[:2].tolist()==[32.,-40.]
    assert pd.isna(result.temp_2m_f.iloc[2])
    assert (result.forecast_hour==6).all()


def test_search_and_member_configuration():
    line='16:11693067:d=2026100500:TMP:2 m above ground:6 hour fcst:ENS=+1'
    for model in ['hrrr','rrfs']:
        pattern=config.HERBIE_XARRAY_STRINGS['temp2m'][model][0]
        assert pattern in line and pattern in line.split(':ENS=')[0]
        assert pattern not in line.replace(':TMP:',':DPT:')
    assert 'temp2m' in config.AVAILABLE_FIELDS['rrfsens']
    assert config.HERBIE_FORECASTS['rrfsens']['temp2m']==list(range(3,61,3))


def test_temperature_ensemble_and_negative_thresholds(tmp_path):
    cycle=pd.Timestamp('2026-10-04',tz='UTC'); valid=cycle+pd.Timedelta(hours=6)
    for model,ids in [('hrrr',['control']),('rrfs',['control']),('rrfsens',config.RRFS_MEMBERS)]:
        rows=[dict(station_id='PAJN',member_id=m,init_time=cycle-pd.Timedelta(hours=lag),
            valid_time=valid,forecast_hour=6+lag,temp_2m_f=-40. if lag else 32.) for lag in [0,6] for m in ids]
        path=tmp_path/'model'/model/'temp2m/2026_10_archive.parquet'
        path.parent.mkdir(parents=True);pd.DataFrame(rows).to_parquet(path)
    process_range(cycle,cycle+pd.Timedelta(hours=6),elements=['temp2m'],forecast_hours=[6],
                  archive_root=tmp_path/'model',output_root=tmp_path/'derived')
    result=pd.read_parquet(tmp_path/'derived/temp2m/2026_10_archive.parquet').iloc[0]
    assert result.n_available==14 and result.complete
    assert result['mean']==-4 and result.p50==-4
    recipe=validate_recipe('temp_2m_f',dict(units='degF',operator='<',thresholds=[-40,0,32]))
    assert recipe['thresholds']==[-40.,0.,32.]
    assert load_threshold_config()['temp_2m_f']['thresholds']==[]


def test_old_custom_threshold_file_still_loads(tmp_path):
    import json
    config=load_threshold_config();config.pop('temp_2m_f')
    path=tmp_path/'old.json';path.write_text(json.dumps(config))
    assert load_threshold_config(path)['temp_2m_f']==dict(units='degF',operator='<',thresholds=[])
