"""Real NetCDF/gzip point extraction and safe manual monthly imports."""
import gzip
import json
from pathlib import Path
import shutil
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from experimental_ndfd import extract_file,monthly_plan,install_plan
from import_ndfd_experimental import main,load_stations,archive_lock


@pytest.fixture
def dataset():
    start = pd.Timestamp('2026-11-09')
    data = xr.Dataset(attrs={'startProcTime':'20261109_0000','creationTimeString':'Fri Nov 6 19:03:57 2026'})
    data['latitude'] = (('y','x'),np.repeat(np.array([58.,59.,60.],dtype='float32')[:,None],3,axis=1))
    data['longitude'] = (('y','x'),np.repeat(np.array([-136.,-135.,-134.],dtype='float32')[None,:],3,axis=0))
    def add(name,starts,hours,units,base):
        starts = pd.DatetimeIndex(starts)
        hours = np.broadcast_to(hours,len(starts))
        ends = starts+pd.to_timedelta(hours,unit='h')
        pairs = np.column_stack((starts.as_unit('s').asi8,ends.as_unit('s').asi8)).reshape(-1)
        values = np.full((len(starts),3,3),base,dtype='float32')
        data[name] = ((name+'_time','y','x'),values)
        data[name].attrs.update(validTimes=pairs,units=units,fillValue=-30000.)
    add('Wind_Mag_SFC',pd.date_range(start,periods=21,freq='6h'),6,'kts',10)
    add('Wind_Dir_SFC',pd.date_range(start,periods=21,freq='6h'),6,'degrees',270)
    add('WindGust_SFC',pd.date_range(start,periods=21,freq='6h'),6,'kts',25)
    add('QPF_SFC',pd.date_range(start,periods=16,freq='6h'),6,'in',0.08)
    add('MaxT_SFC',[start]+list(pd.date_range(start+pd.Timedelta(hours=14),periods=5,freq='D')),
        [5]+[15]*5,'F',54.2)
    add('MinT_SFC',pd.date_range(start+pd.Timedelta(hours=2),periods=6,freq='D'),18,'F',16.4)
    add('MinRH_SFC',[start]+list(pd.date_range(start+pd.Timedelta(hours=12),periods=5,freq='D')),
        [6]+[18]*5,'%',20)
    # Hourly RH begins at f072, before the f084 dump start, and runs past f168.
    times = pd.date_range(start-pd.Timedelta(hours=12),start+pd.Timedelta(days=5),freq='h')
    add('RH_SFC',times,1,'%',60)
    data.RH_SFC.values[(times.hour>=6)&(times.hour<18)] = 5
    return data


@pytest.fixture
def stations():
    return pd.DataFrame(dict(stid=['A','B','OUT','FAR'],latitude=[58.,59.,59.,59.2],
        longitude=[-136.,-135.,-137.,-135.]))


def write(dataset,path):
    dataset.to_netcdf(path,engine='netcdf4')
    return path


def test_all_fields_native_timing_derived_windows_and_domain(dataset,stations,tmp_path):
    source = write(dataset,tmp_path/'VRH_Fcst_ext_20261109_1200.nc')
    frames,report = extract_file(source,stations)
    assert set(frames)=={'wind','gust','precip6hr','maxt','mint','minrh_native','rh','minrh'}
    assert report['init_time']=='2026-11-05 12:00:00'
    assert report['stations_selected']==2
    assert {r['stid']:r['reason'] for r in report['stations_rejected']}=={'OUT':'outside_grid','FAR':'too_far_from_grid_point'}
    assert frames['rh'].forecast_hour.min()==72
    assert frames['wind'].forecast_hour.min()==84 and frames['wind'].forecast_hour.max()>168
    assert frames['maxt'].forecast_hour.min()==89
    assert frames['maxt'].window_hours.unique().tolist()==[5.,15.]
    assert frames['minrh_native'].window_hours.unique().tolist()==[6.,18.]
    assert frames['minrh_native'].minrh.eq(20).all()
    assert frames['minrh'].minrh.eq(60).all()  # Daytime lower values excluded.
    assert frames['minrh'].window_hours.eq(12).all()
    assert frames['minrh'].n_samples.eq(12).all()
    assert frames['minrh'].valid_time.dt.hour.eq(6).all()
    assert frames['minrh'].valid_time.max()==pd.Timestamp('2026-11-13 06:00')
    assert frames['precip6hr'].precip6hr.eq(.08).all()
    assert frames['wind'].wind_speed_kt.eq(10).all() and frames['wind'].wind_dir_deg.eq(270).all()


def test_gzip_and_flipped_rows_use_actual_coordinates(dataset,stations,tmp_path):
    a=write(dataset,tmp_path/'a.nc')
    b=tmp_path/'b.nc.gz'
    with a.open('rb') as inp,gzip.open(b,'wb') as out:shutil.copyfileobj(inp,out)
    cache={}
    fa,_=extract_file(a,stations,cache=cache)
    fb,_=extract_file(b,stations,cache=cache)
    assert len(cache)==1
    for element in fa:
        pd.testing.assert_frame_equal(fa[element].drop(columns=['source_file','source_sha256']),
            fb[element].drop(columns=['source_file','source_sha256']))
    dataset.Wind_Mag_SFC.values[:,0,0]=33
    flipped=write(dataset.isel(y=slice(None,None,-1)),tmp_path/'flipped.nc')
    fields,_=extract_file(flipped,stations,elements=['wind'])
    assert fields['wind'].loc[lambda x:x.station_id=='A','wind_speed_kt'].eq(33).all()


def test_dateline_lookup_and_boundary(dataset,tmp_path):
    dataset['longitude'].values[:]=[179.,180.,-179.]
    path=write(dataset,tmp_path/'dateline.nc')
    sites=pd.DataFrame(dict(stid=['DATE','OUT'],latitude=[59.,59.],longitude=[-180.,-178.]))
    frames,report=extract_file(path,sites,elements=['rh'])
    assert set(frames['rh'].station_id)=={'DATE'}
    assert frames['rh'].grid_longitude.eq(-180).all()
    assert report['stations_rejected']==[{'stid':'OUT','reason':'outside_grid'}]


def test_missing_values_are_not_replaced_by_neighbor_or_partial_min(dataset,stations,tmp_path):
    dataset.RH_SFC.values[10,0,0]=-30000
    dataset.Wind_Mag_SFC.values[0,0,0]=-30000
    path=write(dataset,tmp_path/'missing.nc')
    frames,_=extract_file(path,stations)
    assert np.isnan(frames['wind'].wind_speed_kt.iloc[0])
    assert frames['wind'].wind_dir_deg.iloc[0]==270
    assert len(frames['minrh'].query('station_id=="A"'))==len(frames['minrh'].query('station_id=="B"'))-1


@pytest.mark.parametrize('issue',['pair_count','units','packed','qpf_duration','wind_times','init','coordinates'])
def test_bad_inputs_refused_before_writes(dataset,stations,tmp_path,issue):
    if issue=='pair_count':dataset.RH_SFC.attrs['validTimes']=dataset.RH_SFC.attrs['validTimes'][:-1]
    elif issue=='units':dataset.RH_SFC.attrs['units']='fraction'
    elif issue=='packed':dataset.RH_SFC.attrs['dataMultiplier']=1.
    elif issue=='qpf_duration':dataset.QPF_SFC.attrs['validTimes'][1]+=3600
    elif issue=='wind_times':dataset.Wind_Dir_SFC.attrs['validTimes'][0]+=3600
    elif issue=='init':del dataset.attrs['startProcTime']
    elif issue=='coordinates':dataset.longitude.values[0,0]=-30000
    path=write(dataset,tmp_path/f'{issue}.nc')
    with pytest.raises((ValueError,KeyError)):extract_file(path,stations)
    assert not list(tmp_path.glob('**/*.parquet'))


def test_preview_apply_repeat_conflicts_and_backups(dataset,stations,tmp_path):
    path=write(dataset,tmp_path/'input.nc')
    frames,report=extract_file(path,stations,elements=['gust'])
    root=tmp_path/'model'
    plan=monthly_plan(frames,root)
    assert not root.exists()
    manifest=install_plan(plan,report,root)
    assert json.loads(manifest.read_text())['status']=='installed'
    assert not monthly_plan(frames,root)
    destination=plan[0][0]
    before=destination.read_bytes()
    revised={key:value.copy() for key,value in frames.items()}
    revised['gust']['wind_gust_kt']=30
    with pytest.raises(ValueError,match='conflicting'):monthly_plan(revised,root)
    assert destination.read_bytes()==before
    replacement=monthly_plan(revised,root,replace_conflicts=True)
    manifest=install_plan(replacement,report,root)
    assert pd.read_parquet(destination).wind_gust_kt.eq(30).all()
    assert (manifest.parent/'gust/2026_11_archive.parquet').read_bytes()==before


def test_monthly_partition_uses_initialization_and_retains_cross_month_valid_times(dataset,stations,tmp_path):
    dataset.attrs['startProcTime']='20261202_0000'  # init=November 28 12Z
    path=write(dataset,tmp_path/'crossmonth.nc')
    frames,_=extract_file(path,stations,elements=['rh'])
    frames['rh']['valid_time']=pd.Timestamp('2026-12-02')
    plan=monthly_plan(frames,tmp_path)
    assert plan[0][0].name=='2026_11_archive.parquet'


def test_rollback_restores_original_and_removes_new_target(dataset,stations,tmp_path,monkeypatch):
    import experimental_ndfd as module
    path=write(dataset,tmp_path/'source.nc')
    frames,report=extract_file(path,stations,elements=['wind','gust'])
    root=tmp_path/'model'
    original=monthly_plan({'wind':frames['wind']},root)
    install_plan(original,report,root)
    before=original[0][0].read_bytes()
    frames['wind']['wind_speed_kt']=50
    plan=monthly_plan(frames,root,replace_conflicts=True)
    replace=module.os.replace
    calls=[0]
    def fail_once(source,destination):
        calls[0]+=1
        if calls[0]==2:raise OSError('simulated install failure')
        return replace(source,destination)
    monkeypatch.setattr(module.os,'replace',fail_once)
    with pytest.raises(OSError):install_plan(plan,report,root)
    assert original[0][0].read_bytes()==before
    assert not (root/'ndfd_exp/gust/2026_11_archive.parquet').exists()


def test_cli_config_paths_preview_apply_and_offline_inventory(dataset,stations,tmp_path,capsys):
    inputs=tmp_path/'incoming';inputs.mkdir()
    write(dataset,inputs/'first.nc')
    stations.to_csv(tmp_path/'stations.csv',index=False)
    config=tmp_path/'settings.json'
    config.write_text(json.dumps(dict(input_dir='incoming',archive_root='model',station_csv='stations.csv',elements=['gust'])))
    assert main(['--config',str(config)])==0
    assert not (tmp_path/'model').exists()
    assert main(['--config',str(config),'--apply'])==0
    target=tmp_path/'model/ndfd_exp/gust/2026_11_archive.parquet'
    before=target.read_bytes()
    assert main(['--config',str(config),'--apply'])==0
    assert target.read_bytes()==before
    assert 'UNCHANGED' in capsys.readouterr().out


def test_preview_detects_conflicts_between_batch_inputs(dataset,stations,tmp_path):
    inputs=tmp_path/'incoming';inputs.mkdir()
    write(dataset,inputs/'a.nc')
    dataset.WindGust_SFC.values[:]=50
    write(dataset,inputs/'b.nc')
    sites=tmp_path/'stations.csv';stations.to_csv(sites,index=False)
    with pytest.raises(ValueError,match='conflicting'):
        main(['--input-dir',str(inputs),'--station-csv',str(sites),'--archive-root',str(tmp_path/'model'),'--elements','gust'])
    assert not (tmp_path/'model').exists()


def test_lock_and_conflicting_station_coordinates(tmp_path):
    with archive_lock(tmp_path,True):
        with pytest.raises(RuntimeError,match='lock exists'):
            with archive_lock(tmp_path,True):pass
    assert not (tmp_path/'ndfd_exp/.import.lock').exists()
    path=tmp_path/'stations.csv'
    pd.DataFrame(dict(stid=['A','A'],latitude=[58.,59.],longitude=[-135.,-135.])).to_csv(path,index=False)
    with pytest.raises(ValueError,match='Conflicting'):load_stations(path,'alaska')
