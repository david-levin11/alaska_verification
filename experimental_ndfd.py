"""Manual point extraction from GFE experimental NetCDF exports (no S3)."""
from contextlib import contextmanager
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from uuid import uuid4

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
import xarray as xr

FIELDS = {
    'maxt': ('MaxT_SFC','maxt',('f','degf','fahrenheit','°f'),'maximum'),
    'mint': ('MinT_SFC','mint',('f','degf','fahrenheit','°f'),'minimum'),
    'minrh_native': ('MinRH_SFC','minrh',('%','percent'),'minimum'),
    'wind': ('Wind_Mag_SFC','wind_speed_kt',('kts','kt','knots'),'instant'),
    'gust': ('WindGust_SFC','wind_gust_kt',('kts','kt','knots'),'instant'),
    'precip6hr': ('QPF_SFC','precip6hr',('in','inch','inches'),'accumulation'),
    'rh': ('RH_SFC','rh',('%','percent'),'instant'),
}
ELEMENTS = tuple(FIELDS) + ('minrh',)
KEYS = ['station_id','init_time','valid_time','window_start','window_end']
PROVENANCE = {'source_file','source_sha256','grid_latitude','grid_longitude','grid_distance_km'}


def json_default(value):
    if isinstance(value,np.generic): return value.item()
    if isinstance(value,np.ndarray): return value.tolist()
    return str(value)


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''): digest.update(block)
    return digest.hexdigest()


@contextmanager
def netcdf_path(source):
    """Decompress into a private temporary directory; never modify the input."""
    source = Path(source)
    if source.suffix.lower() != '.gz':
        yield source
        return
    with tempfile.TemporaryDirectory(prefix='ndfd_exp_') as directory:
        target = Path(directory)/'source.nc'
        with gzip.open(source,'rb') as inp, target.open('wb') as out:
            shutil.copyfileobj(inp,out)
        yield target


def xyz(latitude,longitude):
    lat,lon = np.deg2rad(latitude),np.deg2rad(longitude)
    return np.column_stack((np.cos(lat)*np.cos(lon),np.cos(lat)*np.sin(lon),np.sin(lat)))


def inside_boundary(latitude,longitude,station_lat,station_lon):
    """Test the actual outer grid boundary in a dateline-safe tangent plane.

    Gnomonic projection keeps spherical great-circle segments straight. No
    projection reconstruction, corner-lat/lon interpolation, or longitude bbox.
    Require a valid rectangular grid perimeter wholly within one hemisphere.
    """
    if min(latitude.shape)<2: raise ValueError('Grid must have at least two rows/columns')
    def ring(a):
        return np.concatenate((a[0,:],a[1:,-1],a[-1,-2::-1],a[-2:0:-1,0]))
    lat,lon = ring(latitude),ring(longitude)
    if not (np.isfinite(lat).all() and np.isfinite(lon).all() and (abs(lat)<=90).all()):
        raise ValueError('Invalid grid boundary coordinates')
    boundary = xyz(lat,lon)
    centre = boundary.mean(axis=0)
    centre /= np.linalg.norm(centre)
    east = np.cross([0.,0.,1.],centre)
    east /= np.linalg.norm(east)
    north = np.cross(centre,east)
    def project(v):
        divisor = v@centre
        return v@east/divisor, v@north/divisor, divisor
    x,y,divisor = project(boundary)
    if (divisor<=0).any(): raise ValueError('Grid perimeter exceeds supported hemisphere')
    sx,sy,sd = project(xyz(station_lat,station_lon))
    x2,y2 = np.roll(x,-1),np.roll(y,-1)
    dx,dy = x2-x,y2-y
    result=[]
    for px,py,pd_ in zip(sx,sy,sd):
        if pd_<=0:
            result.append(False)
            continue
        cross = (px-x)*dy-(py-y)*dx
        on_edge = ((abs(cross)<=1e-10) & (px>=np.minimum(x,x2)-1e-10)
            & (px<=np.maximum(x,x2)+1e-10) & (py>=np.minimum(y,y2)-1e-10)
            & (py<=np.maximum(y,y2)+1e-10)).any()
        crossing = ((y>py)!=(y2>py))
        active = np.flatnonzero(crossing)
        hits = np.count_nonzero(px < x[active]+(py-y[active])*dx[active]/dy[active])
        result.append(bool(on_edge or hits%2))
    return np.asarray(result,dtype=bool)


def station_mapping(dataset,stations,max_distance_km,cache):
    latvar,lonvar = dataset['latitude'],dataset['longitude']
    lat,lon = np.asarray(latvar.values,dtype=float),np.asarray(lonvar.values,dtype=float)
    if lat.ndim!=2 or lat.shape!=lon.shape or latvar.dims!=lonvar.dims:
        raise ValueError('Expected aligned two-dimensional latitude/longitude arrays')
    if not np.isfinite(max_distance_km) or max_distance_km<=0:
        raise ValueError('max_distance_km must be positive and finite')
    valid = np.isfinite(lat)&np.isfinite(lon)&(abs(lat)<=90)&(lon>=-180)&(lon<=360)
    if not valid.any(): raise ValueError('No valid grid coordinates')
    if not (valid[0,:].all() and valid[-1,:].all() and valid[:,0].all() and valid[:,-1].all()):
        raise ValueError('Invalid grid boundary coordinates')
    # Validate before wrapping so longitude fill values cannot become locations.
    lon = (lon+180)%360-180
    signature = hashlib.sha256(lat.tobytes()+lon.tobytes()).hexdigest()
    cache_key = (signature,tuple(latvar.dims),max_distance_km,
        tuple(stations[['stid','latitude','longitude']].itertuples(index=False,name=None)))
    if cache_key not in cache:
        footprint = inside_boundary(lat,lon,stations.latitude.to_numpy(),stations.longitude.to_numpy())
        flat = np.flatnonzero(valid)
        tree = cKDTree(xyz(lat.ravel()[flat],lon.ravel()[flat]))
        chord,index = tree.query(xyz(stations.latitude.to_numpy(),stations.longitude.to_numpy()))
        distance = 2*6371.0088*np.arcsin(np.clip(chord/2,0,1))
        iy,ix = np.unravel_index(flat[index],lat.shape)
        accepted = footprint & (distance<=max_distance_km)
        mapping = stations.loc[accepted].copy().reset_index(drop=True)
        mapping['iy'],mapping['ix'] = iy[accepted],ix[accepted]
        mapping['grid_latitude'],mapping['grid_longitude'] = lat[iy[accepted],ix[accepted]],lon[iy[accepted],ix[accepted]]
        mapping['grid_distance_km'] = distance[accepted]
        rejected = stations.loc[~accepted,['stid']].copy()
        rejected['reason'] = np.where(~footprint[~accepted],'outside_grid','too_far_from_grid_point')
        cache[cache_key] = (mapping,rejected.to_dict('records'))
    mapping,rejected = cache[cache_key]
    if mapping.empty: raise ValueError('No extraction stations inside grid/distance limit')
    return mapping,latvar.dims,signature,rejected


def point_values(dataset,name,mapping,grid_dims,units):
    var = dataset[name]
    if var.dtype.kind!='f' or any(k in var.attrs for k in ('dataMultiplier','dataOffset','scale_factor','add_offset')):
        raise ValueError(f'{name}: expected unpacked floating-point export')
    if str(var.attrs.get('units','')).strip().lower() not in units:
        raise ValueError(f'{name}: unexpected units {var.attrs.get("units")}')
    time_dims = [d for d in var.dims if d not in grid_dims]
    if len(time_dims)!=1 or not set(grid_dims)<=set(var.dims):
        raise ValueError(f'{name}: expected one time dimension and the coordinate grid')
    var = var.transpose(time_dims[0],*grid_dims)
    if tuple(var.shape[1:]) != tuple(dataset['latitude'].shape):
        raise ValueError(f'{name}: field and coordinate grid differ')
    raw_times = np.asarray(var.attrs.get('validTimes',[]))
    if raw_times.ndim!=1 or len(raw_times)!=2*var.shape[0]:
        raise ValueError(f'{name}: validTimes must contain two timestamps per grid')
    times = pd.to_datetime(raw_times,unit='s',errors='raise')
    starts,ends = times[::2],times[1::2]
    if times.isna().any() or (ends<=starts).any() or pd.MultiIndex.from_arrays([starts,ends]).has_duplicates:
        raise ValueError(f'{name}: missing, duplicate, or invalid validity periods')
    if not starts.is_monotonic_increasing:
        raise ValueError(f'{name}: periods must be ordered by start time')
    points=[]
    # One spatial slice at a time: never materialize time × full grid arrays.
    for i in range(var.shape[0]):
        spatial = np.asarray(var.isel({time_dims[0]:i}).values,dtype=float)
        values = spatial[mapping.iy.to_numpy(),mapping.ix.to_numpy()].copy()
        for attr in ('fillValue','_FillValue','missing_value'):
            for fill in np.atleast_1d(var.attrs.get(attr,[])):
                values[values==fill] = np.nan
        values[~np.isfinite(values)] = np.nan
        points.append(np.round(values,2))
    return starts,ends,np.asarray(points)


def records(mapping,init,starts,ends,values,value_names,instant,source,digest,**extra):
    valid = starts if instant else ends
    nstation,ntime = len(mapping),len(starts)
    out = pd.DataFrame(dict(station_id=np.tile(mapping.stid.to_numpy(),ntime),
        init_time=init,valid_time=np.repeat(valid.to_numpy(),nstation),
        window_start=np.repeat(starts.to_numpy(),nstation),window_end=np.repeat(ends.to_numpy(),nstation)))
    for column,array in zip(value_names,values): out[column] = array.reshape(-1)
    out['forecast_hour'] = (out.valid_time-init).dt.total_seconds()/3600
    out['forecast_hour_start'] = (out.window_start-init).dt.total_seconds()/3600
    out['forecast_hour_end'] = (out.window_end-init).dt.total_seconds()/3600
    out['window_hours'] = (out.window_end-out.window_start).dt.total_seconds()/3600
    for column in ('grid_latitude','grid_longitude','grid_distance_km'):
        out[column] = np.tile(mapping[column].to_numpy(),ntime)
    out['source_file'],out['source_sha256'] = source.name,digest
    for name,value in extra.items(): out[name] = value
    return out


def extract_file(source,stations,*,elements=ELEMENTS,max_distance_km=5,cache=None):
    source = Path(source)
    cache = {} if cache is None else cache
    unknown = set(elements)-set(ELEMENTS)
    if unknown: raise ValueError(f'Unsupported elements: {sorted(unknown)}')
    digest = sha256(source)
    frames,stats,read = {},{},{}
    with netcdf_path(source) as path, xr.open_dataset(path,engine='netcdf4',decode_cf=False,cache=False) as dataset:
        start_proc = datetime.strptime(str(dataset.attrs['startProcTime']),'%Y%m%d_%H%M')
        init = pd.Timestamp(start_proc)-pd.Timedelta(hours=84)
        mapping,grid_dims,signature,rejected = station_mapping(dataset,stations,max_distance_km,cache)
        def get(name,units):
            if name not in read: read[name] = point_values(dataset,name,mapping,grid_dims,units)
            return read[name]
        for element in elements:
            if element=='minrh': continue
            variable,column,units,statistic = FIELDS[element]
            needed = [variable]+(['Wind_Dir_SFC'] if element=='wind' else [])
            if not all(v in dataset for v in needed):
                stats[element] = {'status':'missing_variable','required':needed}
                continue
            starts,ends,values = get(variable,units)
            arrays,columns = [values],[column]
            if element=='precip6hr' and not ((ends-starts)==pd.Timedelta(hours=6)).all():
                raise ValueError('QPF_SFC contains periods other than six hours')
            if element=='wind':
                ds,de,direction = get('Wind_Dir_SFC',('degrees','degree','deg'))
                if not starts.equals(ds) or not ends.equals(de):
                    raise ValueError('Wind speed/direction periods differ')
                arrays.append(direction);columns.append('wind_dir_deg')
            frames[element] = records(mapping,init,starts,ends,arrays,columns,statistic=='instant',source,digest,
                statistic=statistic)
        if 'minrh' in elements:
            if 'RH_SFC' not in dataset:
                stats['minrh'] = {'status':'missing_variable','required':['RH_SFC']}
            else:
                starts,ends,values = get('RH_SFC',('%','percent'))
                if not ((ends-starts)==pd.Timedelta(hours=1)).all():
                    raise ValueError('Derived 12h MinRH requires hourly RH validity blocks')
                if starts.has_duplicates: raise ValueError('Duplicate RH valid times')
                groups=[]
                candidates = pd.date_range(starts.min().normalize()+pd.Timedelta(hours=6),
                    ends.max().normalize()+pd.Timedelta(hours=6),freq='D')
                skipped=0
                skipped_station_windows=0
                for endpoint in candidates:
                    begin = endpoint-pd.Timedelta(hours=12)
                    expected = pd.date_range(begin,periods=12,freq='h')
                    indices = starts.get_indexer(expected)
                    if (indices<0).any():
                        skipped += 1
                        skipped_station_windows += len(mapping)
                        continue
                    subset = values[indices]
                    complete = np.isfinite(subset).all(axis=0)
                    skipped_station_windows += int((~complete).sum())
                    if not complete.any():
                        skipped += 1
                        continue
                    selected = mapping.loc[complete].reset_index(drop=True)
                    minimum = subset[:,complete].min(axis=0)[None,:]
                    groups.append(records(selected,init,pd.DatetimeIndex([begin]),pd.DatetimeIndex([endpoint]),
                        [minimum],['minrh'],False,source,digest,statistic='minimum_of_hourly_rh',n_samples=12,
                        coverage_complete=True))
                stats['minrh'] = {'incomplete_or_unavailable_windows':skipped,
                    'incomplete_station_windows':skipped_station_windows}
                if groups: frames['minrh'] = pd.concat(groups,ignore_index=True)
        for element,frame in frames.items():
            value_cols = [c for c in frame if c in ('maxt','mint','minrh','wind_speed_kt','wind_dir_deg','wind_gust_kt','precip6hr','rh')]
            stats.setdefault(element,{}).update(rows=len(frame),periods=frame.window_start.nunique(),
                first_valid=str(frame.valid_time.min()),last_valid=str(frame.valid_time.max()),
                first_projection=float(frame.forecast_hour.min()),last_projection=float(frame.forecast_hour.max()),
                missing_values={c:int(frame[c].isna().sum()) for c in value_cols})
        report = dict(source=str(source.resolve()),source_sha256=digest,init_time=str(init),
            startProcTime=str(start_proc),grid_sha256=signature,grid_shape=list(dataset.latitude.shape),
            stations_selected=len(mapping),stations_rejected=rejected,elements=stats,
            creation_time=dataset.attrs.get('creationTimeString'),status='preview')
    if not frames: raise ValueError('No requested forecast fields could be extracted')
    return frames,report


def monthly_plan(frames,archive_root,*,replace_conflicts=False,existing=None):
    """Partition by initialization month, as for other model archives."""
    plan=[]
    for element,frame in frames.items():
        if frame.duplicated(KEYS).any(): raise ValueError(f'Duplicate extracted keys for {element}')
        for month,new in frame.groupby(frame.init_time.dt.strftime('%Y_%m')):
            destination = Path(archive_root)/'ndfd_exp'/element/f'{month}_archive.parquet'
            old = ((existing or {}).get(destination) if destination in (existing or {}) else
                (pd.read_parquet(destination) if destination.exists() else pd.DataFrame()))
            conflicts=0
            if not old.empty:
                if not set(KEYS)<=set(old): raise ValueError(f'Existing file lacks import keys: {destination}')
                if old.duplicated(KEYS).any(): raise ValueError(f'Existing duplicate keys: {destination}')
                a,b = old.set_index(KEYS),new.set_index(KEYS)
                shared = a.index.intersection(b.index)
                compare = [c for c in b.columns if c not in PROVENANCE]
                if len(shared):
                    left,right = a.reindex(index=shared,columns=compare),b.reindex(index=shared,columns=compare)
                    equal = left.eq(right)|(left.isna()&right.isna())
                    conflicts = int((~equal.all(axis=1)).sum())
                    if conflicts and not replace_conflicts:
                        raise ValueError(f'{destination}: {conflicts} conflicting rows; preview/review then use --replace-conflicts')
                    # Identical forecasts are no-ops even if provenance differs.
                    unchanged = shared[equal.all(axis=1).to_numpy()]
                    new = new.loc[~pd.MultiIndex.from_frame(new[KEYS]).isin(unchanged)]
            if new.empty: continue
            combined = pd.concat([old,new],ignore_index=True).drop_duplicates(KEYS,keep='last').reset_index(drop=True)
            plan.append((destination,combined,dict(new_or_replaced_rows=len(new),conflicts=conflicts)))
    return plan


def install_plan(plan,report,archive_root):
    """Stage and validate all files before backup/install; roll back exceptions."""
    root = Path(archive_root)/'ndfd_exp'
    import_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'_'+uuid4().hex[:8]
    backup = root/'import_backups'/import_id
    backup.mkdir(parents=True,exist_ok=False)
    manifest = backup/'manifest.json'
    report = dict(report,status='staging',changes=[dict(path=str(p),**details) for p,_,details in plan])
    staged,installed = [],[]
    def save(): manifest.write_text(json.dumps(report,indent=2,default=json_default),encoding='utf-8')
    save()
    try:
        for destination,data,details in plan:
            destination.parent.mkdir(parents=True,exist_ok=True)
            fd,name = tempfile.mkstemp(prefix='.ndfd_exp_',suffix='.parquet',dir=destination.parent)
            os.close(fd)
            stage = Path(name)
            staged.append((destination,stage))
            data.to_parquet(stage,index=False)
            pd.testing.assert_frame_equal(data,pd.read_parquet(stage),check_dtype=False)
            if destination.exists():
                original = backup/destination.relative_to(root)
                original.parent.mkdir(parents=True,exist_ok=True)
                shutil.copy2(destination,original)
        report['status']='installing';save()
        for destination,stage in staged:
            os.replace(stage,destination)
            installed.append(destination)
        report['status']='installed';save()
    except Exception:
        for destination in reversed(installed):
            original = backup/destination.relative_to(root)
            if original.exists(): shutil.copy2(original,destination)
            else: destination.unlink(missing_ok=True)
        report['status']='rolled_back';save()
        raise
    finally:
        for _,stage in staged: stage.unlink(missing_ok=True)
    return manifest
