"""Parquet IO for REFS. Monthly updates require a single writer per output root."""
import os
from pathlib import Path
import tempfile
import pandas as pd
import fsspec


def filesystem(path, profile=None):
    options = {'profile': profile} if str(path).startswith('s3://') and profile else {}
    return fsspec.core.url_to_fs(str(path), **options)


def read_source(root, model, element, month, profile=None):
    """Accept the local layout or the existing flat per-model S3 archive layout."""
    prefix = str(root).rstrip('/')
    paths = [f'{prefix}/{model}/{element.lower()}/{month}_archive.parquet']
    if prefix.startswith('s3://'):
        paths.append(f'{prefix}/{model}/{month}_{model}_{element.lower()}_archive.parquet')
    for path in paths:
        fs, key = filesystem(path, profile)
        if fs.exists(key):
            with fs.open(key, 'rb') as stream:
                return pd.read_parquet(stream)
    return pd.DataFrame()


def write_parquet(frame, path, profile=None):
    fs, key = filesystem(path, profile)
    if str(path).startswith('s3://'):
        # Upload a fully serialized object; failures propagate to the cron runner.
        with tempfile.TemporaryFile() as stream:
            frame.to_parquet(stream, index=False)
            stream.seek(0)
            with fs.open(key, 'wb') as dest:
                import shutil
                shutil.copyfileobj(stream, dest)
    else:
        target = Path(path).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        handle, temporary = tempfile.mkstemp(prefix=target.name+'.', suffix='.tmp', dir=target.parent)
        os.close(handle)
        try:
            frame.to_parquet(temporary, index=False)
            os.replace(temporary, target)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


def update_monthly(frame, output_root, element, profile=None):
    """Upsert by station/cycle/valid/variable, retaining an older complete result
    if a rerun has become incomplete. Partition by ensemble initialization month.
    Reject mixed statistic recipes in a single monthly file.
    """
    if frame.empty:
        return []
    keys = ['station_id','ensemble_init_time','valid_time','value_column']
    outputs = []
    for month, new in frame.groupby(pd.to_datetime(frame.ensemble_init_time, utc=True).dt.strftime('%Y_%m')):
        path = f"{str(output_root).rstrip('/')}/{element.lower()}/{month}_archive.parquet"
        fs, key = filesystem(path, profile)
        combined = new.copy()
        if fs.exists(key):
            with fs.open(key,'rb') as stream:
                old = pd.read_parquet(stream)
            if not old.empty:
                if 'statistics_config' not in old or 'statistics_config' not in new:
                    raise ValueError(f'Statistics recipe differs in {path}; use a separate output root for this experiment')
                for variable, rows in pd.concat([old,new],ignore_index=True).groupby('value_column'):
                    if rows.statistics_config.isna().any() or rows.statistics_config.nunique() != 1:
                        raise ValueError(f'Statistics recipe differs for {variable} in {path}; rebuild this derived file or use a separate output root')
                combined = pd.concat([old,new],ignore_index=True)
        # Stable sort: complete beats incomplete; otherwise the newest row wins.
        combined = combined.sort_values('complete',kind='stable').drop_duplicates(keys,keep='last')
        combined = combined.sort_values(keys).reset_index(drop=True)
        write_parquet(combined,path,profile)
        print(f'Updated {path}: {len(new):,} processed rows; {len(combined):,} total rows')
        outputs.append(path)
    return outputs
