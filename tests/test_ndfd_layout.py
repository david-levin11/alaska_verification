"""Test local NDFD migration and real writer routing without GRIB downloads."""
import ast
import os
from pathlib import Path
from types import SimpleNamespace
import shutil

import pandas as pd
import pytest
from dateutil.relativedelta import relativedelta

from migrate_ndfd_archives import migrate


def frame(values):
    return pd.DataFrame({"station_id": ["PAJN"] * len(values),
                         "valid_time": [pd.Timestamp("2026-05-02")] * len(values),
                         "forecast_hour": [24] * len(values), "wind_speed_kt": values})


def write(path, values):
    path.parent.mkdir(parents=True, exist_ok=True)
    frame(values).to_parquet(path, index=False)


def test_preview_move_backups_and_repeat_run(tmp_path):
    source = tmp_path / "ndfd/wind/2026_05_ndfd_wind_archive.parquet"
    destination = tmp_path / "model/ndfd/wind/2026_05_archive.parquet"
    write(source, [10])
    original = source.read_bytes()
    migrate(tmp_path)
    assert source.read_bytes() == original and not destination.exists()
    assert not list(tmp_path.glob("ndfd_migration_backup_*"))
    backup = migrate(tmp_path, apply=True)
    assert destination.read_bytes() == original and not source.exists()
    assert (backup / source.relative_to(tmp_path)).read_bytes() == original
    assert migrate(tmp_path, apply=True) is None


def test_merge_preserves_differing_values_and_backs_up_destination(tmp_path):
    source = tmp_path / "ndfd/Wind/2026_05_ndfd_wind_archive.parquet"
    destination = tmp_path / "model/ndfd/wind/2026_05_archive.parquet"
    write(source, [10, 20])
    write(destination, [10, 30])
    previous = destination.read_bytes()
    backup = migrate(tmp_path, apply=True)
    assert pd.read_parquet(destination).wind_speed_kt.tolist() == [10, 30, 20]
    assert (backup / destination.relative_to(tmp_path)).read_bytes() == previous
    assert not source.exists()


def test_all_regions_and_flat_legacy_sources(tmp_path):
    for prefix in ("", "hawaii/"):
        write(tmp_path / f"{prefix}ndfd/2026_05_ndfd_gust_archive.parquet", [10])
    migrate(tmp_path, region="all", apply=True)
    for prefix in ("", "hawaii/"):
        assert (tmp_path / f"{prefix}model/ndfd/gust/2026_05_archive.parquet").exists()


def test_multiple_sources_merge_into_one_destination(tmp_path):
    write(tmp_path / "ndfd/2026_05_ndfd_wind_archive.parquet", [10])
    write(tmp_path / "ndfd/wind/2026_05_archive.parquet", [10, 20])
    migrate(tmp_path, apply=True)
    destination = tmp_path / "model/ndfd/wind/2026_05_archive.parquet"
    assert pd.read_parquet(destination).wind_speed_kt.tolist() == [10, 20]
    assert not list((tmp_path / "ndfd").rglob("*.parquet"))


@pytest.mark.parametrize("region,prefix", [("alaska", ""), ("hawaii", "hawaii/")])
def test_rename_legacy_filename_already_in_model_directory(tmp_path, region, prefix):
    source = tmp_path / f"{prefix}model/ndfd/precip6hr/2026_09_ndfd_precip6hr_archive.parquet"
    destination = source.with_name("2026_09_archive.parquet")
    write(source, [10, 20])
    original = source.read_bytes()
    migrate(tmp_path, region=region)
    assert source.read_bytes() == original and not destination.exists()
    backup = migrate(tmp_path, region=region, apply=True)
    assert not source.exists()
    assert destination.read_bytes() == original
    assert (backup / source.relative_to(tmp_path)).read_bytes() == original
    assert migrate(tmp_path, region=region, apply=True) is None
    assert destination.read_bytes() == original


def test_merge_both_source_layouts_with_existing_standard_archive(tmp_path):
    old = tmp_path / "ndfd/wind/2026_09_ndfd_wind_archive.parquet"
    moved = tmp_path / "model/ndfd/wind/2026_09_ndfd_wind_archive.parquet"
    destination = moved.with_name("2026_09_archive.parquet")
    write(old, [10, 20]); write(moved, [20, 30]); write(destination, [10, 40])
    originals = {p: p.read_bytes() for p in (old, moved, destination)}
    backup = migrate(tmp_path, apply=True)
    assert not old.exists() and not moved.exists()
    assert set(pd.read_parquet(destination).wind_speed_kt) == {10, 20, 30, 40}
    assert len(pd.read_parquet(destination)) == 4
    for path, original in originals.items():
        assert (backup / path.relative_to(tmp_path)).read_bytes() == original
    assert migrate(tmp_path, apply=True) is None


def test_standard_files_are_not_touched_when_another_month_is_migrated(tmp_path):
    standard = tmp_path / "model/ndfd/wind/2026_08_archive.parquet"
    legacy = tmp_path / "model/ndfd/wind/2026_09_ndfd_wind_archive.parquet"
    write(standard, [10]); write(legacy, [20])
    original = standard.read_bytes()
    backup = migrate(tmp_path, apply=True)
    assert standard.read_bytes() == original
    assert not (backup / standard.relative_to(tmp_path)).exists()
    assert (legacy.parent / '2026_09_archive.parquet').exists()


def test_mismatched_element_in_current_layout_refuses_before_backup(tmp_path):
    source = tmp_path / "model/ndfd/gust/2026_09_ndfd_wind_archive.parquet"
    write(source, [10])
    with pytest.raises(ValueError, match="disagree"):
        migrate(tmp_path, apply=True)
    assert source.exists()
    assert not list(tmp_path.glob("ndfd_migration_backup_*"))


def test_mismatched_element_refuses_before_any_changes(tmp_path):
    source = tmp_path / "ndfd/gust/2026_05_ndfd_wind_archive.parquet"
    write(source, [10])
    with pytest.raises(ValueError, match="disagree"):
        migrate(tmp_path, apply=True)
    assert source.exists()
    assert not list(tmp_path.glob("ndfd_migration_backup_*"))


def test_failed_staging_keeps_sources_and_destination(tmp_path, monkeypatch):
    source = tmp_path / "ndfd/wind/2026_05_ndfd_wind_archive.parquet"
    destination = tmp_path / "model/ndfd/wind/2026_05_archive.parquet"
    write(source, [20]); write(destination, [10])
    original = destination.read_bytes()
    def fail(*a, **k):
        raise OSError("simulated full disk")
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fail)
    with pytest.raises(OSError, match="full disk"):
        migrate(tmp_path, apply=True)
    assert source.exists() and destination.read_bytes() == original
    assert not list(destination.parent.glob(".ndfd_migration_*"))


@pytest.mark.parametrize("region", ["alaska", "hawaii"])
@pytest.mark.parametrize("local", [True, False])
@pytest.mark.parametrize("element", ["wind", "minrh"])
def test_production_writer_routing(tmp_path, region, local, element):
    path = Path(__file__).parents[1] / "run_ndfd_archiver.py"
    tree = ast.parse(path.read_text())
    method = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run_monthly_archiving")
    model_root = tmp_path / ("hawaii/model" if region == "hawaii" else "model")
    cfg = SimpleNamespace(configure_region=lambda r: None, TMP=str(tmp_path / "tmp"),
                          MODEL_DIR=str(model_root), NDFD_DIR="legacy-should-not-be-used",
                          NDFD_FILE_STRINGS={"Wind": ["wspd", "wdir"], "minrh": ["minrh"]},
                          S3_URLS={"ndfd": "s3://example/ndfd/"})
    calls = []
    fake = SimpleNamespace(fetch_file_list=lambda *a: {"wspd": ["source"], "minrh": ["source"]},
        process_files=lambda files: frame([10]),
        write_local_output=lambda data, p, **kwargs: calls.append(p),
        write_to_s3=lambda data, p, **kwargs: calls.append(p))
    ns = dict(config=cfg, tempfile=SimpleNamespace(), pd=pd, relativedelta=relativedelta,
              shutil=shutil, os=os, NDFDArchiver=lambda *a, **k: fake)
    exec(compile(ast.Module(body=[method], type_ignores=[]), str(path), "exec"), ns)
    ns[method.name](pd.Timestamp("2026-05-01"), pd.Timestamp("2026-05-31"), element, local, region)
    expected = str(model_root / f"ndfd/{element}/2026_05_archive.parquet") if local else f"s3://example/ndfd/2026_05_ndfd_{element}_archive.parquet"
    assert calls == [expected]


@pytest.mark.parametrize('region', ['alaska', 'hawaii'])
@pytest.mark.parametrize('local', [True, False])
@pytest.mark.parametrize('start,end,expected', [
    ('2026-05-15 06:00','2026-06-15 12:00',[
        ('202605150600','202605312359','2026_05'),
        ('202606010000','202606151200','2026_06')]),
    ('2026-01-31','2026-03-02',[
        ('202601310000','202601312359','2026_01'),
        ('202602010000','202602282359','2026_02'),
        ('202603010000','202603020000','2026_03')]),
    ('2024-02-15','2024-03-01',[
        ('202402150000','202402292359','2024_02'),
        ('202403010000','202403010000','2024_03')]),
    ('2026-12-15','2027-01-02',[
        ('202612150000','202612312359','2026_12'),
        ('202701010000','202701020000','2027_01')]),
])
def test_backfill_uses_calendar_month_chunks(tmp_path,region,local,start,end,expected):
    # Exercise the real runner and real Parquet writes without network/GRIBs.
    path=Path(__file__).parents[1]/'run_ndfd_archiver.py'
    tree=ast.parse(path.read_text())
    method=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='run_monthly_archiving')
    model_root=tmp_path/('hawaii/model' if region=='hawaii' else 'model')
    cfg=SimpleNamespace(configure_region=lambda r:None,TMP=str(tmp_path/'tmp'),
        MODEL_DIR=str(model_root),NDFD_FILE_STRINGS={'minrh':['minrh']},
        S3_URLS={'ndfd':'s3://example/ndfd/'})
    windows=[]
    writes=[]
    def fetch(first,last):
        windows.append((first,last))
        return {'minrh':[first]}
    def process(files):
        ref=pd.to_datetime(files['minrh'][0],format='%Y%m%d%H%M')
        return pd.DataFrame(dict(station_id=['PAJN'],init_time=[ref],
            valid_time=[ref+pd.Timedelta(hours=18)],forecast_hour=[18],minrh=[42.]))
    def write(data,destination,**options):
        writes.append((destination,options))
        # Redirect S3 writes to a local test file; retain the actual URL above.
        target=Path(destination) if local else tmp_path/destination.rsplit('/',1)[1]
        target.parent.mkdir(parents=True,exist_ok=True)
        data.to_parquet(target,index=False)
        stored=pd.read_parquet(target)
        pd.testing.assert_frame_equal(data,stored)
    fake=SimpleNamespace(fetch_file_list=fetch,process_files=process,
        write_local_output=write,write_to_s3=write)
    ns=dict(config=cfg,tempfile=SimpleNamespace(),pd=pd,os=os,shutil=shutil,
        NDFDArchiver=lambda *a,**k:fake)
    exec(compile(ast.Module(body=[method],type_ignores=[]),str(path),'exec'),ns)
    ns[method.name](pd.Timestamp(start),pd.Timestamp(end),'minrh',local,region)
    assert windows==[(first,last) for first,last,month in expected]
    paths=[str(model_root/f'ndfd/minrh/{month}_archive.parquet') if local
        else f's3://example/ndfd/{month}_ndfd_minrh_archive.parquet'
        for first,last,month in expected]
    assert [p for p,options in writes]==paths
    assert all(options=={'dedup_columns':['station_id','init_time','valid_time']}
        for p,options in writes)
