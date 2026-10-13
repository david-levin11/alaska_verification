"""Verify temperature replacement through the real runner and Parquet writers."""
import ast
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import pandas as pd
import pytest
from dateutil.relativedelta import relativedelta
import os
import sys

ROOT = Path(__file__).parents[1]
KEYS = ["stid", "date", "window_start", "window_end"]


def load_function(path, name, namespace, class_name=None):
    tree = ast.parse((ROOT / path).read_text())
    body = tree.body
    if class_name:
        body = next(n for n in body if isinstance(n, ast.ClassDef)
                    and n.name == class_name).body
    node = next(n for n in body if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), path, "exec"), namespace)
    return namespace[name]


def sample(element, values):
    date = pd.Timestamp("2026-06-02", tz="UTC")
    start = date + pd.Timedelta(hours=12 if element == "maxt" else 0)
    end = start + pd.Timedelta(hours=18)
    return pd.DataFrame([
        dict(stid="A", date=date, window_start=start, window_end=end,
             **{"tmax" if element == "maxt" else "tmin": value}, lat=str(60+i))
        for i, value in enumerate(values)
    ])


@pytest.mark.parametrize("element", ["maxt", "mint"])
@pytest.mark.parametrize("local", [True, False])
@pytest.mark.parametrize("existing", [True, False])
def test_runner_keeps_latest_temperature(tmp_path, element, local, existing):
    config = SimpleNamespace(
        configure_region=Mock(), OBS_VARS={element: ["air_temp"]},
        OBS=str(tmp_path), MODEL_DIR=str(tmp_path),
        S3_URLS={"obs": str(tmp_path) + "/"})
    writer_ns = dict(pd=pd, Path=Path, fsspec=SimpleNamespace(
        filesystem=lambda *a, **k: SimpleNamespace(
            exists=lambda p: Path(p).exists(), open=lambda p, mode: open(p, mode))))
    local_writer = load_function("archiver_base.py", "write_local_output",
                                 writer_ns, "Archiver")
    s3_writer = load_function("archiver_base.py", "write_to_s3",
                              writer_ns, "Archiver")
    incoming = sample(element, [51., 49.])
    # A second station must survive; latest value can be lower even for Tmax.
    incoming = pd.concat([incoming, incoming.iloc[[0]].assign(stid="B")],
                         ignore_index=True)
    class FakeArchiver:
        def __init__(self, cfg):
            pass
        def get_station_metadata(self):
            return ["A", "B"]
        def fetch_tmax_12to06_timeseries(self, *args):
            return incoming.copy()
        fetch_tmin_00to18_timeseries = fetch_tmax_12to06_timeseries
        write_local_output = local_writer
        write_to_s3 = s3_writer
    ns = dict(pd=pd, config=config, ObsArchiver=FakeArchiver,
              relativedelta=relativedelta, os=os, sys=sys)
    run = load_function("run_obs_archiver.py", "run_monthly_obs_archiving", ns)
    path = (tmp_path / "obs" / element / "2026_06_archive.parquet" if local else
            tmp_path / ("2026_06_obs_" + element + "_archive.parquet"))
    if existing:
        path.parent.mkdir(parents=True, exist_ok=True)
        sample(element, [40., 45.]).to_parquet(path, index=False)
    run(pd.Timestamp("2026-06-02"), pd.Timestamp("2026-06-02"), element, local)
    result = pd.read_parquet(path)
    value = "tmax" if element == "maxt" else "tmin"
    assert not result.duplicated(KEYS).any()
    assert result.set_index("stid")[value].to_dict() == {"A": 49., "B": 51.}
    assert result.loc[result.stid == "A", "lat"].iloc[0] == "61"
    # Repeating the same request does not add rows.
    run(pd.Timestamp("2026-06-02"), pd.Timestamp("2026-06-02"), element, local)
    pd.testing.assert_frame_equal(result, pd.read_parquet(path))
