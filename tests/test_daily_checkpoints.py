"""Exercise the production runner with in-memory sources and archive writes."""
import ast
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd


class DailyCheckpointTests(unittest.TestCase):
    def run_case(self, model, start, end, fail_day=None):
        with tempfile.TemporaryDirectory() as tmp:
            config = SimpleNamespace(
                configure_region=lambda region: None, TMP=tmp, MODEL_DIR=tmp,
                HERBIE_MODELS=[model], AVAILABLE_FIELDS={model: ['Wind']},
                NBM_START_HOURS={model: [1, 7, 13, 19] if model == 'nbm' else [0, 6, 12, 18]},
                HERBIE_CYCLES={model: '6h'},
            )
            calls, writes = [], []
            def fetch(start, end):
                calls.append((start, end))
                return [start]
            def process(urls):
                if urls[0].day == fail_day:
                    raise RuntimeError('download exhausted')
                return pd.DataFrame({'station_id': ['PAJN'], 'init_time': urls})
            archiver = SimpleNamespace(fetch_file_list=fetch, process_files=process,
                write_local_output=lambda df, path, **kw: writes.append((df.copy(), path)))
            tree = ast.parse((Path(__file__).parents[1] / 'run_model_archiver.py').read_text())
            nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'run_monthly_archiving']
            ns = dict(config=config, tempfile=SimpleNamespace(), pd=pd, os=os,
                      shutil=shutil, sys=sys, ModelArchiver=Mock(return_value=archiver))
            exec(compile(ast.Module(body=nodes, type_ignores=[]), 'runner', 'exec'), ns)
            try:
                ns['run_monthly_archiving'](pd.Timestamp(start), pd.Timestamp(end), model, 'Wind', True)
            except RuntimeError:
                if fail_day is None:
                    raise
            return calls, writes

    def test_daily_chunks_preserve_cycle_alignment_at_month_boundary(self):
        for model, hour in [('hrrr', 0), ('nbm', 1), ('nbmqmd', 0), ('rrfsens', 0)]:
            with self.subTest(model=model):
                calls, writes = self.run_case(model, f'2026-06-30 {hour:02}:00', f'2026-07-02 {hour:02}:00')
                self.assertEqual(len(calls), 3)
                self.assertTrue(all(start.hour == hour for start, _ in calls))
                self.assertTrue(all(start.date() == end.date() for start, end in calls))
                self.assertTrue(writes[0][1].endswith('2026_06_archive.parquet'))
                self.assertTrue(writes[1][1].endswith('2026_07_archive.parquet'))
                self.assertEqual(calls[-1][0], calls[-1][1])

    def test_exhausted_download_keeps_prior_day_and_does_not_write_failed_day(self):
        calls, writes = self.run_case('hrrr', '2026-06-01', '2026-06-04', fail_day=2)
        self.assertEqual(len(calls), 2)
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0][0].init_time.iloc[0], pd.Timestamp('2026-06-01'))
