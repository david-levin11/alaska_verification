"""Test production response handling without optional GRIB dependencies."""
import ast
import contextlib
import io
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
import pandas as pd
import requests


class ObservationResponses(unittest.TestCase):
    def run_fetch(self, responses, station_ids=None):
        tree = ast.parse((Path(__file__).parents[1] / 'obs_archiver.py').read_text())
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'ObsArchiver')
        method = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == 'fetch_observations')
        get = Mock(side_effect=responses)
        sleep = Mock()
        ns = dict(pd=pd, requests=SimpleNamespace(get=get), sleep=sleep)
        exec(compile(ast.Module(body=[method], type_ignores=[]), 'obs_archiver.py', 'exec'), ns)
        instance = SimpleNamespace(
            _chunk_station_ids=lambda ids: [ids[i:i+1] for i in range(len(ids))],
            initial_wait=1, max_retries=3, obs_fields=['wind_speed'],
            hfmetar=0, api_token='secret', url='https://example.test',
            process_obs_data=lambda rows: pd.DataFrame(rows))
        out = io.StringIO()
        self.get, self.sleep, self.out = get, sleep, out
        with contextlib.redirect_stdout(out):
            return ns['fetch_observations'](instance, ['A'] if station_ids is None else station_ids, '202610060000', '202610070000')

    @staticmethod
    def response(payload, status=200):
        r = Mock()
        r.json.return_value = payload
        if status >= 400:
            r.raise_for_status.side_effect = requests.HTTPError('URL contains secret')
        return r

    def test_no_matching_stations_does_not_retry(self):
        for code in (2, '2'):
            df = self.run_fetch([self.response({'SUMMARY': {'RESPONSE_CODE': code}})])
            self.assertTrue(df.empty)
            self.assertEqual(self.get.call_count, 1)
            self.sleep.assert_not_called()
            self.assertIn('No matching stations or access unavailable', self.out.getvalue())
            self.assertIn('stations=A', self.out.getvalue())
            self.assertIn('requested=1, returned=0', self.out.getvalue())

    def test_mixed_batches_preserve_success(self):
        df = self.run_fetch([self.response({'SUMMARY': {'RESPONSE_CODE': 2}}),
                             self.response({'SUMMARY': {'RESPONSE_CODE': 1}, 'STATION': [{'STID': 'B'}]})], ['A', 'B'])
        self.assertEqual(df.STID.tolist(), ['B'])
        self.assertIn('requested=2, returned=1', self.out.getvalue())

    def test_transient_errors_retry_and_recover(self):
        df = self.run_fetch([requests.ConnectionError('secret'), self.response({}, 503),
                             self.response({'STATION': [{'STID': 'A'}]})])
        self.assertEqual(len(df), 1)
        self.assertEqual(self.get.call_count, 3)
        self.assertEqual(self.sleep.call_count, 2)
        self.assertNotIn('secret', self.out.getvalue())

    def test_unexpected_response_fails_after_retries(self):
        with self.assertRaises(RuntimeError):
            self.run_fetch([self.response({'SUMMARY': {'RESPONSE_CODE': 99}})] * 3)
        self.assertEqual(self.get.call_count, 3)
        self.assertEqual(self.sleep.call_count, 2)

    def test_empty_station_list(self):
        self.assertTrue(self.run_fetch([], []).empty)
        self.get.assert_not_called()

    def test_success_with_empty_station_array(self):
        self.assertTrue(self.run_fetch([self.response({'SUMMARY': {'RESPONSE_CODE': 1}, 'STATION': []})]).empty)
        self.sleep.assert_not_called()


if __name__ == '__main__':
    unittest.main()
