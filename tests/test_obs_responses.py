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
    def run_metadata(self, get, state='ak'):
        tree = ast.parse((Path(__file__).parents[1] / 'obs_archiver.py').read_text())
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'ObsArchiver')
        method = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == 'get_station_metadata')
        ns = dict(requests=SimpleNamespace(get=get))
        exec(compile(ast.Module(body=[method], type_ignores=[]), 'obs_archiver.py', 'exec'), ns)
        instance = SimpleNamespace(state=state, hfmetar=0, api_token='secret',
                                   metadata_url='https://example.test')
        selected = ns['get_station_metadata'](instance)
        return selected, instance

    def test_alaska_discovery_includes_missing_state_and_western_aleutians(self):
        inventory = [
            dict(STID='NKXA2', STATE='AK', STATUS='INACTIVE', LATITUDE=58.255, LONGITUDE=-134.945, CWA='AJK'),
            dict(STID='NDBCRIXA2', STATE=None, STATUS='ACTIVE', LATITUDE=58.177, LONGITUDE=-135.052),
            dict(STID='WESTERN_ISLAND', STATE=None, STATUS='INACTIVE', LATITUDE=52, LONGITUDE=173),
            dict(STID='PHNL', STATE='HI', STATUS='ACTIVE', LATITUDE=21.3, LONGITUDE=-157.9),
        ]
        def metadata_response(url, params, timeout):
            west, south, east, north = map(float, params['bbox'].split(','))
            rows = [s for s in inventory if west <= s['LONGITUDE'] <= east and
                    south <= s['LATITUDE'] <= north and
                    (not params.get('state') or s['STATE'] == params['state'].upper()) and
                    (not params.get('status') or s['STATUS'].lower() == params['status'].lower())]
            return self.response({'SUMMARY': {'RESPONSE_CODE': 1}, 'STATION': rows})
        get = Mock(side_effect=metadata_response)
        selected, instance = self.run_metadata(get)
        self.assertEqual(set(selected), {'NKXA2', 'NDBCRIXA2', 'WESTERN_ISLAND'})
        self.assertEqual(instance.station_metadata['NKXA2']['cwa'], 'AJK')
        self.assertEqual(get.call_count, 2)
        for call in get.call_args_list:
            for excluded in ('state', 'status', 'network'):
                self.assertNotIn(excluded, call.kwargs['params'])

    def test_metadata_keeps_aliases_but_deduplicates_repeated_ids(self):
        payload = {'SUMMARY': {'RESPONSE_CODE': 1}, 'STATION': [
            {'STID': 'RIXA2'}, {'STID': 'NDBCRIXA2'}]}
        selected, _ = self.run_metadata(Mock(return_value=self.response(payload)))
        self.assertEqual(selected, ['RIXA2', 'NDBCRIXA2'])

    def test_hawaii_retains_state_selection_and_inactive_listings(self):
        get = Mock(return_value=self.response({'SUMMARY': {'RESPONSE_CODE': 1}, 'STATION': [
            {'STID': 'PHNL', 'STATUS': 'ACTIVE'}, {'STID': 'OLD_HI', 'STATUS': 'INACTIVE'}]}))
        selected, _ = self.run_metadata(get, state='hi')
        self.assertEqual(selected, ['PHNL', 'OLD_HI'])
        self.assertEqual(get.call_count, 1)
        self.assertEqual(get.call_args.kwargs['params']['state'], 'hi')
        self.assertNotIn('status', get.call_args.kwargs['params'])

    def test_empty_box_preserves_other_box_results(self):
        get = Mock(side_effect=[self.response({'SUMMARY': {'RESPONSE_CODE': '2'}}),
            self.response({'SUMMARY': {'RESPONSE_CODE': 1}, 'STATION': [{'STID': 'WESTERN_ISLAND'}]})])
        self.assertEqual(self.run_metadata(get)[0], ['WESTERN_ISLAND'])

    def test_metadata_error_does_not_return_partial_inventory(self):
        get = Mock(side_effect=[
            self.response({'SUMMARY': {'RESPONSE_CODE': 1}, 'STATION': [{'STID': 'NKXA2'}]}),
            self.response({'SUMMARY': {'RESPONSE_CODE': 200}})])
        with self.assertRaisesRegex(ValueError, 'metadata response'):
            self.run_metadata(get)

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
