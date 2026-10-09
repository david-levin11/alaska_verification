import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from download_retry import get_with_retry
from test_precip_subset_selection import downloader


class DownloadRetryTests(unittest.TestCase):
    @patch('download_retry.time.sleep')
    @patch('download_retry.requests.get')
    def test_connection_timeout_and_broken_body_recover(self, get, sleep):
        good = Mock(status_code=206, content=b'GRIB')
        get.side_effect = [requests.ConnectionError(), requests.Timeout(),
                           requests.exceptions.ChunkedEncodingError(), good]
        self.assertIs(get_with_retry('https://example.test/grib'), good)
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [2, 4, 8])

    @patch('download_retry.time.sleep')
    @patch('download_retry.requests.get')
    def test_server_and_rate_limit_exhaustion(self, get, sleep):
        for status in [429, 500, 503]:
            response = Mock(status_code=status)
            response.raise_for_status.side_effect = requests.HTTPError()
            get.reset_mock()
            sleep.reset_mock()
            get.return_value = response
            with self.assertRaisesRegex(RuntimeError, 'after 5 attempts: https://example.test/idx'):
                get_with_retry('https://example.test/idx')
            self.assertEqual(get.call_count, 5)
            self.assertEqual([c.args[0] for c in sleep.call_args_list], [2, 4, 8, 16])
            self.assertEqual(response.close.call_count, 5)

    @patch('download_retry.requests.get')
    def test_missing_file_is_not_retried(self, get):
        response = Mock(status_code=404, content=b'')
        get.return_value = response
        self.assertIs(get_with_retry('https://example.test/idx'), response)
        get.assert_called_once()

    def test_failed_subset_removes_partial_and_preserves_previous_file(self):
        download, _ = downloader('1:0:d=2026100318:APCP:surface:0-51 hour acc fcst:')
        original_get = download.__globals__['get_with_retry']
        def fail_body(url, **kwargs):
            if url.endswith('.idx'):
                return original_get(url, **kwargs)
            raise RuntimeError('exhausted retries')
        download.__globals__['get_with_retry'] = fail_body
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'subset.grib2'
            path.write_bytes(b'previous complete file')
            with self.assertRaisesRegex(RuntimeError, 'exhausted'):
                download('https://example.test/model.f051.ak.grib2', str(path),
                         [':APCP:surface:'], 'rrfs', 'precip6hr')
            self.assertFalse(Path(str(path) + '.part').exists())
            self.assertEqual(path.read_bytes(), b'previous complete file')
