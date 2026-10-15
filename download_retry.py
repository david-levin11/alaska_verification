"""Bounded retries for model indexes and GRIB byte-range downloads."""
import time

import requests


def get_with_retry(url, *, attempts=5, **kwargs):
    """Retry transient failures; return permanent HTTP responses to the caller."""
    if attempts < 1:
        raise ValueError("attempts must be positive")
    kwargs.setdefault("timeout", 60)
    for attempt in range(1, attempts + 1):
        response = None
        try:
            response = requests.get(url, **kwargs)
            if response.status_code == 429 or 500 <= response.status_code < 600:
                response.raise_for_status()
            # Force body transfer inside the retry boundary, including broken bodies.
            response.content
            return response
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError, requests.HTTPError) as exc:
            if response is not None:
                response.close()
            if attempt == attempts:
                raise RuntimeError(
                    f"Download failed after {attempts} attempts: {url} "
                    f"({type(exc).__name__}); current chunk was not saved."
                ) from exc
            delay = 2 ** attempt
            print(f"WARNING: Download attempt {attempt}/{attempts} failed for {url} "
                  f"({type(exc).__name__}); retrying in {delay}s")
            time.sleep(delay)
