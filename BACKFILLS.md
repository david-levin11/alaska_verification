# Model backfills and recovery

`run_model_archiver.py` now processes one UTC calendar day at a time for every
supported model, including HRRR, NBM core, and NBMQMD. Each completed day's
records are appended and deduplicated in the existing monthly Parquet file.
Local/S3 routing, regional separation, forecast schedules, and the inclusive
command-line end time are unchanged. The daily wrapper inherits this behavior.

Model `.idx` and GRIB byte-range requests retry connection failures, timeouts,
broken response bodies, HTTP 429, and HTTP 5xx up to five total attempts, with
2/4/8/16-second waits. Missing indexes (404) and unmatched fields retain the
existing skip behavior; other permanent index errors stop the chunk immediately.
These changes apply to model downloads, not the separate NDFD/observation clients.

Subsets are written to `.part` files and renamed only when complete. Failed
transfers remove their partial file. Exhausted retries stop the current chunk
without writing it and print the failing URL and restart date. Previously written
days remain saved. This is daily saving, not automatic detection of completed days:
rerun from the failed day's printed `--start`, using your original end date.
Rerunning earlier dates is supported by the existing archive deduplication.

Example: rebuild June HRRR wind locally:

```shell
python run_model_archiver.py --region alaska --model hrrr --element Wind --start 202606010000 --end 202606302359 --local
```

Wait for the archive save message before considering a day written. Daily saving
reduces memory use and work lost to a download failure, but reads/rewrites the
monthly Parquet file more frequently. Do not run concurrent writers targeting
the same monthly file.

Focused offline tests (with requests and pandas installed):

```shell
python -m unittest discover -s tests -p test_download_retry.py
python -m unittest discover -s tests -p test_daily_checkpoints.py
python -m unittest discover -s tests -p test_precip_subset_selection.py
```
