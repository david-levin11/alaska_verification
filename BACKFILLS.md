# Model backfills and recovery

## Standardized local NDFD layout

New local NDFD writes use `model/ndfd/<element>/YYYY_MM_archive.parquet`, or
`hawaii/model/ndfd/<element>/YYYY_MM_archive.parquet` for Hawaii. S3 routing and
filenames are unchanged. Existing legacy files are not moved automatically.

With archive writers stopped, preview and then apply the migration:

```shell
python migrate_ndfd_archives.py
python migrate_ndfd_archives.py --apply
```

The default root is this script's directory. Use `--root /path/to/archive-root`
for another tree and `--region hawaii` or `--region all` for regional selection.
Legacy inputs can be under `ndfd/<element>/` or directly in `ndfd/`.
All affected source and existing destination files are copied to a timestamped
`ndfd_migration_backup_*` directory before any moves. Existing destinations are
merged with sources, removing only exact duplicate rows and preserving differing
records. A lone source without a destination is moved without rewriting its bytes.
The backup's `manifest.json` records completed files and any failure; migration is
safe to rerun. On failure, previously completed files may already be migrated,
and each successfully migrated file has its original preserved in the backup.
Empty legacy folders remain.

## OBS station discovery

Alaska observation runs discover stations using two geographic boxes in
`west,south,east,north` order: `-180,50,-129,73` and `170,50,180,73`.
The second box includes the western Aleutians across the dateline. API requests
use `-179.999999,50,-129,73` and `170,50,179.999999,73`: live probes on
2026-10-21 showed that exact -180 and +180 endpoints returned thousands of
out-of-box stations, while both inset requests returned zero out-of-box stations.
This tiny inset (less than 0.1 m at these latitudes) avoids the observed API
endpoint behavior; local validation retains the original full geographic bounds.
No state,
network, or active-status filter is applied to Alaska OBS discovery. These
regional bounds intentionally include offshore and some neighboring stations;
they are not an exact Alaska jurisdiction boundary. Hawaii retains its state
selection, including all statuses.
Returned coordinates are also checked locally against the union of both boxes
before requesting observations. Out-of-region listings and listings with missing
or invalid coordinates are rejected and counted in the log, even if the API
returns them for a bbox query.

Stations returned by both queries are deduplicated by STID; distinct prefixed
and unprefixed IDs are preserved. The subsequent observation requests still
select the requested variables and dates. The legacy diagnostic filename
`obs/obs_stations_active.txt` now lists all selected statuses. Discovery runs
fresh on each OBS invocation, so no metadata cache needs to be deleted.
Rerun OBS backfills to add missing station observations to existing archives.
This does not change model or NDFD station discovery, or verification alias matching.

## Model checkpoints

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
