# Model backfills and recovery

## Deterministic NBM forecasts from QMD

Use `--model nbm` with `precip6hr`, `precip24hr`, `maxt`, or `mint` to archive
the unannotated deterministic interval field from the operational QMD files.
NBMQMD continues to archive the percentile fields separately. The deterministic
selection excludes percentile levels, exceedance probabilities, ensemble means,
and standard deviations, and matches the exact 6/24/18/18-hour interval.
For example, at f036, precip6hr selects `30-36 hour acc fcst`, while precip24hr
selects `12-36 hour acc fcst`; `24-36` is a 12-hour amount and is not selected.

Local files use the normal regional NBM paths:

| Element | Alaska output | Value column / units |
| --- | --- | --- |
| precip6hr | `model/nbm/precip6hr/YYYY_MM_archive.parquet` | `precip6hr`, inches |
| precip24hr | `model/nbm/precip24hr/YYYY_MM_archive.parquet` | `precip24hr`, inches |
| maxt | `model/nbm/maxt/YYYY_MM_archive.parquet` | `maxt`, degrees F |
| mint | `model/nbm/mint/YYYY_MM_archive.parquet` | `mint`, degrees F |

Hawaii uses `hawaii/model/nbm/...`. Forecast leads and cycle spacing are copied
from NBMQMD: precip6hr f006 through f120 every 6 hours; precip24hr f024, f030,
f036, then f048 through f168 every 12 hours; temperatures f018 through f174
every 12 hours. A 00Z start uses 00/12Z source cycles, just like NBMQMD's current
12-hour schedule. Not every requested file contains both maximum and minimum
temperature intervals; absent fields are reported and skipped, never replaced
by the opposite temperature statistic or an instantaneous value.

Initialization and valid times come from the GRIB reference and explicit interval
endpoint, rather than inferring the reference from `pygrib.validDate`. Daily
checkpoints and monthly deduplication apply normally. The daily wrapper now
includes these four elements under NBM and uses the QMD 00Z date window for them;
NBM core wind and snow keep their existing 01/07/13/19Z schedule.

Example (all times UTC):

```shell
python run_model_archiver.py --region alaska --model nbm --element maxt --start 202610250000 --end 202610251200 --local
```

For these four QMD-sourced elements, use 00/06/12/18 UTC start/end hours, not
NBM core's one-hour-offset cycles. Existing NBMQMD archives are not converted
or moved; the deterministic values must be extracted from their source fields.

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


## Rolling precipitation observations

Both `run_obs_archiver.py` and the direct `obs_archiver.py` entry point request
`precip24hr` totals at nominal endpoints 00/06/12/18 UTC. For each station batch,
four offset `pmode=intervals&interval=24` requests return independent daily totals
for those endpoint hours over the requested range. The archiver stores each
returned `total` directly; it does not sum six-hour amounts. Each offset request
starts 24 hours before its first requested endpoint and ends exactly at its
last requested endpoint, so the first day is covered without partial trailing
intervals. `precip6hr` uses one request with `interval=6`.

`start_time`/`end_time` retain actual report boundaries (for example 23:53Z
rather than 00Z); `first_report`/`last_report` repeat those explicit bounds.
`nominal_start_time`/`nominal_end_time` identify the requested UTC window by
aligning `last_report` to the accumulation grid anchored by the request start.
The API `interval` ordinal is not treated as a calendar slot: numbering may
be compacted when an interval is omitted. Monthly outputs are
partitioned by the nominal endpoint's calendar month. The actual duration and
both actual boundaries must fit the requested window within the configured
API interval tolerance (30 minutes by default). Missing, negative, nonfinite,
short, shifted or conflicting totals are excluded. Identical repeated reports
are deduplicated. API errors exhausted after retries fail the chunk instead of
silently omitting a station batch. These checks also apply to `precip6hr`.

Existing `precip24hr` files are not automatically repaired. Pause concurrent
writers, back up the entire affected `model/obs/precip24hr/` directory (or its
Hawaii equivalent), and rebuild into an empty directory. Appending alone cannot
remove invalid legacy totals. Retain the originals for comparison. Example:

```shell
python run_obs_archiver.py --region alaska --element precip24hr --start "2026-05-01 00:00" --end "2026-10-28 00:00" --local
```

Use the desired inclusive nominal endpoint range; the archiver requests its
own preceding accumulation period, so do not shift the command-line start back a day. Confirm four
nominal endpoint hours per complete day for stations with sufficient reports,
inspect actual durations, and check for duplicate station/accumulation/endpoint
keys before replacing the verification app's observation copies. Stations with
missing or inconsistent reports can legitimately have fewer endpoints. The code
has offline response and Parquet regression tests; validate representative live
stations before treating a historical rebuild as complete.


## NDFD regional station coverage

Every NDFD element now fetches a fresh regional station inventory using the
same discovery function as OBS. Alaska uses the mainland/western Aleutian
bounding boxes, the dateline endpoint workaround and independent local
coordinate validation. No network, active-status, observed-variable or
precipitation-sensor filter restricts model extraction. Hawaii retains its
state selector and includes all statuses. Station IDs remain separate even
when two IDs describe one physical site.

Legacy `obs/alaska_{element}_obs_metadata.csv` files (and Hawaii equivalents)
are no longer read by NDFD. They can remain in place. Each NDFD invocation
requires `SYNOPTIC_API_KEY` and writes a diagnostic snapshot to
`obs/ndfd_{element}_model_sites.csv` (under `hawaii/obs/` for Hawaii). These
snapshots are never reused as selection caches. A locked snapshot produces a
warning; API failures or an empty usable inventory stop the run rather than
silently falling back to a restricted or partial list.

Pull the complete repository update, test a single day and compare its station
IDs with the OBS inventory before starting a historical backfill. Existing
Parquet archives are unchanged by the code update; rerun affected NDFD months
to extract forecasts for previously omitted stations. Avoid concurrent writers
to the same monthly file. This change does not modify the separate
`ModelArchiver` station-discovery policy for NBM/HRRR/URMA/etc.


## NDFD decoder concurrency

NDFD downloads and point extraction can still use `MAX_WORKERS`, but native
GRIB reads are serialized by one shared lock. `ndfd_grib.py` opens, fully loads,
and closes each cfgrib dataset inside that lock. This includes both wind
components and all scalar elements. Loading eagerly prevents later `.values`
access from invoking the native decoder concurrently in another worker.

Each read prints `Decoding NDFD GRIB: <source>` with flushing enabled so a
native termination identifies the source being processed. Python-level read
or download failures now propagate and prevent the monthly chunk from being
saved as an incomplete archive. Pending workers are cancelled after a failure;
previously saved monthly files remain in place. A native fatal scanner error
cannot be handled by Python exception retries; serialization addresses the
observed concurrent-decoder failure path.

Validation includes concurrent-download/serialized-decoder regression tests,
Hawaii wind extraction, failure propagation and decoder cleanup. Real NOAA
July 1 QPF and MaxT GRIBs were also loaded from four workers and extracted to
Parquet in the Linux test environment. The original Windows scanner crash
cannot be reproduced or ruled out for every library build by those tests;
retry the failing July chunk after deployment and retain its log/exit status.


### Monthly precipitation gaps and endpoint alignment

The first direct-total implementation used the API's `interval` ordinal as a
fixed offset from the request start. This can discard all subsequent valid
reports after a missing interval if the response numbers only returned rows.
One-day tests did not expose this failure. A September regression reproduces
the uploaded PAJN pattern (76 rows) from only two brief gaps, then verifies
that the corrected mapper keeps the remaining 118 independent valid windows.
That is a synthetic regression, not a claim that PAJN's live September archive
will necessarily contain 118 rows.

The mapper now uses the actual last_report timestamp and the request's offset
accumulation grid; first_report, last_report and duration are still validated.
Both sparse and compact interval numbering work. The same fix applies to
precip6hr because it uses this mapper too. Re-request affected monthly OBS
ranges after updating. Files produced by the direct-total implementation can
be backed up and refetched to merge missing windows; earlier legacy files
without validated nominal endpoints should still be rebuilt as described above.
A retained monthly Parquet alone cannot establish which absent totals were
available from Synoptic; compare a rebuilt month with independent daily calls
or the raw API results before declaring complete coverage.


## Normalize NDFD filenames already under model/ndfd

`migrate_ndfd_archives.py` now scans both `ndfd/` and `model/ndfd/`, including
the equivalent directories under `hawaii/`. This covers archives whose folders
were moved previously but whose names still look like
`2026_09_ndfd_precip6hr_archive.parquet`. Their destination is
`model/ndfd/precip6hr/2026_09_archive.parquet`. Already standardized files are
skipped and never treated as sources for themselves. When a standard-named
file exists, the migration backs up both it and the legacy files, merges them
and removes only exact duplicate rows. Differing records remain preserved.

Stop concurrent archive writers, then preview and apply using the archive
root containing `model/` (not `model/ndfd/` itself):

```shell
python migrate_ndfd_archives.py --root /path/to/alaska_verification --region all
python migrate_ndfd_archives.py --root /path/to/alaska_verification --region all --apply
```

Keep the timestamped backup directory and manifest. The default root is the
script's directory. Future local `run_ndfd_archiver.py --local` writes already
use `model/ndfd/<element>/YYYY_MM_archive.parquet`. The S3 writer still uses
legacy filenames; this local migration does not change remote objects.
