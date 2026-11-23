# Import experimental NDFD NetCDF exports

`import_ndfd_experimental.py` reads batches from a directory on the machine
running the importer. It accepts `.nc`, `.nc.gz`, `.netcdf`, and `.netcdf.gz`.
Run it manually after receiving exports, or enable the optional daily workflow below.
It never downloads forecast grids or changes the source exports.

## Setup and configuration

Activate the existing archiving Python environment. The additional dependency is:

```sh
conda install -c conda-forge "netcdf4>=1.7,<2"
```

Copy `ndfd_exp_config.example.json` to `ndfd_exp_config.json`, then edit
`input_dir` to the directory containing the exports and `archive_root` to the
production **model directory**, not its parent. JSON relative paths resolve
relative to the configuration file. Use forward slashes or escaped backslashes
in Windows JSON paths. The local configuration is ignored by Git.

Preview, then install (the commands work in PowerShell and Linux shells):

```sh
python import_ndfd_experimental.py --config ndfd_exp_config.json
python import_ndfd_experimental.py --config ndfd_exp_config.json --apply
```

Alternatively, configure everything on the command line:

```sh
python import_ndfd_experimental.py --input-dir /data/experimental_exports --archive-root /data/model
python import_ndfd_experimental.py --input-dir /data/experimental_exports --archive-root /data/model --apply
```

CLI options override JSON. Add `--recursive` to search subdirectories, or
`--elements wind gust` to select particular products. All supported products are
selected by default.

Without `--station-csv`, the importer uses `SYNOPTIC_API_KEY` to discover a fresh
regional inventory, including all statuses and networks. Alaska uses the shared
geographic bounds and local coordinate validation rather than a STATE tag.
This metadata request is the only network access. For a fully offline import,
provide `--station-csv /path/to/sites.csv`, with columns `stid,latitude,longitude`.
The supplied CSV determines the inventory; the importer does not expand it.

Stations must lie within the actual exported grid perimeter and within
`max_distance_km` of a grid point (default 5 km). Spherical nearest-neighbor
selection handles the dateline and the supplied coordinate arrays handle row
orientation. Rejected station IDs and reasons are recorded in the import
manifest. Missing field values remain missing; the importer does not search for
a different grid point with a nonmissing value.

## Products and timing

The supported export is the floating-point format with aligned two-dimensional
`latitude` and `longitude` arrays. Fill values are masked. Unexpected units,
packing attributes, or malformed validity arrays cause an error rather than an
assumed conversion. Each spatial slice is read separately to limit memory use.

Initialization is `startProcTime - 72 hours`, using the export convention
confirmed for these files. For example, `20261109_0000` gives initialization
`2026-11-06 00:00 UTC`. Filenames and creation timestamps do not set initialization.
All projections are retained, including f072 and projections beyond f168.
Rows and manifests record `initialization_offset_hours=72`.

### Correct archives written with the earlier 84-hour offset

Before reimporting into an existing experimental archive root, preview and apply:

```sh
python repair_ndfd_exp_initialization.py --archive-root experimental_test/model
python repair_ndfd_exp_initialization.py --archive-root experimental_test/model --apply
```

Use `--archive-root model` for production. This changes only experimental
forecasts under `ndfd_exp`: initialization moves forward twelve hours, and
`forecast_hour`, `forecast_hour_start`, and `forecast_hour_end` decrease by
twelve hours. Values, valid times, native window bounds, and observation archives
remain unchanged. Records are repartitioned when initialization crosses a month
boundary. Originals are backed up; writes are staged, validated, and rolled back
on caught errors using the same importer lock/manifest mechanism.

Rows without an offset marker are recognized as the earlier 84-hour importer
format; explicit 84-hour markers are also supported. Rows already marked 72
remain unchanged, so repeating the repair is safe. Unknown offsets, inconsistent
time/lead metadata, and duplicate keys after correction stop the repair before
installation. Use this only for archives from this importer. Preview checks all
affected elements before writing. Reimporting afterward adds any new products
or stations normally; it does not create a second copy under the old run time.
The importer checks all forecast months and rejects a root with unrepaired rows,
including when corrected initialization would land in a different monthly file.
A fresh output root does not need this repair.

For grid i, `validTimes[2*i]` is its start and `validTimes[2*i+1]` its end.
Timestamps are UTC, stored without a timezone like the other model archives.
Every row preserves `window_start`, `window_end`, `window_hours`, and forecast
hours at both bounds. `forecast_hour` corresponds to `valid_time`.

| Directory | Source variable | Value column | Valid time / units |
|---|---|---|---|
| `maxt_native` | `MaxT_SFC` | `maxt` | Native period end / F |
| `mint_native` | `MinT_SFC` | `mint` | Native period end / F |
| `maxt` | Derived from `T_SFC` | `maxt` | Twelve-hour period ending 00Z / F |
| `mint` | Derived from `T_SFC` | `mint` | Twelve-hour period ending 12Z / F |
| `minrh_native` | `MinRH_SFC` | `minrh` | Native period end / % |
| `wind` | `Wind_Mag_SFC`, `Wind_Dir_SFC` | `wind_speed_kt`, `wind_dir_deg` | Instantaneous at period start / knots, degrees |
| `gust` | `WindGust_SFC` | `wind_gust_kt` | Instantaneous at period start / knots |
| `precip6hr` | `QPF_SFC` | `precip6hr` | Six-hour period end / inches |
| `rh` | `RH_SFC` | `rh` | Instantaneous at period start / % |
| `minrh` | Derived from `RH_SFC` | `minrh` | Twelve-hour period ending 06Z / % |

Native extrema periods remain intact, including partial first periods. In the
inspected export, complete native MaxT spans **14Z–05Z next day (15h)**, MinT
**02Z–20Z (18h)**, and MinRH **12Z–06Z next day (18h)**. These source periods
are never relabeled as twelve hours.

The derived products use separate hourly variables: MaxT is the maximum of
`T_SFC` at **12Z through 23Z**, MinT the minimum at **00Z through 11Z**, and
MinRH the minimum of `RH_SFC` at **18Z through 05Z**. Windows are half-open:
**[12Z,00Z)**, **[00Z,12Z)**, and **[18Z,06Z)**, respectively. Each requires
all twelve hourly timestamps and twelve finite values for each station.
Incomplete windows are omitted and counted in the report. These are extrema
of hourly instantaneous samples; extrema between samples may be missed.
Native extrema may be prepared independently and need not equal extrema of
the hourly variables. Missing hourly variables do not fall back to native grids.

Missing variables are reported and available requested products are processed.
Wind speed and direction must both be present in the same file with matching
periods. Fields split across separate files can otherwise be imported in the
same batch; the importer does not assemble hourly time windows across separate files.

## Upgrade existing experimental temperature archives

Earlier importer versions placed native extrema in `maxt/` and `mint/`. Migrate
those before importing the newly derived twelve-hour products:

```sh
python migrate_ndfd_exp_temperatures.py --archive-root model
python migrate_ndfd_exp_temperatures.py --archive-root model --apply
```

This preserves the old rows under `maxt_native/` and `mint_native/`, stages and
validates the changes, and backs up the original files. The old directories
contain empty monthly files until the NetCDF exports are reimported. Rerun
`import_ndfd_experimental.py` with the same input directory and `--apply` to
populate the derived temperature products. If an existing configuration has an
explicit `elements` list, add `maxt_native` and `mint_native` alongside `maxt`
and `mint`; the example configuration includes both. Matching reruns are safe.
The importer rejects unmigrated native temperature rows in derived directories,
even with `--replace-conflicts`, to prevent mixing period definitions.

## Observations for the two sets of periods

`run_obs_forecast_windows.py` provides two separate observation archives. The
existing OBS runner and files are unchanged. Its legacy temperature methods
currently use **12Z–06Z for MaxT** and **00Z–18Z for MinT** (both 18 hours),
so they are not a twelve-hour reference for the new forecasts.

| Observation archive | Matching forecast | Periods |
|---|---|---|
| `model/obs_exp/maxt/` | `ndfd_exp/maxt_native/` | Daily 14Z–05Z (15h), plus additional archived forecast bounds |
| `model/obs_exp/mint/` | `ndfd_exp/mint_native/` | Daily 02Z–20Z (18h), plus additional archived forecast bounds |
| `model/obs_exp/minrh/` | `ndfd_exp/minrh_native/` | Daily 12Z–06Z (18h), plus additional archived forecast bounds |
| `model/obs_12hr/maxt/` | Derived `ndfd_exp/maxt/` | 12Z–00Z |
| `model/obs_12hr/mint/` | Derived `ndfd_exp/mint/` | 00Z–12Z |
| `model/obs_12hr/minrh/` | `ndfd_exp/minrh/` and twelve-hour operational MinRH | 18Z–06Z |

Forecast archives are optional. Preview observation extraction, then add
`--apply` to write. These commands work in PowerShell and Linux shells:

```sh
python run_obs_forecast_windows.py --profile experimental --archive-root model --start "2026-11-09 00:00" --end "2026-11-14 23:59"
python run_obs_forecast_windows.py --profile 12hr --archive-root model --start "2026-11-09 00:00" --end "2026-11-14 23:59"
```

Both profiles default to `maxt mint minrh`. Use `--elements maxt mint` for just
temperatures. `--forecast-root` overrides the default `<archive-root>/ndfd_exp`
for optional additional native windows. Start/end select **window endpoints inclusively**.
Archive filenames use the **endpoint month**; this differs from forecasts'
initialization-month partitioning. Warm-up requests include the preceding day
where required. Identical station/window pairs across forecast issuances are
queried once; windows with the same endpoint but different starts remain separate.

Both profiles require `SYNOPTIC_API_KEY` and use fresh shared all-status regional
discovery for every daily window, independent of model files. The experimental
profile additionally queries exact station/window pairs from native forecast
archives when present, including partial/unusual periods and historical IDs
absent from current metadata. Duplicate full windows are queried once. The
regular experimental schedule follows the inspected export periods in the table.
Aggregation uses all valid available reports within **[start,end)**, preferring
one primary sensor per station/window with derived/secondary sensor fallback.
It does not combine sensors, interpolate, or discard later valid windows after a
gap. Entirely missing station-windows are omitted and counted; sparse extrema
are retained with `coverage_ok=False`. Coverage requires no gap over 90 minutes,
including gaps to both bounds (`--max-gap-minutes` changes that threshold).
Values include compatibility columns `tmax`/`tmin` plus aliases `maxt`/`mint`,
or `minrh`, in Fahrenheit/percent. Rows retain exact bounds, valid endpoint,
report count, first/last reports, maximum gap, sensor, and available WFO metadata.

These commands write locally under the selected archive root, preview by default, and back up changed
monthly files. Matching reruns are no-ops; changed observations require explicit
`--replace-conflicts`. API failures exhaust retries and abort the current batch
without saving partial results for that element/month; earlier completed
element/month writes remain. The verification app must be configured separately
to select the correct observation source and **both** window bounds. A matching
endpoint alone does not make native and twelve-hour observations interchangeable.

## Optional daily production workflow

Experimental **forecast imports** are disabled by default. Both new observation
profiles follow `RUN_OBS` (normally `1`), so they run every day even if no
experimental forecast has ever been archived. The default output is the
repository's ordinary regional `model/` directory: `model/ndfd_exp`,
`model/obs_exp`, and `model/obs_12hr` for Alaska. No special output directory or
archive-root setting is required. Enable forecast importing by adding these
settings once to the existing Linux launcher that cron invokes:

```sh
export RUN_NDFD_EXP=1
export NDFD_EXP_INPUT_DIR=/data/experimental_ndfd_exports
export RUN_OBS_EXP=1
export RUN_OBS_12HR=1

bash run_daily_archives.sh --region alaska
```

Cron reuses that saved launcher configuration on every invocation; nobody needs
to supply the input path manually on each run. Put the configuration in the
launcher or cron environment, rather than only exporting it in an interactive
terminal. The input directory must already exist. If production uses a custom
archive location, optional `NDFD_EXP_ARCHIVE_ROOT` selects its local **model
directory**, shared by `ndfd_exp/`, `obs_exp/`, and `obs_12hr/`. These products
remain local even when other sources use `SOURCE_STORAGE=s3`. Configure the
region and input directory separately for each regional launcher; do not feed
Alaska grids into a Hawaii import. Repair old initialization timestamps before
reimporting into an existing experimental root, as described above. The existing
Python environment needs `netcdf4`; cron needs `SYNOPTIC_API_KEY` for discovery
and observation requests.

When enabled, the wrapper runs the experimental stages after the ordinary model,
NDFD, OBS, and REFS work:

1. Import the matching local NetCDF/gzip exports with `--allow-empty --apply`.
2. Collect `obs_exp` for regular full native windows every day, adding any
   partial/unusual periods found in existing forecasts.
3. Collect `obs_12hr` for the standard MaxT/MinT/MinRH windows.

An existing input directory with **no matching exports is a successful skip**,
logged as `SKIP: no NetCDF exports ...`. Unrelated files are ignored. A missing
input directory remains an error, so a bad path or unavailable mount is not
mistaken for a quiet day. Source files are not moved or deleted. Matching
reimports are no-ops; all files in the directory are scanned each run. For files
arriving during cron, copy using a nonmatching temporary suffix (for example
`.part`), then rename to `.nc.gz` after the transfer is complete.

Observation collection does not depend on a new file arriving or a successful
import that day, or on the existence of any native forecast archive. It builds
the daily periods directly and queries only endpoints that have finished.
The standalone observation runner caps its requested endpoint at the current
UTC time. Additional archived forecast windows are optional; their absence
does not suppress any scheduled observation period.

By default both new observation profiles follow `RUN_OBS`. Overrides:

| Setting | Default | Meaning |
|---|---|---|
| `RUN_OBS_EXP` | `RUN_OBS` (normally `1`) | Daily experimental native-window observations, with or without forecasts |
| `RUN_OBS_12HR` | `RUN_OBS` (normally `1`) | Daily twelve-hour observations, with or without forecasts |
| `OBS_EXP_LOOKBACK_DAYS` | `7` | Retry the last seven completed UTC days for both profiles |
| `NDFD_EXP_STATION_CSV` | unset | Optional fixed forecast station inventory |
| `NDFD_EXP_RECURSIVE` | `0` | Search input subdirectories when `1` |
| `NDFD_EXP_REPLACE_CONFLICTS` | `0` | Allow forecast revisions/mapping changes with backups when `1` |
| `OBS_EXP_REPLACE_CONFLICTS` | `1` | Save updated/revised observations with backups |

For a run dated November 30, the default observation endpoint range is November
24 at 00Z through December 1 at 00Z, inclusive. This lookback lets later reports
fill missing/sparse earlier periods. Explicit wrapper `--start/--end` bounds
override that lookback for observations. The forecast importer always scans the
whole configured directory; wrapper date bounds do not filter local files.
No observations are fabricated for missing reports. To collect only native
observations, set `RUN_OBS_12HR=0`. To continue observations while disabling
new imports, set `RUN_NDFD_EXP=0 RUN_OBS_EXP=1` (and `RUN_OBS_12HR=1` if wanted).
If exports arrive after their valid periods are older than the lookback, run
the observation command with explicit historical bounds to backfill those dates.

Each observation element/profile is attempted independently. A corrupt forecast
file, exhausted API retries, or an observation write failure makes the overall
wrapper return a nonzero status, but subsequent experimental observation jobs
are still attempted. All logs go to the existing regional daily log. Preview
the exact commands without executing them using:

```sh
DRY_RUN=1 bash run_daily_archives.sh --region alaska
```

This wrapper integration does not configure the verification app's observation
source selection. That remains a separate app setting/change.

## Output and repeat runs

Outputs use a separate model identifier:

```text
model/ndfd_exp/<element>/YYYY_MM_archive.parquet
```

The month is the **initialization month**, consistent with other model archives;
valid periods can cross into the following month. Hawaii may use
`hawaii/model` as its archive root. The verification app must recognize
`ndfd_exp` as a model before displaying these archives; this importer does not
change that app.

Preview is the default and does not write archives. With `--apply`, each source
file's changed monthly archives are staged and read back for validation before
installation. Backups and a JSON manifest are retained under
`ndfd_exp/import_backups/<import-id>/`. The manifest includes source SHA256,
station selection, validity ranges, missing values, and changed destinations.

Reruns skip matching records. Keys include station, initialization, valid time,
and both window bounds. Conflicting values stop the import by default. To review
an intentional revision, preview with `--replace-conflicts`; then add `--apply`
to install it with backups. Console output shows concise product, update, row,
and conflict counts. Detailed period/coverage statistics and up to ten conflict
samples per file (changed values, source hashes, and extraction coordinates)
are retained in the installation manifest rather than printed. Fresh Synoptic inventories
can change station coordinates and consequently the selected grid point; new
station IDs alone do not create conflicts with existing keys. Identical source
hashes plus changed extraction coordinates help distinguish a station-mapping
change from a different export. Changed period bounds represent different records;
replacing an entire superseded issuance with different periods requires review
and removal of those obsolete records separately.

Imports are transactional **per source file**, not per directory. A caught
installation error restores that file's original monthly archives. Earlier
completed source files remain installed if a later source fails. An importer
lock prevents simultaneous writes by another instance of this importer. After
an abrupt machine/process termination, inspect manifests and backups before
removing a stale `.import.lock` or restarting. Do not run other tools that edit
the same `ndfd_exp` files concurrently.

## Validation

```sh
python -m pytest -q tests/test_experimental_ndfd.py tests/test_obs_forecast_windows.py
```

Tests generate real NetCDF, gzip, and Parquet files matching the supplied
inspection. They cover all products, source timing, complete/incomplete derived
MinRH windows, dateline and row orientation, station exclusions, missing data,
invalid inputs, repeat imports, conflicts, month partitioning, and rollback.
The original production export must still be previewed locally to confirm its
metadata matches this format before its first installation.
