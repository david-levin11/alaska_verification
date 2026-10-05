# Daily REFS processing

`run_refs_processing.py` now defaults to batch mode: yesterday's four UTC cycles,
all three-hour forecast leads through f060, and Wind, RH, precipitation and snow.
Wind produces separate wind-speed and gust rows. Six-hour accumulation statistics
start at f006. Forecast valid times may extend beyond the processing day.

```bash
python run_refs_processing.py --date 2026-10-01
python run_refs_processing.py --date 2026-10-01 --lookback-days 2 --elements Wind precip6hr
```

Outputs append/update monthly files by **initialization month**:

```
derived/refs/wind/2026_10_archive.parquet
derived/refs/rh/2026_10_archive.parquet
derived/refs/precip6hr/2026_10_archive.parquet
derived/refs/snow6hr/2026_10_archive.parquet
```

Choose local or S3 destinations independently of the source archives:

```bash
python run_refs_processing.py --date 2026-10-01 --archive-root model --output-root s3://alaska-verification/derived/refs
python run_refs_processing.py --date 2026-10-01 --archive-root s3://alaska-verification --output-root s3://alaska-verification/derived/refs
```

S3 source reading accepts both `model/element/YYYY_MM_archive.parquet` relative
to the source root and the current archiver's `model/YYYY_MM_model_element_archive.parquet`
layout. Use normal AWS credentials, or `--aws-profile NAME`. S3 requires `s3fs`.
No source data are downloaded from NOAA by the statistics script.

For backfills, `--start` is inclusive and `--end` is exclusive:

```bash
python run_refs_processing.py --start 2026-09-30 --end 2026-10-02 --elements Wind
```

`--forecast-hours 6 12 24 48` restricts leads. Probabilities are now generated
by default using `refs_thresholds.json`, alongside mean, spread, and percentiles.
`--threshold-config /path/to/custom.json` selects another configuration.
`--value-column` restricts one element to one variable. For that variable only,
`--thresholds 20 30 40` overrides the configuration with strict `>` probabilities;
`--thresholds` with no values disables probabilities.

The previous `--cycle`, `--valid`, `--value-column`, `--output`, and
`--members-output` single-case mode is preserved. Batch mode uses `--output-root`
and does not persist assembled member rows.

## Monthly update rules

- Rows are unique by station, ensemble initialization, valid time, and variable.
- Reprocessing updates matching rows and preserves unrelated cycles/stations.
- An existing complete result is retained if a retry is incomplete. Other matches
  use the newest result. Missing member values remain missing; statistics require
  completeness unless `--allow-incomplete` is specified.
- The statistical recipe is stored in each row. A monthly file with a different
  recipe is rejected; use another output root for alternate thresholds, weighting
  experiments, completeness policies or member horizons.
- Local updates replace a temporary file atomically. S3 updates serialize and
  replace the complete monthly object. Both require **one writer per output root**;
  do not run separate batch jobs against the same files concurrently.
- No-source cases are logged and cause a nonzero exit after available cases are
  saved. Partially populated ensembles are saved with their member counts.
- Source Parquets are cached per element/month, including the previous month for
  lagged members. Corrupt files and failed writes propagate as job failures.

## Daily shell/cron integration

`run_daily_archives.sh` runs REFS processing after the model archiver loop.
It changes to its own directory so cron need not choose a working directory.
It uses `flock` to prevent overlapping copies of this wrapper on the same host
and log directory. Use the Python interpreter from your existing activated cron
Conda environment/PATH.

Default behavior:

- `RUN_REFS=1`: enable the statistics stage.
- `REFS_LOOKBACK_DAYS=2`: reprocess yesterday and the day before.
- HRRR, RRFS, and RRFSENS source downloads cover that window **plus the preceding
  18 UTC run** needed for the first 00 UTC ensemble's lagged members.
- Statistics run after all selected models finish. A nonzero exit from a REFS
  source archiver skips statistics and makes the wrapper fail. Downloaders may
  report individual missing files without a nonzero exit; completeness counts
  still expose those gaps.
- `SOURCE_STORAGE=local`: source archivers retain local storage by default.
- `REFS_ARCHIVE_ROOT=model`, `REFS_OUTPUT_ROOT=derived/refs`.

Examples:

```bash
# Existing cron invocation now includes REFS automatically.
./run_daily_archives.sh

# Local sources, statistics in S3.
REFS_OUTPUT_ROOT=s3://alaska-verification/derived/refs ./run_daily_archives.sh

# Source archivers use their configured S3 paths; statistics read that bucket.
SOURCE_STORAGE=s3 REFS_OUTPUT_ROOT=s3://alaska-verification/derived/refs ./run_daily_archives.sh

# Review commands without executing Python jobs.
DRY_RUN=1 ./run_daily_archives.sh 2026-10-01

# Disable the added stage and source lookback.
RUN_REFS=0 ./run_daily_archives.sh
```

Other options: `REFS_ELEMENTS="Wind precip6hr"`, `REFS_ALLOW_INCOMPLETE=1`,
`REFS_AWS_PROFILE=NAME`. The latter applies to statistics IO; the existing source
writers still use their configured/default AWS profile. Set `REFS_ARCHIVE_ROOT`
if your configured source bucket/prefix differs. If you restrict `MODELS`, the
omitted sources must already exist; the wrapper does not silently download them.
For explicit wrapper `--start/--end`, automatic lookback is disabled and REFS uses
an exclusive end; source archivers retain their inclusive end convention. Ensure
previous lagged source runs are already archived when using explicit ranges.

Tests: `python -m pytest -q tests/test_refs_batch.py` (includes emulated S3 IO;
no user bucket is written during tests).


## Variable-specific probabilities

Edit `refs_thresholds.json` before creating your derived archives. These are
starting thresholds, not official warning criteria. Units must match the member
archive; no unit conversions are performed by this configuration.

| Variable | Units | Operator | Default thresholds |
| --- | --- | --- | --- |
| wind_speed_kt | knots | > | 10, 15, 20, 25, 30, 35, 40, 50 |
| wind_gust_kt | knots | > | 20, 25, 30, 35, 40, 50, 60 |
| precip_6h | inches / 6 hours | > | 0.01, 0.1, 0.25, 0.5, 1, 2 |
| snow_6h | inches / 6 hours | > | 0.1, 1, 2, 3, 4, 6 |
| rh | percent | < | 15, 20, 25, 30, 40 |

Each entry has `units`, `operator` (`>` or `<`), and `thresholds`. Use an empty
array to disable probability fields for a variable. RH defaults to dry-condition
probabilities; change its operator to `>` if you want high-humidity exceedances.
The loader validates all entries before batch processing begins.

`prob_gt_30` means the fraction of available members strictly above 30 in that
row's variable units. `prob_lt_30` means strictly below 30. Values are fractions
from 0 to 1, not percentages. Equality does not count. Missing members follow the
same completeness policy as percentiles: by default all statistics are NaN until
all expected members are available. With `--allow-incomplete`, the denominator
is `n_available`. Always retain the member count and completeness metadata.

Wind speed and gust share a file but have separate `value_column` rows and
threshold recipes. Columns not configured for a row's variable are NaN; these
structural NaNs do not indicate missing member data. `statistics_config` records
the recipe per variable. Reruns may update a single variable without removing
other variables, but incompatible recipes for the same variable are rejected.

The daily wrapper uses this configuration automatically for local and S3 output:

```bash
./run_daily_archives.sh
REFS_THRESHOLD_CONFIG=/absolute/path/custom_thresholds.json ./run_daily_archives.sh
```

Existing derived monthly files use the previous statistics recipe. Move aside or
remove only the affected files under `derived/refs/` before rebuilding them with
the new fields, or select a fresh `--output-root`. Keep the `model/` member archives;
there is no need to download GRIBs again just to add probabilities. For example,
rebuild October 1 wind statistics from existing sources into a fresh directory:

```bash
python run_refs_processing.py --date 2026-10-01 --elements Wind --output-root derived/refs_threshold_test
```

For a full month, use `--start 2026-10-01 --end 2026-11-01` and the elements whose
sources you have archived. Dates select initialization cycles, not valid dates.
Changing thresholds later requires rebuilding affected derived monthly files or
using a different output root, so a monthly archive never silently mixes recipes.
