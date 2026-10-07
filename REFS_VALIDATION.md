# REFS pre-merge archive validation

Run from the repository root after archiving source members. No GRIB downloads
are made. Uses the same pandas/NumPy/Parquet/fsspec dependencies as REFS processing;
S3 also requires s3fs and your normal AWS credentials.

```bash
python validate_refs_archive.py --date 2026-10-04 --boundary-date 2026-10-01 --report refs_validation.json
```

Defaults: PAJN, precipitation and snow, all four daily cycles, and leads
6, 12, 42, 45, 48, 51, 54, 57, 60. The boundary check uses 00Z f006 on the
specified first day of a month. It needs prior-month 18Z source archives.
Supply only dates you have archived. Missing required sources are failures.

To check other variables or all available stations:

```bash
python validate_refs_archive.py --date 2026-10-04 --boundary-date 2026-10-01 --elements wind rh precip6hr snow6hr temp2m --all-stations
```

`--stations PAJN PANC PAFA` selects an explicit station list and detects stations
absent from every input. `--all-stations` uses the union found per case, so it
cannot detect a station absent from every model. Large all-station checks can
be slow; start with a few sites. `--forecast-hours` can restrict/expand leads.
All variable names for `--elements` are lowercase.

Test actual S3 writes using existing local inputs:

```bash
python validate_refs_archive.py --date 2026-10-04 --boundary-date 2026-10-01 --test-output-root s3://alaska-verification/validation --report refs_s3_validation.json
```

Add `--archive-root s3://alaska-verification` to test S3 source reads as well.
`--aws-profile NAME` selects a profile. No AWS credentials are embedded.
Every invocation creates a unique `refs-validation-<uuid>` child under the test
output root (default `validation_output`). The script prints this path and retains
test files for inspection. It never writes into existing derived monthly files or
modifies source archives. Remove the printed test prefix when finished.

Checks performed:

- Source availability, finite member values, scheduled member counts, and matching
  valid times for current and six-hour-lagged members.
- Independent direct lookup of cumulative endpoints for each station/run/member;
  f006 uses the source total, later leads subtract the total six hours earlier.
  Compares both the assembled interval and stored source interval where present.
  Negative cumulative differences fail rather than being silently clipped.
- Mean, population spread, linear percentiles, and configured probabilities
  recalculated from assembled member values and compared to saved output.
- Expected row count, unique keys, and exact DataFrame equality after rerunning
  production processing into the same test monthly files. This tests local or
  S3 serialization, reading, and monthly updates (not just connectivity).
- Real cross-month lag retrieval and output partition by initialization month.

Exit codes: **0** all requested checks pass; **1** failure; **2** no failures but
month-boundary check skipped because `--boundary-date` was omitted. An interval-only
source without cumulative totals cannot independently establish the accumulation
window and fails that check. A failure for one element does not prevent checks of
other elements. Optional `--report` writes a local JSON summary.

Scope: this checks archived Parquets and production derived processing, not the
original GRIB metadata, grid-to-station selection, downloading, or cron environment.
It does not audit existing production derived files. Run the actual daily wrapper
separately to check its full environment. Stop other writers from modifying source
archives while validation runs. Keep your threshold configuration fixed throughout.
