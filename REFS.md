# REFS station archives

The `refs` branch builds on `rrfs`. Source forecasts are archived once:

- `model/hrrr/{element}/YYYY_MM_archive.parquet`: existing HRRR archive.
- `model/rrfs/{element}/YYYY_MM_archive.parquet`: RRFS control.
- `model/rrfsens/{element}/YYYY_MM_archive.parquet`: five perturbed members,
  distinguished by `member_id` (`m001` through `m005`).

The source `init_time`, `valid_time`, and `forecast_hour` retain their original
meaning. Lagged members are assembled from earlier runs, not downloaded again.
The RRFS control and perturbations use the operational AWS bucket. Perturbed
runs use `rrfsens.YYYYMMDD/CC/mMMM/rrfs.tCCz.mMMM.2dfldnomads.3km.fFFF.ak.grib2`.

## Archive members

Set `SYNOPTIC_API_KEY` in your environment before downloading Synoptic station
metadata or observations. The embedded credential has been removed from the
configuration. Existing cached station metadata can be used without the key.
Because the old credential remains in repository history, rotate it through your
Synoptic account; this branch does not rotate credentials or rewrite history.

Run from the repository directory with the normal GRIB environment/dependencies:

```bash
python run_model_archiver.py --start "2026-09-30 18:00" --end "2026-10-01 00:00" --model rrfsens --element Wind --local
python run_model_archiver.py --start "2026-09-30 18:00" --end "2026-10-01 00:00" --model rrfsens --element precip6hr --local
```

`Wind` includes wind speed, direction, and gust. Additional supported elements
are `rh` and `snow6hr`. Dates are UTC; endpoints are inclusive. Start on a
00/06/12/18 UTC cycle. Perturbed runs are extracted every three hours through
f060. The control retains its existing through-f084 configuration. Make sure
HRRR and RRFS source archives cover both requested cycles and valid times.

`run_daily_archives.sh` now includes `rrfs` and `rrfsens` in its default model
set. To archive only the perturbations:

```bash
MODELS="rrfsens" RUN_NDFD=0 RUN_OBS=0 ./run_daily_archives.sh 2026-09-30
```

Perturbed processing uses daily chunks bounded by calendar month and processes
one member at a time. Member identity participates in local and S3 deduplication.
Rerunning a matching source record replaces its old values. Download failures
remain missing data; inspect the archiver log when a chunk is incomplete.

## Accumulations and existing archives

RRFS and HRRR keep `precip_accum`/`snow_accum` (totals since initialization) and
now both derive `precip_6h`/`snow_6h`. Each interval is computed within one station,
initialization, and member. f006 uses its 0-6 hour total directly; f003 and any
interval lacking an earlier endpoint stay NaN. Missing intervals are no longer
converted to zero by negative-value clipping.

Old Parquets are not automatically rewritten. The ensemble reader recomputes
intervals from cumulative columns when present, including older RRFS archives.
Other applications reading those files directly need to perform that conversion
or use regenerated archives. Negative differences are clipped to zero by default,
matching the existing policy; the helper allows disabling clipping for diagnostics.

## Assemble and summarize

```python
from ensemble_processing import assemble_refs, summarize_refs

members = assemble_refs(
    "model", "Wind", "2026-10-01 00:00", "2026-10-02 00:00",
    station_ids=["PAJN"],
)
summary = summarize_refs(
    members, "wind_speed_kt", thresholds=[20, 30, 40],
)
```

At ensemble f024, current members supply f024 and six-hour-lagged members supply
f030. The reader loads the correct initialization-month files, including the
previous month. Returned timestamps are UTC-aware. New fields include
`source_model`, `ensemble_member_id`, `ensemble_init_time`,
`ensemble_forecast_hour`, `lag_hours`, and `source_available`.

Supply `station_ids` to report stations for which every source is missing;
otherwise the station set is inferred from matching archived source records.
Missing expected members are explicit placeholder rows. Statistics count finite
values for the requested variable, not merely downloaded rows.

The default analysis recipe limits RRFS control contributions to source leads through 60 hours,
perturbations through 60, and HRRR through 48; ensemble leads are limited to 0-60.
Consequently its expected counts are 14 through ensemble f042, 13 through f048,
12 through f054, and 6 through f060.
This is a documented source-availability recipe, **not a guaranteed reproduction
of official REFS membership/products**. Override `max_forecast_hours`, e.g.
`{"rrfs": 84}`, to use the archived longer control runs and retain its lagged
contribution after f054 (seven members).
Verify the appropriate horizons for the source feed and experiment. Missing
files within a configured horizon count as missing, not scheduled departures.
Configured extraction hours also matter: a valid time not archived will be missing.

By default any missing expected value suppresses statistics for that station/time.
Use `require_complete=False` explicitly to summarize available members. Outputs
include `n_expected`, `n_available`, and `complete`. Percentiles use linear
interpolation, spread is population standard deviation, and exceedance
probabilities are equal-weight fractions using strict `>` on a 0-1 scale.
For gust use `wind_gust_kt`; precipitation uses `precip_6h` in inches; snow uses
`snow_6h` in inches. No percentiles are computed from wind direction.

For command-line use and optional derived Parquets:

```bash
python run_refs_processing.py --archive-root model --element Wind --cycle "2026-10-01 00:00" --valid "2026-10-02 00:00" --value-column wind_speed_kt --stations PAJN --thresholds 20 30 40 --output derived/refs_wind_summary.parquet --members-output derived/refs_wind_members.parquet
```

Output paths are replaced if they exist. Source archives are read-only during
assembly. `--allow-incomplete` opts into available-member statistics. The CLI
also exposes `--rrfs-max-hour`, `--rrfsens-max-hour`, and `--hrrr-max-hour`.
Derived files are optional; no NBM archive schemas or verification UI are changed.
These are station-point products, not official neighborhood probabilities.

## Tests

```bash
python -m pip install pytest
python -m pytest -q tests/test_refs.py
```

Tests cover first/missing accumulation intervals, member separation, cross-month
lagging, probabilities, completeness, forecast horizons, URL construction,
deduplication, and separate extraction of members. They use synthetic Parquets
and mocked download discovery, so they do not require NOAA downloads or credentials.
