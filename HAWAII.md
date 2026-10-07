# Hawaii archives

Select `--region hawaii` (`hi` also works). Alaska remains the default;
existing Alaska archive paths and REFS membership are retained.

The daily models are NBM, NBMQMD, RRFS control, five RRFS perturbations
(`rrfsens`), and URMA. NDFD and Synoptic observations are included. HRRR and
snowfall are excluded for Hawaii. Experimental NBM sources remain available
through the model CLI when their upstream Hawaii products are published;
the daily defaults use operational sources.

## Daily runs

Pull the `hawaii` branch and use the existing archiver Python environment.
The Synoptic key must be available as `SYNOPTIC_API_KEY` to the cron process.

```bash
# Preview commands; no source downloads or archive writes.
DRY_RUN=1 bash run_daily_archives.sh --region hawaii

# Previous UTC day, with the existing REFS two-day retry/lookback policy.
bash run_daily_archives.sh --region hawaii

# Explicit UTC initialization day.
bash run_daily_archives.sh --region hawaii 2026-10-06
```

`REGION=hawaii` is also supported by the daily shell wrapper; `--region`
overrides it. Python entry points use their explicit `--region` option.
All other daily overrides work as before. An unsupported `MODELS` override
(such as `hrrr` for Hawaii) is rejected before processing. Keep `rrfs rrfsens`
in the selected models for a complete source-and-derived REFS run.

The wrapper still defaults to **local storage**. No Alaska files need moving.
Paths below are relative to the repository:

| Content | Alaska (unchanged) | Hawaii |
| --- | --- | --- |
| Model/member sources | `model/{model}/{element}/{YYYY_MM}_archive.parquet` | `hawaii/model/{model}/{element}/{YYYY_MM}_archive.parquet` |
| Observations | `model/obs/{element}/{YYYY_MM}_archive.parquet` | `hawaii/model/obs/{element}/{YYYY_MM}_archive.parquet` |
| NDFD | `ndfd/{element}/{YYYY_MM}_ndfd_{element}_archive.parquet` | `hawaii/ndfd/{element}/{YYYY_MM}_ndfd_{element}_archive.parquet` |
| REFS | `derived/refs/{element}/{YYYY_MM}_archive.parquet` | `hawaii/derived/refs/{element}/{YYYY_MM}_archive.parquet` |
| Station metadata | `obs/` | `hawaii/obs/` |
| Temporary GRIBs | `tmp_cache/` | `hawaii/tmp_cache/` |
| Daily logs and lock | `logs/` | `hawaii/logs/` |

An explicit Hawaii REFS source/output root must contain a `hawaii` path
component, preventing accidental use of the default Alaska directories.
If using cloud storage later, regional source keys also have a `hawaii/`
prefix; source and derived output destinations remain independently selectable.

## Sources and grids

- RRFS control: `rrfs.YYYYMMDD/HH/rrfs.tHHz.2dfld.2p5km.fFFF.hi.grib2`.
  The control retains the configured f003–f084 archive range.
- Perturbations: `rrfsens.YYYYMMDD/HH/m001/rrfs.tHHz.m001.2dfldnomads.2p5km.fFFF.hi.grib2`
  (m001–m005, f003–f060).
- NBM core and QMD use the existing paths with `.hi.grib2`.
- URMA: `hiurma.YYYYMMDD/hiurma.tHHz.2dvaranl_ndfd.grb2`.
  Hawaii does not use Alaska's `_3p0` filename suffix.
- NDFD uses the same `noaa-ndfd-pds/wmo` directories, with Hawaii's `S`
  as the third WMO-prefix character: YCSZ/YBSZ (wind), YWSZ (gust),
  YISZ (QPF), YGSZ/YHSZ (max/min temperature), and YRSZ (RH).
- Synoptic queries use `state=hi`. Model extraction uses all active stations;
  observation/NDFD selection follows the existing variable-selection methods.

Point extraction uses the coordinates decoded from each GRIB, rather than
an Alaska projection or grid dimensions. Both 2-D arrays and cfgrib's flattened
Mercator point arrays are supported. Index caches include grid identity and
station coordinates. Hawaii stations farther than 0.1 degree from the nearest
grid point are omitted, avoiding spurious edge values for remote islands.

The max/min observation UTC windows and NBM lead schedules are unchanged from
the existing workflow; they are not redefined as Hawaii local-calendar days.

## REFS membership and statistics

Hawaii statistics use the RRFS control and five perturbations, with current
and six-hour-lagged runs. HRRR is excluded from both data reads and expected
counts, including if an HRRR max-hour override is supplied.

The existing derived recipe caps RRFS/member source leads at 60 hours:

- Ensemble f003–f054: 12 expected members.
- Ensemble f057–f060: 6 expected members, because the lagged sources exceed f060.
- Six-hour precipitation statistics start at ensemble f006. Preserve source
  f003 for intervals ending at f009, including lagged member calculations.

This is the archive's explicit member recipe, not a claim that it reproduces
every official REFS product. Thresholds still come from `refs_thresholds.json`;
Hawaii uses the same configured wind/gust, RH, precipitation, and temperature
thresholds. Missing expected members suppress statistics by default.

## Individual runs and validation

```bash
python run_model_archiver.py --region hawaii --model rrfs --element temp2m \
  --start '2026-10-06 00:00' --end '2026-10-06 00:00' --local

python run_ndfd_archiver.py --region hawaii --element Wind \
  --start 2026-10-06 --end 2026-10-07 --local

python run_obs_archiver.py --region hawaii --element Wind \
  --start 2026-10-06 --end 2026-10-07 --local

# Once both source models and lagged runs have been archived:
python run_refs_processing.py --region hawaii --date 2026-10-06

python validate_refs_archive.py --region hawaii --date 2026-10-06 \
  --elements rh --stations PHNL PHOG PHTO --forecast-hours 6 12 48 54 57 60
```

The validator defaults to PHNL and precipitation only for Hawaii, with artifacts
under `hawaii/validation_output/`. As before, an omitted optional month-boundary
check produces SKIP and exit code 2; any failure produces exit code 1.

Automated offline tests: `python -m pytest -q tests/test_regions.py`.
A full production daily run and authenticated Synoptic collection still need
validation in the deployment environment before merging to main.

## Validation performed during implementation

On October 7, 2026, NOAA bucket listings/index files confirmed the Hawaii
RRFS control/member, URMA, NBMQMD, and NDFD routes described above. Live
October 6 00Z f006 subsets decoded successfully at PHNL for RRFS wind/gust
and NBMQMD wind percentiles. Additional downloads encountered environment
proxy errors/timeouts; those are not claimed as end-to-end passes.

Offline regional tests cover URL construction, state selection and separate
metadata, flattened Mercator extraction (including NDFD), out-of-grid points,
daily routing, Hawaii membership, monthly boundary handling, repeat writes,
and rejection of unscoped Hawaii roots. Existing Alaska regression tests are
also retained. No authenticated Synoptic or full daily production run was made.
