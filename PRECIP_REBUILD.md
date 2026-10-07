# Rebuild precipitation after the accumulation-selector fix

The old substring selector could include both `50-51 hour acc fcst` and
`0-51 hour acc fcst` when requesting a cumulative total. The new selector
matches a complete colon-delimited field, accepting equivalent hour/day labels.
It applies to HRRR and RRFS extraction (including the RRFS perturbations reused
by `rrfsens`). Snow remains cumulative; one-hour snow is not required.

Six-hour precipitation is cumulative(f) minus cumulative(f-6) within the same
station, source initialization, and member. f006 uses the total directly.
For lagged members, both endpoints belong to the lagged source run.

Pull the fixed `refs` branch, then re-archive precipitation for your test day and
its preceding 18Z run. This downloads corrected source subsets and upserts the
source monthly files. It does not repair other dates automatically. Repeat for
all previously archived dates you need to retain.

```bash
(
  for model in hrrr rrfs rrfsens; do
    python run_model_archiver.py --model "$model" --element precip6hr \
      --start "2026-10-03 18:00" --end "2026-10-04 18:00" --local || exit 1
  done
)
```

Only after all source jobs succeed, regenerate the affected derived day:

```bash
python run_refs_processing.py --date 2026-10-04 --elements precip6hr
python validate_refs_archive.py --date 2026-10-04 --elements precip6hr --report precip_validation.json
```

No month-boundary data is required for this test. Exit code 2 means the checks
passed but the optional month-boundary check was skipped. Exit code 1 means
failure. Reprocessing the same derived keys replaces their previous complete
results; source corrections alone do not update existing derived statistics.
Leave your threshold configuration unchanged during rebuilding or use a new
output root to avoid mixing statistics recipes.

The separate trace-sized HRRR snow decrease has not been changed or waived by
this precipitation fix. Regression tests mock HTTP byte-range downloads and
exercise the production selector; they do not validate live GRIB point values.
