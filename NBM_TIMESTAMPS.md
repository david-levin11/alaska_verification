# NBM percentile timestamp correction

The old pygrib decoder used `validDate` as the endpoint and subtracted the
filename lead to infer initialization. Live operational GRIB samples show that
this property can instead denote the interval start. Both archived timestamps
then become early by the interval duration; the filename forecast hour and
percentile values are not changed by this correction.

The shared NBM/NBMQMD percentile decoder now reads the GRIB reference date/time
and explicit statistical interval endpoint. Instantaneous fields use GRIB
validityDate/validityTime. It rejects inconsistent filename leads and mixed
timestamps among percentile messages. Alaska and Hawaii use the same helper;
experimental products use it too, but have not been live-tested in this change.

Confirmed legacy offsets from operational samples:

| Element | Both timestamps early by |
| --- | ---: |
| MaxT / MinT | 18 hours |
| Precipitation 6 / 24 hours | 6 / 24 hours |
| Alaska snow 6 / 24 / 48 / 72 hours | 6 / 24 / 48 / 72 hours |
| QMD wind, gust, RH | 0 hours |

Snow indexes use annotated `acc@(fcst,dt=...)` labels; the audit now recognizes
these without changing production snow selection.

## Read-only historical inventory

From the repository root, run:

```bash
python assess_archive_timestamps.py --roots model hawaii/model
```

Send the printed JSON report for review. It reports dates, cycle-hour counts,
duplicates, candidate offsets, month crossings, and collisions with existing
keys. It does not modify archives or prove which historical software produced
any row. Correctly scheduled initialization hours alone cannot rule out an
offset, especially for 6/24/48/72-hour fields.

Do not append corrected backfills into affected monthly files before deciding
how to handle their old rows. Corrected keys can coexist with incorrectly dated
ones; de-duplication does not repair this. Preserve backups and provenance.

## GRIB diagnostic

```bash
python audit_nbm_timestamps.py --dates 2026-10-06 --regions alaska hawaii --models nbm nbmqmd
```

`OFFSET` continues to describe the legacy calculation, while
`corrected_matches_metadata` tests the fixed decoder. A nonzero exit is expected
when legacy offsets are found. `NO_MATCH` is not a pass. Downloads and reports
are retained in a unique directory, with no production archive writes.

## Tests with downloaded subsets

```bash
python -m unittest discover -s tests -p 'test_grib_timestamps.py'
```

To additionally run the production station extraction and local Parquet writer
on retained diagnostic subsets, set `NBM_TIMESTAMP_REPORTS` to one or more
comma-separated audit report paths and rerun the command. These tests use
one station per region and temporary Parquet files, write twice to check
idempotence, and verify timestamps against independent GRIB metadata.
