# HRRR snow precision diagnostic

From the repository root in your existing archiver Python environment:

```bash
python diagnose_hrrr_snow.py
```

Defaults: PAJN, HRRR Alaska 2026-10-04 12Z, f006 and f012. Requires requests,
numpy, pandas, pygrib and the normal dependencies needed to import utils.py.
Uses `obs/alaska_all_active_synoptic_station_metadata.csv` and the production
`ll_to_index` function. It never fetches station metadata or alters an archive.

Override the metadata path with `--metadata PATH`. Alternatively supply both
`--latitude LAT --longitude LON` using your archived station coordinates.
Other options include `--cycle YYYYMMDDHH`, `--leads 6 12`, `--station PAJN`,
`--radius 2` (5x5 neighborhood), and `--output-dir PATH`.

Each invocation creates a unique directory under `snow_diagnostics/` containing
both indexes, the two cumulative ASNOW GRIB records, `report.txt`, and `report.json`.
Attach report.txt for review. If download/decoding fails, attach error.txt instead.
The script requires HTTP range support and verifies message count, initialization,
valid time, meter units and decoded cumulative windows before comparing values.

The report includes native meter values, conversions matching the archiver,
packing metadata where exposed by pygrib, the selected cell and nearby cells,
and counts/minimum differences over the full decoded grid. Missing packing keys
are null. The combined reported packing errors provide a consistency check only;
they do not quantify all model numerical effects or prove why totals decreased.
No validation tolerance is changed. No live GRIB download was performed during
implementation; automated tests cover index selection and packing-bound handling.
