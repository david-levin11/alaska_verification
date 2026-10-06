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


If your decoder labels the NCEP ASNOW parameter name/units as `unknown`, rerun:

```bash
python diagnose_hrrr_snow.py --assume-asnow-meters
```

This explicit option uses the archiver's ASNOW meter convention for unresolved
units only. The report preserves the decoder's original units and labels the
assumption per record, with numeric parameter/table identifiers for further
investigation. Known incompatible units still fail. Accumulation windows and
source timestamps remain strictly checked. Packing-error metadata may still be
unavailable; the report does not substitute a guessed error bound.


## Precipitation packing diagnostic

The same script now accepts `--element precip` to select cumulative APCP. For
PAJN's October 1 00Z f018/f024 decrease:

```bash
python diagnose_hrrr_snow.py --element precip --cycle 2026100100 --leads 18 24 --station PAJN --output-dir precip_diagnostics
```

APCP decoded in kg/m² is numerically equivalent to millimeters of liquid water;
`mm` is accepted directly, while decoded `m` is converted to mm. Unknown or
incompatible precipitation units are rejected. The snow-only assumption flag is
not accepted for precipitation. The report preserves native endpoint values and
original packing metadata, and labels comparison values as mm (including point,
neighborhood, and full-grid differences). Inch conversions use mm / 25.4.
Reported packing errors are converted to comparison units before combining them.
No bound is guessed when the decoder does not expose packingError.

Exact index matching accepts 0–24 hours or 0–1 day, while excluding one-hour
APCP records. Downloads go into a unique `hrrr-precip-*` subdirectory. Attach
report.txt from the printed directory. Existing snow commands/defaults and snow
report meter fields are preserved. Archives and validation tolerances are unchanged.
