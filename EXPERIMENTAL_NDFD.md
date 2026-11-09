# Import experimental NDFD NetCDF exports

`import_ndfd_experimental.py` reads batches from a directory on the machine
running the importer. It accepts `.nc`, `.nc.gz`, `.netcdf`, and `.netcdf.gz`.
Run it manually after receiving exports; it is separate from the daily launcher.
It never downloads forecast grids or changes the source exports.

## Setup and configuration

Activate the existing archiving Python environment. The additional dependency is:

```sh
python -m pip install "netCDF4>=1.7,<2"
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

Initialization is `startProcTime - 84 hours`, using the export convention
confirmed for these files. For example, `20261109_0000` gives initialization
`2026-11-05 12:00 UTC`. Filenames and creation timestamps do not set initialization.
All projections are retained, including f072 and projections beyond f168.

For grid i, `validTimes[2*i]` is its start and `validTimes[2*i+1]` its end.
Timestamps are UTC, stored without a timezone like the other model archives.
Every row preserves `window_start`, `window_end`, `window_hours`, and forecast
hours at both bounds. `forecast_hour` corresponds to `valid_time`.

| Directory | Source variable | Value column | Valid time / units |
|---|---|---|---|
| `maxt` | `MaxT_SFC` | `maxt` | Native period end / F |
| `mint` | `MinT_SFC` | `mint` | Native period end / F |
| `minrh_native` | `MinRH_SFC` | `minrh` | Native period end / % |
| `wind` | `Wind_Mag_SFC`, `Wind_Dir_SFC` | `wind_speed_kt`, `wind_dir_deg` | Instantaneous at period start / knots, degrees |
| `gust` | `WindGust_SFC` | `wind_gust_kt` | Instantaneous at period start / knots |
| `precip6hr` | `QPF_SFC` | `precip6hr` | Six-hour period end / inches |
| `rh` | `RH_SFC` | `rh` | Instantaneous at period start / % |
| `minrh` | Derived from `RH_SFC` | `minrh` | Twelve-hour period ending 06Z / % |

Native extrema periods remain intact, including partial first periods and the
18-hour MinRH. The separate `minrh` product uses hourly RH at 18Z through 05Z,
representing **[18Z, 06Z)**. It requires all twelve hourly timestamps and twelve
finite values for each station. Incomplete windows are omitted and counted in
the report. This is a minimum of hourly instantaneous RH samples; it cannot
capture a minimum between those samples. It does not shorten or reinterpret the
native 18-hour minimum. Verification should match actual window bounds,
especially for native temperature extrema with nonstandard durations.

Missing variables are reported and available requested products are processed.
Wind speed and direction must both be present in the same file with matching
periods. Fields split across separate files can otherwise be imported in the
same batch; the importer does not assemble RH time windows across separate files.

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
to install it with backups. Changed period bounds represent different records;
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
python -m pytest -q tests/test_experimental_ndfd.py
```

Tests generate real NetCDF, gzip, and Parquet files matching the supplied
inspection. They cover all products, source timing, complete/incomplete derived
MinRH windows, dateline and row orientation, station exclusions, missing data,
invalid inputs, repeat imports, conflicts, month partitioning, and rollback.
The original production export must still be previewed locally to confirm its
metadata matches this format before its first installation.
