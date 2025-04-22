# Alaska Forecast Verification & Archiving

This repository provides a modular pipeline for extracting, processing, and archiving point-based weather forecast data from gridded datasets like the National Digital Forecast Database (NDFD) and the National Blend of Models (NBM). It converts GRIB2 data into tabular station-based data and writes it to cloud-based storage (e.g., S3) or local disk as partitioned Parquet files for easy access and analysis.

---

## 🔧 Features

- ⬇️ Downloads GRIB2 forecast data (e.g., wind, temperature, gusts) from public S3-hosted datasets (e.g., NDFD, NBM)
- 📍 Maps gridded data to station locations from Synoptic metadata
- ⚙️ Processes data in parallel with optional byte-range subsetting
- 🧱 Writes output as Parquet files to cloud or local storage, chunked by month
- ⭯️ Supports modular `Archiver` classes for different data sources (NDFD, NBM, more coming)
- 🧩 Configurable via `archiver_config.py` or runtime command-line arguments

---

## 🗂️ Project Structure

```bash
.
├── archiver_base.py       # Abstract base class for archiving functionality
├── ndfd_archiver.py       # Archiver class for NDFD gridded forecasts
├── model_archiver.py      # Archiver class for models like NBM, HRRR, URMA
├── run_ndfd_archiver.py   # CLI for archiving NDFD data by month
├── run_model_archiver.py  # CLI for archiving model data (e.g., NBM)
├── utils.py               # Shared functions for file pairing, downloading, and extraction
├── archiver_config.py     # Centralized configuration module
```

---

## 🚀 Getting Started

### 1. Clone the repo
```bash
git clone https://github.com/david-levin11/alaska-verification.git
cd alaska-verification
```

### 2. Set up your environment
```bash
conda env create -f environment.yml
conda activate alaska-verify
```

Or manually:
```bash
pip install -r requirements.txt
```

### 3. Run the pipeline

#### Archive NDFD Data
```bash
python run_ndfd_archiver.py \
  --start "2022-01-01 00:00" \
  --end "2022-02-01" \
  --element Wind \
  [--local]  # Optional: saves files locally instead of S3
```

#### Archive Model Data (e.g., NBM)
```bash
python run_model_archiver.py \
  --start "2022-01-01" \
  --end "2022-03-01" \
  --model nbm \
  --element Wind \
  [--local]  # Optional toggle for local output
```

Supported models: `nbm`, `gfs`, `hrrrak`, `rtma_ak`, `urma_ak`

---

## 📤 Output Format

Processed data is saved as monthly partitioned Parquet files in:

```
s3://your-bucket/forecast_data/
├── 2022_01_ndfd_wind_archive.parquet
├── 2022_02_nbm_wind_archive.parquet
```

Or locally in:
```
model/nbm/wind/2022_01_archive.parquet
ndfd/wind/2022_01_ndfd_wind_archive.parquet
```

Each row contains:

| station_id | init_time | valid_time | forecast_hour | wind_speed_kt | wind_dir_deg | ... |
|------------|------------|------------|----------------|----------------|---------------|-----|

---

## 🧱 Extending the Archiver

To add support for new models:
1. Create a new subclass of `Archiver` (e.g., `MyNewModelArchiver`)
2. Implement:
   - `fetch_file_list(start, end)` to return GRIB file paths or URLs
   - `process_files(file_list)` to extract and format station-level data
3. Add appropriate logic to `utils.py` or your own module for file reading/indexing

---

## 🤝 Contributing

Have ideas for improvements or want to support additional datasets? Open a pull request or file an issue!

---

