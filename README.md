# Alaska Forecast Verification & Archiving

This repository provides a modular pipeline for extracting, processing, and archiving point-based weather forecast data from gridded datasets like the National Digital Forecast Database (NDFD). It converts GRIB2 data into tabular station-based data and writes it to cloud-based storage (e.g., S3) as partitioned Parquet files for easy access and analysis.

---

## 🔧 Features

- ⬇️ Downloads GRIB2 forecast data (e.g., wind, temperature) from S3-hosted datasets
- 📍 Maps gridded data to user-specified station locations
- ⚙️ Processes data in parallel for efficiency
- 🧱 Writes individual Parquet files to S3 using a structured naming format
- 🧩 Extensible `Archiver` class for adding support for other models or observational sources

---

## 🗂️ Project Structure

```bash
.
├── archiver_base.py       # Abstract base class for all archivers
├── ndfd_archiver.py       # Implementation for processing NDFD forecasts
├── utils.py               # Shared utilities (file matching, GRIB parsing, metadata)
├── run_archiver.py        # Driver script for monthly archiving
├── archiver_config.py     # User configuration for data source, time range, variables, S3 paths
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

Or manually install:
```bash
pip install -r requirements.txt
```

### 3. Configure your pipeline
Edit `archiver_config.py` to set:
- ✅ Forecast element (`Wind`, `Temperature`, etc.)
- 🗓️ Time range (`OBS_START`, `OBS_END`)
- ☁️ S3 input/output paths
- 📍 Station metadata source

### 4. Run the pipeline
```bash
python run_archiver.py
```

---

## 📤 Output Format

Processed data is saved in S3 as individual Parquet files with the format:
```
s3://your-bucket/forecast_data/
├── 2023_01_ndfd_wind_archive.parquet
├── 2023_02_ndfd_wind_archive.parquet
├── 2023_01_nbm_temperature_archive.parquet
```
Each file corresponds to a specific year, month, model, and forecast element.

Each row in the Parquet file includes:

| station_id | valid_time | forecast_hour | wind_speed_kt | wind_dir_deg |
|------------|------------|----------------|----------------|---------------|

---

## 🧱 Extending the Archiver

To add new data sources:
1. Create a new class (e.g., `NBMArchiver`) that inherits from `Archiver`
2. Implement:
   - `fetch_file_list(start, end)`
   - `process_files(file_list)`
3. Register the class in a factory (optional)

---

## 🤝 Contributing

Have ideas for improvements or want to add support for more data sources? Open a pull request or file an issue!

---


