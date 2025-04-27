# Wind Turbine Data Ingestion, Storage, and Anomaly Detection

## Objective

The objective of this project is to process turbine sensor data, perform basic cleaning and validation, store the data efficiently, generate daily statistical summaries, and detect anomalies in daily turbine performance compared to historical behavior.


## Setup and Installation

### 1. Create a New Python Environment and Install Dependencies

#### Create & activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate       # MacOS / Linux
# .\venv\Scripts\activate    # Windows (PowerShell)

pip install -r requirements.txt
```

### 2. Ensure SQLite is Installed
#### For MacOS:
```bash
brew install sqlite
```
#### For Ubuntu / Debian:
```bash
sudo apt-get update
sudo apt-get install sqlite3 libsqlite3-dev
```

### 3. Run the Pipeline
To run the pipeline, first, ensure the src folder is included in your PYTHONPATH:

(for MacOS or Linux):
```
cd <project-folder>
export PYTHONPATH=$PYTHONPATH:$(pwd)/src
```
(for Windows OS):
```
cd <project-folder>
$env:PYTHONPATH = "$env:PYTHONPATH;$PWD\src"
```
Then:
```
python scripts/run_ingestion_db_pipeline_single_file.py
```
This will read and validate a file resources/data_group_1.csv (default), 
- clean and handle missing values/outliers, 
- and store raw readings, daily summaries, 
- and anomalies in SQLite database (wind_turbine_data.db)


## Summary of What I Did

The project was divided into three main stages: ingestion, persistence, and analysis. Each stage involved careful design decisions and handling of practical assumptions.

### Ingestion

During the ingestion phase, the system reads a CSV file containing turbine data, validates its structure, and performs basic cleaning. Validation ensures that all required columns (`timestamp`, `turbine_id`, `power_output`, `wind_speed`, `wind_direction`) are present, and that turbine IDs match the expected group.

#### In the cleaning phase:

- **For missing values**:
  - Critical fields (`timestamp`, `turbine_id`) are non-recoverable; rows missing these fields are dropped.
  - Sensor readings (`power_output`, `wind_speed`, `wind_direction`) are forward-filled up to 2 consecutive missing entries (`.ffill(limit=2)`).
  - Forward filling was chosen for simplicity and to preserve sudden changes, avoiding the smoothing effect that averaging or interpolation might introduce.

- **For outlier handling**:
  - Physical checks were first applied (e.g., `wind_direction` must be between 0° and 360°, `power_output` cannot be negative).
  - Statistical outlier detection was implemented using both standard deviation-based and IQR-based methods.
  - The standard deviation method was ultimately chosen for its interpretability and suitability given the expected distributions of turbine outputs.

### Persistence

The cleaned data was stored in an SQLite database, chosen for its simplicity and suitability for local development and prototyping.

Three main tables were defined:
- **turbine_reading**: raw sensor readings for each turbine and timestamp.
- **daily_summary**: daily aggregated statistics (mean, min, max, std) for each turbine.
- **daily_anomalies**: records of detected anomalies based on historical behavior.

Future expansions for the database structure could include:
- Adding a **turbine_metadata** table to hold static turbine properties.
- Establishing foreign key relationships to ensure referential integrity.
- Introducing indexing for better query performance.
- Transitioning to a production-grade database such as PostgreSQL.

Helper functions were developed to abstract database reading and writing operations.

### Analysis

The analysis module consisted of two parts: daily statistical summary and anomaly detection.

- **Daily Summary**:
  - Aggregates daily statistics per turbine (`mean`, `min`, `max`, `standard deviation`) and stores them in the `daily_summary` table.

- **Anomaly Detection**:
  - Calculates the total daily power output per turbine.
  - Compares today's total output against the turbine’s historical daily totals over a rolling window (default: 7 days).
  - An anomaly is flagged if today's output falls outside ±2 standard deviations from the historical mean.

Although the basic anomaly detection is based on standard deviation thresholds, more advanced techniques can be incorporated in the future, such as:
- Rolling quantile thresholds
- Hourly, Weekly, or Seasonal decomposition (STL)
- Isolation Forests

## Assumptions and Design Choices

- Missing critical fields (`timestamp`, `turbine_id`) are handled by dropping rows.
- Forward-filling sensor readings conservatively for up to two missing consecutive values.
- Standard deviation-based outlier detection was preferred over IQR for its simplicity and assumption of near-normality.
- Summing hourly readings per day to compute daily total output, instead of using only averages.
- Using a 7-day historical window for anomaly comparisons by default, but keeping it configurable.

## Final Notes

The overall system was built to be modular and extendable. It allows easy addition of more sophisticated anomaly detection strategies, richer turbine metadata, real-time data processing pipelines, and integration with cloud-hosted databases for scaling to production.

