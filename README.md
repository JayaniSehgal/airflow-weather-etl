# Airflow Weather ETL (Astro)

Build an end-to-end ETL pipeline with Apache Airflow (orchestrated by Astronomer/Astro) that pulls public weather data from OpenWeather, cleans it, and loads it into Postgres. This project uses a single DAG (`weather_etl_hourly`) with retry logic, logging, and scheduling built in.

## What this pipeline does

1. **Extract**
   * Fetches current weather data for a configurable list of cities from the OpenWeather API.
2. **Transform**
   * Normalizes nested JSON into a clean, tabular structure.
   * Converts timestamps to UTC and ensures consistent data types.
3. **Load**
   * Creates the destination table (if needed).
   * Upserts observations into Postgres for idempotent loads.

The DAG is scheduled hourly and includes retry logic for robustness.

## Tech Stack

- **Apache Airflow** for orchestration
- **Astronomer (Astro CLI)** for local development
- **PostgreSQL** for storage
- **Python** with `requests` + `pandas`

## Project Structure

```
.
├── weather_etl.py             # Airflow DAG
├── requirements.txt           # Python dependencies
├── create_weather_table.sql   # Optional: table definition
└── README.md
```

## Prerequisites

- **Docker** (required by Astro CLI)
- **Python 3.10+**
- **Astro CLI** (for local Airflow dev)
- An **OpenWeather API key**

## Setup Instructions

### 1. Install Astro CLI

Follow the official install guide: https://www.astronomer.io/docs/astro/cli/install-cli

Quick install (macOS/Linux):

```
# macOS
brew install astro

# Linux
curl -sSL https://install.astronomer.io | sudo bash
```

Verify:

```
astro version
```

### 2. Clone this repo & install dependencies

```
git clone <your-repo-url>
cd airflow-weather-etl
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure Airflow connections and variables

This DAG expects:

**Airflow Variable**

- `OPENWEATHER_API_KEY` — your OpenWeather API key
- `WEATHER_CITIES` — comma-separated list of cities (optional; defaults to major Indian cities)

**Airflow Connection**

- `warehouse_postgres` — Postgres connection (host, db, user, password)

You can configure these in the Airflow UI or via Astro CLI:

```
astro dev start
```

Then open Airflow UI at: http://localhost:8080

### 4. Start Astro dev environment

```
astro dev start
```

This spins up:

- Airflow webserver
- Scheduler
- Postgres

### 5. Trigger the DAG

1. Go to Airflow UI → DAGs
2. Enable `weather_etl_hourly`
3. Click **Trigger DAG**

The DAG runs hourly on a cron schedule (`0 * * * *`) and uses Airflow retries on failure.

## Notes on the DAG

- **Retries**: 2 retries with a 3-minute delay
- **Schedule**: hourly
- **Logging**: Airflow logs are visible in the UI task instances
- **Idempotency**: Uses `ON CONFLICT` upserts on `(city_name, observed_at)`

## Example Table Schema

The table is created automatically if it doesn't exist:

```
CREATE TABLE IF NOT EXISTS public.weather_observations (
    city_name          TEXT NOT NULL,
    country_code       TEXT,
    weather_main       TEXT,
    weather_desc       TEXT,
    temp_c             NUMERIC,
    feels_like_c       NUMERIC,
    humidity_pct       INTEGER,
    wind_speed_ms      NUMERIC,
    observed_at        TIMESTAMPTZ NOT NULL,
    ingested_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (city_name, observed_at)
);
```

## Troubleshooting

- **DAG not showing?** Ensure `weather_etl.py` is under your Airflow DAGs folder (Astro mounts the project).
- **API errors?** Confirm your `OPENWEATHER_API_KEY` is valid and set in Airflow Variables.
- **Database errors?** Check the `warehouse_postgres` connection and DB logs.

## Next Steps (Optional Enhancements)

- Add unit tests for `transform`
- Persist raw JSON for lineage
- Add data quality checks (e.g., Great Expectations)
- Extend to historical datasets
