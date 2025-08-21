from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import List, Dict

import requests
import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ---- Config ----
DEFAULT_CITIES = ["Delhi", "Mumbai", "Hyderabad", "Bengaluru", "Chennai"]
API_BASE = "https://api.openweathermap.org/data/2.5/weather"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="weather_etl_hourly",
    description="Simple hourly ETL: OpenWeather -> Transform -> Postgres",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule_interval="0 * * * *",  # every hour
    catchup=False,
    tags=["etl", "weather", "beginner"],
) as dag:

    @task
    def extract() -> List[Dict]:
        """Fetch current weather for a list of cities from OpenWeather API."""
        api_key = Variable.get("OPENWEATHER_API_KEY")
        cities_csv = Variable.get("WEATHER_CITIES", ",".join(DEFAULT_CITIES))
        cities = [c.strip() for c in cities_csv.split(",") if c.strip()]

        results: List[Dict] = []
        for city in cities:
            params = {"q": city, "appid": api_key, "units": "metric"}
            r = requests.get(API_BASE, params=params, timeout=30)
            r.raise_for_status()
            payload = r.json()
            results.append(payload)
        return results

    @task
    def transform(raw: List[Dict]) -> List[Dict]:
        """Clean and normalize payloads into a tabular list of dicts."""
        rows: List[Dict] = []
        for item in raw:
            city = item.get("name")
            sys = item.get("sys", {})
            weather = (item.get("weather") or [{}])[0]
            main = item.get("main", {})
            wind = item.get("wind", {})
            dt_utc = datetime.fromtimestamp(item.get("dt"), tz=timezone.utc)

            rows.append(
                {
                    "city_name": city,
                    "country_code": sys.get("country"),
                    "weather_main": weather.get("main"),
                    "weather_desc": weather.get("description"),
                    "temp_c": main.get("temp"),
                    "feels_like_c": main.get("feels_like"),
                    "humidity_pct": main.get("humidity"),
                    "wind_speed_ms": wind.get("speed"),
                    "observed_at": dt_utc.isoformat(),
                }
            )

        # quick sanity checks with pandas (optional)
        df = pd.DataFrame(rows)
        # drop completely empty rows (unlikely)
        df = df.dropna(how="all")
        # ensure types (Airflow/psycopg2 will coerce, but let's be explicit)
        if not df.empty:
            df["observed_at"] = pd.to_datetime(df["observed_at"], utc=True)
        return df.to_dict(orient="records")

    @task
    def load(rows: List[Dict]) -> int:
        """Create table if needed and upsert new observations into Postgres."""
        if not rows:
            return 0

        hook = PostgresHook(postgres_conn_id="warehouse_postgres")
        create_sql = """
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
        """
        upsert_sql = """
        INSERT INTO public.weather_observations (
            city_name, country_code, weather_main, weather_desc,
            temp_c, feels_like_c, humidity_pct, wind_speed_ms, observed_at
        )
        VALUES (
            %(city_name)s, %(country_code)s, %(weather_main)s, %(weather_desc)s,
            %(temp_c)s, %(feels_like_c)s, %(humidity_pct)s, %(wind_speed_ms)s, %(observed_at)s
        )
        ON CONFLICT (city_name, observed_at) DO UPDATE SET
            country_code = EXCLUDED.country_code,
            weather_main = EXCLUDED.weather_main,
            weather_desc = EXCLUDED.weather_desc,
            temp_c = EXCLUDED.temp_c,
            feels_like_c = EXCLUDED.feels_like_c,
            humidity_pct = EXCLUDED.humidity_pct,
            wind_speed_ms = EXCLUDED.wind_speed_ms;
        """

        # run within a single connection/transaction
        conn = hook.get_conn()
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                cur.executemany(upsert_sql, rows)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return len(rows)

    loaded = load(transform(extract()))
