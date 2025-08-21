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
