# AirQuality Data Pipeline

Python producer for ingesting **live air-quality measurements** (DE: UBA, NL: Luchtmeetnet, PL: GIOS) into Kafka.

This is the same code as in `dev/services/data-pipeline`, packaged as a standalone service for deployment.

## Quick start (local Kafka)
```bash
python -m venv .venv && source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
cp .env.example .env  # adjust values for your environment

# Full multi-country loop (as in dev compose)
python -m app.main

# Or one-shot jobs per country (for orchestration)
python -m app.de_stations
python -m app.nl_stations
python -m app.pl_stations
python -m app.de_measurements
python -m app.nl_measurements
python -m app.pl_measurements
```

## Environment variables (.env)
- `KAFKA_BOOTSTRAP` - bootstrap server(s), for example `localhost:9092` or a Confluent Cloud endpoint.
- `KAFKA_TOPIC` - target topic with raw measurements (default `airquality.raw`).
- `PIPELINE_LIVE_API` - `true` to use real DE/NL/PL APIs, `false` to emit synthetic demo data.
- `PIPELINE_SLEEP_SECONDS` - pause between ingestion cycles (seconds).
- `POSTGRES_*` - connection settings for Postgres (optional, used for station catalog).
- `PIPELINE_DE_*`, `PIPELINE_NL_*`, `PIPELINE_PL_*` - base URLs and filters for each country (see `.env.example`).

## Docker
```bash
docker build -t airquality-data-pipeline:dev .
docker run --env-file .env airquality-data-pipeline:dev
```

## CI (template)
- Lint with `ruff` (or flake8), then run `pytest`, then build the container image.
- Kafka/Confluent secrets are passed via GitHub Actions secrets; never commit real keys.

## Structure
- `app/` - pipeline logic (`config.py`, `main.py`, country-specific entrypoints).
- `tests/` - unit tests (can be added as needed).
