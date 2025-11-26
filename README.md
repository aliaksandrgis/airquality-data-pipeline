# AirQuality Data Pipeline

Python producer (and optional lightweight consumer) for ingesting air quality measurements into Kafka (Confluent Cloud).

## Quick start
```bash
python -m venv .venv && source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
cp .env.example .env  # fill placeholders
python -m app.main
```

## Environment variables (.env)
- `KAFKA_BOOTSTRAP` — bootstrap server(s) from Confluent.
- `KAFKA_USERNAME` / `KAFKA_PASSWORD` — SASL credentials (if needed).
- `KAFKA_TOPIC` — target topic (default `airquality.raw`).
- `PIPELINE_LIVE_API` — `true` to use OpenAQ, otherwise synthetic data.
- `PIPELINE_SLEEP_SECONDS` — pause between batches.

## Docker
```bash
docker build -t airquality-data-pipeline:dev .
docker run --env-file .env airquality-data-pipeline:dev
```

## CI (template)
- Lint with `ruff` (or flake8), run `pytest`, build container image.
- Secrets (Confluent creds) are injected via GitHub Actions secrets; never commit real keys.

## Structure
- `app/` — producer logic, config, utils.
- `tests/` — unit tests.

