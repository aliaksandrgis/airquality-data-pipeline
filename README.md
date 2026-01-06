# AirQuality Data Pipeline

Kafka producer for live air quality measurements from DE (UBA), NL (Luchtmeetnet), and PL (GIOS).

## Run
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

python -m app.main
```

One-shot tasks (for orchestration):
```bash
python -m app.de_stations
python -m app.nl_stations
python -m app.pl_stations
python -m app.de_measurements
python -m app.nl_measurements
python -m app.pl_measurements
```

## Environment
KAFKA_* for Kafka/Confluent, POSTGRES_* for catalog (optional), PIPELINE_* for toggles.

## Docker
```bash
docker build -t airquality-data-pipeline .
docker run --env-file .env airquality-data-pipeline
```
