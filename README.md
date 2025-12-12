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
- `POSTGRES_*` - connection settings for Postgres (optional, used for station catalog). Set `POSTGRES_SSLMODE=require` if you're pointing at a managed service such as Supabase.
- `PIPELINE_DE_*`, `PIPELINE_NL_*`, `PIPELINE_PL_*` - base URLs and filters for each country (see `.env.example`).

## Docker
```bash
docker build -t airquality-data-pipeline:dev .
docker run --env-file .env airquality-data-pipeline:dev
```

## Raspberry Pi / systemd
Для постоянного запуска на Pi3/4:

1. Настройте `.env` и виртуальное окружение (`python -m venv .venv && source .venv/bin/activate; pip install -r requirements.txt`).
2. Сделайте файл запуска исполняемым: `chmod +x scripts/run_producer.sh`. Скрипт принимает имя python-модуля (по умолчанию `app.main`), активирует venv, загружает `.env` и пишет логи в `logs/<module>.log`. Например:
   ```bash
   scripts/run_producer.sh app.de_stations
   scripts/run_producer.sh app.de_measurements
   scripts/run_producer.sh app.nl_stations
   scripts/run_producer.sh app.nl_measurements
   scripts/run_producer.sh app.pl_stations
   scripts/run_producer.sh app.pl_measurements
   ```
   Airflow/cron может вызывать те же команды для оркестрации отдельных шагов.
3. Скопируйте `systemd/airquality-producer.service` в `/etc/systemd/system/` и поправьте `User`/`WorkingDirectory` при необходимости.
4. Примените unit:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable airquality-producer.service
   sudo systemctl start airquality-producer.service
   ```
   Логи доступны через `journalctl -u airquality-producer -f`.

## CI (template)
- Lint with `ruff` (or flake8), then run `pytest`, then build the container image.
- Kafka/Confluent secrets are passed via GitHub Actions secrets; never commit real keys.

## Structure
- `app/` - pipeline logic (`config.py`, `main.py`, country-specific entrypoints).
- `tests/` - unit tests (can be added as needed).

## Kafka deduplication & backlog cleanup

The producer now keeps track of the freshest timestamp per measurement using the
`ingestion_cursors` table in Supabase/Postgres:

```sql
CREATE TABLE IF NOT EXISTS public.ingestion_cursors (
    source TEXT NOT NULL,
    station_id TEXT NOT NULL,
    pollutant TEXT NOT NULL,
    last_observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source, station_id, pollutant)
);
```

During each loop `app.main`:

1. Loads cursor values for the current source (DE/NL/PL).
2. Drops records whose `observed_at` is not newer than the stored cursor.
3. After a successful Kafka publish updates the cursor table with the latest
   timestamp per `(source, station_id, pollutant)`.

To verify that deduplication works:

```sql
SELECT source, station_id, pollutant, last_observed_at
FROM public.ingestion_cursors
ORDER BY last_observed_at DESC
LIMIT 20;
```

### Cleaning the Kafka backlog in Confluent Cloud

1. On the Raspberry Pi stop both services so they do not write/read while the
   topic is recreated:
   ```bash
   sudo systemctl stop airquality-producer
   sudo systemctl stop airquality-live
   ```
2. In Confluent Cloud open *Topics → airquality.raw → Settings* and click
   **Delete topic**, then create it again with the original settings (1
   partition, cleanup policy `delete`, retention `1 day`).
3. Start only the producer and monitor `logs/app_main.log`. You should see lines
   such as `Filtered NL measurements 612 -> 214 (dedup)` and `Emitted N records
   to airquality.raw`.
4. Check the *Messages* and *Monitor* tabs for the topic. Production/consumption
   per hour should reflect the small deduplicated batch and the consumer lag
   should stay near zero.
5. Once the topic looks healthy, start Spark again:
   ```bash
   sudo systemctl start airquality-live
   ```

This workflow keeps Confluent clean and ensures Supabase receives only the latest
measurements while Kafka throughput stays predictable.
