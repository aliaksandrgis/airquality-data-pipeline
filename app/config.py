from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class Settings:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "airquality.raw")
    # Optional Kafka security settings (e.g. Confluent Cloud)
    kafka_security_protocol: str = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    kafka_sasl_mechanism: str = os.getenv("KAFKA_SASL_MECHANISM", "")
    kafka_sasl_username: str = os.getenv("KAFKA_SASL_USERNAME", "")
    kafka_sasl_password: str = os.getenv("KAFKA_SASL_PASSWORD", "")
    batch_sleep_seconds: int = int(os.getenv("PIPELINE_SLEEP_SECONDS", "300"))
    use_live_api: bool = os.getenv("PIPELINE_LIVE_API", "false").lower() == "true"
    # Postgres for station catalog upsert (optional)
    db_host: str = os.getenv("POSTGRES_HOST", "postgres")
    db_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    db_name: str = os.getenv("POSTGRES_DB", "airquality")
    db_user: str = os.getenv("POSTGRES_USER", "airuser")
    db_password: str = os.getenv("POSTGRES_PASSWORD", "airpassword")
    db_sslmode: str = os.getenv("POSTGRES_SSLMODE", "")
    # DE (Germany, formerly UBA) settings
    de_base_url: str = os.getenv(
        "PIPELINE_DE_BASE_URL", "https://www.umweltbundesamt.de/api/air_data/v2"
    )
    de_stations: list[str] = field(
        default_factory=lambda: os.getenv("PIPELINE_DE_STATIONS", "1250").split(",")
    )
    # Luchtmeetnet (Netherlands) settings
    nl_base_url: str = os.getenv(
        "PIPELINE_NL_BASE_URL", "https://iq.luchtmeetnet.nl/open_api"
    )
    # GIOS (Poland) settings
    pl_base_url: str = os.getenv(
        "PIPELINE_PL_BASE_URL", "https://api.gios.gov.pl/pjp-api/v1/rest"
    )
    pl_sensors: list[str] = field(
        default_factory=lambda: os.getenv("PIPELINE_PL_SENSORS", "").split(",")
    )
    # Only automatic stations/sensors for PL to avoid manual points without live data
    pl_only_auto: bool = os.getenv("PIPELINE_PL_ONLY_AUTO", "true").lower() == "true"
    # Toggles
    disable_de_fetch: bool = (
        os.getenv("PIPELINE_DISABLE_DE", "false").lower() == "true"
    )
    disable_nl_fetch: bool = (
        os.getenv("PIPELINE_DISABLE_NL", "false").lower() == "true"
    )
    disable_pl_fetch: bool = (
        os.getenv("PIPELINE_DISABLE_PL", "false").lower() == "true"
    )


settings = Settings()
