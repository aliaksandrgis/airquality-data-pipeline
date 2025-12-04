from __future__ import annotations

from typing import Dict, List, Any

from .config import settings
from .main import (
    _build_producer,
    _emit_batch,
    _fetch_de_latest,
    _prepare_batch,
    LOGGER,
    _create_synthetic_measurements,
)


def main() -> None:
    """One-shot fetch of DE measurements and emit to Kafka."""
    LOGGER.info("Starting one-shot DE measurements fetch")
    producer = _build_producer()
    if producer is None:
        LOGGER.warning("Kafka producer unavailable; aborting DE measurements run")
        return

    payload: List[Dict[str, Any]]
    if settings.use_live_api:
        data = _fetch_de_latest()
        LOGGER.info("Fetched %s DE measurements", len(data))
        if not data:
            LOGGER.warning("No DE data to emit")
            return
        payload = data
    else:
        LOGGER.info("PIPELINE_LIVE_API=false, using synthetic data")
        payload = _create_synthetic_measurements()

    prepared = _prepare_batch(payload)
    if not prepared:
        LOGGER.warning("Prepared batch is empty; nothing to emit for DE")
        return
    _emit_batch(producer, prepared)


if __name__ == "__main__":
    main()

