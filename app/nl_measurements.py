from __future__ import annotations

from typing import Dict, List, Any

from .config import settings
from .main import (
    _build_producer,
    _emit_batch,
    _fetch_lucht_latest,
    _prepare_batch,
    LOGGER,
    _create_synthetic_measurements,
)


def main() -> None:
    """One-shot fetch of NL measurements (Luchtmeetnet) and emit to Kafka."""
    LOGGER.info("Starting one-shot NL measurements fetch")
    producer = _build_producer()
    if producer is None:
        LOGGER.warning("Kafka producer unavailable; aborting NL measurements run")
        return

    payload: List[Dict[str, Any]]
    if settings.use_live_api:
        request_counter: Dict[str, int] = {"count": 0, "http_429": 0}
        data = _fetch_lucht_latest(request_counter)
        LOGGER.info("Fetched %s NL measurements", len(data))
        nl_429 = request_counter.get("http_429", 0)
        if nl_429:
            LOGGER.warning("NL fetch encountered %s HTTP 429 responses", nl_429)
        if not data:
            LOGGER.warning("No NL data to emit")
            return
        payload = data
    else:
        LOGGER.info("PIPELINE_LIVE_API=false, using synthetic data")
        payload = _create_synthetic_measurements()

    prepared = _prepare_batch(payload)
    if not prepared:
        LOGGER.warning("Prepared batch is empty; nothing to emit for NL")
        return
    _emit_batch(producer, prepared)


if __name__ == "__main__":
    main()

