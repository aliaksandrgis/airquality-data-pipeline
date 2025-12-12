from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple

from .config import settings
from .main import (
    LOGGER,
    _build_producer,
    _commit_cursor_updates,
    _emit_batch,
    _fetch_lucht_latest,
    _filter_new_measurements,
    _prepare_batch,
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
    pending_cursor_updates: Dict[str, Dict[Tuple[str, str], datetime]] = {}
    if settings.use_live_api:
        request_counter: Dict[str, int] = {"count": 0, "http_429": 0}
        data = _fetch_lucht_latest(request_counter)
        original_count = len(data)
        LOGGER.info("Fetched %s NL measurements", original_count)
        nl_429 = request_counter.get("http_429", 0)
        if nl_429:
            LOGGER.warning("NL fetch encountered %s HTTP 429 responses", nl_429)
        if not data:
            LOGGER.warning("No NL data to emit")
            return
        filtered, cursor_updates = _filter_new_measurements("luchtmeetnet", data)
        LOGGER.info(
            "Filtered NL measurements %s -> %s (dedup)", original_count, len(filtered)
        )
        if not filtered:
            LOGGER.warning("No new NL data after deduplication; nothing to emit")
            return
        if cursor_updates:
            pending_cursor_updates["luchtmeetnet"] = cursor_updates
        payload = filtered
    else:
        LOGGER.info("PIPELINE_LIVE_API=false, using synthetic data")
        payload = _create_synthetic_measurements()

    prepared = _prepare_batch(payload)
    if not prepared:
        LOGGER.warning("Prepared batch is empty; nothing to emit for NL")
        return
    try:
        _emit_batch(producer, prepared)
    except Exception:
        LOGGER.exception("Failed to emit NL measurements batch")
    else:
        _commit_cursor_updates(pending_cursor_updates)


if __name__ == "__main__":
    main()

