from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple

from .config import settings
from .main import (
    LOGGER,
    _build_producer,
    _commit_cursor_updates,
    _emit_batch,
    _fetch_gios_latest,
    _filter_new_measurements,
    _prepare_batch,
    _create_synthetic_measurements,
)


def main() -> None:
    """One-shot fetch of PL measurements (GIOS) and emit to Kafka."""
    LOGGER.info("Starting one-shot PL measurements fetch")
    producer = _build_producer()
    if producer is None:
        LOGGER.warning("Kafka producer unavailable; aborting PL measurements run")
        return

    payload: List[Dict[str, Any]]
    pending_cursor_updates: Dict[str, Dict[Tuple[str, str], datetime]] = {}
    if settings.use_live_api:
        data = _fetch_gios_latest()
        original_count = len(data)
        LOGGER.info("Fetched %s PL measurements", original_count)
        if not data:
            LOGGER.warning("No PL data to emit")
            return
        filtered, cursor_updates = _filter_new_measurements("gios", data)
        LOGGER.info(
            "Filtered PL measurements %s -> %s (dedup)", original_count, len(filtered)
        )
        if not filtered:
            LOGGER.warning("No new PL data after deduplication; nothing to emit")
            return
        if cursor_updates:
            pending_cursor_updates["gios"] = cursor_updates
        payload = filtered
    else:
        LOGGER.info("PIPELINE_LIVE_API=false, using synthetic data")
        payload = _create_synthetic_measurements()

    prepared = _prepare_batch(payload)
    if not prepared:
        LOGGER.warning("Prepared batch is empty; nothing to emit for PL")
        return
    try:
        _emit_batch(producer, prepared)
    except Exception:
        LOGGER.exception("Failed to emit PL measurements batch")
    else:
        _commit_cursor_updates(pending_cursor_updates)


if __name__ == "__main__":
    main()

