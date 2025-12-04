from __future__ import annotations

from typing import Dict, List, Any

from .main import _refresh_nl_catalog, _upsert_catalog, LOGGER


def main() -> None:
    """One-shot refresh of NL station catalog and upsert into Postgres."""
    LOGGER.info("Starting NL station catalog refresh (one-shot)")
    request_counter: Dict[str, int] = {"count": 0, "http_429": 0}
    stations: List[Dict[str, Any]] = _refresh_nl_catalog(request_counter)
    if not stations:
        LOGGER.warning("NL catalog refresh returned 0 stations")
    else:
        _upsert_catalog(stations)
    nl_429 = request_counter.get("http_429", 0)
    if nl_429:
        LOGGER.warning("NL catalog refresh encountered %s HTTP 429 responses", nl_429)


if __name__ == "__main__":
    main()

