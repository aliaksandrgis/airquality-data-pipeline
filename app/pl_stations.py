from __future__ import annotations

from typing import Dict, List, Any

from .main import _refresh_pl_catalog, _upsert_catalog, LOGGER


def main() -> None:
    """One-shot refresh of PL station catalog and upsert into Postgres."""
    LOGGER.info("Starting PL station catalog refresh (one-shot)")
    stations: List[Dict[str, Any]] = _refresh_pl_catalog()
    if not stations:
        LOGGER.warning("PL catalog refresh returned 0 stations")
        return
    _upsert_catalog(stations)


if __name__ == "__main__":
    main()

