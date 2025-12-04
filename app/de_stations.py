from __future__ import annotations

from typing import Dict, List, Any

from .main import _refresh_de_catalog, _upsert_catalog, LOGGER


def main() -> None:
    """One-shot refresh of DE station catalog and upsert into Postgres."""
    LOGGER.info("Starting DE station catalog refresh (one-shot)")
    stations: List[Dict[str, Any]] = _refresh_de_catalog()
    if not stations:
        LOGGER.warning("DE catalog refresh returned 0 stations")
        return
    _upsert_catalog(stations)


if __name__ == "__main__":
    main()

