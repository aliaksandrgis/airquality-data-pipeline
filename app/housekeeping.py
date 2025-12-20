from __future__ import annotations

from .main import LOGGER, _get_db_conn


def main(retention_days: int = 7) -> None:
    """Prune curated measurements older than retention_days."""
    if retention_days <= 0:
        LOGGER.warning("Invalid retention_days=%s; skipping cleanup", retention_days)
        return

    LOGGER.info("Pruning measurements_curated older than %s days", retention_days)

    sql = """
    DELETE FROM public.measurements_curated
    WHERE observed_at < (now() - %s::interval)
    """
    interval_literal = f"{retention_days} days"

    try:
        with _get_db_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (interval_literal,))
            deleted = cur.rowcount
        LOGGER.info("Deleted %s rows from public.measurements_curated", deleted)
    except Exception:
        LOGGER.exception("Failed to prune public.measurements_curated")


if __name__ == "__main__":
    main()
