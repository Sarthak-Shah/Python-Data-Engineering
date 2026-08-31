"""
Run the entire medallion flow in ONE process, with no Docker and no Airflow.

    python -m pipeline.demo

This exists because the fastest way to understand a pipeline is to watch it move
data end to end without an orchestrator in the way.  Every function called here
is the exact same function the Airflow DAGs call -- Airflow adds scheduling,
retries, backfills and a UI, but it adds no business logic.  That separation
(pipeline logic in a library, orchestration in DAGs) is the single most useful
structural habit in data engineering: it keeps your transformations testable.
"""

from __future__ import annotations

import argparse

from .layers import bronze, gold, silver
from .logging_utils import configure_logging
from .quality import GOLD_CHECKS, SILVER_CHECKS, run_checks
from .simulator import write_backfill_files
from .warehouse import initialise_warehouse, reader_connection

LOGGER = configure_logging("demo")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run bronze -> silver -> gold once")
    parser.add_argument("--generate", action="store_true",
                        help="create demo batch log files before running")
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--events-per-day", type=int, default=5000)
    args = parser.parse_args()

    if args.generate:
        LOGGER.info("STEP 0 -- generating demo source data")
        write_backfill_files(args.days, args.events_per_day)

    LOGGER.info("STEP 1 -- create warehouse schemas (idempotent)")
    initialise_warehouse()

    LOGGER.info("STEP 2 -- BRONZE: land raw files as Parquet")
    for source_type in ("batch", "stream"):
        LOGGER.info("  bronze[%s]: %s", source_type, bronze.promote_landing_to_bronze(source_type))

    LOGGER.info("STEP 3 -- SILVER: cleanse, type, de-duplicate")
    LOGGER.info("  silver: %s", silver.load_bronze_to_silver())

    LOGGER.info("STEP 4 -- QUALITY GATE on silver")
    silver_quality = run_checks(SILVER_CHECKS)
    LOGGER.info("  silver quality: %d/%d passed",
                silver_quality["total"] - silver_quality["failed"], silver_quality["total"])

    LOGGER.info("STEP 5 -- GOLD: star schema + marts + Parquet export")
    LOGGER.info("  gold: %s", gold.build_gold())

    LOGGER.info("STEP 6 -- QUALITY GATE on gold")
    gold_quality = run_checks(GOLD_CHECKS)
    LOGGER.info("  gold quality: %d/%d passed",
                gold_quality["total"] - gold_quality["failed"], gold_quality["total"])

    LOGGER.info("STEP 7 -- read the published mart the way the dashboard does")
    _preview_marts()


def _preview_marts() -> None:
    """
    Query the GOLD Parquet exports through an in-memory DuckDB.

    This is exactly what FastAPI and Streamlit do: no warehouse file is opened,
    so this works even while the pipeline is mid-write.
    """
    from .config import get_settings

    exports = get_settings().gold_exports
    with reader_connection() as con:
        rows = con.execute(
            f"""
            SELECT service,
                   sum(events)  AS events,
                   sum(errors)  AS errors,
                   round(100.0 * sum(errors) / nullif(sum(events), 0), 2) AS error_rate_pct
            FROM read_parquet('{exports}/mart_service_health_hourly.parquet')
            GROUP BY service
            ORDER BY errors DESC
            """
        ).fetchall()
    print("\n  service            events   errors   error_rate_pct")
    print("  " + "-" * 52)
    for service, events, errors, rate in rows:
        print(f"  {service:<18} {events:>6}   {errors:>6}   {rate:>8}")
    print()


if __name__ == "__main__":
    main()
