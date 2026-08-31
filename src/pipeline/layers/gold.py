"""
GOLD -- "optimised, denormalised, consumption-ready".

Input  : silver.log_events
Output : a small STAR SCHEMA          gold.dim_service, gold.dim_date,
                                      gold.fct_log_events_hourly
         two flat MARTS               gold.mart_service_health_hourly
                                      gold.mart_error_hotspots_daily
         Parquet copies of the marts  lake/gold_exports/*.parquet

WHY A STAR SCHEMA *AND* FLAT MARTS?
-----------------------------------
They answer different questions and both belong in gold:

  * The star schema (facts + conformed dimensions) is the reusable analytical
    model.  A new question -- "errors by region" -- means adding a column to
    `dim_service`, not rewriting every query.
  * The flat marts are the last mile.  The Streamlit dashboard and the FastAPI
    endpoints read exactly one table, with no joins, because a dashboard that
    joins four tables is a dashboard that gets slower every month.

WHY FULL REBUILD INSTEAD OF INCREMENTAL?
----------------------------------------
Gold is *derived* data: everything here can be recomputed from silver.  Rebuilding
the affected dates outright is simpler and always self-correcting -- a late-arriving
event from yesterday just changes yesterday's numbers on the next run, with no
merge logic to get wrong.  When gold gets big enough that this hurts, the usual
next step is to rebuild only the recent partitions (which is what `since_date`
below already lets you do).

WHY EXPORT TO PARQUET?
----------------------
So the serving layer never opens the warehouse file.  DuckDB allows a single
read-write process; the pipeline holds that role.  FastAPI and Streamlit read
these Parquet exports with an in-memory DuckDB instead, which means a running
pipeline can never lock out the dashboard.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

from ..config import get_settings
from ..warehouse import table_count, writer_connection

LOGGER = logging.getLogger(__name__)

# The marts that get exported to Parquet for the serving layer.
EXPORTED_MARTS = (
    "mart_service_health_hourly",
    "mart_error_hotspots_daily",
    "mart_pipeline_health",
    "fct_log_events_hourly",
    "dim_service",
)


def build_gold(since_date: str | None = None) -> dict[str, int]:
    """
    Rebuild the gold layer.

    `since_date` (YYYY-MM-DD) limits the rebuild to recent data -- the streaming
    DAG passes "today - 1" so a 5-minute run stays cheap, while the nightly batch
    DAG passes None and rebuilds everything.
    """
    stats: dict[str, int] = {}
    # Build the incremental predicate once, in three aliased flavours, rather
    # than string-patching one clause -- generated SQL should stay readable.
    silver_where = f"WHERE e.event_date >= DATE '{since_date}'" if since_date else ""
    fact_where = f"WHERE event_date >= DATE '{since_date}'" if since_date else ""
    error_where = f"AND event_date >= DATE '{since_date}'" if since_date else ""

    with writer_connection() as con:
        # ------------------------------------------------------------------
        # DIMENSION: service
        # A conformed dimension -- one row per service, with the attributes every
        # report wants to slice by.  In a real project `tier`/`owner` would come
        # from a service catalogue; deriving them here keeps the demo self-contained.
        # ------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS gold.dim_service")
        con.execute(
            """
            CREATE TABLE gold.dim_service AS
            SELECT
                row_number() OVER (ORDER BY service) AS service_key,  -- surrogate key
                service                              AS service_name, -- natural key
                count(DISTINCT host)                 AS host_count,
                min(event_ts)                        AS first_seen_at,
                max(event_ts)                        AS last_seen_at,
                count(*)                             AS lifetime_events
            FROM silver.log_events
            GROUP BY service
            """
        )
        stats["dim_service"] = table_count(con, "gold.dim_service")

        # ------------------------------------------------------------------
        # DIMENSION: date
        # Generated from a range rather than from the fact table, so that a day
        # with zero events still exists as a row.  Reports that "skip" empty days
        # are one of the most common and most confusing BI bugs.
        # ------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS gold.dim_date")
        con.execute(
            """
            CREATE TABLE gold.dim_date AS
            WITH bounds AS (
                SELECT min(event_date) AS d0, max(event_date) AS d1 FROM silver.log_events
            ),
            days AS (
                SELECT cast(unnest(generate_series(d0, d1, INTERVAL 1 DAY)) AS DATE) AS d
                FROM bounds
            )
            SELECT
                cast(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
                d                                      AS calendar_date,
                extract('year'  FROM d)                AS year,
                extract('month' FROM d)                AS month,
                extract('day'   FROM d)                AS day,
                strftime(d, '%A')                      AS day_name,
                extract('dow' FROM d) IN (0, 6)        AS is_weekend
            FROM days
            """
        )
        stats["dim_date"] = table_count(con, "gold.dim_date")

        # ------------------------------------------------------------------
        # FACT: hourly aggregate at grain (service x hour x level)
        #
        # This is an *aggregate fact table*, not a copy of silver.  Collapsing
        # millions of raw events into one row per service/hour/level is what makes
        # the dashboard instant; the raw events stay in silver for drill-down.
        # ------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS gold.fct_log_events_hourly")
        con.execute(
            f"""
            CREATE TABLE gold.fct_log_events_hourly AS
            SELECT
                cast(strftime(e.event_date, '%Y%m%d') AS INTEGER) AS date_key,
                d.service_key,
                e.event_date,
                e.event_hour,
                e.service,
                e.level,
                count(*)                                   AS event_count,
                count(*) FILTER (WHERE e.is_error)         AS error_count,
                count(DISTINCT e.host)                     AS host_count,
                avg(e.latency_ms)                          AS avg_latency_ms,
                -- Percentiles matter far more than averages for latency: an
                -- average hides the slow tail that users actually feel.
                quantile_cont(e.latency_ms, 0.50)          AS p50_latency_ms,
                quantile_cont(e.latency_ms, 0.95)          AS p95_latency_ms,
                max(e.latency_ms)                          AS max_latency_ms
            FROM silver.log_events e
            LEFT JOIN gold.dim_service d ON d.service_name = e.service
            {silver_where}
            GROUP BY ALL
            """
        )
        stats["fct_log_events_hourly"] = table_count(con, "gold.fct_log_events_hourly")

        # ------------------------------------------------------------------
        # MART: service health per hour  (the dashboard's main time series)
        # One row per service+hour, every number the UI needs already computed.
        # ------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS gold.mart_service_health_hourly")
        con.execute(
            f"""
            CREATE TABLE gold.mart_service_health_hourly AS
            SELECT
                event_date,
                event_hour,
                -- A real timestamp column saves every consumer from rebuilding one.
                cast(event_date AS TIMESTAMP) + to_hours(cast(event_hour AS BIGINT)) AS event_hour_ts,
                service,
                sum(event_count)                                        AS events,
                sum(error_count)                                        AS errors,
                sum(event_count) FILTER (WHERE level = 'WARN')          AS warnings,
                round(100.0 * sum(error_count) / nullif(sum(event_count), 0), 2) AS error_rate_pct,
                round(avg(avg_latency_ms), 1)                           AS avg_latency_ms,
                round(max(p95_latency_ms), 1)                           AS p95_latency_ms,
                max(host_count)                                         AS hosts
            FROM gold.fct_log_events_hourly
            {fact_where}
            GROUP BY ALL
            """
        )
        stats["mart_service_health_hourly"] = table_count(con, "gold.mart_service_health_hourly")

        # ------------------------------------------------------------------
        # MART: daily error hotspots  (the "what broke yesterday?" table)
        # ------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS gold.mart_error_hotspots_daily")
        con.execute(
            f"""
            CREATE TABLE gold.mart_error_hotspots_daily AS
            WITH errors AS (
                SELECT event_date, service, level, message, host, status_code
                FROM silver.log_events
                WHERE is_error
                  {error_where}
            )
            SELECT
                event_date,
                service,
                count(*)                                     AS error_events,
                count(DISTINCT host)                         AS affected_hosts,
                count(*) FILTER (WHERE level = 'FATAL')      AS fatal_events,
                count(*) FILTER (WHERE status_code >= 500)   AS server_error_responses,
                -- mode() gives the single most frequent message: the headline of
                -- the incident, without shipping every message to the dashboard.
                mode(message)                                AS top_error_message
            FROM errors
            GROUP BY ALL
            ORDER BY event_date DESC, error_events DESC
            """
        )
        stats["mart_error_hotspots_daily"] = table_count(con, "gold.mart_error_hotspots_daily")

        # ------------------------------------------------------------------
        # MART: pipeline health  (observability data, served like any other mart)
        #
        # The latest result of every quality check, plus the layer row counts.
        # Exposing *operational* metadata through the same serving path as
        # business data is what lets the dashboard answer "can I trust this?"
        # right next to the numbers themselves.
        # ------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS gold.mart_pipeline_health")
        con.execute(
            """
            CREATE TABLE gold.mart_pipeline_health AS
            WITH latest AS (
                SELECT *, row_number() OVER (
                    PARTITION BY check_name ORDER BY checked_at DESC
                ) AS rn
                FROM silver.quality_results
            )
            SELECT check_name, severity, passed, observed, threshold, details, checked_at
            FROM latest WHERE rn = 1
            ORDER BY passed, severity DESC, check_name
            """
        )
        stats["mart_pipeline_health"] = table_count(con, "gold.mart_pipeline_health")

        # ------------------------------------------------------------------
        # Publish: copy each mart out as Parquet for the serving layer.
        # COPY ... TO is DuckDB's bulk export; it writes a single, compressed,
        # self-describing file that FastAPI/Streamlit can read without any lock.
        # ------------------------------------------------------------------
        exports = get_settings().gold_exports
        exports.mkdir(parents=True, exist_ok=True)
        for mart in EXPORTED_MARTS:
            target = exports / f"{mart}.parquet"
            # Write to a temp name and rename: a reader polling the directory can
            # never observe a half-written file (rename is atomic on POSIX).
            tmp = exports / f".{mart}.parquet.tmp"
            con.execute(f"COPY gold.{mart} TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            tmp.replace(target)
        LOGGER.info("gold: exported %d marts to %s", len(EXPORTED_MARTS), exports)

    LOGGER.info("gold: %s", stats)
    return stats


def default_since_date(days_back: int = 2) -> str:
    """
    Convenience for the streaming DAG: only rebuild the last couple of days.

    Two days rather than one, because events arriving just after midnight (or
    late, out of a retrying producer) still belong to yesterday.
    """
    return (date.today() - timedelta(days=days_back)).isoformat()
