"""
Warehouse access layer -- everything that touches DuckDB goes through here.

WHY DUCKDB?
-----------
The medallion picture calls for a "data warehouse" in the middle.  For a
Python-first project running on a laptop, DuckDB is the pragmatic pick:

  * embedded  -- no server to run, it is `pip install duckdb`
  * columnar + vectorised -- real OLAP performance on millions of rows
  * speaks Postgres-flavoured SQL, so the transformations you learn transfer
  * reads Parquet/CSV/JSON *in place*, which is what lets us keep BRONZE as
    plain files in the lake and still query it with SQL

Its one hard limitation matters a lot for pipeline design, so we handle it
explicitly rather than hiding it:

    ONE read-write process at a time.

Two parallel Airflow tasks opening the same .duckdb file read-write will crash.
Our answer is deliberate, and it is the same answer real warehouses use:

  1. There is exactly one writer role (the Airflow DAGs).  Serialised with a
     cross-process file lock -- see `writer_connection()`.
  2. Serving components (FastAPI, Streamlit) NEVER open the warehouse file.
     They read the GOLD Parquet exports through an in-memory DuckDB, which has
     no locking at all -- see `reader_connection()`.

That second rule is not a hack; it is the standard "serve from the mart, not
from the warehouse" separation, and it means the dashboard cannot ever be taken
down by a running pipeline.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterator

import duckdb
from filelock import FileLock

from .config import get_settings

LOGGER = logging.getLogger(__name__)

# Layers live in separate DuckDB schemas so the medallion structure is visible
# in the catalogue itself: `SHOW TABLES` tells you the story.
SCHEMAS = ("silver", "gold")


@contextlib.contextmanager
def writer_connection(timeout_seconds: int = 300) -> Iterator[duckdb.DuckDBPyConnection]:
    """
    Open the warehouse read-write, holding an exclusive cross-process lock.

    Use this for every pipeline task that writes.  Keep the block SHORT: the lock
    is held for its whole duration, so anything else that wants to write waits.
    """
    settings = get_settings()
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)

    lock = FileLock(str(settings.warehouse_lock_path), timeout=timeout_seconds)
    with lock:  # blocks until the other writer finishes (or raises after timeout)
        con = duckdb.connect(str(settings.warehouse_path))
        try:
            yield con
        finally:
            con.close()


@contextlib.contextmanager
def reader_connection() -> Iterator[duckdb.DuckDBPyConnection]:
    """
    Open an IN-MEMORY DuckDB for querying the GOLD Parquet exports.

    No file is opened, so no lock is taken and concurrent readers are free.
    Query the marts with `read_parquet('/data/lake/gold_exports/<mart>.parquet')`
    -- or just use `query_mart()` below, which builds that path for you.
    """
    con = duckdb.connect(":memory:")
    try:
        yield con
    finally:
        con.close()


def initialise_warehouse() -> None:
    """
    Create the schemas and tables.  Idempotent -- safe on every DAG run.

    Note the deliberate difference between the two layers:

      SILVER is normalised, one row per event, optimised for *writes* and for
      being the trustworthy historical record.  It is the enterprise repository.

      GOLD is a small star schema plus flat, denormalised marts, optimised for
      *reads*.  Nothing here is expensive to recompute -- gold is derived data.
    """
    with writer_connection() as con:
        for schema in SCHEMAS:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # ------------------------------------------------------------------
        # SILVER: cleansed, typed, de-duplicated events (the 3NF-ish core)
        # ------------------------------------------------------------------
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS silver.log_events (
                event_id     VARCHAR PRIMARY KEY,  -- de-duplication key
                event_ts     TIMESTAMP NOT NULL,
                event_date   DATE      NOT NULL,
                event_hour   TINYINT   NOT NULL,
                service      VARCHAR   NOT NULL,
                host         VARCHAR,
                level        VARCHAR   NOT NULL,
                is_error     BOOLEAN   NOT NULL,
                message      VARCHAR,
                latency_ms   INTEGER,
                status_code  INTEGER,
                trace_id     VARCHAR,
                source_type  VARCHAR   NOT NULL,   -- lineage: 'stream' or 'batch'
                source_file  VARCHAR,              -- lineage: originating bronze file
                ingested_at  TIMESTAMP NOT NULL,
                processed_at TIMESTAMP NOT NULL
            )
            """
        )

        # Rows that could not be trusted.  A silver layer that throws bad data
        # away is a silver layer you cannot audit, so we keep it with a reason.
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS silver.rejected_events (
                rejected_at  TIMESTAMP NOT NULL,
                reason       VARCHAR   NOT NULL,
                source_file  VARCHAR,
                raw_payload  VARCHAR
            )
            """
        )

        # Operational metadata: which bronze files have already been loaded.
        # This is what makes re-running a DAG safe (idempotency by bookkeeping).
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS silver.load_audit (
                source_file   VARCHAR PRIMARY KEY,
                layer         VARCHAR   NOT NULL,
                rows_read     BIGINT    NOT NULL,
                rows_loaded   BIGINT    NOT NULL,
                rows_rejected BIGINT    NOT NULL,
                loaded_at     TIMESTAMP NOT NULL
            )
            """
        )

        # Results of every data-quality check, kept as a time series so you can
        # see quality trends rather than just the latest pass/fail.
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS silver.quality_results (
                checked_at  TIMESTAMP NOT NULL,
                run_id      VARCHAR   NOT NULL,
                check_name  VARCHAR   NOT NULL,
                severity    VARCHAR   NOT NULL,   -- 'warn' or 'error'
                passed      BOOLEAN   NOT NULL,
                observed    DOUBLE,
                threshold   DOUBLE,
                details     VARCHAR
            )
            """
        )
    LOGGER.info("warehouse initialised at %s", get_settings().warehouse_path)


def table_count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    """Small helper so tasks can log 'we now have N rows' without ceremony."""
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
