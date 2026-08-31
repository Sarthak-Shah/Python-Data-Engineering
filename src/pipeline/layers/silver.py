"""
SILVER -- "deduplicated, cleansed and normalised".

Input  : bronze Parquet part files (raw JSON text + lineage columns)
Output : silver.log_events  (one typed row per *distinct* event)
         silver.rejected_events  (everything we refused, with the reason)
         silver.load_audit       (which bronze files have been consumed)

WHAT SILVER ACTUALLY DOES -- in the order the SQL below does it
---------------------------------------------------------------
1. PARSE      pull typed columns out of the raw JSON payload
2. NORMALISE  'WARNING'/'warn'/'Warn' all become 'WARN'; whitespace trimmed
3. VALIDATE   split the rows into "trustworthy" and "rejected + a reason"
4. ENRICH     derive event_date / event_hour / is_error once, here, so that
              every downstream consumer agrees on what an "error" is
5. DE-DUPLICATE  MQTT is at-least-once and batch files can be replayed, so the
              same event_id WILL arrive twice.  We keep the first copy.

IDEMPOTENCY -- the property that makes a pipeline operable
----------------------------------------------------------
Running this twice must not change the result.  Two independent mechanisms give
us that, and it is worth having both:
  * bookkeeping: `silver.load_audit` remembers which bronze files were consumed,
    so a normal re-run does no work at all;
  * data-level: `INSERT ... ON CONFLICT DO NOTHING` on the event_id primary key,
    so even a forced reprocess (`reprocess=True`) cannot create duplicates.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from ..config import get_settings
from ..schemas import LEVEL_ALIASES
from ..warehouse import table_count, writer_connection

LOGGER = logging.getLogger(__name__)


def _level_case_sql(column: str) -> str:
    """
    Generate the SQL that normalises severity levels.

    The alias map lives in schemas.py (Python) and the SQL is generated from it,
    so there is exactly ONE definition of "what counts as a WARN" in the project.
    Copy-pasting that map into a .sql file is how contracts start to rot.
    """
    whens = "\n            ".join(
        f"WHEN '{alias}' THEN '{canonical}'" for alias, canonical in LEVEL_ALIASES.items()
    )
    return f"""CASE upper(trim({column}))
            {whens}
            ELSE NULL          -- unknown level -> the row gets rejected below
        END"""


def _audit_key(path: Path) -> str:
    """
    The key `silver.load_audit` records for a bronze file.

    It is the path RELATIVE to the bronze root, never the absolute path.  The same
    file is `/data/lake/bronze/...` inside a container and `./data/lake/bronze/...`
    when you run the demo from the repo -- keying on the absolute path would make
    the pipeline reprocess everything the first time it is invoked differently.
    Bookkeeping keys must describe the data, not the machine.
    """
    return str(path.relative_to(get_settings().bronze_root))


def _pending_bronze_files(con, event_date: str | None, reprocess: bool) -> list[Path]:
    """Bronze part files not yet consumed by silver (all of them if reprocessing)."""
    settings = get_settings()
    pattern = f"event_date={event_date}/*/*.parquet" if event_date else "*/*/*.parquet"
    candidates = sorted(settings.bronze_root.glob(pattern))

    if reprocess:
        return candidates

    already = {
        row[0]
        for row in con.execute(
            "SELECT source_file FROM silver.load_audit WHERE layer = 'silver'"
        ).fetchall()
    }
    return [p for p in candidates if _audit_key(p) not in already]


def load_bronze_to_silver(event_date: str | None = None, reprocess: bool = False) -> dict[str, int]:
    """
    Transform pending bronze files into silver rows.

    `event_date` narrows the work to one partition (used by the daily batch DAG);
    leaving it None processes everything pending (used by the streaming DAG).
    """
    stats = {"files": 0, "rows_read": 0, "rows_loaded": 0, "rows_rejected": 0}

    with writer_connection() as con:
        pending = _pending_bronze_files(con, event_date, reprocess)
        # DuckDB needs real paths to read; the audit table needs stable keys.
        files = [str(p) for p in pending]
        if not files:
            LOGGER.info("silver: no pending bronze files (event_date=%s)", event_date)
            return stats

        before = table_count(con, "silver.log_events")

        # ------------------------------------------------------------------
        # STEP 1+2: parse and normalise into a staging table.
        # `read_parquet($files)` takes an explicit list, so we read exactly the
        # files we decided to read -- no accidental full-lake scans.
        # ------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS stg_events")
        con.execute(
            f"""
            CREATE TEMP TABLE stg_events AS
            WITH raw AS (
                SELECT
                    source_type,
                    source_file,      -- landing file (lineage back to the source)
                    filename AS bronze_file,  -- bronze part file (lineage for the audit log)
                    ingested_at,
                    raw_payload,
                    -- json_extract_string returns NULL for a missing key, which is
                    -- exactly the signal the validation step below looks for.
                    json_extract_string(raw_payload, '$.event_id')    AS event_id,
                    json_extract_string(raw_payload, '$.ts')          AS ts_raw,
                    trim(json_extract_string(raw_payload, '$.service')) AS service,
                    trim(json_extract_string(raw_payload, '$.host'))    AS host,
                    json_extract_string(raw_payload, '$.level')       AS level_raw,
                    json_extract_string(raw_payload, '$.message')     AS message,
                    json_extract_string(raw_payload, '$.trace_id')    AS trace_id,
                    -- TRY_CAST returns NULL instead of raising: bad values become
                    -- rejected rows rather than a failed task at 3am.
                    try_cast(json_extract_string(raw_payload, '$.latency_ms') AS INTEGER)  AS latency_ms,
                    try_cast(json_extract_string(raw_payload, '$.status_code') AS INTEGER) AS status_code
                FROM read_parquet($files, filename = true)
            ),
            typed AS (
                SELECT
                    *,
                    {_level_case_sql("level_raw")} AS level,
                    -- All producers emit UTC with a trailing 'Z'.  We strip it and
                    -- store a naive UTC TIMESTAMP: one timezone everywhere, decided
                    -- once, here.  Mixing tz-aware and naive timestamps across a
                    -- warehouse is a bug factory.
                    try_cast(regexp_replace(ts_raw, 'Z$', '') AS TIMESTAMP) AS event_ts
                FROM raw
            )
            SELECT
                *,
                -- STEP 3: one column holding the first reason this row is untrustworthy.
                CASE
                    WHEN event_id IS NULL OR event_id = '' THEN 'missing event_id'
                    WHEN event_ts IS NULL                  THEN 'unparseable timestamp'
                    WHEN service IS NULL OR service = ''   THEN 'missing service'
                    -- A service name is a join key, so it has to look like one.
                    -- Placeholders ('-', '?') and junk are rejected rather than
                    -- silently becoming a new row in gold.dim_service.
                    WHEN NOT regexp_matches(service, '^[A-Za-z0-9][A-Za-z0-9._-]{{1,63}}$')
                                                           THEN 'invalid service name: ' || service
                    WHEN level    IS NULL                  THEN 'unknown log level: ' || coalesce(level_raw, '<null>')
                    WHEN latency_ms IS NOT NULL AND latency_ms < 0 THEN 'negative latency'
                    ELSE NULL
                END AS reject_reason
            FROM typed
            """,
            {"files": files},
        )

        stats["files"] = len(files)
        stats["rows_read"] = table_count(con, "stg_events")

        # ------------------------------------------------------------------
        # STEP 3b: park the bad rows.  Auditable, queryable, never lost.
        # ------------------------------------------------------------------
        con.execute(
            """
            INSERT INTO silver.rejected_events (rejected_at, reason, source_file, raw_payload)
            SELECT now()::TIMESTAMP, reject_reason, source_file, raw_payload
            FROM stg_events
            WHERE reject_reason IS NOT NULL
            """
        )
        stats["rows_rejected"] = con.execute(
            "SELECT count(*) FROM stg_events WHERE reject_reason IS NOT NULL"
        ).fetchone()[0]

        # ------------------------------------------------------------------
        # STEP 4+5: enrich, de-duplicate, insert.
        #
        # row_number() de-duplicates WITHIN this batch; ON CONFLICT DO NOTHING
        # de-duplicates against everything already in the table.  You need both:
        # the primary key cannot help you if one INSERT statement itself carries
        # the same event_id twice.
        # ------------------------------------------------------------------
        con.execute(
            """
            INSERT INTO silver.log_events
            SELECT
                event_id,
                event_ts,
                cast(event_ts AS DATE)                AS event_date,
                cast(extract('hour' FROM event_ts) AS TINYINT) AS event_hour,
                service,
                host,
                level,
                level IN ('ERROR', 'FATAL')           AS is_error,
                message,
                latency_ms,
                status_code,
                trace_id,
                source_type,
                source_file,
                ingested_at,
                now()::TIMESTAMP                      AS processed_at
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY event_id
                    -- Keep the earliest capture of an event: the first time we saw
                    -- it is the truth, later copies are retransmissions.
                    ORDER BY ingested_at
                ) AS rn
                FROM stg_events
                WHERE reject_reason IS NULL
            )
            WHERE rn = 1
            ON CONFLICT (event_id) DO NOTHING
            """
        )
        stats["rows_loaded"] = table_count(con, "silver.log_events") - before

        # ------------------------------------------------------------------
        # Bookkeeping: mark each bronze part file as consumed, with its counts.
        # This table is what makes the *next* run a no-op, and it doubles as the
        # operational log you check when somebody asks "did yesterday load?".
        # ------------------------------------------------------------------
        loaded_at = datetime.now(timezone.utc)
        per_file = {
            row[0]: (row[1], row[2])
            for row in con.execute(
                """
                SELECT bronze_file,
                       count(*)                                          AS rows_read,
                       count(*) FILTER (WHERE reject_reason IS NOT NULL)  AS rows_rejected
                FROM stg_events
                GROUP BY bronze_file
                """
            ).fetchall()
        }
        for path in pending:
            rows_read, rows_rejected = per_file.get(str(path), (0, 0))
            con.execute(
                """
                INSERT OR REPLACE INTO silver.load_audit
                    (source_file, layer, rows_read, rows_loaded, rows_rejected, loaded_at)
                VALUES (?, 'silver', ?, ?, ?, ?)
                """,
                [_audit_key(path), rows_read, max(rows_read - rows_rejected, 0),
                 rows_rejected, loaded_at],
            )

        con.execute("DROP TABLE IF EXISTS stg_events")

    LOGGER.info("silver: %s", stats)
    return stats
