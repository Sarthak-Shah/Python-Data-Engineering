"""
Data quality checks -- the gate between "we processed data" and "we trust it".

A pipeline that always succeeds is not a healthy pipeline; it is a pipeline that
has stopped looking.  Each check below runs one SQL query that returns a single
number, compares it to a threshold, and records the outcome in
`silver.quality_results` so quality becomes a *time series* you can chart, not a
pass/fail that vanishes into a log file.

Two severities, and the difference is the whole point:

    error -> raise, which fails the Airflow task and stops the DAG.  Use it when
             publishing the data would actively mislead someone.
    warn  -> record and carry on.  Use it for "somebody should look at this"
             (data is late, reject rate crept up) where blocking would do more
             harm than the imperfect data.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

from .warehouse import writer_connection

LOGGER = logging.getLogger(__name__)

Comparison = Literal["==", "<=", ">=", "<", ">"]


@dataclass(frozen=True)
class Check:
    """One quality rule: a SQL query returning one number, plus its acceptance rule."""

    name: str
    sql: str
    comparison: Comparison
    threshold: float
    severity: Literal["warn", "error"] = "error"
    details: str = ""

    def evaluate(self, observed: float | None) -> bool:
        # A check with nothing to measure (empty table) is a pass, not a failure:
        # we only assert things about data that exists.
        if observed is None:
            return True
        return {
            "==": observed == self.threshold,
            "<=": observed <= self.threshold,
            ">=": observed >= self.threshold,
            "<": observed < self.threshold,
            ">": observed > self.threshold,
        }[self.comparison]


# ----------------------------------------------------------------------------
# The rule set.  Add checks here; the runner needs no changes.
# ----------------------------------------------------------------------------
SILVER_CHECKS: tuple[Check, ...] = (
    Check(
        name="silver_no_duplicate_event_ids",
        sql="SELECT count(*) - count(DISTINCT event_id) FROM silver.log_events",
        comparison="==",
        threshold=0,
        severity="error",
        details="event_id must be unique; duplicates mean de-duplication is broken",
    ),
    Check(
        name="silver_no_null_business_keys",
        sql="""
            SELECT count(*) FROM silver.log_events
            WHERE event_id IS NULL OR service IS NULL OR event_ts IS NULL
        """,
        comparison="==",
        threshold=0,
        severity="error",
        details="the columns every downstream join depends on must never be null",
    ),
    Check(
        name="silver_reject_rate_pct",
        sql="""
            -- Reject rate over the last 24 hours of loading activity.
            WITH recent AS (
                SELECT sum(rows_read) AS read, sum(rows_rejected) AS rejected
                FROM silver.load_audit
                WHERE loaded_at >= now() - INTERVAL 24 HOUR
            )
            SELECT round(100.0 * rejected / nullif(read, 0), 2) FROM recent
        """,
        comparison="<=",
        threshold=10.0,
        severity="warn",
        details="a rising reject rate usually means a producer changed its format",
    ),
    Check(
        name="silver_freshness_minutes",
        sql="""
            SELECT date_diff('minute', max(event_ts), now()::TIMESTAMP)
            FROM silver.log_events
        """,
        comparison="<=",
        threshold=120.0,
        severity="warn",
        details="how stale the newest event is; the streaming path should keep this small",
    ),
    Check(
        name="silver_no_future_timestamps",
        sql="""
            SELECT count(*) FROM silver.log_events
            WHERE event_ts > now()::TIMESTAMP + INTERVAL 1 HOUR
        """,
        comparison="==",
        threshold=0,
        severity="warn",
        details="events from the future normally mean a producer's clock is wrong",
    ),
)

GOLD_CHECKS: tuple[Check, ...] = (
    Check(
        name="gold_service_health_not_empty",
        sql="SELECT count(*) FROM gold.mart_service_health_hourly",
        comparison=">",
        threshold=0,
        severity="error",
        details="an empty mart would silently blank the dashboard",
    ),
    Check(
        name="gold_error_rate_in_range",
        sql="""
            SELECT count(*) FROM gold.mart_service_health_hourly
            WHERE error_rate_pct < 0 OR error_rate_pct > 100
        """,
        comparison="==",
        threshold=0,
        severity="error",
        details="a percentage outside 0-100 means the aggregation logic is wrong",
    ),
    Check(
        name="gold_matches_silver_event_count",
        sql="""
            -- Reconciliation: gold is an aggregate of silver, so the totals must
            -- agree.  This is the single most valuable check in any warehouse.
            SELECT abs(
                (SELECT coalesce(sum(events), 0) FROM gold.mart_service_health_hourly)
              - (SELECT count(*) FROM silver.log_events
                 WHERE event_date >= (SELECT min(event_date) FROM gold.mart_service_health_hourly))
            )
        """,
        comparison="==",
        threshold=0,
        severity="error",
        details="row counts must reconcile between silver and gold",
    ),
)


def run_checks(checks: tuple[Check, ...], run_id: str | None = None) -> dict[str, object]:
    """
    Execute a set of checks, persist the results, and raise if a hard one failed.

    Returns a summary dict so the Airflow task can push it to XCom and you can see
    the outcome directly in the task's UI without opening the warehouse.
    """
    run_id = run_id or uuid.uuid4().hex[:12]
    checked_at = datetime.now(timezone.utc)
    results: list[dict[str, object]] = []

    with writer_connection() as con:
        for check in checks:
            row = con.execute(check.sql).fetchone()
            observed = float(row[0]) if row and row[0] is not None else None
            passed = check.evaluate(observed)
            results.append(
                {
                    "check": check.name,
                    "severity": check.severity,
                    "passed": passed,
                    "observed": observed,
                    "threshold": check.threshold,
                }
            )
            con.execute(
                """
                INSERT INTO silver.quality_results
                    (checked_at, run_id, check_name, severity, passed, observed, threshold, details)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    checked_at, run_id, check.name, check.severity,
                    passed, observed, check.threshold, check.details,
                ],
            )
            log = LOGGER.info if passed else (LOGGER.warning if check.severity == "warn" else LOGGER.error)
            log(
                "quality[%s] %s: observed=%s %s %s -> %s",
                check.severity, check.name, observed, check.comparison,
                check.threshold, "PASS" if passed else "FAIL",
            )

    failures = [r for r in results if not r["passed"] and r["severity"] == "error"]
    summary = {
        "run_id": run_id,
        "total": len(results),
        "failed": sum(1 for r in results if not r["passed"]),
        "blocking_failures": [r["check"] for r in failures],
        "results": results,
    }
    if failures:
        # Raising here is what makes the check a *gate*: Airflow marks the task
        # failed, downstream tasks do not run, and the alert reaches a human.
        raise ValueError(f"blocking data quality failures: {[r['check'] for r in failures]}")
    return summary
