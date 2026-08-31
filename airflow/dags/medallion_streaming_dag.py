"""
DAG 1 of 2 -- the STREAMING path.

    MQTT broker -> (stream-ingestor container) -> landing/stream/*.jsonl
                -> [ this DAG ] -> bronze -> silver -> gold

The ingestor runs continuously outside Airflow and only writes files.  This DAG
runs every 5 minutes and *promotes* whatever has landed since last time.  That
split is intentional and it is how most "real-time" warehouses actually work:

    continuous, dumb ingestion   (never blocked, never loses data)
  + frequent, orchestrated batch (retryable, observable, backfillable)

Airflow is deliberately NOT used to consume MQTT.  An orchestrator schedules
work; it is a poor fit for holding a long-lived socket open.

Everything below is thin: each task calls one function from the `pipeline`
package.  The transformations are testable without Airflow, and Airflow supplies
what it is actually good at -- scheduling, retries, alerting, and a UI that shows
you what ran.
"""

from __future__ import annotations

import _bootstrap  # noqa: F401  -- must come first: it puts `pipeline` on sys.path
import pendulum
from airflow.decorators import dag, task

from pipeline.layers import bronze, gold, silver
from pipeline.quality import GOLD_CHECKS, SILVER_CHECKS, run_checks
from pipeline.warehouse import initialise_warehouse

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    # Exponential backoff: if the warehouse is briefly locked by the daily DAG,
    # waiting a little and retrying is exactly the right response.
    "retry_delay": pendulum.duration(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(minutes=5),
}


@dag(
    dag_id="medallion_streaming",
    description="Every 5 min: promote MQTT micro-batches through bronze -> silver -> gold",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # catchup=False: this DAG processes "whatever is in the landing zone right
    # now", so replaying missed intervals would do the same work twice.
    catchup=False,
    # max_active_runs=1: DuckDB accepts one writer.  Airflow enforcing that here
    # is cheaper and clearer than every task fighting over the file lock.
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["medallion", "streaming", "mqtt"],
    doc_md=__doc__,
)
def medallion_streaming():

    @task
    def init_warehouse() -> str:
        """Create schemas/tables if they do not exist.  Idempotent by design."""
        initialise_warehouse()
        return "ready"

    @task
    def land_to_bronze() -> dict:
        """Convert landed JSONL micro-batches into partitioned bronze Parquet."""
        return bronze.promote_landing_to_bronze(source_type="stream")

    @task
    def load_silver() -> dict:
        """Cleanse, type and de-duplicate every bronze file not yet consumed."""
        return silver.load_bronze_to_silver()

    @task
    def check_silver() -> dict:
        """Quality gate.  A blocking failure here stops gold from being rebuilt."""
        return run_checks(SILVER_CHECKS)

    @task
    def build_gold() -> dict:
        """
        Rebuild only the last two days of marts.

        Gold is derived data, so recomputing beats merging -- but recomputing ALL
        of history every 5 minutes would be wasteful, hence the narrow window.
        """
        return gold.build_gold(since_date=gold.default_since_date(days_back=2))

    @task
    def check_gold() -> dict:
        """Second gate: the published marts must reconcile with silver."""
        return run_checks(GOLD_CHECKS)

    # Linear dependency chain.  Calling the functions is what wires them together
    # in the TaskFlow API -- the return value of one becomes the input of the next.
    init_warehouse() >> land_to_bronze() >> load_silver() >> check_silver() >> build_gold() >> check_gold()


medallion_streaming()
