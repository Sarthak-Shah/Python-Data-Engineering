"""
DAG 2 of 2 -- the BATCH path.

    rotated log file (app-YYYY-MM-DD.log) -> landing/batch
                                          -> [ this DAG ] -> bronze -> silver -> gold

Runs once a day and processes the day that just ended.  Two things make it a
proper batch DAG rather than a cron job:

  * A SENSOR waits for the day's file to actually arrive instead of assuming it.
    Without it, a late log rotation produces a green DAG run and a report with a
    silent hole in it -- the worst possible failure mode.
  * `catchup` + a date-parameterised load make BACKFILLS free.  Ask Airflow to run
    2026-07-01 and it processes exactly that day, because the day is an input to
    the tasks, not "whatever now() happens to be".

Note that after the bronze step this DAG calls the *same* silver and gold
functions as the streaming DAG.  One set of business rules, two arrival patterns.
"""

from __future__ import annotations

import _bootstrap  # noqa: F401  -- must come first: it puts `pipeline` on sys.path
import pendulum
from airflow.decorators import dag, task
from airflow.sensors.python import PythonSensor

from pipeline.layers import bronze, gold, silver
from pipeline.quality import GOLD_CHECKS, SILVER_CHECKS, run_checks
from pipeline.sources.log_files import wait_for_batch_file
from pipeline.warehouse import initialise_warehouse

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=2),
}


@dag(
    dag_id="medallion_batch_daily",
    description="Daily: ingest the previous day's log file through bronze -> silver -> gold",
    schedule="30 0 * * *",  # 00:30 UTC -- after the log file has been rotated
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,  # flip to True (and pick a real start_date) to backfill history
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["medallion", "batch", "log-files"],
    doc_md=__doc__,
)
def medallion_batch_daily():

    @task
    def init_warehouse() -> str:
        initialise_warehouse()
        return "ready"

    # ---------------------------------------------------------------------
    # SENSOR: block until the day's log file exists and is non-empty.
    #
    # mode="reschedule" (not "poke") releases the worker slot between checks, so a
    # sensor waiting four hours does not occupy a slot for four hours.  This is
    # the most common Airflow scaling mistake, and the fix is one keyword.
    # ---------------------------------------------------------------------
    wait_for_file = PythonSensor(
        task_id="wait_for_daily_log_file",
        python_callable=lambda **context: wait_for_batch_file(context["ds"]),
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,   # give up after 6 hours ...
        soft_fail=False,       # ... and fail loudly: a missing day needs a human
    )

    @task
    def land_to_bronze() -> dict:
        """Promote every pending `.log` file (normally just the day's file)."""
        return bronze.promote_landing_to_bronze(source_type="batch")

    @task
    def load_silver(ds: str | None = None) -> dict:
        """
        Load only this run's date partition.

        `ds` is a reserved Airflow context key, so naming the parameter `ds` is
        enough: Airflow injects the run's logical date (YYYY-MM-DD) automatically.
        (It must default to None -- Airflow rejects any other default on a context
        parameter, because the value has to come from the run, not from the code.)

        Taking the date as an INPUT rather than reading the clock inside the
        function is what makes a re-run of an old date reproduce that date's
        result -- the whole basis of backfilling.
        """
        return silver.load_bronze_to_silver(event_date=ds)

    @task
    def check_silver() -> dict:
        return run_checks(SILVER_CHECKS)

    @task
    def build_gold() -> dict:
        """
        Full rebuild.  The daily window is the right moment to pay for it: it
        heals any drift left by the incremental streaming rebuilds and picks up
        late-arriving events for older days.
        """
        return gold.build_gold(since_date=None)

    @task
    def check_gold() -> dict:
        return run_checks(GOLD_CHECKS)

    (
        init_warehouse()
        >> wait_for_file
        >> land_to_bronze()
        >> load_silver()
        >> check_silver()
        >> build_gold()
        >> check_gold()
    )


medallion_batch_daily()
