"""
BATCH source: previous days' rotated log files.

A log shipper writes one file per day, e.g. `app-2026-08-30.log`, and closes it
at midnight.  A daily Airflow DAG then picks up the file for the day that just
ended.  Modelling it this way -- explicit filenames containing the date -- is
deliberate: it makes a backfill trivial (`--date 2026-08-12`) and makes
"did yesterday's file arrive?" a question you can answer with `ls`.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from pathlib import Path

from ..config import get_settings

LOGGER = logging.getLogger(__name__)

FILENAME_TEMPLATE = "app-{date}.log"


def batch_file_for_date(day: date | str) -> Path:
    """Path of the rotated log file that holds `day`'s events."""
    day_str = day if isinstance(day, str) else day.isoformat()
    return get_settings().landing_batch / FILENAME_TEMPLATE.format(date=day_str)


def previous_day(reference: date | None = None) -> date:
    """The day a 'daily batch' run should process (the day that just ended)."""
    return (reference or date.today()) - timedelta(days=1)


def wait_for_batch_file(day: date | str) -> bool:
    """
    Does the batch file for `day` exist and hold data?

    Airflow calls this from a PythonSensor.  A sensor -- rather than assuming the
    file is there -- is what stops the DAG from "succeeding" on an empty day and
    quietly publishing a report with a hole in it.
    """
    path = batch_file_for_date(day)
    exists = path.exists() and path.stat().st_size > 0
    LOGGER.info("batch source: %s -> %s", path, "ready" if exists else "not ready")
    return exists


def describe_landing_zone() -> list[dict[str, object]]:
    """Small inventory helper used by the API's /pipeline/status endpoint."""
    settings = get_settings()
    out: list[dict[str, object]] = []
    for label, folder, pattern in (
        ("stream", settings.landing_stream, "*.jsonl"),
        ("batch", settings.landing_batch, "*.log"),
    ):
        files = sorted(folder.glob(pattern))
        out.append(
            {
                "source_type": label,
                "pending_files": len(files),
                "bytes": sum(f.stat().st_size for f in files),
                "oldest": (
                    datetime.fromtimestamp(min(f.stat().st_mtime for f in files)).isoformat()
                    if files else None
                ),
            }
        )
    return out
