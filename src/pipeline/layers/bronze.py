"""
BRONZE -- "capture raw data as received".

Input  : text files sitting in the landing zone
           * lake/landing/stream/*.jsonl  -- micro-batches from the MQTT ingestor
           * lake/landing/batch/*.log     -- previous days' rotated log files
Output : lake/bronze/event_date=YYYY-MM-DD/event_hour=HH/part-*.parquet

RULES OF THE BRONZE LAYER (and why)
-----------------------------------
1. Keep the payload verbatim.  We store the original JSON text in `raw_payload`.
   If tomorrow we discover the silver logic dropped a field, we can reprocess
   history without going back to the source system -- which may no longer have it.
2. Add lineage, don't change data.  Every row gets `source_type`, `source_file`
   and `ingested_at`.  That is how you answer "where did this number come from?".
3. Convert the *container*, not the *content*: text -> Parquet.  Parquet is
   columnar and compressed, so silver can scan one column of 10M rows cheaply.
4. Partition on the event's own time, not on load time.  Reprocessing "2026-08-29"
   then means touching exactly one directory.
5. Never overwrite.  Each promotion writes a new part file; landing files are
   moved to `archive/` afterwards so a re-run cannot double-load them.
"""

from __future__ import annotations

import json
import logging
import shutil
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ..config import get_settings
from ..schemas import parse_raw_line

LOGGER = logging.getLogger(__name__)

# The physical schema of a bronze part file.  Explicit -- never inferred -- so
# that every part file in the lake is readable as one dataset.
BRONZE_SCHEMA = pa.schema(
    [
        pa.field("ingest_id", pa.string()),      # unique id for this row's capture
        pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
        pa.field("source_type", pa.string()),    # 'stream' | 'batch'
        pa.field("source_file", pa.string()),    # original landing filename
        pa.field("raw_payload", pa.string()),    # the untouched JSON text
        pa.field("event_date", pa.string()),     # partition key (YYYY-MM-DD)
        pa.field("event_hour", pa.int8()),       # partition key (0-23)
    ]
)


def _parse_timestamp(value: object) -> datetime | None:
    """
    Best-effort ISO-8601 parse used ONLY to pick a partition.

    Bronze does not validate; if the timestamp is unusable we still keep the row
    (partitioned by ingestion time) and let SILVER reject it with a reason.
    """
    if not isinstance(value, str):
        return None
    try:
        # Python's fromisoformat does not accept the trailing 'Z' before 3.11.
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _landing_files(source_type: str) -> list[Path]:
    """List landing files awaiting promotion, oldest first (stable ordering)."""
    settings = get_settings()
    root = settings.landing_stream if source_type == "stream" else settings.landing_batch
    pattern = "*.jsonl" if source_type == "stream" else "*.log"
    # `.tmp` files are still being written by the ingestor -- skip them.
    return sorted(p for p in root.glob(pattern) if not p.name.endswith(".tmp"))


def promote_landing_to_bronze(source_type: str, max_files: int | None = None) -> dict[str, int]:
    """
    Promote every pending landing file of `source_type` into bronze Parquet.

    Returns counters that the Airflow task pushes to XCom, so the DAG UI shows
    how much data each run actually moved.
    """
    settings = get_settings()
    settings.ensure_directories()

    files = _landing_files(source_type)
    if max_files is not None:
        files = files[:max_files]

    stats = {"files": 0, "rows_read": 0, "rows_written": 0, "rows_unparseable": 0}
    if not files:
        LOGGER.info("bronze: nothing to promote for source_type=%s", source_type)
        return stats

    for path in files:
        file_stats = _promote_one_file(path, source_type)
        for key, value in file_stats.items():
            stats[key] += value
        stats["files"] += 1

    LOGGER.info("bronze: promoted %s", stats)
    return stats


def _promote_one_file(path: Path, source_type: str) -> dict[str, int]:
    """Convert a single landing file into one Parquet part per (date, hour)."""
    settings = get_settings()
    ingested_at = datetime.now(timezone.utc)

    # Group rows by partition first, then write one file per partition.  Writing
    # a few large files beats writing thousands of tiny ones -- the "small file
    # problem" is the classic way to make a data lake slow.
    partitions: dict[tuple[str, int], list[dict]] = defaultdict(list)
    rows_read = 0
    unparseable = 0

    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if not line.strip():
                continue
            rows_read += 1
            parsed = parse_raw_line(line)
            if not parsed.ok:
                # Unreadable text never reaches Parquet; it goes to quarantine so
                # somebody can look at it later.
                _quarantine(path.name, line, parsed.error or "unparseable")
                unparseable += 1
                continue

            event_ts = _parse_timestamp(parsed.payload.get("ts")) or ingested_at
            key = (event_ts.date().isoformat(), event_ts.hour)
            partitions[key].append(
                {
                    "ingest_id": str(uuid.uuid4()),
                    "ingested_at": ingested_at,
                    "source_type": source_type,
                    "source_file": path.name,
                    # Re-serialise the parsed dict so the prefixed log-file format
                    # and the MQTT format land in bronze identically.
                    "raw_payload": _canonical_json(parsed.payload),
                    "event_date": key[0],
                    "event_hour": key[1],
                }
            )

    rows_written = 0
    for (event_date, event_hour), rows in partitions.items():
        target_dir = settings.bronze_root / f"event_date={event_date}" / f"event_hour={event_hour:02d}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / f"part-{path.stem}-{uuid.uuid4().hex[:8]}.parquet"
        table = pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA)
        # ZSTD gives noticeably smaller files than snappy at similar speed, and
        # bronze is the layer you keep forever, so size matters most here.
        pq.write_table(table, target, compression="zstd")
        rows_written += len(rows)

    # Only archive AFTER the parquet files are safely on disk.  If the process
    # dies mid-write the landing file is still there and the next run retries it.
    archive_dir = settings.archive_root / source_type / ingested_at.strftime("%Y-%m-%d")
    archive_dir.mkdir(parents=True, exist_ok=True)
    shutil.move(str(path), str(archive_dir / path.name))

    LOGGER.info(
        "bronze: %s -> %d rows in %d partitions (%d unparseable)",
        path.name, rows_written, len(partitions), unparseable,
    )
    return {"rows_read": rows_read, "rows_written": rows_written, "rows_unparseable": unparseable}


def _canonical_json(payload: dict) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)


def _quarantine(source_file: str, line: str, reason: str) -> None:
    """Append a bad line to the quarantine zone with the reason it was rejected."""
    settings = get_settings()
    settings.quarantine_root.mkdir(parents=True, exist_ok=True)
    target = settings.quarantine_root / f"bronze-{datetime.now(timezone.utc):%Y-%m-%d}.jsonl"
    with target.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "rejected_at": datetime.now(timezone.utc).isoformat(),
                    "layer": "bronze",
                    "source_file": source_file,
                    "reason": reason,
                    "raw_line": line.rstrip("\n")[:2000],
                },
                separators=(",", ":"),
            )
            + "\n"
        )


def bronze_glob(event_date: str | None = None) -> str:
    """
    Build the glob DuckDB uses to read bronze.

    Passing an `event_date` restricts the scan to one partition directory -- this
    is *partition pruning* done by hand, and it is the difference between reading
    one day and reading the entire history.
    """
    root = get_settings().bronze_root
    if event_date:
        return str(root / f"event_date={event_date}" / "*" / "*.parquet")
    return str(root / "*" / "*" / "*.parquet")
