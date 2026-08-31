"""
The data contract: what a "log event" looks like at every layer.

A pipeline without an explicit contract is a pipeline that breaks silently.  This
module is the single place where we say:

    * what the producer promises to send   -> RAW_EVENT_FIELDS
    * what BRONZE stores                   -> raw payload + ingestion metadata
    * what SILVER guarantees               -> SILVER_COLUMNS (typed, non-null keys)

The domain here is *application log events* -- the kind of thing a fleet of
services emits continuously.  Same shape whether it arrives live over MQTT or is
read from yesterday's rotated `.log` file, which is precisely why one set of
transformations can serve both sources.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

# ----------------------------------------------------------------------------
# The producer-side contract
# ----------------------------------------------------------------------------
# Every event -- streamed or batched -- is a JSON object with these keys.
RAW_EVENT_FIELDS = (
    "event_id",     # uuid4 string, the natural key used for de-duplication
    "ts",           # ISO-8601 UTC timestamp, e.g. 2026-08-31T10:00:00.123Z
    "service",      # logical service name, e.g. "checkout-api"
    "host",         # instance/pod that produced the line
    "level",        # DEBUG | INFO | WARN | ERROR | FATAL (case varies in the wild)
    "message",      # free-text log message
    "latency_ms",   # request latency in milliseconds (may be missing)
    "status_code",  # HTTP status (may be missing)
    "trace_id",     # distributed-trace correlation id (may be missing)
)

# Canonical severity levels after normalisation in SILVER.
LEVELS = ("DEBUG", "INFO", "WARN", "ERROR", "FATAL")

# Producers are messy.  Real ones emit "WARNING", "warn", "Err", "CRITICAL"...
# Normalising this is textbook silver-layer work.
LEVEL_ALIASES = {
    "TRACE": "DEBUG",
    "DEBUG": "DEBUG",
    "INFO": "INFO",
    "INFORMATION": "INFO",
    "NOTICE": "INFO",
    "WARN": "WARN",
    "WARNING": "WARN",
    "ERR": "ERROR",
    "ERROR": "ERROR",
    "CRITICAL": "FATAL",
    "FATAL": "FATAL",
    "EMERG": "FATAL",
}

# Columns the SILVER table exposes.  Downstream (GOLD, API, dashboard) code is
# allowed to depend on these; nothing downstream may depend on bronze internals.
SILVER_COLUMNS = (
    "event_id",
    "event_ts",      # TIMESTAMP (UTC)
    "event_date",    # DATE      -- partition/report key
    "event_hour",    # TINYINT   -- 0..23, the grain of our hourly fact table
    "service",
    "host",
    "level",
    "is_error",      # BOOLEAN   -- pre-computed so marts stay trivial
    "message",
    "latency_ms",
    "status_code",
    "trace_id",
    "source_type",   # 'stream' | 'batch' -- lineage: how did this row arrive?
    "source_file",   # which bronze file it came from -- lineage: where from?
    "ingested_at",   # when BRONZE captured it
)


@dataclass(frozen=True)
class ParsedEvent:
    """One successfully parsed raw event, plus the reason if it failed."""

    payload: dict[str, Any] | None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.payload is not None


# ----------------------------------------------------------------------------
# Parsing
# ----------------------------------------------------------------------------
# Batch log files are plain text, one event per line, in the common
# "<timestamp> <LEVEL> <service> <host> - <json payload>" shape produced by many
# log shippers.  We support both a bare JSON line and that prefixed form.
_PREFIXED_LINE = re.compile(
    r"^(?P<ts>\S+)\s+(?P<level>[A-Za-z]+)\s+(?P<service>\S+)\s+(?P<host>\S+)\s+-\s+(?P<json>\{.*\})\s*$"
)


def parse_raw_line(line: str) -> ParsedEvent:
    """
    Turn one raw text line into a dict, or explain why we could not.

    Note what this function deliberately does NOT do: it does not clean, coerce
    types, or drop anything.  Bronze keeps data "as received"; the only job here
    is to get from bytes on disk to a Python dict so we can store it columnar.
    """
    line = line.strip()
    if not line:
        return ParsedEvent(None, "empty line")

    # Fast path: the line is already a JSON object (this is what MQTT sends).
    if line.startswith("{"):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            return ParsedEvent(None, f"invalid json: {exc.msg}")
        if not isinstance(payload, dict):
            return ParsedEvent(None, "json payload is not an object")
        return ParsedEvent(payload)

    # Slow path: "<ts> <LEVEL> <service> <host> - {json}" from a rotated log file.
    match = _PREFIXED_LINE.match(line)
    if not match:
        return ParsedEvent(None, "line does not match any known log format")
    try:
        payload = json.loads(match.group("json"))
    except json.JSONDecodeError as exc:
        return ParsedEvent(None, f"invalid embedded json: {exc.msg}")
    # The text prefix is authoritative for these fields if the JSON omits them.
    payload.setdefault("ts", match.group("ts"))
    payload.setdefault("level", match.group("level"))
    payload.setdefault("service", match.group("service"))
    payload.setdefault("host", match.group("host"))
    return ParsedEvent(payload)


def format_log_line(event: dict[str, Any]) -> str:
    """
    Render an event the way a log shipper would write it into a daily `.log`
    file.  Used by the simulator to create realistic batch input.
    """
    body = {k: v for k, v in event.items() if k not in {"ts", "level", "service", "host"}}
    return (
        f"{event['ts']} {event['level']} {event['service']} {event['host']} - "
        f"{json.dumps(body, separators=(',', ':'))}"
    )


def utc_now_iso() -> str:
    """Current UTC time as an ISO-8601 string with a trailing 'Z'."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
