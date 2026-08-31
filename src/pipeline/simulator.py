"""
Data generator -- so the project produces something to look at from minute one.

It plays BOTH source roles:

    backfill : writes previous days' rotated log files into landing/batch,
               which is what the daily Airflow DAG consumes.
    stream   : publishes live events to MQTT at a steady rate, which is what the
               ingestor consumes.

It also injects a small amount of deliberately BROKEN data (~3%): missing keys,
unparseable timestamps, unknown log levels, negative latencies and lines that are
not JSON at all.  That is not sloppiness -- it is the point.  Without bad rows you
never see the quarantine zone, the rejected_events table or the quality checks do
anything, and those are the parts of a pipeline that matter in production.

Run it directly:
    python -m pipeline.simulator --mode backfill --days 3
    python -m pipeline.simulator --mode stream --rate 5
"""

from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import date, datetime, timedelta, timezone

from .config import get_settings
from .logging_utils import configure_logging
from .schemas import format_log_line
from .sources.log_files import batch_file_for_date

LOGGER = configure_logging("simulator")

SERVICES = ("checkout-api", "payment-worker", "inventory-svc", "search-api", "notification-svc")
HOSTS = ("node-1", "node-2", "node-3", "node-4")

# Weighted so the data looks like a real system: mostly INFO, a little noise.
LEVEL_WEIGHTS = (("DEBUG", 10), ("INFO", 62), ("WARN", 18), ("ERROR", 9), ("FATAL", 1))

MESSAGES = {
    "DEBUG": ["cache lookup", "config reloaded", "span started"],
    "INFO": ["request completed", "order accepted", "index refreshed", "payment captured"],
    "WARN": ["slow downstream call", "retrying request", "connection pool near limit"],
    "ERROR": ["upstream timeout", "database connection refused", "payment gateway 502"],
    "FATAL": ["out of memory", "unrecoverable disk error"],
}


def _weighted_level() -> str:
    levels, weights = zip(*LEVEL_WEIGHTS)
    return random.choices(levels, weights=weights, k=1)[0]


def make_event(when: datetime, corrupt_probability: float = 0.03) -> dict:
    """Build one log event, occasionally corrupting it on purpose."""
    level = _weighted_level()
    service = random.choice(SERVICES)

    # Errors are slow: latency correlates with severity, as it does in real life.
    base_latency = {"DEBUG": 8, "INFO": 45, "WARN": 180, "ERROR": 900, "FATAL": 1500}[level]
    event = {
        "event_id": str(uuid.uuid4()),
        "ts": when.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "service": service,
        "host": random.choice(HOSTS),
        "level": random.choice([level, level.lower(), level.title()]),  # messy casing, on purpose
        "message": random.choice(MESSAGES[level]),
        "latency_ms": max(1, int(random.gauss(base_latency, base_latency / 3))),
        "status_code": 200 if level in ("DEBUG", "INFO") else random.choice([400, 429, 500, 502, 503]),
        "trace_id": uuid.uuid4().hex[:16],
    }

    if random.random() < corrupt_probability:
        # Each corruption exercises a different rejection path in silver.py.
        match random.randint(0, 4):
            case 0:
                del event["event_id"]              # -> 'missing event_id'
            case 1:
                event["ts"] = "not-a-timestamp"    # -> 'unparseable timestamp'
            case 2:
                event["level"] = "LOUD"            # -> 'unknown log level'
            case 3:
                event["latency_ms"] = -5           # -> 'negative latency'
            case 4:
                event["service"] = ""              # -> 'missing service'
    return event


# ----------------------------------------------------------------------------
# BATCH mode: write previous days' rotated log files
# ----------------------------------------------------------------------------
def write_backfill_files(days: int, events_per_day: int) -> list[str]:
    """
    Create one `.log` file per past day in landing/batch.

    Timestamps are spread across the whole 24 hours so the hourly gold fact table
    has something to show, and the day's file is written in one go -- exactly how
    a rotated log file appears at midnight.
    """
    settings = get_settings()
    settings.ensure_directories()
    written: list[str] = []

    for offset in range(1, days + 1):
        day = date.today() - timedelta(days=offset)
        path = batch_file_for_date(day)
        if path.exists():
            LOGGER.info("backfill: %s already exists, skipping", path.name)
            continue

        midnight = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        lines: list[str] = []
        for _ in range(events_per_day):
            when = midnight + timedelta(seconds=random.randint(0, 86_399))
            event = make_event(when)
            # ~1 line in 200 is not even valid JSON -- e.g. a truncated write or a
            # stack trace fragment.  Bronze quarantines these.
            if random.random() < 0.005:
                lines.append("!! truncated line from a crashed process")
                continue
            lines.append(format_log_line(_with_required_prefix_fields(event)))

        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        written.append(path.name)
        LOGGER.info("backfill: wrote %s (%d lines)", path.name, len(lines))

    return written


def _with_required_prefix_fields(event: dict) -> dict:
    """format_log_line needs ts/level/service/host; supply placeholders if corrupted away."""
    filled = dict(event)
    filled.setdefault("ts", "not-a-timestamp")
    filled.setdefault("level", "INFO")
    filled.setdefault("service", "unknown")
    filled.setdefault("host", "unknown")
    if filled["service"] == "":
        filled["service"] = "-"  # keep the text format parseable; silver still rejects it
    return filled


# ----------------------------------------------------------------------------
# STREAM mode: publish live events to MQTT
# ----------------------------------------------------------------------------
def publish_stream(rate_per_second: float, duration_seconds: float | None = None) -> None:
    """Publish events to MQTT forever (or for `duration_seconds`)."""
    import paho.mqtt.client as mqtt  # imported lazily so batch mode needs no broker

    settings = get_settings()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"simulator-{uuid.uuid4().hex[:6]}")
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    for attempt in range(1, 31):
        try:
            client.connect(settings.mqtt_host, settings.mqtt_port, keepalive=60)
            break
        except OSError as exc:
            LOGGER.warning("waiting for broker (%d/30): %s", attempt, exc)
            time.sleep(min(2 * attempt, 10))
    else:
        raise RuntimeError("simulator could not reach the MQTT broker")

    client.loop_start()
    LOGGER.info("publishing ~%.1f events/s to %s:%s", rate_per_second,
                settings.mqtt_host, settings.mqtt_port)

    interval = 1.0 / max(rate_per_second, 0.01)
    started = time.monotonic()
    published = 0
    try:
        while duration_seconds is None or (time.monotonic() - started) < duration_seconds:
            event = make_event(datetime.now(timezone.utc))
            # Topic hierarchy logs/<service>/<host> lets a consumer subscribe to a
            # slice (logs/checkout-api/#) instead of the whole firehose.
            topic = f"logs/{event.get('service') or 'unknown'}/{event.get('host', 'unknown')}"
            client.publish(topic, json.dumps(event, separators=(",", ":")), qos=settings.mqtt_qos)
            published += 1
            if published % 100 == 0:
                LOGGER.info("published %d events", published)
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()
        LOGGER.info("simulator stopped after %d events", published)


def main() -> None:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Generate demo log data for the pipeline")
    parser.add_argument("--mode", choices=("backfill", "stream", "both"), default="both")
    parser.add_argument("--days", type=int, default=settings.sim_backfill_days,
                        help="how many previous days of batch log files to create")
    parser.add_argument("--events-per-day", type=int, default=settings.sim_events_per_backfill_day)
    parser.add_argument("--rate", type=float, default=settings.sim_events_per_second,
                        help="events per second for stream mode")
    parser.add_argument("--seconds", type=float, default=None,
                        help="stop streaming after N seconds (default: run forever)")
    args = parser.parse_args()

    if args.mode in ("backfill", "both"):
        write_backfill_files(args.days, args.events_per_day)
    if args.mode in ("stream", "both"):
        publish_stream(args.rate, args.seconds)


if __name__ == "__main__":
    main()
