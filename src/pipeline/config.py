"""
Central configuration for the whole medallion pipeline.

WHY A SINGLE CONFIG MODULE?
---------------------------
Every component of this project (Airflow tasks, the MQTT ingestor, the FastAPI
service and the Streamlit dashboard) runs in its *own* Docker container, but they
all read and write the *same* files on a shared Docker volume.  If each component
hard-coded its own paths they would drift apart the first time somebody renamed a
folder.  So: one module owns every path and every tunable, and everything else
imports from here.

All values can be overridden with environment variables (see `.env.example`),
which is how the docker-compose file wires the containers together.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str) -> str:
    """Read an environment variable, falling back to a sensible local default."""
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


@dataclass(frozen=True)
class Settings:
    """Immutable settings object.  Build it once with `get_settings()`."""

    # ------------------------------------------------------------------
    # STORAGE LAYOUT
    # ------------------------------------------------------------------
    # DATA_ROOT is the single shared volume.  Inside the containers it is /data;
    # when you run things directly on your laptop it defaults to ./data.
    data_root: Path = field(default_factory=lambda: Path(_env("DATA_ROOT", "./data")))

    # ------------------------------------------------------------------
    # MQTT (the *streaming* source: live log events from services/devices)
    # ------------------------------------------------------------------
    mqtt_host: str = field(default_factory=lambda: _env("MQTT_HOST", "localhost"))
    mqtt_port: int = field(default_factory=lambda: _env_int("MQTT_PORT", 1883))
    # '#' is the MQTT multi-level wildcard: subscribe to logs/<service>/<host>.
    mqtt_topic: str = field(default_factory=lambda: _env("MQTT_TOPIC", "logs/#"))
    mqtt_client_id: str = field(default_factory=lambda: _env("MQTT_CLIENT_ID", "bronze-ingestor"))
    # QoS 1 = "at least once".  Duplicates are possible, which is exactly why the
    # SILVER layer de-duplicates on event_id.  This is a real streaming trade-off,
    # not an accident: at-least-once delivery + idempotent downstream processing.
    mqtt_qos: int = field(default_factory=lambda: _env_int("MQTT_QOS", 1))

    # How many events the stream ingestor buffers before flushing one file, and
    # the maximum time it will wait before flushing a partial buffer.  This is the
    # classic "micro-batch" knob: bigger batches = fewer, larger files (good for
    # analytics) but higher end-to-end latency.
    stream_batch_size: int = field(default_factory=lambda: _env_int("STREAM_BATCH_SIZE", 200))
    stream_flush_seconds: int = field(default_factory=lambda: _env_int("STREAM_FLUSH_SECONDS", 30))

    # ------------------------------------------------------------------
    # SIMULATOR (generates the demo data so the project runs out of the box)
    # ------------------------------------------------------------------
    sim_events_per_second: float = field(
        default_factory=lambda: float(_env("SIM_EVENTS_PER_SECOND", "5"))
    )
    sim_backfill_days: int = field(default_factory=lambda: _env_int("SIM_BACKFILL_DAYS", 3))
    sim_events_per_backfill_day: int = field(
        default_factory=lambda: _env_int("SIM_EVENTS_PER_BACKFILL_DAY", 5000)
    )

    # ------------------------------------------------------------------
    # DERIVED PATHS -- the physical shape of the medallion architecture
    # ------------------------------------------------------------------
    @property
    def lake_root(self) -> Path:
        """The DATA LAKE: cheap object-ish storage holding files, not tables."""
        return self.data_root / "lake"

    @property
    def landing_stream(self) -> Path:
        """Raw JSONL micro-batches written by the MQTT ingestor (streaming source)."""
        return self.lake_root / "landing" / "stream"

    @property
    def landing_batch(self) -> Path:
        """Raw `.log` text files for previous days (batch source)."""
        return self.lake_root / "landing" / "batch"

    @property
    def bronze_root(self) -> Path:
        """BRONZE: raw-but-columnar Parquet, partitioned by event_date/hour."""
        return self.lake_root / "bronze"

    @property
    def quarantine_root(self) -> Path:
        """Rows that failed parsing/validation.  Never silently dropped."""
        return self.lake_root / "quarantine"

    @property
    def gold_exports(self) -> Path:
        """GOLD marts exported as Parquet so readers never touch the warehouse file."""
        return self.lake_root / "gold_exports"

    @property
    def archive_root(self) -> Path:
        """Landing files that have already been promoted to bronze."""
        return self.lake_root / "archive"

    @property
    def warehouse_path(self) -> Path:
        """
        The DATA WAREHOUSE file itself (DuckDB).

        DuckDB is an embedded, columnar OLAP database -- think "SQLite for
        analytics".  It holds the SILVER (cleansed, normalised) and GOLD
        (denormalised, consumption-ready) layers as real SQL tables.
        """
        return Path(_env("WAREHOUSE_PATH", str(self.data_root / "warehouse" / "medallion.duckdb")))

    @property
    def warehouse_lock_path(self) -> Path:
        """
        Cross-process lock file guarding warehouse writes.

        DuckDB allows exactly ONE read-write process at a time.  Two Airflow tasks
        running in parallel would otherwise crash with "Could not set lock on
        file".  We serialise writers with a plain file lock -- see warehouse.py.
        """
        return self.warehouse_path.with_suffix(".lock")

    def ensure_directories(self) -> None:
        """Create every directory the pipeline expects.  Safe to call repeatedly."""
        for path in (
            self.landing_stream,
            self.landing_batch,
            self.bronze_root,
            self.quarantine_root,
            self.gold_exports,
            self.archive_root,
            self.warehouse_path.parent,
        ):
            path.mkdir(parents=True, exist_ok=True)


_SETTINGS: Settings | None = None


def get_settings() -> Settings:
    """Return the process-wide Settings singleton (built from the environment)."""
    global _SETTINGS
    if _SETTINGS is None:
        _SETTINGS = Settings()
    return _SETTINGS


def reset_settings() -> Settings:
    """
    Rebuild the settings from the current environment.

    Only the tests need this: they point DATA_ROOT at a temp directory so a test
    run can never touch your real lake or warehouse.
    """
    global _SETTINGS
    _SETTINGS = Settings()
    return _SETTINGS
