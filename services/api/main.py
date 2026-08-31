"""
FastAPI -- the SERVING layer of the platform.

It plays two distinct roles, and it is worth being clear about which is which:

  1. READ side (the main job).  It exposes the GOLD marts over HTTP so that any
     consumer -- the Streamlit dashboard, another service, a notebook, a Grafana
     panel -- reads the same numbers through one contract.

  2. WRITE side (`POST /ingest/event`).  An HTTP gateway onto the same MQTT topic
     the devices publish to, for producers that cannot speak MQTT.  Note what it
     does NOT do: it does not write to the warehouse.  It hands the event to the
     broker and returns, so ingestion stays a single, uniform path.

THE IMPORTANT DESIGN DECISION
-----------------------------
This API never opens the DuckDB warehouse file.  It reads the Parquet exports
published by the gold layer, through an in-memory DuckDB.  Consequences:

  * no lock contention -- the pipeline can be mid-write and the API still serves;
  * the API cannot corrupt the warehouse, because it has no handle on it;
  * swapping the storage engine later (Postgres, ClickHouse, S3+Iceberg) changes
    this one module, not the pipeline.

Run locally:  uvicorn services.api.main:app --reload
Docs:         http://localhost:8000/docs
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# Make the shared `pipeline` package importable when running from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pipeline.config import get_settings           # noqa: E402
from pipeline.logging_utils import configure_logging  # noqa: E402
from pipeline.schemas import LEVELS, utc_now_iso   # noqa: E402
from pipeline.sources.log_files import describe_landing_zone  # noqa: E402
from pipeline.warehouse import reader_connection   # noqa: E402

LOGGER = configure_logging("api")
SETTINGS = get_settings()

app = FastAPI(
    title="Medallion Log Analytics API",
    version="0.1.0",
    description=__doc__,
)


# ----------------------------------------------------------------------------
# Mart access helpers
# ----------------------------------------------------------------------------
def mart_path(name: str) -> Path:
    """Absolute path of a published gold mart."""
    return SETTINGS.gold_exports / f"{name}.parquet"


def query_mart(name: str, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    """
    Run SQL against a published mart and return plain dicts.

    `{mart}` in the SQL is replaced by a `read_parquet(...)` call, so the queries
    below read like ordinary SQL against a table.  Values are always passed as
    bound parameters -- never string-formatted into the SQL -- which is what keeps
    a query parameter from becoming a SQL injection.
    """
    path = mart_path(name)
    if not path.exists():
        # A missing mart means the pipeline has not published yet.  503 (not 500)
        # is the honest status: the service is fine, the data is not ready.
        raise HTTPException(
            status_code=503,
            detail=f"mart '{name}' has not been published yet -- run the pipeline first",
        )
    with reader_connection() as con:
        rows = con.execute(sql.replace("{mart}", f"read_parquet('{path}')"), params or []).fetchall()
        columns = [d[0] for d in con.description]
    return [dict(zip(columns, row)) for row in rows]


# ----------------------------------------------------------------------------
# Request/response models -- FastAPI turns these into validation + OpenAPI docs
# ----------------------------------------------------------------------------
class LogEventIn(BaseModel):
    """One log event submitted over HTTP instead of MQTT."""

    service: str = Field(..., min_length=2, max_length=64, examples=["checkout-api"])
    host: str = Field("unknown", max_length=64)
    level: str = Field(..., examples=["ERROR"])
    message: str = Field(..., max_length=2000)
    latency_ms: int | None = Field(None, ge=0)
    status_code: int | None = Field(None, ge=100, le=599)
    trace_id: str | None = None
    ts: str | None = Field(None, description="ISO-8601 UTC; defaults to now")


class IngestResult(BaseModel):
    accepted: bool
    event_id: str
    topic: str


# ----------------------------------------------------------------------------
# Operational endpoints
# ----------------------------------------------------------------------------
@app.get("/health", tags=["ops"])
def health() -> dict[str, Any]:
    """Liveness probe.  Cheap on purpose: it must not touch the data."""
    return {"status": "ok", "time": utc_now_iso()}


@app.get("/pipeline/status", tags=["ops"])
def pipeline_status() -> dict[str, Any]:
    """
    Everything you need to answer "is the pipeline healthy right now?".

    Combines three signals: what is waiting in the landing zone, how fresh the
    published marts are, and the latest data-quality results.
    """
    marts = []
    for name in ("mart_service_health_hourly", "mart_error_hotspots_daily", "mart_pipeline_health"):
        path = mart_path(name)
        marts.append(
            {
                "mart": name,
                "published": path.exists(),
                "published_at": (
                    datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
                    if path.exists() else None
                ),
                "bytes": path.stat().st_size if path.exists() else 0,
            }
        )

    quality: list[dict[str, Any]] = []
    if mart_path("mart_pipeline_health").exists():
        quality = query_mart("mart_pipeline_health", "SELECT * FROM {mart}")

    return {
        "landing_zone": describe_landing_zone(),
        "marts": marts,
        "quality_checks": quality,
        "warehouse_path": str(SETTINGS.warehouse_path),
    }


# ----------------------------------------------------------------------------
# Analytics endpoints -- one thin wrapper per business question
# ----------------------------------------------------------------------------
@app.get("/api/overview", tags=["analytics"])
def overview(days: int = Query(7, ge=1, le=90)) -> dict[str, Any]:
    """Headline KPIs across the whole platform for the last N days."""
    rows = query_mart(
        "mart_service_health_hourly",
        """
        SELECT
            count(DISTINCT service)                          AS services,
            coalesce(sum(events), 0)                         AS events,
            coalesce(sum(errors), 0)                         AS errors,
            round(100.0 * sum(errors) / nullif(sum(events), 0), 2) AS error_rate_pct,
            round(avg(avg_latency_ms), 1)                    AS avg_latency_ms,
            round(max(p95_latency_ms), 1)                    AS worst_p95_latency_ms,
            min(event_date)                                  AS from_date,
            max(event_date)                                  AS to_date
        FROM {mart}
        WHERE event_date >= current_date - CAST(? AS INTEGER)
        """,
        [days],
    )
    return rows[0] if rows else {}


@app.get("/api/services", tags=["analytics"])
def services() -> list[dict[str, Any]]:
    """The service dimension: every service the platform has ever seen."""
    return query_mart(
        "dim_service",
        "SELECT service_name, host_count, lifetime_events, first_seen_at, last_seen_at "
        "FROM {mart} ORDER BY lifetime_events DESC",
    )


@app.get("/api/service-health", tags=["analytics"])
def service_health(
    service: str | None = Query(None, description="filter to one service"),
    hours: int = Query(48, ge=1, le=24 * 30),
) -> list[dict[str, Any]]:
    """The hourly time series behind the dashboard's main chart."""
    return query_mart(
        "mart_service_health_hourly",
        """
        SELECT event_hour_ts, service, events, errors, warnings,
               error_rate_pct, avg_latency_ms, p95_latency_ms, hosts
        FROM {mart}
        WHERE event_hour_ts >= now() - CAST(? AS INTEGER) * INTERVAL 1 HOUR
          AND (? IS NULL OR service = ?)
        ORDER BY event_hour_ts, service
        """,
        [hours, service, service],
    )


@app.get("/api/error-hotspots", tags=["analytics"])
def error_hotspots(days: int = Query(7, ge=1, le=90)) -> list[dict[str, Any]]:
    """Which service broke, on which day, and what the headline message was."""
    return query_mart(
        "mart_error_hotspots_daily",
        """
        SELECT event_date, service, error_events, affected_hosts,
               fatal_events, server_error_responses, top_error_message
        FROM {mart}
        WHERE event_date >= current_date - CAST(? AS INTEGER)
        ORDER BY event_date DESC, error_events DESC
        """,
        [days],
    )


# ----------------------------------------------------------------------------
# Ingestion gateway -- HTTP in, MQTT out
# ----------------------------------------------------------------------------
@app.post("/ingest/event", response_model=IngestResult, tags=["ingest"], status_code=202)
def ingest_event(event: LogEventIn) -> IngestResult:
    """
    Accept one event over HTTP and publish it to MQTT.

    202 Accepted, not 200 OK: the event has been handed to the broker, but it has
    not been processed yet.  Returning 200 would imply it is queryable, and it is
    not -- it becomes queryable a few minutes later, once the pipeline runs.
    """
    import paho.mqtt.publish as publish  # lightweight one-shot publish helper

    if event.level.upper() not in LEVELS:
        raise HTTPException(422, f"level must be one of {LEVELS}")

    payload = event.model_dump()
    payload["event_id"] = str(uuid.uuid4())
    payload["ts"] = event.ts or utc_now_iso()
    payload["level"] = event.level.upper()

    topic = f"logs/{event.service}/{event.host}"
    try:
        publish.single(
            topic,
            json.dumps(payload, separators=(",", ":")),
            qos=SETTINGS.mqtt_qos,
            hostname=SETTINGS.mqtt_host,
            port=SETTINGS.mqtt_port,
        )
    except OSError as exc:
        # The broker being down is an upstream problem, hence 502 rather than 500.
        raise HTTPException(502, f"could not reach MQTT broker: {exc}") from exc

    return IngestResult(accepted=True, event_id=payload["event_id"], topic=topic)


@app.exception_handler(Exception)
def unhandled_exception_handler(request, exc: Exception) -> JSONResponse:
    """Log the stack trace, return a body that leaks nothing about internals."""
    LOGGER.exception("unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(status_code=500, content={"detail": "internal server error"})


if __name__ == "__main__":  # `python services/api/main.py`
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("API_PORT", 8000)))
