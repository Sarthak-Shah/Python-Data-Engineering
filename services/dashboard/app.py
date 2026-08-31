"""
Streamlit -- the BI / consumption layer.

This is the right-hand side of the medallion diagram: the "BI" box that gold
exists to feed.  It is deliberately dumb.  Every number on this page comes
pre-computed out of a gold mart; the dashboard does filtering and drawing, and
nothing else.

Why that matters: business logic that lives in a dashboard is logic no other
consumer can reuse and no test can cover.  If you find yourself writing a
non-trivial calculation here, it belongs in `layers/gold.py`.

The sidebar lets you switch between two ways of reading the same gold layer:

    "Gold marts (direct)" -- read the Parquet exports with an in-memory DuckDB.
                             This is how a BI tool attaches to a warehouse.
    "FastAPI"             -- call the HTTP API.  Slower, but it is the contract
                             every other consumer uses, and it proves the API and
                             the dashboard genuinely agree on the numbers.

Run locally:  streamlit run services/dashboard/app.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pipeline.config import get_settings      # noqa: E402
from pipeline.warehouse import reader_connection  # noqa: E402

SETTINGS = get_settings()
# Inside docker-compose the API is reachable by service name; override for local runs.
API_BASE = os.environ.get("API_BASE", "http://api:8000")

st.set_page_config(page_title="Medallion Log Analytics", page_icon="📊", layout="wide")


# ----------------------------------------------------------------------------
# Data access
# ----------------------------------------------------------------------------
# `ttl=60` means a repeated query within a minute is served from cache.  Without
# it, every widget interaction re-reads Parquet -- fine at this size, painful at
# scale.  Caching at the *data access* boundary is the standard Streamlit fix.
@st.cache_data(ttl=60, show_spinner=False)
def read_mart_direct(name: str) -> pd.DataFrame:
    """Read a published gold mart into a DataFrame via in-memory DuckDB."""
    path = SETTINGS.gold_exports / f"{name}.parquet"
    if not path.exists():
        return pd.DataFrame()
    with reader_connection() as con:
        return con.execute(f"SELECT * FROM read_parquet('{path}')").df()


@st.cache_data(ttl=60, show_spinner=False)
def read_from_api(endpoint: str, **params) -> pd.DataFrame:
    """Fetch a JSON list from the FastAPI service and frame it."""
    response = requests.get(f"{API_BASE}{endpoint}", params=params, timeout=15)
    response.raise_for_status()
    payload = response.json()
    return pd.DataFrame(payload if isinstance(payload, list) else [payload])


def load_health(source: str) -> pd.DataFrame:
    df = (
        read_mart_direct("mart_service_health_hourly")
        if source == "Gold marts (direct)"
        else read_from_api("/api/service-health", hours=24 * 30)
    )
    if not df.empty:
        df["event_hour_ts"] = pd.to_datetime(df["event_hour_ts"])
    return df


def load_hotspots(source: str) -> pd.DataFrame:
    return (
        read_mart_direct("mart_error_hotspots_daily")
        if source == "Gold marts (direct)"
        else read_from_api("/api/error-hotspots", days=30)
    )


# ----------------------------------------------------------------------------
# Sidebar: source switch + filters
# ----------------------------------------------------------------------------
st.sidebar.title("⚙️ Controls")
source = st.sidebar.radio(
    "Read gold layer via",
    ("Gold marts (direct)", "FastAPI"),
    help="Both paths read the same gold layer -- the numbers must match.",
)
if st.sidebar.button("🔄 Refresh now"):
    st.cache_data.clear()
    st.rerun()

health = load_health(source)

st.title("📊 Service Log Analytics")
st.caption(
    "Gold-layer marts built from MQTT streaming events and daily batch log files, "
    "orchestrated by Airflow through bronze → silver → gold."
)

if health.empty:
    st.warning(
        "No gold marts published yet.\n\n"
        "Run the pipeline first: `docker compose run --rm pipeline python -m pipeline.demo --generate` "
        "or trigger the **medallion_batch_daily** DAG in Airflow."
    )
    st.stop()

all_services = sorted(health["service"].unique())
selected_services = st.sidebar.multiselect("Services", all_services, default=all_services)

min_day, max_day = health["event_hour_ts"].min().date(), health["event_hour_ts"].max().date()
date_range = st.sidebar.date_input(
    "Date range", value=(min_day, max_day), min_value=min_day, max_value=max_day
)
# Streamlit returns a 1-tuple while the user is still picking the second date.
start_day, end_day = (date_range if isinstance(date_range, tuple) and len(date_range) == 2
                      else (min_day, max_day))

view = health[
    health["service"].isin(selected_services)
    & (health["event_hour_ts"].dt.date >= start_day)
    & (health["event_hour_ts"].dt.date <= end_day)
]

# ----------------------------------------------------------------------------
# KPI row -- the numbers somebody glances at before reading anything else
# ----------------------------------------------------------------------------
events = int(view["events"].sum())
errors = int(view["errors"].sum())
error_rate = (100.0 * errors / events) if events else 0.0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Events", f"{events:,}")
c2.metric("Errors", f"{errors:,}")
c3.metric("Error rate", f"{error_rate:.2f}%")
c4.metric("Avg latency", f"{view['avg_latency_ms'].mean():.0f} ms" if not view.empty else "—")
c5.metric("Worst p95", f"{view['p95_latency_ms'].max():.0f} ms" if not view.empty else "—")

# ----------------------------------------------------------------------------
# Time series
# ----------------------------------------------------------------------------
st.subheader("Events per hour")
st.caption("Straight from `gold.mart_service_health_hourly` -- one row per service per hour.")
st.line_chart(
    view.pivot_table(index="event_hour_ts", columns="service", values="events", aggfunc="sum"),
    height=280,
)

left, right = st.columns(2)
with left:
    st.subheader("Error rate % per hour")
    st.line_chart(
        view.pivot_table(index="event_hour_ts", columns="service",
                         values="error_rate_pct", aggfunc="mean"),
        height=260,
    )
with right:
    st.subheader("p95 latency (ms) per hour")
    st.caption("p95, not the average: the tail is what users actually feel.")
    st.line_chart(
        view.pivot_table(index="event_hour_ts", columns="service",
                         values="p95_latency_ms", aggfunc="max"),
        height=260,
    )

# ----------------------------------------------------------------------------
# Per-service leaderboard
# ----------------------------------------------------------------------------
st.subheader("By service")
by_service = (
    view.groupby("service")
    .agg(events=("events", "sum"), errors=("errors", "sum"),
         warnings=("warnings", "sum"), p95_latency_ms=("p95_latency_ms", "max"))
    .assign(error_rate_pct=lambda d: (100 * d.errors / d.events).round(2))
    .sort_values("errors", ascending=False)
)
st.dataframe(by_service, width="stretch")

# ----------------------------------------------------------------------------
# Daily error hotspots
# ----------------------------------------------------------------------------
st.subheader("Error hotspots by day")
hotspots = load_hotspots(source)
if hotspots.empty:
    st.info("No errors recorded in the published marts.")
else:
    st.dataframe(
        hotspots[hotspots["service"].isin(selected_services)],
        width="stretch", hide_index=True,
    )

# ----------------------------------------------------------------------------
# Pipeline health -- "can I trust the numbers above?"
# ----------------------------------------------------------------------------
st.subheader("🩺 Pipeline health")
st.caption(
    "The latest run of every data-quality check, published as a gold mart like any "
    "other table. A failing `error` check means the pipeline stopped before publishing."
)
quality = read_mart_direct("mart_pipeline_health")
if quality.empty:
    st.info("No quality results yet.")
else:
    quality = quality.copy()
    quality["status"] = quality["passed"].map({True: "✅ pass", False: "❌ fail"})
    st.dataframe(
        quality[["status", "check_name", "severity", "observed", "threshold", "details", "checked_at"]],
        width="stretch", hide_index=True,
    )

st.sidebar.caption(f"Marts: `{SETTINGS.gold_exports}`")
