"""
End-to-end tests for the medallion flow.

These are the tests that would actually catch a regression: they run real files
through real DuckDB, and assert the properties an operator cares about --
idempotency, de-duplication, quarantining, and gold/silver reconciliation.
"""

from __future__ import annotations

import json

from pipeline.layers import bronze, gold, silver
from pipeline.quality import GOLD_CHECKS, SILVER_CHECKS, run_checks
from pipeline.warehouse import initialise_warehouse, writer_connection


def _write_stream_batch(settings, events: list[dict], name: str = "stream-test.jsonl") -> None:
    """Drop a landing file exactly like the MQTT ingestor would."""
    path = settings.landing_stream / name
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")


def _event(event_id: str, **overrides) -> dict:
    event = {
        "event_id": event_id,
        "ts": "2026-08-30T10:15:00.000Z",
        "service": "checkout-api",
        "host": "node-1",
        "level": "INFO",
        "message": "request completed",
        "latency_ms": 42,
        "status_code": 200,
        "trace_id": "t1",
    }
    event.update(overrides)
    return event


def test_full_flow_produces_gold_marts(isolated_data_root):
    settings = isolated_data_root
    initialise_warehouse()
    _write_stream_batch(settings, [_event(f"e{i}") for i in range(10)])

    assert bronze.promote_landing_to_bronze("stream")["rows_written"] == 10
    assert silver.load_bronze_to_silver()["rows_loaded"] == 10

    stats = gold.build_gold()
    assert stats["mart_service_health_hourly"] > 0
    # The mart must also be published as Parquet -- that file IS the API contract.
    assert (settings.gold_exports / "mart_service_health_hourly.parquet").exists()


def test_rerunning_changes_nothing(isolated_data_root):
    """Idempotency: the same input processed twice yields the same row count."""
    settings = isolated_data_root
    initialise_warehouse()
    _write_stream_batch(settings, [_event(f"e{i}") for i in range(5)])
    bronze.promote_landing_to_bronze("stream")
    silver.load_bronze_to_silver()

    second = silver.load_bronze_to_silver()          # nothing new has landed
    assert second["rows_loaded"] == 0

    forced = silver.load_bronze_to_silver(reprocess=True)  # deliberate replay
    assert forced["rows_loaded"] == 0                # ON CONFLICT DO NOTHING held

    with writer_connection() as con:
        assert con.execute("SELECT count(*) FROM silver.log_events").fetchone()[0] == 5


def test_duplicate_event_ids_are_collapsed(isolated_data_root):
    """MQTT QoS 1 redelivers; silver must keep exactly one copy."""
    settings = isolated_data_root
    initialise_warehouse()
    # Same event id three times, spread across two landing files.
    _write_stream_batch(settings, [_event("dup"), _event("dup")], "a.jsonl")
    _write_stream_batch(settings, [_event("dup")], "b.jsonl")

    bronze.promote_landing_to_bronze("stream")
    silver.load_bronze_to_silver()

    with writer_connection() as con:
        assert con.execute("SELECT count(*) FROM silver.log_events").fetchone()[0] == 1


def test_bad_rows_are_rejected_with_a_reason(isolated_data_root):
    """Every corruption the simulator injects must land in rejected_events."""
    settings = isolated_data_root
    initialise_warehouse()
    _write_stream_batch(
        settings,
        [
            _event("good"),
            _event("bad-ts", ts="not-a-timestamp"),
            _event("bad-level", level="LOUD"),
            _event("bad-latency", latency_ms=-1),
            _event("bad-service", service=""),
            {k: v for k, v in _event("no-id").items() if k != "event_id"},
        ],
    )
    bronze.promote_landing_to_bronze("stream")
    stats = silver.load_bronze_to_silver()

    assert stats["rows_loaded"] == 1
    assert stats["rows_rejected"] == 5
    with writer_connection() as con:
        reasons = {r[0].split(":")[0] for r in
                   con.execute("SELECT reason FROM silver.rejected_events").fetchall()}
    assert reasons == {
        "unparseable timestamp", "unknown log level", "negative latency",
        "missing service", "missing event_id",
    }


def test_unparseable_lines_are_quarantined_not_dropped(isolated_data_root):
    """A line that is not JSON must survive as a quarantine record."""
    settings = isolated_data_root
    initialise_warehouse()
    path = settings.landing_stream / "broken.jsonl"
    path.write_text(json.dumps(_event("ok")) + "\n!! truncated write\n", encoding="utf-8")

    stats = bronze.promote_landing_to_bronze("stream")
    assert stats["rows_written"] == 1
    assert stats["rows_unparseable"] == 1
    quarantined = list(settings.quarantine_root.glob("*.jsonl"))
    assert quarantined and "truncated write" in quarantined[0].read_text()


def test_quality_gates_pass_on_clean_data(isolated_data_root):
    settings = isolated_data_root
    initialise_warehouse()
    _write_stream_batch(settings, [_event(f"e{i}") for i in range(20)])
    bronze.promote_landing_to_bronze("stream")
    silver.load_bronze_to_silver()

    # Freshness is a `warn`, and the fixture data is deliberately backdated, so
    # the run must not raise -- warnings never block a publish.
    silver_summary = run_checks(SILVER_CHECKS)
    assert silver_summary["blocking_failures"] == []

    gold.build_gold()
    gold_summary = run_checks(GOLD_CHECKS)
    # Includes the silver/gold row-count reconciliation.
    assert gold_summary["blocking_failures"] == []


def test_batch_and_stream_converge_on_the_same_tables(isolated_data_root):
    """The whole point of the landing zone: one code path, two arrival patterns."""
    settings = isolated_data_root
    initialise_warehouse()

    _write_stream_batch(settings, [_event("live-1"), _event("live-2")])
    from pipeline.schemas import format_log_line

    (settings.landing_batch / "app-2026-08-29.log").write_text(
        "\n".join(
            format_log_line(_event(f"batch-{i}", ts="2026-08-29T08:00:00.000Z"))
            for i in range(3)
        ) + "\n",
        encoding="utf-8",
    )

    bronze.promote_landing_to_bronze("stream")
    bronze.promote_landing_to_bronze("batch")
    silver.load_bronze_to_silver()

    with writer_connection() as con:
        rows = dict(
            con.execute(
                "SELECT source_type, count(*) FROM silver.log_events GROUP BY 1"
            ).fetchall()
        )
    assert rows == {"stream": 2, "batch": 3}
