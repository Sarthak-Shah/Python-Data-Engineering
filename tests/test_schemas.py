"""Unit tests for the data contract -- the cheapest tests in the project."""

from __future__ import annotations

from pipeline.schemas import LEVEL_ALIASES, format_log_line, parse_raw_line


def test_parses_bare_json_line():
    """MQTT publishes bare JSON; that is the fast path in parse_raw_line."""
    parsed = parse_raw_line('{"event_id":"abc","service":"checkout-api"}')
    assert parsed.ok
    assert parsed.payload["event_id"] == "abc"


def test_parses_prefixed_log_file_line():
    """Rotated log files use '<ts> <LEVEL> <service> <host> - {json}'."""
    line = "2026-08-30T10:00:00Z WARN checkout-api node-1 - {\"event_id\":\"x\",\"message\":\"slow\"}"
    parsed = parse_raw_line(line)
    assert parsed.ok
    # Fields from the text prefix fill in what the embedded JSON omits.
    assert parsed.payload["service"] == "checkout-api"
    assert parsed.payload["level"] == "WARN"
    assert parsed.payload["message"] == "slow"


def test_round_trips_through_the_log_file_format():
    event = {
        "event_id": "e1", "ts": "2026-08-30T10:00:00Z", "service": "search-api",
        "host": "node-2", "level": "ERROR", "message": "boom", "latency_ms": 900,
    }
    parsed = parse_raw_line(format_log_line(event))
    assert parsed.ok
    assert parsed.payload == event


def test_rejects_garbage_with_a_reason():
    """Bad input must produce an explanation, never a silent None."""
    for bad in ("", "not json at all", "{broken"):
        parsed = parse_raw_line(bad)
        assert not parsed.ok
        assert parsed.error


def test_level_aliases_cover_the_common_producer_spellings():
    for spelling in ("WARNING", "warn", "Err", "CRITICAL"):
        assert spelling.upper() in LEVEL_ALIASES
