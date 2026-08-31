"""
Shared test fixtures.

The important one is `isolated_data_root`: every test gets its own DATA_ROOT in a
temp directory, so the suite can never read or corrupt your real lake/warehouse.
Being able to run the whole pipeline against a throwaway directory is a direct
benefit of keeping every path in `config.py` instead of hard-coding them.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline.config import reset_settings  # noqa: E402


@pytest.fixture()
def isolated_data_root(tmp_path, monkeypatch):
    """Point the whole pipeline at a temp directory for the duration of a test."""
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("WAREHOUSE_PATH", str(tmp_path / "warehouse" / "test.duckdb"))
    settings = reset_settings()
    settings.ensure_directories()
    yield settings
    reset_settings()  # restore whatever the environment says outside the test
