from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import aiosqlite

from app import worker_healthcheck as worker_healthcheck_module
from app.services.worker_health import (
    evaluate_worker_heartbeat,
    select_worker_heartbeat,
)
from app.worker_healthcheck import (
    _periodic_quick_check,
    _quick_check_marker_is_fresh,
)


NOW = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc)


def test_worker_health_reports_missing_heartbeat():
    assert evaluate_worker_heartbeat(None, None, now=NOW) == (
        "unavailable",
        "analysis_worker_heartbeat_missing",
    )


def test_worker_health_reports_failed_worker():
    assert evaluate_worker_heartbeat(NOW.isoformat(), "failed", now=NOW) == (
        "unavailable",
        "analysis_worker_failed",
    )


def test_worker_health_reports_stale_heartbeat():
    stale = (NOW - timedelta(minutes=5)).isoformat()
    assert evaluate_worker_heartbeat(stale, "idle", now=NOW) == (
        "unavailable",
        "analysis_worker_heartbeat_stale",
    )


def test_worker_health_accepts_recent_idle_heartbeat():
    recent = (NOW - timedelta(seconds=5)).isoformat()
    assert evaluate_worker_heartbeat(recent, "idle", now=NOW) == ("ok", None)


def test_worker_health_rejects_future_heartbeat():
    future = (NOW + timedelta(minutes=5)).isoformat()
    assert evaluate_worker_heartbeat(future, "idle", now=NOW) == (
        "unavailable",
        "analysis_worker_heartbeat_future",
    )


def test_worker_health_rejects_non_serving_status():
    assert evaluate_worker_heartbeat(NOW.isoformat(), "stopping", now=NOW) == (
        "unavailable",
        "analysis_worker_status_invalid",
    )


def test_worker_selection_prefers_live_worker_and_retains_future_diagnostic():
    async def scenario():
        db = await aiosqlite.connect(":memory:")
        try:
            await db.execute(
                """CREATE TABLE analysis_worker_state (
                   worker_id TEXT PRIMARY KEY, heartbeat_at TEXT, status TEXT
                )"""
            )
            await db.executemany(
                "INSERT INTO analysis_worker_state VALUES (?,?,?)",
                (
                    (
                        "clock-skewed-old-worker",
                        (NOW + timedelta(minutes=5)).isoformat(),
                        "idle",
                    ),
                    (
                        "current-live-worker",
                        (NOW - timedelta(seconds=2)).isoformat(),
                        "working",
                    ),
                ),
            )
            selection = await select_worker_heartbeat(db, now=NOW)
        finally:
            await db.close()

        assert selection.health_status == "ok"
        assert selection.warning is None
        assert selection.worker_id == "current-live-worker"
        assert selection.diagnostics == ("analysis_worker_heartbeat_future",)

    asyncio.run(scenario())


def test_worker_selection_with_only_future_heartbeats_is_unavailable():
    async def scenario():
        db = await aiosqlite.connect(":memory:")
        try:
            await db.execute(
                """CREATE TABLE analysis_worker_state (
                   worker_id TEXT PRIMARY KEY, heartbeat_at TEXT, status TEXT
                )"""
            )
            await db.execute(
                "INSERT INTO analysis_worker_state VALUES (?,?,?)",
                (
                    "clock-skewed-worker",
                    (NOW + timedelta(minutes=5)).isoformat(),
                    "idle",
                ),
            )
            selection = await select_worker_heartbeat(db, now=NOW)
        finally:
            await db.close()

        assert selection.health_status == "unavailable"
        assert selection.warning == "analysis_worker_heartbeat_future"
        assert selection.diagnostics == ("analysis_worker_heartbeat_future",)

    asyncio.run(scenario())


def test_worker_selection_prefers_live_worker_over_newer_failed_history():
    async def scenario():
        db = await aiosqlite.connect(":memory:")
        try:
            await db.execute(
                """CREATE TABLE analysis_worker_state (
                   worker_id TEXT PRIMARY KEY, heartbeat_at TEXT, status TEXT
                )"""
            )
            await db.executemany(
                "INSERT INTO analysis_worker_state VALUES (?,?,?)",
                (
                    (
                        "newer-failed-worker",
                        (NOW - timedelta(seconds=1)).isoformat(),
                        "failed",
                    ),
                    (
                        "slightly-older-live-worker",
                        (NOW - timedelta(seconds=3)).isoformat(),
                        "idle",
                    ),
                ),
            )
            selection = await select_worker_heartbeat(db, now=NOW)
        finally:
            await db.close()

        assert selection.health_status == "ok"
        assert selection.worker_id == "slightly-older-live-worker"
        assert selection.diagnostics == ()

    asyncio.run(scenario())


def test_worker_selection_reports_missing_heartbeat_for_empty_table():
    async def scenario():
        db = await aiosqlite.connect(":memory:")
        try:
            await db.execute(
                """CREATE TABLE analysis_worker_state (
                   worker_id TEXT PRIMARY KEY, heartbeat_at TEXT, status TEXT
                )"""
            )
            return await select_worker_heartbeat(db, now=NOW)
        finally:
            await db.close()

    selection = asyncio.run(scenario())
    assert selection.health_status == "unavailable"
    assert selection.warning == "analysis_worker_heartbeat_missing"
    assert selection.worker_id is None


def test_quick_check_runs_once_per_interval_and_refreshes_marker(tmp_path):
    async def scenario():
        marker = tmp_path / "quick-check.ok"
        statements: list[str] = []
        db = await aiosqlite.connect(":memory:")
        try:
            await db.set_trace_callback(statements.append)
            assert await _periodic_quick_check(
                db,
                marker_path=marker,
                interval_seconds=1800,
                clock=lambda: 100.0,
            )
            assert marker.is_file()
            assert await _periodic_quick_check(
                db,
                marker_path=marker,
                interval_seconds=1800,
                clock=lambda: 150.0,
            )
            assert await _periodic_quick_check(
                db,
                marker_path=marker,
                interval_seconds=1800,
                clock=lambda: 1900.0,
            )
        finally:
            await db.close()
        assert statements.count("PRAGMA quick_check") == 2

    asyncio.run(scenario())


def test_quick_check_rejects_future_marker_timestamp(tmp_path):
    marker = tmp_path / "quick-check.ok"
    marker.write_text("ok\n", encoding="ascii")
    os.utime(marker, (200.0, 200.0))

    assert not _quick_check_marker_is_fresh(
        marker,
        1800,
        now=100.0,
    )


def test_failed_quick_check_is_not_cached(tmp_path):
    class Cursor:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return None

        async def fetchone(self):
            return ("database disk image is malformed",)

    class Database:
        def execute(self, statement):
            assert statement == "PRAGMA quick_check"
            return Cursor()

    async def scenario():
        marker = tmp_path / "quick-check.ok"
        assert not await _periodic_quick_check(
            Database(),
            marker_path=marker,
            interval_seconds=1800,
            clock=lambda: 100.0,
        )
        assert not marker.exists()

    asyncio.run(scenario())


def test_cached_quick_check_still_validates_contract_heartbeat_and_leases(
    tmp_path,
    monkeypatch,
):
    queries: list[str] = []
    heartbeat_at = datetime.now(timezone.utc).isoformat()

    class Cursor:
        def __init__(self, row):
            self.row = row

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return None

        async def fetchone(self):
            return self.row

        async def fetchall(self):
            return [self.row]

    class Database:
        def execute(self, statement):
            queries.append(statement)
            if "sqlite_master" in statement:
                return Cursor((1,))
            if "analysis_worker_state" in statement:
                return Cursor(("health-worker", heartbeat_at, "idle"))
            if "COUNT(*)" in statement:
                return Cursor((0,))
            raise AssertionError(f"Unexpected database probe: {statement}")

        async def close(self):
            return None

    database = Database()

    async def get_db():
        return database

    marker = tmp_path / "quick-check.ok"
    marker.write_text("ok\n", encoding="ascii")
    monkeypatch.setattr(worker_healthcheck_module, "get_db", get_db)

    assert asyncio.run(
        worker_healthcheck_module.check(quick_check_marker=marker)
    ) == 0
    assert any("sqlite_master" in query for query in queries)
    assert any("analysis_worker_state" in query for query in queries)
    assert sum("COUNT(*)" in query for query in queries) == 2
    assert all("PRAGMA quick_check" not in query for query in queries)
