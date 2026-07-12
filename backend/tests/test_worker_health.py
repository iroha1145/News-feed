from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.services.worker_health import evaluate_worker_heartbeat


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
