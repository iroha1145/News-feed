from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

import aiosqlite

from app.config import settings


WorkerHealthStatus = Literal["ok", "unavailable"]


@dataclass(frozen=True)
class WorkerHeartbeatSelection:
    """The serving worker plus diagnostics from unusable clock-skewed rows."""

    health_status: WorkerHealthStatus
    warning: str | None
    worker_id: str | None = None
    heartbeat_at: str | None = None
    worker_status: str | None = None
    diagnostics: tuple[str, ...] = ()


def evaluate_worker_heartbeat(
    heartbeat_at: str | None,
    worker_status: str | None,
    *,
    now: datetime | None = None,
) -> tuple[WorkerHealthStatus, str | None]:
    """Return one shared health decision for HTTP and container probes."""

    if not heartbeat_at:
        return "unavailable", "analysis_worker_heartbeat_missing"
    normalized_status = str(worker_status or "").lower()
    if normalized_status == "failed":
        return "unavailable", "analysis_worker_failed"
    if normalized_status not in {"idle", "working"}:
        return "unavailable", "analysis_worker_status_invalid"
    try:
        parsed = datetime.fromisoformat(str(heartbeat_at).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return "unavailable", "analysis_worker_heartbeat_invalid"
    if parsed.tzinfo is None:
        return "unavailable", "analysis_worker_heartbeat_invalid"
    checked_at = now or datetime.now(timezone.utc)
    if parsed.astimezone(timezone.utc) > (
        checked_at.astimezone(timezone.utc) + timedelta(seconds=5)
    ):
        return "unavailable", "analysis_worker_heartbeat_future"
    maximum_age = timedelta(
        seconds=max(30, settings.analysis_worker_poll_seconds * 3)
    )
    if checked_at.astimezone(timezone.utc) - parsed.astimezone(timezone.utc) > maximum_age:
        return "unavailable", "analysis_worker_heartbeat_stale"
    return "ok", None


def _parsed_heartbeat(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


async def select_worker_heartbeat(
    db: aiosqlite.Connection,
    *,
    now: datetime | None = None,
) -> WorkerHeartbeatSelection:
    """Select a live worker without letting a future heartbeat hide it.

    Clock-skewed rows remain in the table and are returned as diagnostics. If
    no row is currently serving, a real-time row is preferred for the failure
    reason; a future-only table still fails closed.
    """

    checked_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    future_boundary = checked_at + timedelta(seconds=5)
    async with db.execute(
        """SELECT worker_id,heartbeat_at,status FROM analysis_worker_state
           ORDER BY heartbeat_at DESC"""
    ) as cursor:
        rows = [tuple(row) for row in await cursor.fetchall()]

    if not rows:
        status, warning = evaluate_worker_heartbeat(None, None, now=checked_at)
        return WorkerHeartbeatSelection(status, warning)

    evaluated = []
    future_seen = False
    for worker_id, heartbeat_at, worker_status in rows:
        heartbeat_text = str(heartbeat_at) if heartbeat_at is not None else None
        normalized_worker_status = (
            str(worker_status) if worker_status is not None else None
        )
        parsed = _parsed_heartbeat(heartbeat_text)
        if parsed is not None and parsed > future_boundary:
            future_seen = True
        health_status, warning = evaluate_worker_heartbeat(
            heartbeat_text,
            normalized_worker_status,
            now=checked_at,
        )
        evaluated.append(
            (
                str(worker_id),
                heartbeat_text,
                normalized_worker_status,
                health_status,
                warning,
                parsed,
            )
        )

    diagnostics = (("analysis_worker_heartbeat_future",) if future_seen else ())
    healthy = [item for item in evaluated if item[3] == "ok"]
    if healthy:
        selected = max(
            healthy,
            key=lambda item: item[5] or datetime.min.replace(tzinfo=timezone.utc),
        )
        return WorkerHeartbeatSelection(
            health_status="ok",
            warning=None,
            worker_id=selected[0],
            heartbeat_at=selected[1],
            worker_status=selected[2],
            diagnostics=diagnostics,
        )

    nonfuture = [
        item
        for item in evaluated
        if item[5] is not None and item[5] <= future_boundary
    ]
    candidates = nonfuture or [item for item in evaluated if item[5] is not None]
    selected = (
        max(candidates, key=lambda item: item[5])
        if candidates
        else evaluated[0]
    )
    return WorkerHeartbeatSelection(
        health_status="unavailable",
        warning=selected[4],
        worker_id=selected[0],
        heartbeat_at=selected[1],
        worker_status=selected[2],
        diagnostics=diagnostics,
    )
