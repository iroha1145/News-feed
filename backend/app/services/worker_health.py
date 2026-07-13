from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Literal

from app.config import settings


WorkerHealthStatus = Literal["ok", "unavailable"]


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
