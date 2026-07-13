from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Callable
from pathlib import Path

import aiosqlite

from app.config import settings
from app.integrations.option_pro.contract import CONTRACT_PATH, generated_bytes
from app.models.database import get_db
from app.services.worker_health import evaluate_worker_heartbeat


QUICK_CHECK_MARKER = Path("/tmp/macrolens-analysis-worker-quick-check.ok")


def _quick_check_marker_is_fresh(
    marker_path: Path,
    interval_seconds: int,
    *,
    now: float,
) -> bool:
    try:
        checked_at = marker_path.stat().st_mtime
    except OSError:
        return False
    age = now - checked_at
    return 0 <= age < interval_seconds


def _record_quick_check(marker_path: Path, *, checked_at: float) -> bool:
    temporary_path = marker_path.with_name(
        f".{marker_path.name}.{os.getpid()}.tmp"
    )
    try:
        temporary_path.write_text("ok\n", encoding="ascii")
        os.utime(temporary_path, (checked_at, checked_at))
        temporary_path.replace(marker_path)
    except OSError:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
        return False
    return True


async def _periodic_quick_check(
    db: aiosqlite.Connection,
    *,
    marker_path: Path,
    interval_seconds: int,
    clock: Callable[[], float] = time.time,
) -> bool:
    if _quick_check_marker_is_fresh(
        marker_path,
        interval_seconds,
        now=clock(),
    ):
        return True
    async with db.execute("PRAGMA quick_check") as cursor:
        row = await cursor.fetchone()
    if row is None or row[0] != "ok":
        return False
    return _record_quick_check(marker_path, checked_at=clock())


async def check(*, quick_check_marker: Path = QUICK_CHECK_MARKER) -> int:
    if not CONTRACT_PATH.is_file() or CONTRACT_PATH.read_bytes() != generated_bytes():
        return 1
    db = await get_db()
    try:
        async with db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='analysis_jobs'"
        ) as cursor:
            if await cursor.fetchone() is None:
                return 1
        async with db.execute(
            "SELECT heartbeat_at,status FROM analysis_worker_state ORDER BY heartbeat_at DESC LIMIT 1"
        ) as cursor:
            heartbeat = await cursor.fetchone()
        worker_status, _ = evaluate_worker_heartbeat(
            heartbeat[0] if heartbeat else None,
            heartbeat[1] if heartbeat else None,
        )
        if worker_status != "ok":
            return 1
        async with db.execute(
            """SELECT COUNT(*) FROM analysis_jobs
               WHERE lease_owner IS NOT NULL AND lease_expires_at IS NULL"""
        ) as cursor:
            if int((await cursor.fetchone())[0]) != 0:
                return 1
        async with db.execute(
            """SELECT COUNT(*) FROM calendar_analysis_jobs
               WHERE lease_owner IS NOT NULL AND lease_expires_at IS NULL"""
        ) as cursor:
            if int((await cursor.fetchone())[0]) != 0:
                return 1
        if not await _periodic_quick_check(
            db,
            marker_path=quick_check_marker,
            interval_seconds=settings.analysis_worker_quick_check_interval_seconds,
        ):
            return 1
    finally:
        await db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(check()))
