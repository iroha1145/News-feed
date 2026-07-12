from __future__ import annotations

import asyncio
from app.integrations.option_pro.contract import CONTRACT_PATH, generated_bytes
from app.models.database import get_db
from app.services.worker_health import evaluate_worker_heartbeat


async def check() -> int:
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
        async with db.execute("PRAGMA quick_check") as cursor:
            row = await cursor.fetchone()
            if row is None or row[0] != "ok":
                return 1
    finally:
        await db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(check()))
