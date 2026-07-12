"""Compatibility entry points for the retired synchronous news analyzer.

All paid work is now represented by a durable analysis_jobs row and executed by
the dedicated Responses worker. Keeping these names avoids breaking older admin
imports while making it impossible to open a Chat Completions request here.
"""

from __future__ import annotations

from typing import Optional

import aiosqlite

from app.models.database import get_analysis_for_news
from app.services.analysis_jobs import create_or_get_job, enqueue_auto_jobs


async def analyze_news_item(news_item: dict, db: aiosqlite.Connection) -> Optional[dict]:
    result = await create_or_get_job(db, int(news_item["id"]), force=False, priority=100)
    if result.job["status"] in {"completed", "insufficient_context"}:
        return await get_analysis_for_news(db, int(news_item["id"]))
    return None


async def run_analysis_batch(batch_size: Optional[int] = None) -> int:
    return await enqueue_auto_jobs(batch_size)
