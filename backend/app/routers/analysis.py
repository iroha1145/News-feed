import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks, Depends

from app.deps.auth import require_admin
from app.models.database import (
    get_db,
    get_analyses,
    get_latest_analyses,
    get_analysis_stats,
    get_analysis_for_news,
    get_news_item_by_id,
)
from app.services.llm_analyzer import run_analysis_batch

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/analysis", tags=["analysis"])


@router.get("/by-news/{news_id}")
async def get_analysis_by_news_id(news_id: int):
    """Get analysis for a specific news item by its news_id."""
    db = await get_db()
    try:
        analysis = await get_analysis_for_news(db, news_id)
        if not analysis:
            raise HTTPException(status_code=404, detail="No analysis found for this news item")
        news = await get_news_item_by_id(db, news_id)
        return {"analysis": analysis, "news": news}
    finally:
        await db.close()


@router.get("")
async def list_analyses(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    classification: Optional[str] = Query(None, pattern="^(bullish|bearish|neutral)$"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    db = await get_db()
    try:
        total, items = await get_analyses(
            db,
            page=page,
            page_size=page_size,
            classification=classification,
            date_from=date_from,
            date_to=date_to,
        )
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "items": items,
        }
    finally:
        await db.close()


@router.get("/latest")
async def latest_analyses(
    limit: int = Query(10, ge=1, le=500),
    n: int = Query(None, ge=1, le=500),
):
    db = await get_db()
    try:
        actual_limit = n or limit
        items = await get_latest_analyses(db, limit=actual_limit)
        return items
    finally:
        await db.close()


@router.get("/stats")
async def analysis_stats():
    db = await get_db()
    try:
        stats = await get_analysis_stats(db)
        return stats
    finally:
        await db.close()


@router.post("/trigger")
async def trigger_analysis(
    background_tasks: BackgroundTasks,
    batch_size: int = Query(5, ge=1, le=50),
    _: None = Depends(require_admin),
):
    """Manually trigger analysis for unanalyzed news items."""
    background_tasks.add_task(run_analysis_batch, batch_size)
    return {"status": "triggered", "batch_size": batch_size}
