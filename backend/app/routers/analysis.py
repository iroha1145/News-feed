import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Depends

from app.config import settings
from app.deps.auth import require_admin
from app.models.database import (
    get_db,
    get_analyses,
    get_latest_analyses,
    get_analysis_stats,
    get_analysis_for_news,
    get_news_item_by_id,
)
from app.services.analysis_jobs import (
    enqueue_manual_jobs_with_status,
    retry_failed_jobs,
)

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
        news = await get_news_item_by_id(db, news_id, include_internal=False)
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
async def analysis_stats(days: int = Query(7, ge=1, le=365)):
    db = await get_db()
    try:
        stats = await get_analysis_stats(db, days=days)
        stats["manual_analysis_capability"] = settings.manual_news_analysis_capability
        return stats
    finally:
        await db.close()


@router.post("/trigger")
async def trigger_analysis(
    batch_size: int = Query(5, ge=1, le=50),
    _: None = Depends(require_admin),
):
    """Manually trigger analysis for unanalyzed news items."""
    capability = settings.manual_news_analysis_capability
    if capability != "enabled":
        raise HTTPException(
            status_code=409,
            detail={
                "code": capability,
                "message": "Manual analysis is disabled until both daily budgets are configured.",
            },
        )
    result = await enqueue_manual_jobs_with_status(batch_size)
    response = {
        "status": (
            "queued"
            if result.enqueued
            else "budget_blocked"
            if result.stop_reason
            else "no_eligible_news"
        ),
        "batch_size": batch_size,
        "enqueued": result.enqueued,
        "capability": capability,
    }
    if result.stop_reason:
        response["stop_reason"] = result.stop_reason
    return response


@router.post("/retry-failed")
async def retry_failed_analyses(
    news_id: Optional[int] = Query(None, ge=1),
    _: None = Depends(require_admin),
):
    """Reset failed analyses after an operator has corrected the underlying issue."""
    capability = settings.manual_news_analysis_capability
    if capability != "enabled":
        raise HTTPException(
            status_code=409,
            detail={
                "code": capability,
                "message": "Manual analysis is disabled until both daily budgets are configured.",
            },
        )
    db = await get_db()
    try:
        jobs = await retry_failed_jobs(db, news_id=news_id)
        return {
            "status": "requeued",
            "count": len(jobs),
            "news_id": news_id,
            "job_ids": [job["job_id"] for job in jobs],
        }
    finally:
        await db.close()
