import logging

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Depends

from app.deps.auth import require_admin
from app.models.database import get_db, get_latest_x_sentiment, get_x_sentiment_history
from app.services.grok_x_monitor import run_x_sentiment_analysis, get_last_error

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/x-sentiment", tags=["x-sentiment"])


SANITIZED_ERROR_MESSAGE = "The last sentiment refresh failed. Check server logs for details."


@router.get("")
async def get_latest_sentiment():
    db = await get_db()
    try:
        sentiment = await get_latest_x_sentiment(db)
        if not sentiment:
            last_error = get_last_error()
            msg = "No social sentiment estimate yet. Trigger a refresh to generate an LLM-based analysis."
            sanitized_last_error = None
            if last_error:
                msg = SANITIZED_ERROR_MESSAGE
                sanitized_last_error = SANITIZED_ERROR_MESSAGE
            return {"message": msg, "data": None, "last_error": sanitized_last_error}
        return {"data": sentiment}
    finally:
        await db.close()


@router.post("/refresh")
async def refresh_x_sentiment(
    background_tasks: BackgroundTasks,
    _: None = Depends(require_admin),
):
    """Trigger a new LLM-based social sentiment estimation via Grok."""
    background_tasks.add_task(run_x_sentiment_analysis)
    return {"status": "triggered", "message": "Social sentiment estimation started in background"}


@router.get("/history")
async def x_sentiment_history(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    db = await get_db()
    try:
        total, items = await get_x_sentiment_history(db, page=page, page_size=page_size)
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "items": items,
        }
    finally:
        await db.close()
