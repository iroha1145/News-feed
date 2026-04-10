import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Depends

from app.deps.auth import require_admin
from app.models.database import get_db, get_news_items, get_news_item_by_id, get_analysis_for_news
from app.services.news_aggregator import aggregate_all_news

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/news", tags=["news"])


@router.get("")
async def list_news(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    source: Optional[str] = Query(None),
):
    db = await get_db()
    try:
        total, items = await get_news_items(db, page=page, page_size=page_size, source=source)
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "items": items,
        }
    finally:
        await db.close()


@router.get("/{news_id}")
async def get_news_item(news_id: int):
    db = await get_db()
    try:
        item = await get_news_item_by_id(db, news_id)
        if not item:
            raise HTTPException(status_code=404, detail="News item not found")
        analysis = await get_analysis_for_news(db, news_id)
        return {**item, "analysis": analysis}
    finally:
        await db.close()


@router.post("/fetch")
async def trigger_fetch_news(_: None = Depends(require_admin)):
    """Manually trigger news fetching from all sources."""
    count = await aggregate_all_news()
    return {"status": "fetched", "new_items": count}
