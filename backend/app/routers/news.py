import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Depends

from app.deps.auth import require_admin
from app.models.database import get_db, get_news_items, get_news_item_by_id, get_analysis_for_news
from app.services.news_aggregator import (
    aggregate_enabled,
    aggregate_source,
    get_source_statuses,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/news", tags=["news"])


@router.get("")
async def list_news(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    source: Optional[str] = Query(None),
    classification: Optional[str] = Query(None, pattern="^(bullish|bearish|neutral)$"),
):
    db = await get_db()
    try:
        total, items = await get_news_items(
            db,
            page=page,
            page_size=page_size,
            source=source,
            classification=classification,
        )
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "items": items,
        }
    finally:
        await db.close()


@router.get("/sources")
async def list_news_sources():
    """Return configuration and recent health metrics for every news source."""
    return {"sources": get_source_statuses()}


@router.post("/fetch")
async def trigger_fetch_news(
    source: Optional[str] = Query(None),
    force: bool = Query(False),
    _: None = Depends(require_admin),
):
    """Fetch all enabled sources, or one named source, without coupling their schedules."""
    try:
        result = await aggregate_source(source, force=force) if source else await aggregate_enabled(force=force)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    response = dict(result)
    if "status" in response:
        response["source_status"] = response.pop("status")
    response.update(status="fetched", new_items=result.get("inserted", 0))
    return response


@router.get("/{news_id}")
async def get_news_item(news_id: int):
    db = await get_db()
    try:
        item = await get_news_item_by_id(db, news_id, include_internal=False)
        if not item:
            raise HTTPException(status_code=404, detail="News item not found")
        analysis = await get_analysis_for_news(db, news_id)
        return {**item, "analysis": analysis}
    finally:
        await db.close()
