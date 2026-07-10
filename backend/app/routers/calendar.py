import logging
from fastapi import APIRouter, Depends
from app.deps.auth import require_admin
from app.services.calendar_client import fetch_economic_calendar, get_calendar_status
from app.services.calendar_analyzer import (
    analyze_calendar_events,
    get_cached_analysis,
    get_calendar_model_identity,
    merge_analysis,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/calendar", tags=["calendar"])


@router.get("")
async def get_economic_calendar():
    """Get this week's high/medium impact economic events for major economies."""
    events = await fetch_economic_calendar()
    provider_name, model = await get_calendar_model_identity()
    cached = get_cached_analysis(events, provider_name, model)
    if cached:
        events = merge_analysis(events, cached)
    return {"events": events, "count": len(events), **get_calendar_status()}


@router.post("/analyze")
async def analyze_economic_calendar(_: None = Depends(require_admin)):
    """Trigger AI analysis of this week's calendar events."""
    events = await fetch_economic_calendar()
    analyzed = await analyze_calendar_events(events)
    merged = merge_analysis(events, analyzed)
    return {
        "events": merged,
        "count": len(merged),
        "analyzed": len(analyzed),
        **get_calendar_status(),
    }
