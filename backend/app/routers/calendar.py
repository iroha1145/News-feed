import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path

from app.deps.auth import require_admin
from app.config import settings
from app.models.database import get_db
from app.services.calendar_client import fetch_economic_calendar, get_calendar_status
from app.services.calendar_analyzer import (
    get_calendar_model_identity,
    merge_analysis,
)
from app.services.calendar_analysis_jobs import (
    create_or_get_calendar_job,
    get_calendar_job,
    load_completed_calendar_analysis,
    public_calendar_job,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/calendar", tags=["calendar"])


@router.get("")
async def get_economic_calendar():
    """Read calendar data and any already-completed model result."""
    events = await fetch_economic_calendar()
    provider_name, model = await get_calendar_model_identity()
    db = await get_db()
    try:
        analyzed = await load_completed_calendar_analysis(
            db,
            events,
            provider=provider_name,
            model=model,
        )
    finally:
        await db.close()
    if analyzed is not None:
        events = merge_analysis(events, analyzed)
    return {
        "events": events,
        "count": len(events),
        "analyzed": len(analyzed or []),
        "analysis_capability": settings.manual_calendar_analysis_capability,
        **get_calendar_status(),
    }


@router.post("/analyze", status_code=202)
async def analyze_economic_calendar(
    force: bool = False,
    _: None = Depends(require_admin),
):
    """Persist a calendar analysis job without opening a model request."""
    capability = settings.manual_calendar_analysis_capability
    if capability != "enabled":
        raise HTTPException(
            status_code=409,
            detail={
                "code": capability,
                "message": "Calendar analysis is disabled until both daily budgets are configured.",
            },
        )
    events = await fetch_economic_calendar()
    provider_name, model = await get_calendar_model_identity()
    db = await get_db()
    try:
        created = await create_or_get_calendar_job(
            db,
            events,
            provider=provider_name,
            model=model,
            force=force,
        )
    finally:
        await db.close()
    return {**public_calendar_job(created.job), "created": created.created}


@router.get("/analyze/{job_id}")
async def get_calendar_analysis_job(
    job_id: Annotated[str, Path(pattern=r"^calj_[0-9a-f]{32}$")],
    _: None = Depends(require_admin),
):
    """Poll a persisted calendar analysis job. This endpoint never calls a model."""
    db = await get_db()
    try:
        job = await get_calendar_job(db, job_id)
    finally:
        await db.close()
    if job is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "calendar_analysis_job_not_found",
                "message": "The requested calendar analysis job was not found.",
                "retryable": False,
            },
        )
    return public_calendar_job(job)
