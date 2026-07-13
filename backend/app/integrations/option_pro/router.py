from __future__ import annotations

import json
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Annotated, Literal, Optional

from fastapi import APIRouter, Depends, Path, Query, Request
from fastapi.responses import JSONResponse

from app.config import settings
from app.integrations.option_pro.auth import (
    IntegrationAPIError,
    IntegrationPrincipal,
    require_action,
    require_read,
)
from app.integrations.option_pro.contract import schema_sha256
from app.integrations.option_pro.repository import (
    catalyst_result_status,
    get_news_item,
    query_calendar,
    query_feed,
    query_latest,
    query_ticker,
)
from app.models.catalysts import (
    AnalysisJobCreateRequest,
    AnalysisJobResponse,
    BatchTickerResult,
    CalendarResponse,
    CatalystBatchRequest,
    CatalystBatchResponse,
    CatalystTickerResponse,
    ComponentHealth,
    ErrorBody,
    FeedResponse,
    IntegrationHealthResponse,
    HotspotListResponse,
    HotspotStatusResponse,
    LatestResponse,
    MarketFocusCycleCreateRequest,
    MarketFocusCycleResponse,
    NewsImpactAnalysis,
    NewsResponse,
    PublicAnalysis,
    PublicDataStatus,
    QueueHealth,
    SCHEMA_VERSION,
)
from app.models.database import get_db
from app.services.analysis_jobs import (
    InputVersionConflict,
    create_or_get_job,
    parse_utc,
    request_cancel,
    utc_now,
)
from app.services.responses_runtime import OpenAIResponsesProvider
from app.services.worker_health import evaluate_worker_heartbeat
from app.services.market_focus import (
    CycleConflict,
    create_market_focus_cycle,
    get_hotspot_status,
    list_prepared_hotspots,
    request_market_focus_cancel,
    retry_market_focus_cycle,
)

PREFIX = "/api/integrations/option-pro/v1"
router = APIRouter(prefix=PREFIX, tags=["option-pro-integration"])
REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{8,100}$")


def request_id(request: Request) -> str:
    supplied = request.headers.get("X-Request-Id", "")
    return supplied if REQUEST_ID_PATTERN.fullmatch(supplied) else f"req_{uuid.uuid4().hex}"


def metadata(request: Request) -> dict[str, str]:
    return {
        "schema_version": SCHEMA_VERSION,
        "schema_sha256": schema_sha256(),
        "request_id": request_id(request),
    }


def bounded_as_of(value: Optional[datetime]) -> datetime:
    result = value or utc_now()
    if result.tzinfo is None or result.utcoffset() is None:
        raise IntegrationAPIError(400, "timezone_required", "as_of must include a timezone.")
    result = result.astimezone(timezone.utc)
    if result > utc_now() + timedelta(minutes=5):
        raise IntegrationAPIError(400, "as_of_in_future", "as_of is outside the allowed future window.")
    return result


async def job_response(request: Request, db, job: dict) -> AnalysisJobResponse:
    async with db.execute(
        """SELECT * FROM analysis_revisions WHERE job_id=?
           ORDER BY revision DESC LIMIT 1""",
        (job["job_id"],),
    ) as cursor:
        revision = await cursor.fetchone()
    result = None
    if revision is not None:
        async with db.execute(
            """SELECT m.ticker,m.validation_status,m.validated_at,m.focus_revision,
                      m.universe_version,m.association_method
               FROM news_ticker_mentions m
               WHERE m.news_id=? AND m.association_method='llm_inference'
                 AND m.id=(
                   SELECT MAX(newest.id) FROM news_ticker_mentions newest
                   WHERE newest.news_id=m.news_id AND newest.ticker=m.ticker
                     AND newest.association_method='llm_inference'
                 )
               ORDER BY m.ticker""",
            (job["news_id"],),
        ) as validation_cursor:
            stock_validations = [dict(row) for row in await validation_cursor.fetchall()]
        payload = NewsImpactAnalysis.model_validate_json(revision["payload_json"])
        result = PublicAnalysis(
            **payload.model_dump(),
            analysis_id=revision["id"],
            revision=revision["revision"],
            model=revision["model"],
            reasoning=revision["reasoning_effort"],
            prompt_version=revision["prompt_version"],
            schema_version=revision["schema_version"],
            analyzed_at=revision["analyzed_at"],
            available_at=revision["available_at"],
            stock_validations=stock_validations,
        )
    retry_after = None
    if job.get("next_attempt_at"):
        retry_after = max(0, int((parse_utc(job["next_attempt_at"]) - utc_now()).total_seconds()))
    return AnalysisJobResponse(
        **metadata(request),
        job_id=job["job_id"],
        news_id=job["news_id"],
        content_hash=job["content_hash"],
        input_hash=job.get("source_input_hash") or job["input_hash"],
        change_sequence=job.get("change_sequence"),
        status=job["status"],
        model=job["model"],
        reasoning=job["reasoning_effort"],
        submitted_at=job.get("submitted_at"),
        updated_at=job["updated_at"],
        completed_at=job.get("completed_at"),
        error_code=job.get("error_code"),
        retry_after=retry_after,
        result=result,
    )


@router.get("/health", response_model=IntegrationHealthResponse)
async def integration_health(
    request: Request,
    _: IntegrationPrincipal = Depends(require_read),
):
    now = utc_now()
    db = await get_db()
    try:
        async with db.execute(
            """SELECT status, COUNT(*) AS count, MIN(created_at) AS oldest
               FROM analysis_jobs GROUP BY status"""
        ) as cursor:
            queue_rows = [dict(row) for row in await cursor.fetchall()]
        counts = {row["status"]: int(row["count"]) for row in queue_rows}
        oldest_values = [parse_utc(row["oldest"]) for row in queue_rows if row["oldest"] and row["status"] in {"pending", "queued", "in_progress"}]
        async with db.execute("SELECT MAX(COALESCE(updated_at,fetched_at)) FROM news_items") as cursor:
            data_row = await cursor.fetchone()
        data_through = parse_utc(data_row[0]) if data_row and data_row[0] else None
        async with db.execute("SELECT * FROM source_health ORDER BY source") as cursor:
            source_rows = [dict(row) for row in await cursor.fetchall()]
        async with db.execute(
            "SELECT heartbeat_at,status FROM analysis_worker_state "
            "ORDER BY heartbeat_at DESC LIMIT 1"
        ) as cursor:
            worker_row = await cursor.fetchone()
        async with db.execute(
            """SELECT COUNT(*) FROM market_focus_cycles
               WHERE status='failed' AND error_code='submission_outcome_unknown'"""
        ) as cursor:
            unknown_cycle_count = int((await cursor.fetchone())[0])
    finally:
        await db.close()

    from app.utils.scheduler import get_scheduler

    scheduler = get_scheduler()
    scheduler_running = bool(scheduler is not None and scheduler.running)
    provider = OpenAIResponsesProvider()
    capabilities = provider.capabilities()
    await provider.close()
    provider_configured = bool(
        settings.openai_api_key
        or (
            settings.default_llm_provider == "openai"
            and settings.default_llm_api_key
        )
    )
    action_configured = bool(settings.option_pro_action_key_id and settings.option_pro_action_secret)
    trigger_enabled = (
        action_configured
        and settings.default_llm_provider == "openai"
        and provider_configured
        and capabilities.status == "ok"
        and settings.manual_news_analysis_capability == "enabled"
    )
    budget_status: Literal["ok", "budget_configuration_required", "budget_blocked"] = "ok"
    if settings.news_llm_auto_analyze_enabled and settings.automatic_news_analysis_capability != "enabled":
        budget_status = "budget_configuration_required"
        warnings_auto_budget = True
    else:
        warnings_auto_budget = False
    if counts.get("budget_blocked", 0):
        budget_status = "budget_blocked"
    warnings: list[str] = []
    if warnings_auto_budget:
        warnings.append("automatic_analysis_budget_configuration_required")
    if settings.manual_news_analysis_capability == "budget_configuration_required":
        warnings.append("manual_analysis_budget_configuration_required")
    if unknown_cycle_count:
        warnings.append(f"market_focus_submission_outcome_unknown:{unknown_cycle_count}")
    if not action_configured:
        warnings.append("analysis_action_key_not_configured")
    if not provider_configured:
        warnings.append("openai_key_not_configured")
    if settings.default_llm_provider != "openai":
        warnings.append("analysis_provider_not_openai")
    if capabilities.status == "unsupported_provider_capability":
        warnings.append("unsupported_provider_capability")
    if not scheduler_running:
        warnings.append("scheduler_not_running")
    worker_status, worker_warning = evaluate_worker_heartbeat(
        worker_row["heartbeat_at"] if worker_row else None,
        worker_row["status"] if worker_row else None,
        now=now,
    )
    if worker_warning:
        warnings.append(worker_warning)
    trigger_enabled = trigger_enabled and worker_status == "ok"
    sources = {
        row["source"]: ComponentHealth(
            status=row["status"],
            last_attempt_at=row.get("last_attempt_at"),
            last_success_at=row.get("last_success_at"),
            data_through=row.get("data_through"),
            consecutive_failures=row.get("consecutive_failures", 0),
            next_attempt_at=row.get("next_attempt_at"),
            raw_count=row.get("raw_count"),
            inserted_count=row.get("inserted_count"),
            duplicates_count=row.get("duplicates_count"),
            source_fetch_status=row.get("source_fetch_status"),
            news_persistence_status=row.get("news_persistence_status"),
            event_projection_status=row.get("event_projection_status"),
            detail=row.get("error_code"),
        )
        for row in source_rows
    }
    status_value: Literal["ok", "degraded", "unavailable", "not_configured"] = "ok"
    if warnings or any(source.status in {"degraded", "unavailable"} for source in sources.values()):
        status_value = "degraded"
    return IntegrationHealthResponse(
        **metadata(request),
        status=status_value,
        as_of=now,
        data_through=data_through,
        database=ComponentHealth(status="ok", last_success_at=now, data_through=data_through),
        scheduler=ComponentHealth(status="ok" if scheduler_running else "degraded", last_success_at=now if scheduler_running else None),
        analysis_queue=QueueHealth(
            status=(
                "unavailable"
                if worker_status != "ok"
                else "ok" if capabilities.status == "ok" else "degraded"
            ),
            pending=counts.get("pending", 0),
            queued=counts.get("queued", 0),
            in_progress=counts.get("in_progress", 0),
            oldest_job_at=min(oldest_values) if oldest_values else None,
            budget_status=budget_status,
        ),
        model=settings.default_llm_model,
        reasoning=settings.openai_reasoning,
        execution_mode=settings.openai_execution_mode,
        analysis_trigger_enabled=trigger_enabled,
        sources=sources,
        warnings=warnings,
    )


@router.get("/feed", response_model=FeedResponse)
async def feed(
    request: Request,
    as_of: Optional[datetime] = None,
    window_hours: Annotated[int, Query(ge=1, le=24 * 30)] = 72,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: Annotated[Optional[str], Query(max_length=4096)] = None,
    source: Annotated[Optional[str], Query(min_length=1, max_length=500)] = None,
    classification: Optional[Literal["bullish", "bearish", "neutral"]] = None,
    min_confidence: Annotated[int, Query(ge=0, le=100)] = 0,
    min_abs_impact: Annotated[int, Query(ge=0, le=100)] = 0,
    include_unanalyzed: bool = True,
    analysis_status: Optional[Literal[
        "not_requested", "pending", "queued", "in_progress", "completed", "failed",
        "cancelled", "insufficient_context", "budget_blocked", "incomplete_output"
    ]] = None,
    _: IntegrationPrincipal = Depends(require_read),
):
    cutoff = bounded_as_of(as_of)
    db = await get_db()
    try:
        items, next_cursor, has_more, data_through = await query_feed(
            db,
            as_of=cutoff,
            window_hours=window_hours,
            limit=limit,
            cursor=cursor,
            source=source,
            classification=classification,
            min_confidence=min_confidence,
            min_abs_impact=min_abs_impact,
            analysis_status=analysis_status,
            include_unanalyzed=include_unanalyzed,
        )
    finally:
        await db.close()
    return FeedResponse(
        **metadata(request), as_of=cutoff, data_through=data_through,
        items=items, next_cursor=next_cursor, has_more=has_more,
    )


@router.get("/latest", response_model=LatestResponse)
async def latest(
    request: Request,
    updated_after: Optional[datetime] = None,
    cursor: Annotated[Optional[str], Query(max_length=4096)] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    _: IntegrationPrincipal = Depends(require_read),
):
    cutoff = bounded_as_of(updated_after) if updated_after is not None else None
    db = await get_db()
    try:
        snapshot, items, next_updated_after, next_cursor, has_more, data_through = await query_latest(
            db, updated_after=cutoff, limit=limit, cursor=cursor,
        )
    finally:
        await db.close()
    return LatestResponse(
        **metadata(request), snapshot_token=snapshot, data_through=data_through,
        next_updated_after=next_updated_after, next_cursor=next_cursor,
        has_more=has_more, items=items,
    )


@router.get("/news/{news_id}", response_model=NewsResponse)
async def news(
    request: Request,
    news_id: int,
    as_of: Optional[datetime] = None,
    _: IntegrationPrincipal = Depends(require_read),
):
    cutoff = bounded_as_of(as_of)
    db = await get_db()
    try:
        item = await get_news_item(db, news_id, cutoff)
    finally:
        await db.close()
    if item is None:
        raise IntegrationAPIError(404, "news_not_found", "The requested news item was not found.")
    return NewsResponse(**metadata(request), item=item)


@router.get("/catalysts/{ticker}", response_model=CatalystTickerResponse)
async def catalysts_for_ticker(
    request: Request,
    ticker: Annotated[str, Path(pattern=r"^[A-Za-z0-9][A-Za-z0-9.^/_-]{0,19}$")],
    as_of: Optional[datetime] = None,
    window_hours: Annotated[int, Query(ge=1, le=24 * 30)] = 72,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    cursor: Annotated[Optional[str], Query(max_length=4096)] = None,
    min_confidence: Annotated[int, Query(ge=0, le=100)] = 0,
    include_neutral: bool = False,
    include_unanalyzed: bool = True,
    _: IntegrationPrincipal = Depends(require_read),
):
    cutoff = bounded_as_of(as_of)
    normalized = ticker.upper()
    db = await get_db()
    try:
        items, next_cursor, has_more, data_through = await query_ticker(
            db,
            ticker=normalized,
            as_of=cutoff,
            window_hours=window_hours,
            limit=limit,
            cursor=cursor,
            min_confidence=min_confidence,
            include_neutral=include_neutral,
            include_unanalyzed=include_unanalyzed,
        )
        result_status = await catalyst_result_status(db, items)
    finally:
        await db.close()
    return CatalystTickerResponse(
        **metadata(request), ticker=normalized,
        status=PublicDataStatus(result_status),
        as_of=cutoff, data_through=data_through, items=items,
        next_cursor=next_cursor, has_more=has_more,
    )


@router.post("/catalysts/batch", response_model=CatalystBatchResponse)
async def catalyst_batch(
    request: Request,
    body: CatalystBatchRequest,
    _: IntegrationPrincipal = Depends(require_read),
):
    cutoff = bounded_as_of(body.as_of)
    db = await get_db()
    try:
        results: dict[str, BatchTickerResult] = {}
        for ticker in body.tickers:
            items, next_cursor, _, data_through = await query_ticker(
                db,
                ticker=ticker,
                as_of=cutoff,
                window_hours=body.window_hours,
                limit=body.limit,
                cursor=None,
                min_confidence=body.min_confidence,
                include_neutral=body.include_neutral,
                include_unanalyzed=body.include_unanalyzed,
            )
            result_status = await catalyst_result_status(db, items)
            results[ticker] = BatchTickerResult(
                status=PublicDataStatus(result_status),
                data_through=data_through,
                items=items,
                next_cursor=next_cursor,
            )
    finally:
        await db.close()
    return CatalystBatchResponse(**metadata(request), as_of=cutoff, results=results)


@router.get("/calendar", response_model=CalendarResponse)
async def calendar(
    request: Request,
    date_from: date,
    date_to: date,
    as_of: Optional[datetime] = None,
    currencies: Annotated[Optional[str], Query(max_length=500)] = None,
    min_impact: Literal["low", "medium", "high"] = "low",
    _: IntegrationPrincipal = Depends(require_read),
):
    if date_to < date_from or (date_to - date_from).days > 366:
        raise IntegrationAPIError(400, "invalid_date_range", "The calendar date range is invalid.")
    cutoff = bounded_as_of(as_of)
    currency_values = []
    if currencies:
        currency_values = [value.strip().upper() for value in currencies.split(",") if value.strip()]
        if any(not re.fullmatch(r"[A-Z]{3}", value) for value in currency_values):
            raise IntegrationAPIError(400, "invalid_currency", "A calendar currency is invalid.")
    db = await get_db()
    try:
        items, data_through = await query_calendar(
            db, date_from=date_from, date_to=date_to, as_of=cutoff,
            currencies=currency_values, min_impact=min_impact,
        )
    finally:
        await db.close()
    return CalendarResponse(
        **metadata(request), as_of=cutoff, data_through=data_through, items=items,
    )


@router.post("/analysis-jobs", response_model=AnalysisJobResponse, status_code=202)
async def create_analysis_job(
    request: Request,
    body: AnalysisJobCreateRequest,
    _: IntegrationPrincipal = Depends(require_action),
):
    capability = settings.manual_news_analysis_capability
    if capability != "enabled":
        raise IntegrationAPIError(
            409,
            capability,
            "Manual analysis is disabled until both daily budgets are configured.",
        )
    db = await get_db()
    try:
        try:
            created = await create_or_get_job(
                db,
                body.news_id,
                force=body.force,
                priority=100,
                expected_content_hash=body.expected_content_hash,
                expected_change_sequence=body.expected_change_sequence,
            )
        except LookupError as exc:
            raise IntegrationAPIError(404, "news_not_found", "The requested news item was not found.") from exc
        except InputVersionConflict as exc:
            raise IntegrationAPIError(
                409,
                "news_version_conflict",
                "The requested news version is no longer current. Refresh before creating another job.",
            ) from exc
        response = await job_response(request, db, created.job)
    finally:
        await db.close()
    return response


@router.get("/analysis-jobs/{job_id}", response_model=AnalysisJobResponse)
async def get_analysis_job(
    request: Request,
    job_id: Annotated[str, Path(pattern=r"^mlj_[0-9a-f]{32}$")],
    _: IntegrationPrincipal = Depends(require_read),
):
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM analysis_jobs WHERE job_id=?", (job_id,)) as cursor:
            row = await cursor.fetchone()
        if row is None:
            raise IntegrationAPIError(404, "job_not_found", "The requested analysis job was not found.")
        return await job_response(request, db, dict(row))
    finally:
        await db.close()


@router.post("/analysis-jobs/{job_id}/cancel", response_model=AnalysisJobResponse)
async def cancel_analysis_job(
    request: Request,
    job_id: Annotated[str, Path(pattern=r"^mlj_[0-9a-f]{32}$")],
    _: IntegrationPrincipal = Depends(require_action),
):
    db = await get_db()
    try:
        job = await request_cancel(db, job_id)
        if job is None:
            raise IntegrationAPIError(404, "job_not_found", "The requested analysis job was not found.")
        return await job_response(request, db, job)
    finally:
        await db.close()


def _public_cycle(row: dict) -> dict:
    result = dict(row)
    for key in (
        "lease_owner", "lease_expires_at", "fencing_token", "input_json",
        "openai_response_id", "prompt_cache_key",
    ):
        result.pop(key, None)
    value = result.pop("result_json", None)
    result["result"] = json.loads(value) if value else None
    result["no_new_hot_events"] = bool(result["no_new_hot_events"])
    return result


@router.get("/hotspots/status", response_model=HotspotStatusResponse)
async def hotspot_status(
    request: Request,
    _: IntegrationPrincipal = Depends(require_read),
):
    db = await get_db()
    try:
        status = await get_hotspot_status(db)
    finally:
        await db.close()
    return {**metadata(request), **status}


@router.get("/hotspots", response_model=HotspotListResponse)
async def hotspots(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    as_of: Optional[datetime] = None,
    _: IntegrationPrincipal = Depends(require_read),
):
    cutoff = bounded_as_of(as_of)
    db = await get_db()
    try:
        items = await list_prepared_hotspots(db, limit=limit, as_of=cutoff)
    finally:
        await db.close()
    return {**metadata(request), "as_of": cutoff, "items": items}


@router.post(
    "/market-focus-cycles",
    status_code=202,
    response_model=MarketFocusCycleResponse,
)
async def create_focus_cycle(
    request: Request,
    body: MarketFocusCycleCreateRequest,
    _: IntegrationPrincipal = Depends(require_action),
):
    if body.trigger != "manual":
        raise IntegrationAPIError(
            403,
            "scheduled_trigger_reserved",
            "Scheduled cycle triggers are reserved for the server scheduler.",
        )
    db = await get_db()
    try:
        try:
            if body.retry_cycle_id:
                cycle = await retry_market_focus_cycle(db, body.retry_cycle_id)
            else:
                cycle = await create_market_focus_cycle(
                    db,
                    trigger_type=body.trigger,
                    expected_prepared_revision=body.expected_prepared_revision,
                )
        except CycleConflict as exc:
            status_code = 429 if exc.code in {"daily_job_limit_reached", "daily_output_token_limit_reached"} else 409
            raise IntegrationAPIError(
                status_code,
                exc.code,
                "The market-focus cycle cannot be created in the current state.",
                retryable=exc.retry_after is not None,
                retry_after_seconds=exc.retry_after,
            ) from exc
    finally:
        await db.close()
    return {**metadata(request), "cycle": _public_cycle(cycle)}


@router.get("/market-focus-cycles/latest", response_model=MarketFocusCycleResponse)
async def latest_focus_cycle(
    request: Request,
    _: IntegrationPrincipal = Depends(require_read),
):
    db = await get_db()
    try:
        async with db.execute(
            "SELECT * FROM market_focus_cycles ORDER BY created_at DESC LIMIT 1"
        ) as cursor:
            row = await cursor.fetchone()
    finally:
        await db.close()
    return {**metadata(request), "cycle": _public_cycle(dict(row)) if row else None}


@router.get(
    "/market-focus-cycles/{cycle_id}", response_model=MarketFocusCycleResponse
)
async def focus_cycle(
    request: Request,
    cycle_id: Annotated[str, Path(pattern=r"^mfc_[a-f0-9]{32}$")],
    _: IntegrationPrincipal = Depends(require_read),
):
    db = await get_db()
    try:
        async with db.execute(
            "SELECT * FROM market_focus_cycles WHERE cycle_id=?", (cycle_id,)
        ) as cursor:
            row = await cursor.fetchone()
    finally:
        await db.close()
    if row is None:
        raise IntegrationAPIError(404, "cycle_not_found", "The market-focus cycle was not found.")
    return {**metadata(request), "cycle": _public_cycle(dict(row))}


@router.post(
    "/market-focus-cycles/{cycle_id}/cancel",
    response_model=MarketFocusCycleResponse,
)
async def cancel_focus_cycle(
    request: Request,
    cycle_id: Annotated[str, Path(pattern=r"^mfc_[a-f0-9]{32}$")],
    _: IntegrationPrincipal = Depends(require_action),
):
    db = await get_db()
    try:
        cycle = await request_market_focus_cancel(db, cycle_id)
    finally:
        await db.close()
    if cycle is None:
        raise IntegrationAPIError(404, "cycle_not_found", "The market-focus cycle was not found.")
    return {**metadata(request), "cycle": _public_cycle(cycle)}


def error_response(request: Request, error: IntegrationAPIError) -> JSONResponse:
    body = ErrorBody(
        **metadata(request),
        code=error.code,
        message=error.message,
        retryable=error.retryable,
        retry_after_seconds=error.retry_after_seconds,
        resync_from=error.resync_from,
        server_time=error.server_time,
        latest_window_days=error.latest_window_days,
    )
    headers = {}
    if error.retry_after_seconds is not None:
        headers["Retry-After"] = str(error.retry_after_seconds)
    return JSONResponse(
        status_code=error.status_code,
        content=body.model_dump(mode="json"),
        headers=headers,
    )
