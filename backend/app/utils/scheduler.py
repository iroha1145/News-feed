import logging
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.config import settings as app_settings

logger = logging.getLogger(__name__)

_scheduler: Optional[AsyncIOScheduler] = None


async def _job_fetch_source(source: str) -> None:
    try:
        from app.services.news_aggregator import aggregate_source

        result = await aggregate_source(source)
        logger.info(
            "[Scheduler] %s fetch: status=%s inserted=%s duplicates=%s",
            source,
            result.get("status", "ok"),
            result.get("inserted", 0),
            result.get("duplicates", 0),
        )
    except Exception as exc:
        logger.error("[Scheduler] source %s job failed: %s", source, type(exc).__name__)


async def _job_analyze_news() -> None:
    try:
        from app.services.analysis_jobs import enqueue_auto_jobs

        count = await enqueue_auto_jobs()
        logger.info("[Scheduler] Persistent analysis jobs enqueued: %s", count)
    except Exception as exc:
        logger.error("[Scheduler] Analysis job failed: %s", type(exc).__name__)


async def _job_retry_event_projections() -> None:
    try:
        from app.services.news_aggregator import process_projection_retry_queue

        result = await process_projection_retry_queue()
        if result["attempted"]:
            logger.info(
                "[Scheduler] Event projection retries: attempted=%s completed=%s failed=%s",
                result["attempted"],
                result["completed"],
                result["failed"],
            )
    except Exception as exc:
        logger.error("[Scheduler] Event projection retry failed: %s", type(exc).__name__)


async def _job_x_sentiment() -> None:
    if not app_settings.x_sentiment_enabled:
        return
    try:
        from app.services.grok_x_monitor import run_x_sentiment_analysis

        result = await run_x_sentiment_analysis()
        if result:
            logger.info("[Scheduler] News-grounded market scenario complete")
        else:
            logger.debug("[Scheduler] Market scenario skipped (missing key or news context)")
    except Exception as exc:
        logger.error("[Scheduler] Market scenario job failed: %s", type(exc).__name__)


async def _job_fetch_calendar() -> None:
    try:
        from app.services.calendar_client import fetch_economic_calendar

        events = await fetch_economic_calendar(force=True)
        logger.info("[Scheduler] Economic calendar refreshed: %s events", len(events))
    except Exception as exc:
        logger.error("[Scheduler] Economic calendar job failed: %s", type(exc).__name__)


async def _job_pull_focus_context() -> None:
    try:
        from app.services.focus_context import pull_focus_context

        result = await pull_focus_context()
        logger.info("[Scheduler] Option Pro focus context: %s", result.get("status"))
    except Exception as exc:
        logger.error("[Scheduler] Focus context pull failed: %s", type(exc).__name__)


async def _job_market_focus_cycle() -> None:
    if not app_settings.hot_cycle_schedule_enabled:
        return
    if app_settings.automatic_hot_cycle_capability != "enabled":
        logger.warning("[Scheduler] Market-focus cycle gated: %s", app_settings.automatic_hot_cycle_capability)
        return
    try:
        from app.models.database import get_db
        from app.services.market_focus import CycleConflict, create_market_focus_cycle
        from app.services.market_schedule import due_cycle_trigger

        trigger = due_cycle_trigger()
        if trigger is None:
            return
        db = await get_db()
        try:
            await create_market_focus_cycle(db, trigger_type=trigger)
        except CycleConflict as exc:
            if exc.code not in {"no_new_hot_events", "prepared_revision_changed"}:
                logger.warning("[Scheduler] Market-focus cycle gated: %s", exc.code)
        finally:
            await db.close()
    except Exception as exc:
        logger.error("[Scheduler] Market-focus cycle failed: %s", type(exc).__name__)


async def _get_db_interval(key: str, fallback: int) -> int:
    try:
        from app.models.database import get_db, get_setting

        db = await get_db()
        try:
            value = await get_setting(db, key)
            if value is not None:
                return max(30, int(value))
        finally:
            await db.close()
    except Exception:
        pass
    return fallback


def _add_interval_job(
    scheduler: AsyncIOScheduler,
    function,
    *,
    seconds: int,
    job_id: str,
    name: str,
    args: Optional[list] = None,
) -> None:
    jitter = min(60, max(5, seconds // 10))
    scheduler.add_job(
        function,
        trigger=IntervalTrigger(seconds=seconds, jitter=jitter),
        args=args or [],
        id=job_id,
        name=name,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=max(30, min(seconds, 300)),
    )


async def start_scheduler() -> None:
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        logger.warning("Scheduler already running, skipping start")
        return

    from app.services.news_aggregator import (
        SOURCE_DEFINITIONS,
        initialize_source_health,
        get_enabled_sources,
        get_source_interval,
    )

    _scheduler = AsyncIOScheduler()
    await initialize_source_health()
    source_intervals: dict[str, int] = {}
    for source in get_enabled_sources():
        definition = SOURCE_DEFINITIONS[source]
        fallback_interval = get_source_interval(source)
        interval = await _get_db_interval(
            f"{definition.settings_prefix}_interval",
            fallback_interval,
        )
        source_intervals[source] = interval
        _add_interval_job(
            _scheduler,
            _job_fetch_source,
            seconds=interval,
            job_id=f"fetch_news_{source}",
            name=f"Fetch news: {source}",
            args=[source],
        )

    _add_interval_job(
        _scheduler,
        _job_analyze_news,
        seconds=60,
        job_id="analyze_news",
        name="Analyze unanalyzed news",
    )

    _add_interval_job(
        _scheduler,
        _job_retry_event_projections,
        seconds=60,
        job_id="retry_event_projections",
        name="Retry local event projections",
    )

    _add_interval_job(
        _scheduler,
        _job_fetch_calendar,
        seconds=app_settings.calendar_fetch_interval_seconds,
        job_id="fetch_economic_calendar",
        name="Fetch economic calendar",
    )

    _add_interval_job(
        _scheduler,
        _job_pull_focus_context,
        seconds=app_settings.option_pro_focus_interval_seconds,
        job_id="pull_option_pro_focus_context",
        name="Pull Option Pro focus context",
    )

    _add_interval_job(
        _scheduler,
        _job_market_focus_cycle,
        seconds=60,
        job_id="market_focus_cycle_schedule",
        name="Market-focus cycle schedule",
    )

    if app_settings.x_sentiment_enabled:
        x_sentiment_interval = await _get_db_interval(
            "x_sentiment_interval",
            getattr(app_settings, "x_sentiment_interval", 1800),
        )
        x_sentiment_interval = max(300, x_sentiment_interval)
        _add_interval_job(
            _scheduler,
            _job_x_sentiment,
            seconds=x_sentiment_interval,
            job_id="x_sentiment",
            name="News-grounded model market scenario",
        )

    _scheduler.start()
    logger.info("Scheduler started with independent news jobs: %s", source_intervals)


def stop_scheduler() -> None:
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")


def get_scheduler() -> Optional[AsyncIOScheduler]:
    return _scheduler
