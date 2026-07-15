import logging
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.config import settings

logger = logging.getLogger(__name__)

_scheduler: Optional[AsyncIOScheduler] = None


async def _job_fetch_source(source: str) -> None:
    try:
        from app.services.news_aggregator import aggregate_source

        result = await aggregate_source(source)
        logger.info(
            "Source %s fetch: status=%s inserted=%s duplicates=%s",
            source,
            result.get("status", "ok"),
            result.get("inserted", 0),
            result.get("duplicates", 0),
        )
    except Exception as exc:
        logger.error("Source %s job failed: %s", source, type(exc).__name__)


async def _job_fetch_calendar() -> None:
    try:
        from app.services.calendar_client import fetch_economic_calendar
        from app.services.calendar_client import get_calendar_status

        events = await fetch_economic_calendar(force=True)
        calendar_status = get_calendar_status()
        if calendar_status.get("last_error"):
            logger.warning(
                "Economic calendar refresh is %s: %s events",
                "degraded" if events else "unavailable",
                len(events),
            )
        else:
            logger.info("Economic calendar refreshed: %s events", len(events))
    except Exception as exc:
        logger.error("Economic calendar job failed: %s", type(exc).__name__)


async def _job_retention() -> None:
    try:
        from app.models.database import run_retention

        await run_retention()
    except Exception as exc:
        logger.error("ETL retention job failed: %s", type(exc).__name__)


def _add_interval_job(
    scheduler: AsyncIOScheduler,
    function,
    *,
    seconds: int,
    job_id: str,
    name: str,
    args: list | None = None,
) -> None:
    scheduler.add_job(
        function,
        trigger=IntervalTrigger(seconds=seconds, jitter=min(60, max(5, seconds // 10))),
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
        logger.warning("Scheduler already running; duplicate start was ignored")
        return

    from app.services.news_aggregator import initialize_source_health

    _scheduler = AsyncIOScheduler()
    await initialize_source_health()
    source_intervals: dict[str, int] = {}
    for source, source_config in settings.sources.items():
        if not source_config.enabled:
            continue
        source_intervals[source] = source_config.interval_seconds
        _add_interval_job(
            _scheduler,
            _job_fetch_source,
            seconds=source_config.interval_seconds,
            job_id=f"fetch_news_{source}",
            name=f"Fetch news: {source}",
            args=[source],
        )

    _add_interval_job(
        _scheduler,
        _job_fetch_calendar,
        seconds=settings.calendar.interval_seconds,
        job_id="fetch_economic_calendar",
        name="Fetch economic calendar",
    )
    _add_interval_job(
        _scheduler,
        _job_retention,
        seconds=settings.storage.retention_interval_seconds,
        job_id="etl_retention",
        name="Apply ETL retention",
    )
    _scheduler.start()
    logger.info("ETL scheduler started: %s", source_intervals)


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("ETL scheduler stopped")
    _scheduler = None


def get_scheduler() -> Optional[AsyncIOScheduler]:
    return _scheduler
