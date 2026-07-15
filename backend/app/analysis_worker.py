from __future__ import annotations

import asyncio
import logging
import os
import signal
import sqlite3
import socket
import uuid

from app.config import settings
from app.models.database import init_db
from app.services.analysis_jobs import run_worker_once
from app.services.calendar_analysis_jobs import run_calendar_worker_once
from app.services.responses_runtime import OpenAIResponsesProvider
from app.services.market_focus import run_market_focus_worker_once
from app.utils.http import configure_safe_network_logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
configure_safe_network_logging()
logger = logging.getLogger(__name__)


def _is_transient_database_lock(exc: sqlite3.OperationalError) -> bool:
    message = str(exc).lower()
    return "database" in message and ("locked" in message or "busy" in message)


async def _wait_for_next_poll(stop: asyncio.Event) -> None:
    try:
        await asyncio.wait_for(
            stop.wait(),
            timeout=settings.analysis_worker_poll_seconds,
        )
    except asyncio.TimeoutError:
        pass


async def _initialize_database(stop: asyncio.Event) -> bool:
    while not stop.is_set():
        try:
            await init_db()
            return True
        except sqlite3.OperationalError as exc:
            if not _is_transient_database_lock(exc):
                raise
            logger.warning(
                "SQLite write contention deferred database initialization; retrying without restarting"
            )
            await _wait_for_next_poll(stop)
    return False


async def _run_worker_loop(
    *,
    stop: asyncio.Event,
    provider: OpenAIResponsesProvider,
    worker_id: str,
) -> None:
    while not stop.is_set():
        try:
            worked_news = await run_worker_once(provider=provider, worker_id=worker_id)
            worked_calendar = await run_calendar_worker_once(
                provider=provider,
                worker_id=worker_id,
            )
            worked_market_focus = await run_market_focus_worker_once(
                provider=provider,
                worker_id=worker_id,
            )
        except sqlite3.OperationalError as exc:
            if not _is_transient_database_lock(exc):
                raise
            logger.warning(
                "SQLite write contention deferred; retrying without restarting the analysis worker"
            )
            await _wait_for_next_poll(stop)
            continue
        if worked_news or worked_calendar or worked_market_focus:
            continue
        await _wait_for_next_poll(stop)


async def run() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop.set)
        except NotImplementedError:
            pass
    if not await _initialize_database(stop):
        logger.info("MacroLens analysis worker stopped before database initialization")
        return
    logger.info("MacroLens analysis worker ready")
    worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    provider = OpenAIResponsesProvider()
    capabilities = provider.capabilities()
    if capabilities.status != "ok":
        logger.warning("OpenAI runtime is not ready: %s", capabilities.status)
    try:
        await _run_worker_loop(stop=stop, provider=provider, worker_id=worker_id)
    finally:
        await provider.close()
    logger.info("MacroLens analysis worker stopped")


if __name__ == "__main__":
    asyncio.run(run())
