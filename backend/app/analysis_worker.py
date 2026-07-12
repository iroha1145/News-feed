from __future__ import annotations

import asyncio
import logging
import signal
import os
import socket
import uuid

from app.config import settings
from app.models.database import init_db
from app.services.analysis_jobs import run_worker_once
from app.services.calendar_analysis_jobs import run_calendar_worker_once
from app.services.responses_runtime import OpenAIResponsesProvider
from app.utils.http import configure_safe_network_logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
configure_safe_network_logging()
logger = logging.getLogger(__name__)


async def run() -> None:
    await init_db()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop.set)
        except NotImplementedError:
            pass
    logger.info("MacroLens analysis worker ready")
    worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    provider = OpenAIResponsesProvider()
    capabilities = provider.capabilities()
    if capabilities.status != "ok":
        logger.warning("OpenAI runtime is not ready: %s", capabilities.status)
    try:
        while not stop.is_set():
            worked_news = await run_worker_once(provider=provider, worker_id=worker_id)
            worked_calendar = await run_calendar_worker_once(
                provider=provider,
                worker_id=worker_id,
            )
            if worked_news or worked_calendar:
                continue
            try:
                await asyncio.wait_for(stop.wait(), timeout=settings.analysis_worker_poll_seconds)
            except asyncio.TimeoutError:
                pass
    finally:
        await provider.close()
    logger.info("MacroLens analysis worker stopped")


if __name__ == "__main__":
    asyncio.run(run())
