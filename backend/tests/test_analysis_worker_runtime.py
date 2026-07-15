from __future__ import annotations

import asyncio
import sqlite3

import pytest

from app import analysis_worker


class _Provider:
    pass


def test_worker_loop_retries_transient_database_lock(monkeypatch):
    async def scenario() -> None:
        stop = asyncio.Event()
        attempts = 0
        poll_waits = 0

        async def run_news_once(**_kwargs):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise sqlite3.OperationalError("database is locked")
            stop.set()
            return False

        async def no_work(**_kwargs):
            return False

        async def wait_for_poll(_stop):
            nonlocal poll_waits
            poll_waits += 1

        monkeypatch.setattr(analysis_worker, "run_worker_once", run_news_once)
        monkeypatch.setattr(analysis_worker, "run_calendar_worker_once", no_work)
        monkeypatch.setattr(analysis_worker, "run_market_focus_worker_once", no_work)
        monkeypatch.setattr(analysis_worker, "_wait_for_next_poll", wait_for_poll)

        await analysis_worker._run_worker_loop(
            stop=stop,
            provider=_Provider(),
            worker_id="test-worker",
        )

        assert attempts == 2
        assert poll_waits == 2

    asyncio.run(scenario())


def test_worker_loop_does_not_hide_other_sqlite_errors(monkeypatch):
    async def fail_once(**_kwargs):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(analysis_worker, "run_worker_once", fail_once)

    async def scenario() -> None:
        with pytest.raises(sqlite3.OperationalError, match="disk I/O error"):
            await analysis_worker._run_worker_loop(
                stop=asyncio.Event(),
                provider=_Provider(),
                worker_id="test-worker",
            )

    asyncio.run(scenario())
