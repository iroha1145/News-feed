from __future__ import annotations

import json
import xml.etree.ElementTree as ET

import pytest

from app.models import database
from app.services import finnhub_client, massive_client, seekingalpha_client


CALENDAR_EVENT = {
    "date": "2026-07-16T08:30:00-04:00",
    "title": "Consumer Price Index",
    "country_code": "USD",
    "country": "美国",
    "impact": "high",
    "impact_zh": "高",
    "forecast": "2.5%",
    "previous": "2.4%",
    "actual": "",
}


@pytest.mark.asyncio
async def test_identical_stale_calendar_cache_reuses_complete_snapshot(clean_db):
    db = await database.get_db()
    try:
        token1, sequence1 = await database.record_calendar_snapshot(
            db,
            [CALENDAR_EVENT],
            source_fetched_at="2026-07-15T00:00:00Z",
            stale=True,
            observed_at="2026-07-15T01:00:00Z",
        )
        token2, sequence2 = await database.record_calendar_snapshot(
            db,
            [CALENDAR_EVENT],
            source_fetched_at="2026-07-15T00:00:00Z",
            stale=True,
            observed_at="2026-07-15T02:00:00Z",
        )
        items, more = await database.query_calendar_page(
            db, snapshot_sequence=sequence2, after_ordinal=0, limit=10
        )
    finally:
        await db.close()
    assert (token2, sequence2) == (token1, sequence1)
    assert not more
    assert [item["title"] for item in items] == ["Consumer Price Index"]


@pytest.mark.asyncio
async def test_equal_fresh_values_with_new_fetch_time_advance_data_through(clean_db):
    db = await database.get_db()
    try:
        _, first = await database.record_calendar_snapshot(
            db,
            [CALENDAR_EVENT],
            source_fetched_at="2026-07-15T01:00:00Z",
            stale=False,
            observed_at="2026-07-15T01:00:01Z",
        )
        _, second = await database.record_calendar_snapshot(
            db,
            [CALENDAR_EVENT],
            source_fetched_at="2026-07-15T02:00:00Z",
            stale=False,
            observed_at="2026-07-15T02:00:01Z",
        )
        async with db.execute(
            "SELECT data_through FROM etl_calendar_snapshots WHERE snapshot_sequence=?",
            (second,),
        ) as cursor:
            data_through = str((await cursor.fetchone())[0])
    finally:
        await db.close()
    assert second > first
    assert data_through == "2026-07-15T02:00:00Z"


@pytest.mark.asyncio
async def test_calendar_failure_without_cache_persists_unavailable_health(
    clean_db, tmp_path, monkeypatch
):
    from app.services import calendar_client

    class FailedClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def get(self, *args, **kwargs):
            raise RuntimeError("offline test failure")

    monkeypatch.setattr(calendar_client, "CACHE_FILE", tmp_path / "missing-cache.json")
    monkeypatch.setattr(calendar_client.httpx, "AsyncClient", lambda *args, **kwargs: FailedClient())
    monkeypatch.setattr(calendar_client, "_calendar_cache", [])
    monkeypatch.setattr(calendar_client, "_cache_time", 0.0)
    events = await calendar_client.fetch_economic_calendar(force=True)
    db = await database.get_db()
    try:
        async with db.execute(
            """SELECT status,consecutive_failures,next_attempt_at,error_code
               FROM source_health WHERE source='faireconomy'"""
        ) as cursor:
            health = await cursor.fetchone()
    finally:
        await db.close()
    assert events == []
    assert tuple(health)[:2] == ("unavailable", 1)
    assert health[2]
    assert health[3] == "calendar_source_failed"


def test_source_parsers_preserve_native_tickers():
    finnhub = finnhub_client._parse_item(
        {
            "headline": "Chip news",
            "url": "https://example.com/finnhub",
            "datetime": 1_752_000_000,
            "related": "AMD,NVDA",
            "source": "wire",
        }
    )
    massive = massive_client._parse_item(
        {
            "title": "Chip news",
            "article_url": "https://example.com/massive",
            "published_utc": "2026-07-15T00:00:00Z",
            "publisher": {"name": "wire"},
            "tickers": ["AMD", "NVDA"],
        }
    )
    element = ET.fromstring(
        """<item><title>Chip news</title><link>https://example.com/sa</link>
        <pubDate>Wed, 15 Jul 2026 00:00:00 GMT</pubDate>
        <category domain="symbol">AMD</category></item>"""
    )
    seekingalpha = seekingalpha_client._parse_sa_item(element)
    assert finnhub["source_tickers"] == ["AMD", "NVDA"]
    assert massive["source_tickers"] == ["AMD", "NVDA"]
    assert seekingalpha["source_tickers"] == ["AMD"]


@pytest.mark.asyncio
async def test_aggregator_is_offline_testable_and_persists_health(clean_db, monkeypatch):
    from app.services import news_aggregator

    async def fake_fetch():
        return [
            {
                "source": "offline-a",
                "title": "Offline duplicate",
                "summary": None,
                "url": "https://example.com/offline/a",
                "image_url": None,
                "published_at": "2026-07-15T00:00:00Z",
                "source_tickers": ["AMD"],
            },
            {
                "source": "offline-b",
                "title": "Offline duplicate",
                "summary": None,
                "url": "https://example.com/offline/b",
                "image_url": None,
                "published_at": "2026-07-15T00:00:00Z",
                "source_tickers": ["NVDA"],
            },
        ]

    monkeypatch.setitem(
        news_aggregator.SOURCE_DEFINITIONS,
        "google",
        news_aggregator.SourceDefinition("google", fake_fetch),
    )
    result = await news_aggregator.aggregate_source("google", force=True)
    db = await database.get_db()
    try:
        async with db.execute("SELECT source_tickers FROM news_items") as cursor:
            tickers = json.loads((await cursor.fetchone())[0])
        async with db.execute(
            "SELECT status,data_through FROM source_health WHERE source='google'"
        ) as cursor:
            health = await cursor.fetchone()
    finally:
        await db.close()
    assert result["inserted"] == 1
    assert result["duplicates"] == 1
    assert tickers == ["AMD", "NVDA"]
    assert tuple(health)[0] == "ok"
    assert health[1]


@pytest.mark.asyncio
async def test_scheduler_contains_only_fetch_calendar_and_retention_jobs(clean_db):
    from app.utils.scheduler import get_scheduler, start_scheduler, stop_scheduler

    await start_scheduler()
    try:
        scheduler = get_scheduler()
        job_ids = {job.id for job in scheduler.get_jobs()}
    finally:
        stop_scheduler()
    assert "fetch_economic_calendar" in job_ids
    assert "etl_retention" in job_ids
    assert any(job_id.startswith("fetch_news_") for job_id in job_ids)
    assert not any(
        word in job_id
        for job_id in job_ids
        for word in ("analysis", "projection", "focus", "sentiment", "worker")
    )


@pytest.mark.asyncio
async def test_scheduler_does_not_log_empty_failed_calendar_as_success(
    clean_db, monkeypatch, caplog
):
    from app.services import calendar_client
    from app.utils import scheduler

    async def failed_refresh(*, force=False):
        return []

    monkeypatch.setattr(calendar_client, "fetch_economic_calendar", failed_refresh)
    monkeypatch.setattr(
        calendar_client,
        "get_calendar_status",
        lambda: {"last_error": "offline", "stale": True},
    )
    with caplog.at_level("WARNING"):
        await scheduler._job_fetch_calendar()
    assert "unavailable" in caplog.text
    assert "refreshed" not in caplog.text
