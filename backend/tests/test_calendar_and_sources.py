from __future__ import annotations

import json
import xml.etree.ElementTree as ET

import aiosqlite
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
    assert health[3] == "calendar_upstream_failed"


RAW_CALENDAR_EVENT = {
    "date": "2026-07-17T08:30:00-04:00",
    "title": "Producer Price Index",
    "country": "USD",
    "impact": "high",
    "forecast": "2.6%",
    "previous": "2.5%",
    "actual": "",
}


class _CalendarResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class _CalendarClient:
    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def get(self, *args, **kwargs):
        if isinstance(self.payload, RuntimeError):
            raise self.payload
        return _CalendarResponse(self.payload)


async def _seed_calendar_state():
    old_event = dict(CALENDAR_EVENT)
    db = await database.get_db()
    try:
        await database.record_calendar_snapshot(
            db,
            [old_event],
            source_fetched_at="2026-07-15T00:00:00Z",
            stale=False,
            observed_at="2026-07-15T00:00:01Z",
        )
        await database.upsert_source_health(
            db,
            source="faireconomy",
            status="ok",
            last_attempt_at="2026-07-15T00:00:00Z",
            last_success_at="2026-07-15T00:00:00Z",
            data_through="2026-07-15T00:00:00Z",
            consecutive_failures=0,
            next_attempt_at=None,
            raw_count=1,
            inserted_count=None,
            duplicates_count=None,
            error_code=None,
        )
    finally:
        await db.close()
    return old_event


async def _calendar_database_state():
    db = await database.get_db()
    try:
        async with db.execute(
            "SELECT COUNT(*),COALESCE(MAX(snapshot_sequence),0) FROM etl_calendar_snapshots"
        ) as cursor:
            snapshot_count, sequence = tuple(await cursor.fetchone())
        async with db.execute(
            """SELECT status,last_success_at,data_through,consecutive_failures,error_code
               FROM source_health WHERE source='faireconomy'"""
        ) as cursor:
            health = await cursor.fetchone()
    finally:
        await db.close()
    return (int(snapshot_count), int(sequence)), tuple(health) if health else None


def _prepare_calendar_client(monkeypatch, calendar_client, old_event, *, cache_time=321.0):
    old_cache = [{**old_event, "is_stale": False, "source_fetched_at": "2026-07-15T00:00:00Z"}]
    monkeypatch.setattr(calendar_client, "_calendar_cache", old_cache)
    monkeypatch.setattr(calendar_client, "_cache_time", cache_time)
    monkeypatch.setattr(calendar_client, "_cache_ttl", calendar_client.CACHE_TTL)
    monkeypatch.setattr(
        calendar_client,
        "_calendar_status",
        {
            "source": "faireconomy",
            "stale": False,
            "as_of": "2026-07-15T00:00:00Z",
            "last_attempt": "2026-07-15T00:00:00Z",
            "last_success": "2026-07-15T00:00:00Z",
            "last_error": None,
            "error_code": None,
        },
    )
    return old_cache


@pytest.mark.asyncio
async def test_calendar_snapshot_failure_rolls_back_and_keeps_memory(
    clean_db, tmp_path, monkeypatch
):
    from app.services import calendar_client

    old_event = await _seed_calendar_state()
    old_cache = _prepare_calendar_client(monkeypatch, calendar_client, old_event)
    before_db = await _calendar_database_state()
    original = calendar_client.record_calendar_snapshot

    async def fail_after_snapshot(*args, **kwargs):
        await original(*args, **kwargs)
        raise aiosqlite.OperationalError("simulated snapshot failure")

    monkeypatch.setattr(
        calendar_client.httpx,
        "AsyncClient",
        lambda *args, **kwargs: _CalendarClient([RAW_CALENDAR_EVENT]),
    )
    monkeypatch.setattr(calendar_client, "record_calendar_snapshot", fail_after_snapshot)
    monkeypatch.setattr(calendar_client, "CACHE_FILE", tmp_path / "calendar-cache.json")

    events = await calendar_client.fetch_economic_calendar(force=True)
    after_db = await _calendar_database_state()

    assert events == old_cache
    assert calendar_client._calendar_cache == old_cache
    assert calendar_client._cache_time == 321.0
    assert after_db[0] == before_db[0]
    assert after_db[1][1:3] == before_db[1][1:3]
    assert after_db[1][0] == "degraded"
    assert after_db[1][4] == "calendar_persistence_failed"
    assert calendar_client.get_calendar_status()["last_error"] == "calendar_persistence_failed"
    assert not calendar_client.CACHE_FILE.exists()


@pytest.mark.asyncio
async def test_calendar_source_health_failure_rolls_back_snapshot(
    clean_db, tmp_path, monkeypatch
):
    from app.services import calendar_client

    old_event = await _seed_calendar_state()
    old_cache = _prepare_calendar_client(monkeypatch, calendar_client, old_event)
    before_db = await _calendar_database_state()
    original = calendar_client.upsert_source_health

    async def fail_transaction_health(*args, **kwargs):
        if kwargs.get("commit") is False:
            raise aiosqlite.OperationalError("simulated health failure")
        return await original(*args, **kwargs)

    monkeypatch.setattr(
        calendar_client.httpx,
        "AsyncClient",
        lambda *args, **kwargs: _CalendarClient([RAW_CALENDAR_EVENT]),
    )
    monkeypatch.setattr(calendar_client, "upsert_source_health", fail_transaction_health)
    monkeypatch.setattr(calendar_client, "CACHE_FILE", tmp_path / "calendar-cache.json")

    events = await calendar_client.fetch_economic_calendar(force=True)
    after_db = await _calendar_database_state()

    assert events == old_cache
    assert after_db[0] == before_db[0]
    assert after_db[1][1:3] == before_db[1][1:3]
    assert after_db[1][4] == "calendar_source_health_persistence_failed"
    assert calendar_client.get_calendar_status()["last_error"] == (
        "calendar_source_health_persistence_failed"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_message",
    ["database is locked", "database or disk is full", "no such table: etl_calendar_snapshots"],
)
async def test_calendar_sqlite_failures_do_not_advance_cache_or_snapshot(
    clean_db, monkeypatch, failure_message
):
    from app.services import calendar_client

    old_event = await _seed_calendar_state()
    old_cache = _prepare_calendar_client(monkeypatch, calendar_client, old_event)
    before_db = await _calendar_database_state()

    async def fail_snapshot(*args, **kwargs):
        raise aiosqlite.OperationalError(failure_message)

    monkeypatch.setattr(
        calendar_client.httpx,
        "AsyncClient",
        lambda *args, **kwargs: _CalendarClient([RAW_CALENDAR_EVENT]),
    )
    monkeypatch.setattr(calendar_client, "record_calendar_snapshot", fail_snapshot)

    assert await calendar_client.fetch_economic_calendar(force=True) == old_cache
    assert calendar_client._cache_time == 321.0
    assert (await _calendar_database_state())[0] == before_db[0]


@pytest.mark.asyncio
async def test_stale_fallback_persistence_failure_keeps_prior_memory(
    clean_db, tmp_path, monkeypatch
):
    from app.services import calendar_client

    old_event = await _seed_calendar_state()
    old_cache = _prepare_calendar_client(monkeypatch, calendar_client, old_event)
    cache_path = tmp_path / "calendar-cache.json"
    cache_path.write_text(
        json.dumps(
            {"fetched_at": "2026-07-16T00:00:00Z", "events": [RAW_CALENDAR_EVENT]}
        ),
        encoding="utf-8",
    )

    async def fail_snapshot(*args, **kwargs):
        raise aiosqlite.OperationalError("simulated stale persistence failure")

    monkeypatch.setattr(calendar_client, "CACHE_FILE", cache_path)
    monkeypatch.setattr(
        calendar_client.httpx,
        "AsyncClient",
        lambda *args, **kwargs: _CalendarClient(RuntimeError("upstream offline")),
    )
    monkeypatch.setattr(calendar_client, "record_calendar_snapshot", fail_snapshot)

    assert await calendar_client.fetch_economic_calendar(force=True) == old_cache
    assert calendar_client._calendar_cache == old_cache
    assert calendar_client._cache_time == 321.0
    assert calendar_client.get_calendar_status()["last_error"] == "calendar_persistence_failed"


@pytest.mark.asyncio
async def test_calendar_recovers_on_the_next_successful_transaction(
    clean_db, tmp_path, monkeypatch
):
    from app.services import calendar_client

    old_event = await _seed_calendar_state()
    old_cache = _prepare_calendar_client(monkeypatch, calendar_client, old_event)
    before_db = await _calendar_database_state()
    original = calendar_client.record_calendar_snapshot
    calls = 0

    async def fail_once(*args, **kwargs):
        nonlocal calls
        calls += 1
        result = await original(*args, **kwargs)
        if calls == 1:
            raise aiosqlite.OperationalError("first transaction failed")
        return result

    monkeypatch.setattr(
        calendar_client.httpx,
        "AsyncClient",
        lambda *args, **kwargs: _CalendarClient([RAW_CALENDAR_EVENT]),
    )
    monkeypatch.setattr(calendar_client, "record_calendar_snapshot", fail_once)
    monkeypatch.setattr(calendar_client, "CACHE_FILE", tmp_path / "calendar-cache.json")

    assert await calendar_client.fetch_economic_calendar(force=True) == old_cache
    recovered = await calendar_client.fetch_economic_calendar(force=True)
    after_db = await _calendar_database_state()

    assert [event["title"] for event in recovered] == ["Producer Price Index"]
    assert calendar_client._cache_time != 321.0
    assert calendar_client.get_calendar_status()["last_error"] is None
    assert after_db[0][0] == before_db[0][0] + 1
    assert after_db[1][0] == "ok"


@pytest.mark.asyncio
async def test_calendar_parse_failure_uses_distinct_error_code(
    clean_db, tmp_path, monkeypatch
):
    from app.services import calendar_client

    monkeypatch.setattr(calendar_client, "CACHE_FILE", tmp_path / "missing-cache.json")
    monkeypatch.setattr(calendar_client, "_calendar_cache", [])
    monkeypatch.setattr(calendar_client, "_cache_time", 0.0)
    monkeypatch.setattr(
        calendar_client.httpx,
        "AsyncClient",
        lambda *args, **kwargs: _CalendarClient(ValueError("invalid json")),
    )

    assert await calendar_client.fetch_economic_calendar(force=True) == []
    state = await _calendar_database_state()
    assert state[1][4] == "calendar_parse_failed"
    assert calendar_client.get_calendar_status()["last_error"] == "calendar_parse_failed"


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
