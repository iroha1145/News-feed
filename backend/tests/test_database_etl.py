from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import aiosqlite
import pytest

from app.config import StorageConfig
from app.models import database


def _item(
    title: str,
    *,
    source: str = "source-a",
    ticker: str = "AMD",
    fetched_at: str = "2026-07-15T00:00:00Z",
    updated_at: str | None = None,
) -> dict:
    return {
        "source": source,
        "title": title,
        "summary": "raw summary",
        "url": f"https://example.com/{title.replace(' ', '-').lower()}/{source}",
        "image_url": None,
        "published_at": "2026-07-14T23:00:00Z",
        "fetched_at": fetched_at,
        "updated_at": updated_at or fetched_at,
        "source_tickers": [ticker],
    }


@pytest.mark.asyncio
async def test_fresh_schema_contains_only_etl_owned_tables(clean_db):
    db = await database.get_db()
    try:
        async with db.execute("SELECT name FROM sqlite_master WHERE type='table'") as cursor:
            names = {str(row[0]) for row in await cursor.fetchall()}
    finally:
        await db.close()
    assert {
        "news_items",
        "news_changes",
        "news_source_observations",
        "source_health",
        "etl_calendar_snapshots",
        "etl_calendar_events",
    } <= names
    assert "analyses" not in names
    assert "analysis_jobs" not in names


@pytest.mark.asyncio
async def test_legacy_analysis_table_is_preserved_and_not_migrated(tmp_path, monkeypatch):
    path = tmp_path / "legacy.db"
    async with aiosqlite.connect(path) as db:
        await db.execute(
            """CREATE TABLE news_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL,title TEXT NOT NULL,
                summary TEXT,url TEXT NOT NULL,image_url TEXT,published_at TEXT,
                fetched_at TEXT NOT NULL,content_hash TEXT NOT NULL UNIQUE
            )"""
        )
        await db.execute(
            "CREATE TABLE analyses (id INTEGER PRIMARY KEY,news_id INTEGER,payload TEXT)"
        )
        await db.execute(
            """INSERT INTO news_items
               (source,title,summary,url,image_url,published_at,fetched_at,content_hash)
               VALUES ('legacy','Old item','old','https://example.com/old',NULL,
                       '2025-01-01T00:00:00Z','2025-01-01T00:01:00Z','legacy-hash')"""
        )
        await db.execute("INSERT INTO analyses VALUES (7,1,'historic analysis')")
        await db.commit()

    migration_time = datetime(2026, 7, 15, 12, 34, 56, 789, tzinfo=timezone.utc)
    monkeypatch.setattr(database, "DB_PATH", path)
    monkeypatch.setattr(database, "utc_now", lambda: migration_time)
    await database.init_db()
    db = await database.get_db()
    try:
        async with db.execute("SELECT * FROM analyses") as cursor:
            rows = [tuple(row) for row in await cursor.fetchall()]
        async with db.execute("PRAGMA table_info(analyses)") as cursor:
            columns = [str(row[1]) for row in await cursor.fetchall()]
        async with db.execute("SELECT source_tickers,updated_at FROM news_items WHERE id=1") as cursor:
            raw = await cursor.fetchone()
        async with db.execute("SELECT COUNT(*) FROM news_changes WHERE news_id=1") as cursor:
            baseline = int((await cursor.fetchone())[0])
        async with db.execute(
            """SELECT updated_at,available_at FROM news_changes
               WHERE news_id=1 ORDER BY change_sequence LIMIT 1"""
        ) as cursor:
            source_updated_at, available_at = tuple(await cursor.fetchone())
        async with db.execute(
            """SELECT observed_at FROM news_source_observations
               WHERE canonical_news_id=1 ORDER BY observation_id LIMIT 1"""
        ) as cursor:
            observed_at = str((await cursor.fetchone())[0])
    finally:
        await db.close()
    assert rows == [(7, 1, "historic analysis")]
    assert columns == ["id", "news_id", "payload"]
    assert raw[0] == "[]"
    assert raw[1] == "2025-01-01T00:01:00Z"
    assert baseline == 1
    assert source_updated_at == "2025-01-01T00:01:00Z"
    assert available_at == "2026-07-15T12:34:56.000789Z"
    assert observed_at == available_at


@pytest.mark.asyncio
async def test_invalid_published_at_is_omitted_from_wire_data(clean_db):
    item = _item("Bad source date")
    item["published_at"] = "sometime yesterday"
    db = await database.get_db()
    try:
        news_id = await database.insert_news_item(db, item)
        async with db.execute(
            "SELECT published_at FROM news_items WHERE id=?", (news_id,)
        ) as cursor:
            stored = await cursor.fetchone()
        watermark = await database.news_change_watermark(
            db, as_of=database.utc_now() + timedelta(seconds=1)
        )
        changes, _ = await database.query_news_changes(
            db,
            updated_after=datetime(1970, 1, 1, tzinfo=timezone.utc),
            as_of=database.utc_now() + timedelta(seconds=1),
            after_sequence=0,
            checkpoint_sequence=0,
            watermark_sequence=watermark,
            limit=10,
        )
    finally:
        await db.close()
    assert stored[0] is None
    assert changes[0]["news"]["published_at"] is None


@pytest.mark.asyncio
async def test_upgrade_removes_obsolete_integration_write_trigger(tmp_path, monkeypatch):
    path = tmp_path / "legacy-trigger.db"
    async with aiosqlite.connect(path) as db:
        await db.execute(
            """CREATE TABLE news_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,source TEXT NOT NULL,title TEXT NOT NULL,
                summary TEXT,url TEXT NOT NULL,image_url TEXT,published_at TEXT,
                fetched_at TEXT NOT NULL,content_hash TEXT NOT NULL UNIQUE,
                source_tickers TEXT NOT NULL DEFAULT '[]',updated_at TEXT
            )"""
        )
        await db.execute(
            "CREATE TABLE integration_changes (change_sequence INTEGER PRIMARY KEY AUTOINCREMENT,entity_id TEXT)"
        )
        await db.execute(
            """CREATE TABLE source_health (
                source TEXT PRIMARY KEY,status TEXT NOT NULL,last_attempt_at TEXT,
                last_success_at TEXT,data_through TEXT,consecutive_failures INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT,raw_count INTEGER,inserted_count INTEGER,duplicates_count INTEGER,
                error_code TEXT,source_fetch_status TEXT NOT NULL DEFAULT 'unavailable',
                news_persistence_status TEXT NOT NULL DEFAULT 'unavailable',
                event_projection_status TEXT NOT NULL DEFAULT 'unavailable',updated_at TEXT NOT NULL
            )"""
        )
        await db.execute(
            """CREATE TRIGGER trg_news_integration_insert AFTER INSERT ON news_items
               BEGIN INSERT INTO integration_changes(entity_id) VALUES (CAST(NEW.id AS TEXT)); END"""
        )
        await db.execute(
            """CREATE TRIGGER trg_source_health_integration_insert AFTER INSERT ON source_health
               BEGIN INSERT INTO integration_changes(entity_id) VALUES (NEW.source); END"""
        )
        await db.commit()
    monkeypatch.setattr(database, "DB_PATH", path)
    await database.init_db()
    db = await database.get_db()
    try:
        await database.insert_news_item(db, _item("Post-upgrade item"))
        await database.upsert_source_health(
            db,
            source="legacy-source",
            status="ok",
            last_attempt_at="2026-07-15T00:00:00Z",
            last_success_at="2026-07-15T00:00:00Z",
            data_through="2026-07-15T00:00:00Z",
            consecutive_failures=0,
            next_attempt_at=None,
            raw_count=1,
            inserted_count=1,
            duplicates_count=0,
            error_code=None,
        )
        async with db.execute("SELECT COUNT(*) FROM integration_changes") as cursor:
            legacy_writes = int((await cursor.fetchone())[0])
        async with db.execute(
            """SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'
               AND name IN ('trg_news_integration_insert','trg_source_health_integration_insert')"""
        ) as cursor:
            trigger_count = int((await cursor.fetchone())[0])
        async with db.execute(
            """SELECT source_fetch_status,news_persistence_status,event_projection_status
               FROM source_health WHERE source='legacy-source'"""
        ) as cursor:
            compatibility_defaults = tuple(await cursor.fetchone())
    finally:
        await db.close()
    assert legacy_writes == 0
    assert trigger_count == 0
    assert compatibility_defaults == ("unavailable", "unavailable", "unavailable")


@pytest.mark.asyncio
async def test_dedup_merges_only_source_native_tickers(clean_db):
    db = await database.get_db()
    try:
        result = await database.insert_news_items_batch(
            db,
            [
                _item("Chip demand rises", source="finnhub", ticker="AMD"),
                _item("Chip demand rises", source="massive", ticker="NVDA"),
            ],
        )
        async with db.execute("SELECT source_tickers FROM news_items") as cursor:
            tickers = json.loads((await cursor.fetchone())[0])
        async with db.execute("SELECT COUNT(*) FROM news_changes") as cursor:
            changes = int((await cursor.fetchone())[0])
        async with db.execute(
            """SELECT canonical_news_id,source,original_title,original_url,
                      source_tickers,observed_at,observation_hash
               FROM news_source_observations ORDER BY observation_id"""
        ) as cursor:
            observations = await cursor.fetchall()
    finally:
        await db.close()
    assert result == {"inserted": 1, "duplicates": 1}
    assert tickers == ["AMD", "NVDA"]
    assert changes == 2
    assert [str(row["source"]) for row in observations] == ["finnhub", "massive"]
    assert len({str(row["observation_hash"]) for row in observations}) == 2
    assert all(int(row["canonical_news_id"]) == 1 for row in observations)
    assert all(str(row["original_title"]) == "Chip demand rises" for row in observations)
    assert all(str(row["original_url"]).startswith("https://example.com/") for row in observations)
    assert all(str(row["observed_at"]).endswith("Z") for row in observations)


@pytest.mark.asyncio
async def test_incremental_window_uses_local_availability_not_old_source_time(clean_db):
    before = database.utc_now() - timedelta(milliseconds=1)
    db = await database.get_db()
    try:
        news_id = await database.insert_news_item(
            db,
            _item(
                "Late arrival",
                fetched_at="2026-07-15T00:00:00Z",
                updated_at="2020-01-01T00:00:00Z",
            ),
        )
        cutoff = database.utc_now() + timedelta(milliseconds=1)
        watermark = await database.news_change_watermark(
            db, as_of=cutoff
        )
        changes, more = await database.query_news_changes(
            db,
            updated_after=before,
            as_of=cutoff,
            after_sequence=0,
            watermark_sequence=watermark,
            limit=10,
        )
    finally:
        await db.close()
    assert news_id is not None
    assert not more
    assert [change["news_id"] for change in changes] == [news_id]
    assert changes[0]["source_updated_at"] == "2020-01-01T00:00:00Z"
    assert changes[0]["changed_at"] > "2020-01-01T00:00:00Z"


@pytest.mark.asyncio
async def test_incremental_window_keeps_microsecond_precision(clean_db, monkeypatch):
    available = datetime(2026, 7, 15, 12, 0, 0, 500, tzinfo=timezone.utc)
    monkeypatch.setattr(database, "utc_now", lambda: available)
    db = await database.get_db()
    try:
        news_id = await database.insert_news_item(db, _item("Microsecond boundary"))
        watermark = await database.news_change_watermark(
            db, as_of=available + timedelta(microseconds=100)
        )
        changes, _ = await database.query_news_changes(
            db,
            updated_after=available - timedelta(microseconds=100),
            as_of=available + timedelta(microseconds=100),
            after_sequence=0,
            watermark_sequence=watermark,
            limit=10,
        )
    finally:
        await db.close()
    assert [change["news_id"] for change in changes] == [news_id]


@pytest.mark.asyncio
async def test_retention_writes_tombstone_and_protects_legacy_foreign_key(clean_db, monkeypatch):
    db = await database.get_db()
    try:
        unreferenced_item = _item("Unreferenced old", fetched_at="2020-01-01T00:00:00Z")
        unreferenced_item["published_at"] = "2020-01-01T00:00:00Z"
        referenced_item = _item("Referenced old", fetched_at="2020-01-01T00:00:00Z")
        referenced_item["published_at"] = "2020-01-01T00:00:00Z"
        unreferenced = await database.insert_news_item(db, unreferenced_item)
        referenced = await database.insert_news_item(db, referenced_item)
        await db.execute(
            """CREATE TABLE legacy_analysis (
                id INTEGER PRIMARY KEY,
                news_id INTEGER NOT NULL REFERENCES news_items(id) ON DELETE CASCADE,
                payload TEXT NOT NULL
            )"""
        )
        await db.execute(
            "INSERT INTO legacy_analysis VALUES (1,?,'must remain')", (referenced,)
        )
        await db.commit()
        storage = StorageConfig(
            database_path=clean_db,
            news_retention_days=1,
            change_retention_days=365,
            calendar_snapshot_retention_days=90,
            retention_interval_seconds=3600,
        )
        monkeypatch.setattr(database, "settings", SimpleNamespace(storage=storage))
        result = await database.cleanup_retained_data(
            db, now=datetime(2026, 7, 15, tzinfo=timezone.utc)
        )
        async with db.execute("SELECT id FROM news_items ORDER BY id") as cursor:
            remaining = [int(row[0]) for row in await cursor.fetchall()]
        async with db.execute("SELECT * FROM legacy_analysis") as cursor:
            legacy = [tuple(row) for row in await cursor.fetchall()]
        async with db.execute(
            "SELECT operation FROM news_changes WHERE news_id=? ORDER BY change_sequence DESC LIMIT 1",
            (unreferenced,),
        ) as cursor:
            tombstone = str((await cursor.fetchone())[0])
    finally:
        await db.close()
    assert result["news_items"] == 1
    assert remaining == [referenced]
    assert legacy == [(1, referenced, "must remain")]
    assert tombstone == "delete"


@pytest.mark.asyncio
async def test_retention_protects_legacy_logical_news_link_without_foreign_key(
    clean_db, monkeypatch
):
    db = await database.get_db()
    try:
        old_item = _item("Logical reference", fetched_at="2020-01-01T00:00:00Z")
        old_item["published_at"] = "2020-01-01T00:00:00Z"
        news_id = await database.insert_news_item(db, old_item)
        await db.execute(
            "CREATE TABLE analyses (id INTEGER PRIMARY KEY,news_id INTEGER,payload TEXT)"
        )
        await db.execute("INSERT INTO analyses VALUES (1,?,'historic')", (news_id,))
        await db.commit()
        storage = StorageConfig(
            database_path=clean_db,
            news_retention_days=1,
            change_retention_days=365,
            calendar_snapshot_retention_days=90,
            retention_interval_seconds=3600,
        )
        monkeypatch.setattr(database, "settings", SimpleNamespace(storage=storage))
        await database.cleanup_retained_data(
            db, now=datetime(2026, 7, 15, tzinfo=timezone.utc)
        )
        async with db.execute("SELECT COUNT(*) FROM news_items WHERE id=?", (news_id,)) as cursor:
            news_count = int((await cursor.fetchone())[0])
        async with db.execute("SELECT payload FROM analyses WHERE id=1") as cursor:
            payload = str((await cursor.fetchone())[0])
    finally:
        await db.close()
    assert news_count == 1
    assert payload == "historic"
