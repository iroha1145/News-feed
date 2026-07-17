from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

import aiosqlite

from app.config import settings
from app.utils.dedup import (
    compute_content_hash,
    compute_legacy_content_hash,
    normalize_title,
    normalize_url,
    publication_bucket,
    similar_titles,
)
from app.utils.tickers import normalize_ticker

logger = logging.getLogger(__name__)

DB_PATH = settings.database_path
SQLITE_BUSY_TIMEOUT_MS = 30_000
RETENTION_BATCH_SIZE = 500
EXACT_KEY_QUERY_CHUNK_SIZE = 250
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


CREATE_NEWS_ITEMS = """
CREATE TABLE IF NOT EXISTS news_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT,
    url TEXT NOT NULL,
    image_url TEXT,
    published_at TEXT,
    fetched_at TEXT NOT NULL,
    content_hash TEXT NOT NULL UNIQUE,
    source_tickers TEXT NOT NULL DEFAULT '[]',
    updated_at TEXT NOT NULL
)
"""

CREATE_NEWS_CHANGES = """
CREATE TABLE IF NOT EXISTS news_changes (
    change_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    news_id INTEGER NOT NULL,
    operation TEXT NOT NULL CHECK(operation IN ('upsert','delete')),
    source TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT,
    url TEXT NOT NULL,
    image_url TEXT,
    published_at TEXT,
    fetched_at TEXT NOT NULL,
    source_tickers TEXT NOT NULL DEFAULT '[]',
    content_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    available_at_us INTEGER NOT NULL,
    payload_hash TEXT NOT NULL,
    UNIQUE(news_id, operation, payload_hash, updated_at)
)
"""

CREATE_NEWS_SOURCE_OBSERVATIONS = """
CREATE TABLE IF NOT EXISTS news_source_observations (
    observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_news_id INTEGER NOT NULL
        REFERENCES news_items(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    original_title TEXT NOT NULL,
    original_url TEXT NOT NULL,
    source_tickers TEXT NOT NULL DEFAULT '[]',
    observed_at TEXT NOT NULL,
    observed_at_us INTEGER NOT NULL,
    observation_hash TEXT NOT NULL,
    UNIQUE(canonical_news_id, observation_hash)
)
"""

CREATE_SOURCE_HEALTH = """
CREATE TABLE IF NOT EXISTS source_health (
    source TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK(status IN (
        'ok','degraded','unavailable','not_configured','disabled'
    )),
    last_attempt_at TEXT,
    last_success_at TEXT,
    data_through TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK(consecutive_failures >= 0),
    next_attempt_at TEXT,
    raw_count INTEGER CHECK(raw_count IS NULL OR raw_count >= 0),
    inserted_count INTEGER CHECK(inserted_count IS NULL OR inserted_count >= 0),
    duplicates_count INTEGER CHECK(duplicates_count IS NULL OR duplicates_count >= 0),
    error_code TEXT,
    updated_at TEXT NOT NULL
)
"""

CREATE_CALENDAR_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS etl_calendar_snapshots (
    snapshot_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_token TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    source_fetched_at TEXT NOT NULL,
    data_through TEXT,
    is_stale INTEGER NOT NULL DEFAULT 0 CHECK(is_stale IN (0,1)),
    available_at TEXT NOT NULL,
    available_at_us INTEGER NOT NULL,
    payload_hash TEXT NOT NULL
)
"""

CREATE_CALENDAR_EVENTS = """
CREATE TABLE IF NOT EXISTS etl_calendar_events (
    snapshot_sequence INTEGER NOT NULL
        REFERENCES etl_calendar_snapshots(snapshot_sequence) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 1),
    event_id TEXT NOT NULL,
    country_code TEXT NOT NULL,
    country TEXT NOT NULL,
    title TEXT NOT NULL,
    impact TEXT NOT NULL CHECK(impact IN ('low','medium','high','holiday')),
    impact_zh TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    scheduled_at_utc TEXT NOT NULL,
    forecast TEXT,
    previous TEXT,
    actual TEXT,
    is_stale INTEGER NOT NULL DEFAULT 0 CHECK(is_stale IN (0,1)),
    source_fetched_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    PRIMARY KEY(snapshot_sequence, ordinal),
    UNIQUE(snapshot_sequence, event_id)
)
"""

RAW_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_news_published_at ON news_items(published_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_news_effective_time ON news_items(COALESCE(published_at,fetched_at) DESC)",
    "CREATE INDEX IF NOT EXISTS idx_news_source_time ON news_items(source,COALESCE(published_at,fetched_at) DESC)",
    "CREATE INDEX IF NOT EXISTS idx_news_url ON news_items(url)",
    "CREATE INDEX IF NOT EXISTS idx_news_changes_window ON news_changes(available_at_us,change_sequence)",
    "CREATE INDEX IF NOT EXISTS idx_news_changes_item ON news_changes(news_id,available_at DESC,change_sequence DESC)",
    "CREATE INDEX IF NOT EXISTS idx_news_observations_item ON news_source_observations(canonical_news_id,observed_at_us,observation_id)",
    "CREATE INDEX IF NOT EXISTS idx_calendar_snapshots_available ON etl_calendar_snapshots(available_at_us DESC,snapshot_sequence DESC)",
    "CREATE INDEX IF NOT EXISTS idx_calendar_events_time ON etl_calendar_events(snapshot_sequence,scheduled_at_utc,ordinal)",
)

LEGACY_INTEGRATION_TRIGGERS = (
    "trg_news_integration_insert",
    "trg_analysis_revision_integration_insert",
    "trg_ticker_validation_integration_insert",
    "trg_calendar_revision_integration_insert",
    "trg_source_health_integration_insert",
    "trg_source_health_integration_update",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_text(value: datetime | None = None) -> str:
    current = value or utc_now()
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("timestamp must include a UTC offset")
    return current.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_utc(value: str | datetime, *, field: str = "timestamp") -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def _legacy_wire_utc_text(
    value: Any,
    *,
    field: str,
    optional: bool = False,
) -> str | None:
    """Canonicalize known legacy UTC values without weakening write validation.

    Early MacroLens releases stored UTC ``datetime`` values without an offset.
    Keep the database history unchanged, but restore the missing UTC marker when
    those rows cross the current internal API boundary.
    """

    if value is None or not str(value).strip():
        if optional:
            return None
        raise ValueError(f"{field} must be a timestamp")
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError as exc:
        if optional:
            return None
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return utc_text(parsed)


def epoch_microseconds(value: str | datetime) -> int:
    parsed = parse_utc(value)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = parsed - epoch
    return (
        delta.days * 86_400_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )


def _database_path() -> Path:
    return Path(DB_PATH)


async def get_db() -> aiosqlite.Connection:
    path = _database_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(path, timeout=SQLITE_BUSY_TIMEOUT_MS / 1_000)
    db.row_factory = aiosqlite.Row
    await db.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    await db.execute("PRAGMA foreign_keys=ON")
    return db


async def _add_raw_column(
    db: aiosqlite.Connection,
    table: str,
    column: str,
    definition: str,
) -> None:
    try:
        await db.execute(f'ALTER TABLE "{table}" ADD COLUMN "{column}" {definition}')
    except aiosqlite.OperationalError as exc:
        if "duplicate column" not in str(exc).lower():
            raise


async def init_db() -> None:
    """Create only ETL-owned tables and additive raw-news columns.

    Legacy analysis tables are intentionally neither created, migrated, updated,
    nor deleted. They may remain in an upgraded database for historical reads.
    """

    db = await get_db()
    migration_now = utc_text()
    try:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute(CREATE_NEWS_ITEMS)
        # Older databases received these raw columns from the former Catalyst
        # migration. Keep that upgrade here before the old module is removed.
        await _add_raw_column(db, "news_items", "source_tickers", "TEXT NOT NULL DEFAULT '[]'")
        await _add_raw_column(db, "news_items", "updated_at", "TEXT")
        await db.execute(
            "UPDATE news_items SET updated_at=COALESCE(updated_at,fetched_at) WHERE updated_at IS NULL"
        )
        await db.execute(CREATE_NEWS_CHANGES)
        await db.execute(CREATE_NEWS_SOURCE_OBSERVATIONS)
        await db.execute(CREATE_SOURCE_HEALTH)
        await db.execute(CREATE_CALENDAR_SNAPSHOTS)
        await db.execute(CREATE_CALENDAR_EVENTS)
        # These triggers belonged to the removed signed integration surface.
        # Leaving them behind would make raw ETL writes mutate its obsolete
        # change table even though no reader remains.
        for trigger in LEGACY_INTEGRATION_TRIGGERS:
            await db.execute(f'DROP TRIGGER IF EXISTS "{trigger}"')
        for statement in RAW_INDEXES:
            await db.execute(statement)

        # Older databases discarded duplicate-source evidence. Preserve the
        # canonical source as the first observation without touching any
        # historical analysis table.
        async with db.execute(
            """SELECT id,source,title,url,source_tickers
               FROM news_items
               WHERE NOT EXISTS (
                   SELECT 1 FROM news_source_observations o
                   WHERE o.canonical_news_id=news_items.id
               )"""
        ) as cursor:
            while True:
                rows = await cursor.fetchmany(RETENTION_BATCH_SIZE)
                if not rows:
                    break
                for row in rows:
                    await _record_news_observation(
                        db,
                        canonical_news_id=int(row["id"]),
                        source=str(row["source"]),
                        original_title=str(row["title"]),
                        original_url=str(row["url"]),
                        source_tickers=str(row["source_tickers"] or "[]"),
                        observed_at=migration_now,
                    )

        # Backfill a point-in-time baseline for pre-ETL raw rows. This touches
        # only the new change log; the source row and all legacy analysis data
        # remain unchanged.
        await db.execute(
            """INSERT OR IGNORE INTO news_changes
               (news_id,operation,source,title,summary,url,image_url,published_at,
                fetched_at,source_tickers,content_hash,updated_at,available_at,
                available_at_us,payload_hash)
               SELECT id,'upsert',source,title,summary,url,image_url,published_at,
                      fetched_at,COALESCE(source_tickers,'[]'),content_hash,
                      COALESCE(updated_at,fetched_at),?,?,content_hash
               FROM news_items n
               WHERE NOT EXISTS (
                   SELECT 1 FROM news_changes c WHERE c.news_id=n.id
               )""",
            (migration_now, epoch_microseconds(migration_now)),
        )
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    finally:
        await db.close()


def _serialize_source_tickers(value: Any) -> str:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            value = []
    if not isinstance(value, list):
        value = []
    tickers: list[str] = []
    for item in value:
        ticker = normalize_ticker(item)
        if ticker and ticker not in tickers:
            tickers.append(ticker)
    return json.dumps(tickers[:100], separators=(",", ":"))


def _deserialize_tickers(value: Any) -> list[str]:
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return [str(item) for item in parsed if isinstance(item, str)] if isinstance(parsed, list) else []


def _observation_hash(
    *,
    source: str,
    original_title: str,
    original_url: str,
    source_tickers: str,
) -> str:
    payload = {
        "source": source.strip().casefold(),
        "title": original_title.strip(),
        "url": normalize_url(original_url),
        "source_tickers": _deserialize_tickers(_serialize_source_tickers(source_tickers)),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


async def _record_news_observation(
    db: aiosqlite.Connection,
    *,
    canonical_news_id: int,
    source: str,
    original_title: str,
    original_url: str,
    source_tickers: str,
    observed_at: str | None = None,
) -> bool:
    try:
        observation_time = utc_text(
            parse_utc(str(observed_at or utc_text()), field="observed_at")
        )
    except ValueError:
        observation_time = utc_text()
    serialized_tickers = _serialize_source_tickers(source_tickers)
    stable_hash = _observation_hash(
        source=source,
        original_title=original_title,
        original_url=original_url,
        source_tickers=serialized_tickers,
    )
    cursor = await db.execute(
        """INSERT OR IGNORE INTO news_source_observations
           (canonical_news_id,source,original_title,original_url,source_tickers,
            observed_at,observed_at_us,observation_hash)
           VALUES (?,?,?,?,?,?,?,?)""",
        (
            canonical_news_id,
            source[:500],
            original_title[:4_000],
            original_url[:8_000],
            serialized_tickers,
            observation_time,
            epoch_microseconds(observation_time),
            stable_hash,
        ),
    )
    return cursor.rowcount == 1


def _prepare_news(item: dict[str, Any]) -> dict[str, Any]:
    fetched_at = utc_text(parse_utc(str(item.get("fetched_at") or utc_text()), field="fetched_at"))
    published = item.get("published_at")
    published_at = None
    if published:
        try:
            published_at = utc_text(parse_utc(str(published), field="published_at"))
        except ValueError:
            # The internal API contract accepts only timezone-aware ISO-8601.
            # Keep the news item but omit a malformed source timestamp.
            published_at = None
    updated_at = utc_text(
        parse_utc(str(item.get("updated_at") or fetched_at), field="updated_at")
    )
    original_title = str(item.get("title") or "").strip()
    original_url = str(item.get("url") or "").strip()
    title = original_title
    url = normalize_url(original_url)
    source = str(item.get("source") or "").strip()
    if not source or not title or not url:
        raise ValueError("source, title, and url are required")
    content_hash = str(item.get("content_hash") or "") or compute_content_hash(
        title, url, published_at
    )
    return {
        "source": source[:500],
        "title": title[:4_000],
        "summary": str(item["summary"])[:50_000] if item.get("summary") is not None else None,
        "url": url[:8_000],
        "image_url": str(item["image_url"])[:8_000] if item.get("image_url") else None,
        "published_at": published_at,
        "fetched_at": fetched_at,
        "content_hash": content_hash,
        "legacy_content_hash": str(item.get("legacy_content_hash") or "")
        or compute_legacy_content_hash(title, url),
        "source_tickers": _serialize_source_tickers(item.get("source_tickers")),
        "updated_at": updated_at,
        "observation_title": original_title[:4_000],
        "observation_url": original_url[:8_000],
    }


async def _record_news_change(
    db: aiosqlite.Connection,
    row: MappingLike,
    *,
    operation: str,
    available_at: str,
    changed_at: str | None = None,
) -> int:
    updated_at = changed_at or str(row["updated_at"])
    payload_hash = hashlib.sha256(
        json.dumps(
            {
                "operation": operation,
                "news_id": int(row["id"]),
                "content_hash": str(row["content_hash"]),
                "updated_at": updated_at,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()
    cursor = await db.execute(
        """INSERT OR IGNORE INTO news_changes
           (news_id,operation,source,title,summary,url,image_url,published_at,
            fetched_at,source_tickers,content_hash,updated_at,available_at,
            available_at_us,payload_hash)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            int(row["id"]),
            operation,
            str(row["source"]),
            str(row["title"]),
            row["summary"],
            str(row["url"]),
            row["image_url"],
            row["published_at"],
            str(row["fetched_at"]),
            str(row["source_tickers"] or "[]"),
            str(row["content_hash"]),
            updated_at,
            available_at,
            epoch_microseconds(available_at),
            payload_hash,
        ),
    )
    return int(cursor.lastrowid or 0)


class MappingLike(Protocol):
    def __getitem__(self, key: str) -> Any: ...


async def _insert_prepared_news(db: aiosqlite.Connection, item: dict[str, Any]) -> int | None:
    cursor = await db.execute(
        """INSERT OR IGNORE INTO news_items
           (source,title,summary,url,image_url,published_at,fetched_at,content_hash,
            source_tickers,updated_at)
           VALUES (:source,:title,:summary,:url,:image_url,:published_at,:fetched_at,
                   :content_hash,:source_tickers,:updated_at)""",
        item,
    )
    if cursor.rowcount != 1:
        return None
    news_id = int(cursor.lastrowid)
    stored = {**item, "id": news_id}
    available_at = utc_text()
    await _record_news_observation(
        db,
        canonical_news_id=news_id,
        source=str(item["source"]),
        original_title=str(item["observation_title"]),
        original_url=str(item["observation_url"]),
        source_tickers=str(item["source_tickers"]),
        observed_at=available_at,
    )
    await _record_news_change(db, stored, operation="upsert", available_at=available_at)
    return news_id


async def insert_news_item(db: aiosqlite.Connection, item: dict[str, Any]) -> int | None:
    prepared = _prepare_news(item)
    try:
        async with db.execute(
            """SELECT id FROM news_items
               WHERE content_hash IN (?,?) OR url=? ORDER BY id LIMIT 1""",
            (
                prepared["content_hash"],
                prepared["legacy_content_hash"],
                prepared["url"],
            ),
        ) as cursor:
            duplicate = await cursor.fetchone()
        if duplicate is not None:
            await _record_duplicate_news(db, int(duplicate[0]), prepared)
            await db.commit()
            return None
        news_id = await _insert_prepared_news(db, prepared)
        if news_id is None:
            async with db.execute(
                "SELECT id FROM news_items WHERE content_hash IN (?,?) ORDER BY id LIMIT 1",
                (prepared["content_hash"], prepared["legacy_content_hash"]),
            ) as cursor:
                duplicate = await cursor.fetchone()
            if duplicate is not None:
                await _record_duplicate_news(db, int(duplicate[0]), prepared)
        await db.commit()
        return news_id
    except Exception:
        await db.rollback()
        raise


async def _merge_source_tickers(
    db: aiosqlite.Connection,
    news_id: int,
    incoming_tickers: str,
) -> bool:
    async with db.execute("SELECT * FROM news_items WHERE id=?", (news_id,)) as cursor:
        row = await cursor.fetchone()
    if row is None:
        return False
    current = _deserialize_tickers(row["source_tickers"])
    merged = list(current)
    for ticker in _deserialize_tickers(incoming_tickers):
        if ticker not in merged:
            merged.append(ticker)
    if merged == current:
        return False
    changed_at = utc_text()
    serialized = _serialize_source_tickers(merged)
    await db.execute(
        "UPDATE news_items SET source_tickers=?,updated_at=? WHERE id=?",
        (serialized, changed_at, news_id),
    )
    updated = dict(row)
    updated["source_tickers"] = serialized
    updated["updated_at"] = changed_at
    await _record_news_change(
        db,
        updated,
        operation="upsert",
        available_at=changed_at,
    )
    return True


async def _record_duplicate_news(
    db: aiosqlite.Connection,
    news_id: int,
    item: dict[str, Any],
) -> None:
    observed_at = utc_text()
    observation_inserted = await _record_news_observation(
        db,
        canonical_news_id=news_id,
        source=str(item["source"]),
        original_title=str(item["observation_title"]),
        original_url=str(item["observation_url"]),
        source_tickers=str(item["source_tickers"]),
        observed_at=observed_at,
    )
    tickers_changed = await _merge_source_tickers(
        db, news_id, str(item["source_tickers"])
    )
    if not observation_inserted or tickers_changed:
        return

    # A newly corroborating source is itself a material ETL change even when
    # it carries no new ticker. Advance the canonical row and change stream so
    # incremental readers can observe the higher source count.
    changed_at = utc_text()
    await db.execute(
        "UPDATE news_items SET updated_at=? WHERE id=?",
        (changed_at, news_id),
    )
    async with db.execute("SELECT * FROM news_items WHERE id=?", (news_id,)) as cursor:
        row = await cursor.fetchone()
    if row is not None:
        await _record_news_change(
            db,
            row,
            operation="upsert",
            available_at=changed_at,
        )


def _chunked_exact_keys(values: set[str]) -> list[tuple[str, ...]]:
    ordered = sorted(value for value in values if value)
    return [
        tuple(ordered[offset : offset + EXACT_KEY_QUERY_CHUNK_SIZE])
        for offset in range(0, len(ordered), EXACT_KEY_QUERY_CHUNK_SIZE)
    ]


async def _load_exact_news_ids(
    db: aiosqlite.Connection,
    items: list[dict[str, Any]],
) -> tuple[dict[str, int], dict[str, int]]:
    hashes = {
        str(value)
        for item in items
        for value in (item.get("content_hash"), item.get("legacy_content_hash"))
        if value
    }
    urls = {normalize_url(str(item.get("url") or "")) for item in items}
    urls.discard("")
    hashes_to_ids: dict[str, int] = {}
    urls_to_ids: dict[str, int] = {}

    for chunk in _chunked_exact_keys(hashes):
        placeholders = ",".join("?" for _ in chunk)
        async with db.execute(
            f"""SELECT id,content_hash FROM news_items
                WHERE content_hash IN ({placeholders}) ORDER BY id""",
            chunk,
        ) as cursor:
            for row in await cursor.fetchall():
                hashes_to_ids.setdefault(str(row["content_hash"]), int(row["id"]))

    for chunk in _chunked_exact_keys(urls):
        placeholders = ",".join("?" for _ in chunk)
        async with db.execute(
            f"SELECT id,url FROM news_items WHERE url IN ({placeholders}) ORDER BY id",
            chunk,
        ) as cursor:
            for row in await cursor.fetchall():
                normalized = normalize_url(str(row["url"] or ""))
                if normalized:
                    urls_to_ids.setdefault(normalized, int(row["id"]))
    return hashes_to_ids, urls_to_ids


async def insert_news_items_batch(
    db: aiosqlite.Connection,
    items: list[dict[str, Any]],
) -> dict[str, int]:
    if not items:
        return {"inserted": 0, "duplicates": 0}
    prepared_items = [_prepare_news(item) for item in items]

    inserted = 0
    duplicates = 0
    try:
        # Serialize the exact-key snapshot with inserts so concurrent batches
        # cannot both create different hashes for one normalized URL.
        await db.execute("BEGIN IMMEDIATE")
        hashes_to_ids, urls_to_ids = await _load_exact_news_ids(db, prepared_items)
        async with db.execute(
            """SELECT id,title,url,published_at,fetched_at,content_hash,source_tickers
               FROM news_items
               ORDER BY COALESCE(published_at,fetched_at) DESC LIMIT 2000"""
        ) as cursor:
            existing = await cursor.fetchall()
        titles_by_day: dict[str, list[tuple[str, int]]] = {}
        for row in existing:
            news_id = int(row["id"])
            title = normalize_title(str(row["title"] or ""))
            if title:
                bucket = publication_bucket(row["published_at"] or row["fetched_at"])
                titles_by_day.setdefault(bucket, []).append((title, news_id))

        for item in prepared_items:
            normalized_url = normalize_url(str(item["url"]))
            duplicate_id = (
                hashes_to_ids.get(item["content_hash"])
                or hashes_to_ids.get(item["legacy_content_hash"])
                or urls_to_ids.get(normalized_url)
            )
            title = normalize_title(item["title"])
            bucket = publication_bucket(item["published_at"] or item["fetched_at"])
            if duplicate_id is None and title:
                duplicate_id = next(
                    (
                        news_id
                        for prior_title, news_id in titles_by_day.setdefault(bucket, [])
                        if similar_titles(title, prior_title)
                    ),
                    None,
                )
            if duplicate_id is not None:
                duplicates += 1
                await _record_duplicate_news(db, duplicate_id, item)
                continue
            news_id = await _insert_prepared_news(db, item)
            if news_id is None:
                duplicates += 1
                concurrent_hashes, concurrent_urls = await _load_exact_news_ids(db, [item])
                concurrent_id = (
                    concurrent_hashes.get(item["content_hash"])
                    or concurrent_hashes.get(item["legacy_content_hash"])
                    or concurrent_urls.get(normalized_url)
                )
                if concurrent_id is not None:
                    await _record_duplicate_news(db, concurrent_id, item)
            else:
                inserted += 1
                hashes_to_ids[item["content_hash"]] = news_id
                hashes_to_ids[item["legacy_content_hash"]] = news_id
                urls_to_ids[normalized_url] = news_id
                if title:
                    titles_by_day.setdefault(bucket, []).append((title, news_id))
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    return {"inserted": inserted, "duplicates": duplicates}


def _news_payload(
    row: MappingLike,
    *,
    sources: list[str] | None = None,
) -> dict[str, Any]:
    source_names = sources or [str(row["source"])]
    return {
        "id": int(row["news_id"]),
        "source": str(row["source"]),
        "title": str(row["title"]),
        "summary": row["summary"],
        "url": str(row["url"]),
        "image_url": row["image_url"],
        "published_at": _legacy_wire_utc_text(
            row["published_at"], field="published_at", optional=True
        ),
        "fetched_at": _legacy_wire_utc_text(row["fetched_at"], field="fetched_at"),
        "updated_at": _legacy_wire_utc_text(row["updated_at"], field="updated_at"),
        "source_tickers": _deserialize_tickers(row["source_tickers"]),
        "content_hash": str(row["content_hash"]),
        "sources": source_names,
        "source_count": len(source_names),
    }


async def _source_evidence_by_news_ids(
    db: aiosqlite.Connection,
    news_ids: list[int],
    *,
    as_of: datetime,
) -> dict[int, list[str]]:
    unique_ids = list(dict.fromkeys(news_ids))
    if not unique_ids:
        return {}
    placeholders = ",".join("?" for _ in unique_ids)
    async with db.execute(
        f"""SELECT canonical_news_id,source
            FROM news_source_observations
            WHERE canonical_news_id IN ({placeholders}) AND observed_at_us<=?
            ORDER BY canonical_news_id,observation_id""",
        [*unique_ids, epoch_microseconds(as_of)],
    ) as cursor:
        rows = await cursor.fetchall()
    sources_by_id: dict[int, list[str]] = {}
    seen_by_id: dict[int, set[str]] = {}
    for row in rows:
        news_id = int(row["canonical_news_id"])
        source = str(row["source"])
        key = source.casefold()
        if key in seen_by_id.setdefault(news_id, set()):
            continue
        seen_by_id[news_id].add(key)
        sources_by_id.setdefault(news_id, []).append(source)
    return sources_by_id


async def _source_evidence_by_changes(
    db: aiosqlite.Connection,
    changes: list[aiosqlite.Row],
) -> dict[int, list[str]]:
    """Project source names at each immutable change's own availability time."""

    upserts = [row for row in changes if row["operation"] == "upsert"]
    news_ids = list(dict.fromkeys(int(row["news_id"]) for row in upserts))
    if not news_ids:
        return {}
    maximum_cutoff = max(int(row["available_at_us"]) for row in upserts)
    placeholders = ",".join("?" for _ in news_ids)
    async with db.execute(
        f"""SELECT canonical_news_id,source,observed_at_us,observation_id
            FROM news_source_observations
            WHERE canonical_news_id IN ({placeholders}) AND observed_at_us<=?
            ORDER BY canonical_news_id,observed_at_us,observation_id""",
        [*news_ids, maximum_cutoff],
    ) as cursor:
        observations = await cursor.fetchall()

    by_news_id: dict[int, list[aiosqlite.Row]] = {}
    for observation in observations:
        by_news_id.setdefault(int(observation["canonical_news_id"]), []).append(observation)

    projected: dict[int, list[str]] = {}
    for change in upserts:
        cutoff = int(change["available_at_us"])
        seen: set[str] = set()
        sources: list[str] = []
        for observation in by_news_id.get(int(change["news_id"]), []):
            if int(observation["observed_at_us"]) > cutoff:
                break
            source = str(observation["source"])
            key = source.casefold()
            if key in seen:
                continue
            seen.add(key)
            sources.append(source)
        projected[int(change["change_sequence"])] = sources
    return projected


async def news_change_watermark(
    db: aiosqlite.Connection,
    *,
    as_of: datetime,
) -> int:
    async with db.execute(
        """SELECT COALESCE(MAX(change_sequence),0) FROM news_changes
           WHERE available_at_us<=?""",
        (epoch_microseconds(as_of),),
    ) as cursor:
        row = await cursor.fetchone()
    return int(row[0] if row else 0)


async def query_news_changes(
    db: aiosqlite.Connection,
    *,
    updated_after: datetime,
    as_of: datetime,
    after_sequence: int,
    checkpoint_sequence: int | None = None,
    watermark_sequence: int,
    limit: int,
) -> tuple[list[dict[str, Any]], bool]:
    sequence_floor = max(after_sequence, checkpoint_sequence or 0)
    if checkpoint_sequence is None:
        statement = """SELECT * FROM news_changes
                       WHERE change_sequence>? AND change_sequence<=?
                         AND available_at_us>?
                         AND available_at_us<=?
                       ORDER BY change_sequence LIMIT ?"""
        parameters = (
            sequence_floor,
            watermark_sequence,
            epoch_microseconds(updated_after),
            epoch_microseconds(as_of),
            limit + 1,
        )
    else:
        # Sequence checkpoints close the commit-time race inherent in wall-clock
        # filters: a row stamped before ``as_of`` may become visible only after
        # that read transaction has completed.
        statement = """SELECT * FROM news_changes
                       WHERE change_sequence>? AND change_sequence<=?
                         AND available_at_us<=?
                       ORDER BY change_sequence LIMIT ?"""
        parameters = (
            sequence_floor,
            watermark_sequence,
            epoch_microseconds(as_of),
            limit + 1,
        )
    async with db.execute(statement, parameters) as cursor:
        rows = await cursor.fetchall()
    has_more = len(rows) > limit
    visible_rows = rows[:limit]
    sources_by_change = await _source_evidence_by_changes(db, visible_rows)
    changes: list[dict[str, Any]] = []
    for row in visible_rows:
        news_id = int(row["news_id"])
        changes.append(
            {
                "sequence": int(row["change_sequence"]),
                "operation": str(row["operation"]),
                "changed_at": str(row["available_at"]),
                "source_updated_at": _legacy_wire_utc_text(
                    row["updated_at"], field="source_updated_at"
                ),
                "available_at": str(row["available_at"]),
                "news": (
                    _news_payload(
                        row,
                        sources=sources_by_change.get(int(row["change_sequence"])),
                    )
                    if row["operation"] == "upsert"
                    else None
                ),
                "news_id": news_id,
            }
        )
    return changes, has_more


async def get_news_as_of(
    db: aiosqlite.Connection,
    news_id: int,
    *,
    as_of: datetime,
) -> tuple[dict[str, Any], int, str] | None:
    async with db.execute(
        """SELECT * FROM news_changes
           WHERE news_id=? AND available_at_us<=?
           ORDER BY change_sequence DESC LIMIT 1""",
        (news_id, epoch_microseconds(as_of)),
    ) as cursor:
        row = await cursor.fetchone()
    if row is None or row["operation"] == "delete":
        return None
    # The selected change is immutable. Project source evidence at that change's
    # own availability boundary so later corroboration cannot rewrite history.
    sources_by_id = await _source_evidence_by_news_ids(
        db,
        [news_id],
        as_of=parse_utc(str(row["available_at"]), field="available_at"),
    )
    return (
        _news_payload(row, sources=sources_by_id.get(news_id)),
        int(row["change_sequence"]),
        str(row["available_at"]),
    )


async def upsert_source_health(
    db: aiosqlite.Connection,
    *,
    source: str,
    status: str,
    last_attempt_at: str | None,
    last_success_at: str | None,
    data_through: str | None,
    consecutive_failures: int,
    next_attempt_at: str | None,
    raw_count: int | None,
    inserted_count: int | None,
    duplicates_count: int | None,
    error_code: str | None,
    commit: bool = True,
) -> None:
    await db.execute(
        """INSERT INTO source_health
           (source,status,last_attempt_at,last_success_at,data_through,consecutive_failures,
            next_attempt_at,raw_count,inserted_count,duplicates_count,error_code,updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(source) DO UPDATE SET
             status=excluded.status,last_attempt_at=excluded.last_attempt_at,
             last_success_at=excluded.last_success_at,data_through=excluded.data_through,
             consecutive_failures=excluded.consecutive_failures,
             next_attempt_at=excluded.next_attempt_at,raw_count=excluded.raw_count,
             inserted_count=excluded.inserted_count,duplicates_count=excluded.duplicates_count,
             error_code=excluded.error_code,updated_at=excluded.updated_at""",
        (
            source[:200],
            status,
            last_attempt_at,
            last_success_at,
            data_through,
            max(0, int(consecutive_failures)),
            next_attempt_at,
            max(0, int(raw_count)) if raw_count is not None else None,
            max(0, int(inserted_count)) if inserted_count is not None else None,
            max(0, int(duplicates_count)) if duplicates_count is not None else None,
            error_code[:100] if error_code else None,
            utc_text(),
        ),
    )
    if commit:
        await db.commit()


async def get_source_health(db: aiosqlite.Connection) -> list[dict[str, Any]]:
    async with db.execute(
        """SELECT source,status,last_attempt_at,last_success_at,data_through,
                  consecutive_failures,next_attempt_at,raw_count,inserted_count,
                  duplicates_count,error_code,updated_at
           FROM source_health ORDER BY source"""
    ) as cursor:
        return [dict(row) for row in await cursor.fetchall()]


def _calendar_event_id(event: dict[str, Any]) -> str:
    identity = "\n".join(
        (
            str(event.get("date") or event.get("scheduled_at") or ""),
            str(event.get("country_code") or "").upper(),
            str(event.get("title") or "").strip(),
        )
    )
    return hashlib.sha256(identity.encode()).hexdigest()


async def record_calendar_snapshot(
    db: aiosqlite.Connection,
    events: list[dict[str, Any]],
    *,
    source_fetched_at: str,
    stale: bool,
    observed_at: str,
    source: str = "faireconomy",
    commit: bool = True,
) -> tuple[str, int]:
    fetched = utc_text(parse_utc(source_fetched_at, field="source_fetched_at"))
    observed = utc_text(max(parse_utc(observed_at, field="observed_at"), parse_utc(fetched)))
    normalized: list[dict[str, Any]] = []
    for event in events:
        country_code = str(event.get("country_code") or "").upper()
        title = str(event.get("title") or "").strip()
        scheduled = str(event.get("date") or event.get("scheduled_at") or "").strip()
        impact = str(event.get("impact") or "low").lower()
        if len(country_code) != 3 or not title or not scheduled:
            continue
        if impact not in {"low", "medium", "high", "holiday"}:
            continue
        scheduled_utc = utc_text(parse_utc(scheduled, field="scheduled_at"))
        row = {
            "event_id": _calendar_event_id(event),
            "country_code": country_code,
            "country": str(event.get("country") or country_code),
            "title": title[:4_000],
            "impact": impact,
            "impact_zh": str(event.get("impact_zh") or impact),
            "scheduled_at": scheduled,
            "scheduled_at_utc": scheduled_utc,
            "forecast": str(event.get("forecast") or "") or None,
            "previous": str(event.get("previous") or "") or None,
            "actual": str(event.get("actual") or "") or None,
        }
        row["content_hash"] = hashlib.sha256(
            json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest()
        normalized.append(row)
    normalized.sort(key=lambda row: (row["scheduled_at_utc"], row["event_id"]))
    canonical = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    payload_hash = hashlib.sha256(canonical.encode()).hexdigest()
    # Repeated reads of the same stale cache reuse one immutable snapshot.
    # A successful fresh fetch gets a new snapshot even when values are equal,
    # because source_fetched_at is itself a newer data-through observation.
    token = "cal_" + hashlib.sha256(
        f"{fetched}:{int(stale)}:{payload_hash}".encode()
    ).hexdigest()[:40]
    try:
        cursor = await db.execute(
            """INSERT OR IGNORE INTO etl_calendar_snapshots
               (snapshot_token,source,source_fetched_at,data_through,is_stale,available_at,
                available_at_us,payload_hash)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                token,
                source,
                fetched,
                fetched,
                int(stale),
                observed,
                epoch_microseconds(observed),
                payload_hash,
            ),
        )
        if cursor.rowcount == 0:
            async with db.execute(
                "SELECT snapshot_sequence FROM etl_calendar_snapshots WHERE snapshot_token=?",
                (token,),
            ) as query:
                existing = await query.fetchone()
            if commit:
                await db.commit()
            return token, int(existing[0])
        sequence = int(cursor.lastrowid)
        for ordinal, event in enumerate(normalized, 1):
            await db.execute(
                """INSERT INTO etl_calendar_events
                   (snapshot_sequence,ordinal,event_id,country_code,country,title,impact,
                    impact_zh,scheduled_at,scheduled_at_utc,forecast,previous,actual,
                    is_stale,source_fetched_at,available_at,content_hash)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    sequence,
                    ordinal,
                    event["event_id"],
                    event["country_code"],
                    event["country"],
                    event["title"],
                    event["impact"],
                    event["impact_zh"],
                    event["scheduled_at"],
                    event["scheduled_at_utc"],
                    event["forecast"],
                    event["previous"],
                    event["actual"],
                    int(stale),
                    fetched,
                    observed,
                    event["content_hash"],
                ),
            )
        if commit:
            await db.commit()
        return token, sequence
    except Exception:
        await db.rollback()
        raise


async def calendar_watermark(
    db: aiosqlite.Connection,
    *,
    updated_after: datetime,
    as_of: datetime,
) -> dict[str, Any] | None:
    async with db.execute(
        """SELECT *,
                  CASE WHEN available_at_us>? THEN 1 ELSE 0 END AS has_changes
           FROM etl_calendar_snapshots
           WHERE available_at_us<=?
           ORDER BY snapshot_sequence DESC LIMIT 1""",
        (epoch_microseconds(updated_after), epoch_microseconds(as_of)),
    ) as cursor:
        row = await cursor.fetchone()
    return dict(row) if row else None


async def query_calendar_page(
    db: aiosqlite.Connection,
    *,
    snapshot_sequence: int,
    after_ordinal: int,
    limit: int,
) -> tuple[list[dict[str, Any]], bool]:
    async with db.execute(
        """SELECT * FROM etl_calendar_events
           WHERE snapshot_sequence=? AND ordinal>?
           ORDER BY ordinal LIMIT ?""",
        (snapshot_sequence, after_ordinal, limit + 1),
    ) as cursor:
        rows = await cursor.fetchall()
    has_more = len(rows) > limit
    items = []
    for row in rows[:limit]:
        items.append(
            {
                "event_id": str(row["event_id"]),
                "country_code": str(row["country_code"]),
                "country": str(row["country"]),
                "title": str(row["title"]),
                "impact": str(row["impact"]),
                "impact_zh": str(row["impact_zh"]),
                "scheduled_at": str(row["scheduled_at"]),
                "scheduled_at_utc": str(row["scheduled_at_utc"]),
                "forecast": row["forecast"],
                "previous": row["previous"],
                "actual": row["actual"],
                "is_stale": bool(row["is_stale"]),
                "source_fetched_at": str(row["source_fetched_at"]),
                "available_at": str(row["available_at"]),
                "ordinal": int(row["ordinal"]),
            }
        )
    return items, has_more


def _quote_identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError("invalid SQLite identifier")
    return f'"{value}"'


async def _legacy_news_references(db: aiosqlite.Connection) -> list[tuple[str, str]]:
    async with db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ) as cursor:
        table_names = [str(row[0]) for row in await cursor.fetchall()]
    references: set[tuple[str, str]] = set()
    for table in table_names:
        if table in {"news_items", "news_changes", "news_source_observations"}:
            continue
        quoted = _quote_identifier(table)
        async with db.execute(f"PRAGMA table_info({quoted})") as cursor:
            columns = {str(row[1]) for row in await cursor.fetchall()}
        # Some very old installations used a logical news_id link without a
        # declared foreign key. Protect it as historical data all the same.
        if "news_id" in columns:
            references.add((table, "news_id"))
        async with db.execute(f"PRAGMA foreign_key_list({quoted})") as cursor:
            for row in await cursor.fetchall():
                if str(row[2]) == "news_items" and str(row[4] or "id") == "id":
                    references.add((table, str(row[3])))
    return sorted(references)


async def cleanup_retained_data(
    db: aiosqlite.Connection,
    *,
    now: datetime | None = None,
) -> dict[str, int]:
    """Apply ETL retention without modifying legacy analysis tables.

    Raw news rows referenced by any legacy foreign key are retained. This keeps
    cascade rules from silently deleting historical analysis records.
    """

    current = now or utc_now()
    storage = settings.storage
    deleted = {"news_items": 0, "news_changes": 0, "calendar_snapshots": 0}
    try:
        if storage.news_retention_days:
            cutoff = utc_text(current - timedelta(days=storage.news_retention_days))
            references = await _legacy_news_references(db)
            protections = [
                f"NOT EXISTS (SELECT 1 FROM {_quote_identifier(table)} legacy "
                f"WHERE legacy.{_quote_identifier(column)}=news_items.id)"
                for table, column in references
            ]
            protected_sql = " AND " + " AND ".join(protections) if protections else ""
            async with db.execute(
                f"""SELECT * FROM news_items
                    WHERE julianday(COALESCE(published_at,fetched_at))<julianday(?)
                    {protected_sql}
                    ORDER BY COALESCE(published_at,fetched_at),id LIMIT ?""",
                (cutoff, RETENTION_BATCH_SIZE),
            ) as cursor:
                candidates = await cursor.fetchall()
            deletion_time = utc_text(current)
            for row in candidates:
                await _record_news_change(
                    db,
                    row,
                    operation="delete",
                    available_at=deletion_time,
                    changed_at=deletion_time,
                )
            if candidates:
                placeholders = ",".join("?" for _ in candidates)
                cursor = await db.execute(
                    f"DELETE FROM news_items WHERE id IN ({placeholders})",
                    [int(row["id"]) for row in candidates],
                )
                deleted["news_items"] = max(0, int(cursor.rowcount))

        change_cutoff = utc_text(current - timedelta(days=storage.change_retention_days))
        cursor = await db.execute(
            """DELETE FROM news_changes
               WHERE available_at_us<?
                 AND change_sequence NOT IN (
                     SELECT MAX(change_sequence) FROM news_changes GROUP BY news_id
                 )""",
            (epoch_microseconds(change_cutoff),),
        )
        deleted["news_changes"] = max(0, int(cursor.rowcount))

        calendar_cutoff = utc_text(
            current - timedelta(days=storage.calendar_snapshot_retention_days)
        )
        cursor = await db.execute(
            """DELETE FROM etl_calendar_snapshots
               WHERE available_at_us<?
                 AND snapshot_sequence<>(
                     SELECT COALESCE(MAX(snapshot_sequence),0) FROM etl_calendar_snapshots
                 )""",
            (epoch_microseconds(calendar_cutoff),),
        )
        deleted["calendar_snapshots"] = max(0, int(cursor.rowcount))
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    return deleted


async def run_retention() -> dict[str, int]:
    db = await get_db()
    try:
        result = await cleanup_retained_data(db)
    finally:
        await db.close()
    if any(result.values()):
        logger.info("ETL retention removed rows: %s", result)
    return result
