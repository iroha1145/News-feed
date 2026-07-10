import json
import logging
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import aiosqlite

from app.config import settings
from app.utils.dedup import normalize_title, publication_bucket, similar_titles

logger = logging.getLogger(__name__)

INTERNAL_NEWS_FIELDS = {
    "analysis_attempts",
    "analysis_error",
    "analysis_claimed_at",
    "analysis_lease_expires_at",
}


def _without_internal_news_fields(item: dict) -> dict:
    for field in INTERNAL_NEWS_FIELDS:
        item.pop(field, None)
    return item


def _resolve_db_path() -> str:
    parsed = urlparse(settings.database_url)
    if parsed.scheme != "sqlite+aiosqlite":
        raise ValueError(f"Unsupported database_url scheme: {settings.database_url}")

    prefix = "sqlite+aiosqlite:///"
    if not settings.database_url.startswith(prefix) or parsed.netloc:
        raise ValueError("database_url must use sqlite+aiosqlite:///path syntax")
    # Three slashes mean a relative path; four preserve the leading slash.
    db_path = settings.database_url[len(prefix):]

    if not db_path:
        raise ValueError("database_url must include a SQLite database path")

    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


DB_PATH = _resolve_db_path()

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
    analysis_status TEXT DEFAULT 'pending',
    analysis_attempts INTEGER DEFAULT 0,
    analysis_error TEXT DEFAULT '',
    analysis_claimed_at TEXT,
    analysis_lease_expires_at TEXT
)
"""

CREATE_ANALYSES = """
CREATE TABLE IF NOT EXISTS analyses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    news_id INTEGER NOT NULL REFERENCES news_items(id) ON DELETE CASCADE,
    title_zh TEXT DEFAULT '',
    headline_summary TEXT DEFAULT '',
    overall_sentiment INTEGER NOT NULL,
    classification TEXT NOT NULL,
    confidence INTEGER NOT NULL,
    affected_stocks TEXT NOT NULL DEFAULT '[]',
    affected_sectors TEXT NOT NULL DEFAULT '[]',
    affected_commodities TEXT NOT NULL DEFAULT '[]',
    logic_chain TEXT NOT NULL,
    key_factors TEXT NOT NULL DEFAULT '[]',
    llm_provider TEXT NOT NULL,
    llm_model TEXT NOT NULL,
    analyzed_at TEXT NOT NULL,
    UNIQUE(news_id)
)
"""

CREATE_X_SENTIMENTS = """
CREATE TABLE IF NOT EXISTS x_sentiments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query TEXT NOT NULL,
    trending_tickers TEXT NOT NULL DEFAULT '[]',
    retail_sentiment_score INTEGER NOT NULL,
    key_narratives TEXT NOT NULL DEFAULT '[]',
    meme_stocks TEXT NOT NULL DEFAULT '[]',
    raw_analysis TEXT NOT NULL,
    fear_greed_estimate INTEGER DEFAULT 50,
    analyzed_at TEXT NOT NULL
)
"""

CREATE_SETTINGS = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_news_published_at ON news_items(published_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_news_effective_published ON news_items(COALESCE(published_at, fetched_at) DESC)",
    "CREATE INDEX IF NOT EXISTS idx_news_source_published ON news_items(source, published_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_news_analysis_status_published ON news_items(analysis_status, published_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_news_analysis_lease ON news_items(analysis_status, analysis_lease_expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_analyses_analyzed_at ON analyses(analyzed_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_analyses_classification_analyzed ON analyses(classification, analyzed_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_x_sentiments_analyzed_at ON x_sentiments(analyzed_at DESC)",
]

SQLITE_BUSY_TIMEOUT_MS = 5_000
DEFAULT_ANALYSIS_LEASE_SECONDS = 10 * 60
MAX_ANALYSIS_ATTEMPTS = 3


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA foreign_keys=ON")
    await db.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    return db


async def init_db() -> None:
    logger.info("Initializing database tables...")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys=ON")
        await db.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        await db.execute(CREATE_NEWS_ITEMS)
        await db.execute(CREATE_ANALYSES)
        await db.execute(CREATE_X_SENTIMENTS)
        await db.execute(CREATE_SETTINGS)
        # Centralized migration list: (table, column, definition)
        migrations = [
            ("news_items", "analysis_status", "TEXT DEFAULT 'pending'"),
            ("news_items", "analysis_attempts", "INTEGER DEFAULT 0"),
            ("news_items", "analysis_error", "TEXT DEFAULT ''"),
            ("news_items", "analysis_claimed_at", "TEXT"),
            ("news_items", "analysis_lease_expires_at", "TEXT"),
            ("analyses", "title_zh", "TEXT DEFAULT ''"),
            ("analyses", "headline_summary", "TEXT DEFAULT ''"),
            ("x_sentiments", "fear_greed_estimate", "INTEGER DEFAULT 50"),
        ]
        for table, col, definition in migrations:
            try:
                await db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {definition}")
            except Exception as e:
                if "duplicate column" not in str(e).lower():
                    raise
        for statement in CREATE_INDEXES:
            await db.execute(statement)
        await db.commit()
        await cleanup_retained_data(db)
    logger.info("Database tables initialized successfully")


def _retention_days(value: Any, setting_name: str) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        days = int(value)
    except (TypeError, ValueError):
        logger.warning("Ignoring invalid retention setting %s=%r", setting_name, value)
        return None
    if not 1 <= days <= 3650:
        logger.warning("Ignoring out-of-range retention setting %s=%r", setting_name, value)
        return None
    return days


def _analysis_retention_limit(value: Any) -> int:
    if value in (None, ""):
        return 350
    try:
        limit = int(value)
    except (TypeError, ValueError):
        logger.warning("Invalid analysis_retention_limit=%r; using 350", value)
        return 350
    if not 1 <= limit <= 100_000:
        logger.warning("Out-of-range analysis_retention_limit=%r; using 350", value)
        return 350
    return limit


async def cleanup_retained_data(db: aiosqlite.Connection) -> dict[str, int]:
    """Keep 350 analyses by default; other data is deleted only when explicitly configured."""
    news_setting = await get_setting(db, "news_retention_days")
    x_setting = await get_setting(db, "x_sentiment_retention_days")
    analysis_setting = await get_setting(db, "analysis_retention_limit")
    news_days = _retention_days(
        news_setting if news_setting is not None else settings.news_retention_days,
        "news_retention_days",
    )
    x_days = _retention_days(
        x_setting if x_setting is not None else settings.x_sentiment_retention_days,
        "x_sentiment_retention_days",
    )
    analysis_limit = _analysis_retention_limit(
        analysis_setting if analysis_setting is not None else settings.analysis_retention_limit
    )

    deleted = {"news_items": 0, "analyses": 0, "x_sentiments": 0}
    try:
        await db.execute(
            """UPDATE news_items
               SET analysis_status = 'skipped',
                   analysis_error = 'Analysis removed by retention policy',
                   analysis_claimed_at = NULL,
                   analysis_lease_expires_at = NULL
               WHERE id IN (
                   SELECT news_id FROM analyses
                   ORDER BY datetime(analyzed_at) DESC, id DESC
                   LIMIT -1 OFFSET ?
               )""",
            (analysis_limit,),
        )
        cursor = await db.execute(
            """DELETE FROM analyses WHERE id IN (
                   SELECT id FROM analyses
                   ORDER BY datetime(analyzed_at) DESC, id DESC
                   LIMIT -1 OFFSET ?
               )""",
            (analysis_limit,),
        )
        deleted["analyses"] += cursor.rowcount

        if news_days is not None:
            modifier = f"-{news_days} days"
            cursor = await db.execute(
                """DELETE FROM analyses WHERE news_id IN (
                       SELECT id FROM news_items
                       WHERE datetime(COALESCE(published_at, fetched_at)) < datetime('now', ?)
                   )""",
                (modifier,),
            )
            deleted["analyses"] += cursor.rowcount
            cursor = await db.execute(
                """DELETE FROM news_items
                   WHERE datetime(COALESCE(published_at, fetched_at)) < datetime('now', ?)""",
                (modifier,),
            )
            deleted["news_items"] = cursor.rowcount

        if x_days is not None:
            cursor = await db.execute(
                "DELETE FROM x_sentiments WHERE datetime(analyzed_at) < datetime('now', ?)",
                (f"-{x_days} days",),
            )
            deleted["x_sentiments"] = cursor.rowcount

        await db.commit()
    except Exception:
        await db.rollback()
        raise

    if any(deleted.values()):
        logger.info("Retention cleanup removed rows: %s", deleted)
    return deleted


def row_to_dict(row: aiosqlite.Row) -> dict:
    return dict(row)


def _bounded_score(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0
    if not math.isfinite(number):
        return 0
    return max(-100, min(100, round(number)))


def _normalize_json_list(field: str, value: Any) -> list:
    if not isinstance(value, list):
        return []
    if field == "affected_stocks":
        normalized = []
        for item in value:
            if not isinstance(item, dict):
                continue
            ticker = str(item.get("ticker") or "").strip().upper().lstrip("$")[:20]
            if not ticker:
                continue
            normalized.append({
                "ticker": ticker,
                "company": str(item.get("company") or ticker).strip()[:200],
                "impact_score": _bounded_score(item.get("impact_score")),
                "reason": str(item.get("reason") or "").strip()[:2000],
            })
        return normalized
    if field == "affected_commodities":
        normalized = []
        for item in value:
            if isinstance(item, str) and item.strip():
                normalized.append({"name": item.strip()[:100], "impact_score": 0, "reason": ""})
            elif isinstance(item, dict):
                name = str(item.get("name") or "").strip()[:100]
                if name:
                    normalized.append({
                        "name": name,
                        "impact_score": _bounded_score(item.get("impact_score")),
                        "reason": str(item.get("reason") or "").strip()[:2000],
                    })
        return normalized
    if field in {"affected_sectors", "key_factors"}:
        return [item.strip()[:500] for item in value if isinstance(item, str) and item.strip()]
    return value


def parse_json_fields(d: dict, fields: list[str]) -> dict:
    for field in fields:
        if field in d and isinstance(d[field], str):
            try:
                d[field] = json.loads(d[field])
            except (json.JSONDecodeError, TypeError):
                d[field] = []
        if field in d:
            d[field] = _normalize_json_list(field, d[field])
    return d


# --- news_items ---

async def insert_news_item(db: aiosqlite.Connection, item: dict) -> Optional[int]:
    try:
        cursor = await db.execute(
            """INSERT INTO news_items (source, title, summary, url, image_url, published_at, fetched_at, content_hash)
               VALUES (:source, :title, :summary, :url, :image_url, :published_at, :fetched_at, :content_hash)""",
            item,
        )
        await db.commit()
        return cursor.lastrowid
    except aiosqlite.IntegrityError:
        await db.rollback()
        return None  # duplicate


async def insert_news_items_batch(db: aiosqlite.Connection, items: list[dict]) -> int:
    """Insert a batch in one transaction and return the number of new rows."""
    if not items:
        return 0
    # Compare with recent stored headlines as independent source jobs cannot
    # perform cross-source fuzzy matching in memory.
    async with db.execute(
        """SELECT title, published_at, fetched_at
           FROM news_items
           ORDER BY COALESCE(published_at, fetched_at) DESC
           LIMIT 2000"""
    ) as cursor:
        existing_rows = await cursor.fetchall()
    titles_by_day: dict[str, list[str]] = {}
    for row in existing_rows:
        normalized = normalize_title(str(row[0] or ""))
        if normalized:
            titles_by_day.setdefault(publication_bucket(row[1] or row[2]), []).append(normalized)

    fuzzy_filtered: list[dict] = []
    for item in items:
        normalized = normalize_title(str(item.get("title") or ""))
        day = publication_bucket(item.get("published_at") or item.get("fetched_at"))
        prior_titles = titles_by_day.setdefault(day, [])
        if normalized and any(similar_titles(normalized, prior) for prior in prior_titles):
            continue
        if normalized:
            prior_titles.append(normalized)
        fuzzy_filtered.append(item)

    # Accept both new and legacy hashes during an upgrade. Existing records keep
    # their original hash, while newly inserted records use the date-aware hash.
    candidate_hashes = {
        str(value)
        for item in fuzzy_filtered
        for value in (item.get("content_hash"), item.get("legacy_content_hash"))
        if value
    }
    existing_hashes: set[str] = set()
    candidates = list(candidate_hashes)
    for start in range(0, len(candidates), 500):
        chunk = candidates[start:start + 500]
        placeholders = ",".join("?" for _ in chunk)
        async with db.execute(
            f"SELECT content_hash FROM news_items WHERE content_hash IN ({placeholders})",
            chunk,
        ) as cursor:
            existing_hashes.update(str(row[0]) for row in await cursor.fetchall())

    filtered_items = [
        item for item in fuzzy_filtered
        if item.get("content_hash") not in existing_hashes
        and item.get("legacy_content_hash") not in existing_hashes
    ]
    if not filtered_items:
        return 0

    before = db.total_changes
    try:
        await db.executemany(
            """INSERT OR IGNORE INTO news_items
               (source, title, summary, url, image_url, published_at, fetched_at, content_hash)
               VALUES (:source, :title, :summary, :url, :image_url, :published_at, :fetched_at, :content_hash)""",
            filtered_items,
        )
        await db.commit()
        inserted = db.total_changes - before
    except Exception:
        await db.rollback()
        raise
    try:
        await cleanup_retained_data(db)
    except Exception:
        logger.exception("Post-ingest retention cleanup failed")
    return inserted


async def get_news_items(
    db: aiosqlite.Connection,
    page: int = 1,
    page_size: int = 20,
    source: Optional[str] = None,
    classification: Optional[str] = None,
) -> tuple[int, list[dict]]:
    offset = (page - 1) * page_size
    conditions: list[str] = []
    filter_params: list[Any] = []
    if source:
        conditions.append("n.source = ?")
        filter_params.append(source)
    if classification:
        conditions.append("a.classification = ?")
        filter_params.append(classification)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    async with db.execute(
        f"""SELECT COUNT(*)
            FROM news_items n
            LEFT JOIN analyses a ON a.news_id = n.id
            {where}""",
        filter_params,
    ) as cur:
        row = await cur.fetchone()
        total = row[0] if row else 0

    # Major news: |sentiment| >= 50 AND confidence >= 70 AND published within 4 hours => pinned
    from datetime import datetime, timedelta, timezone
    pin_cutoff = (datetime.now(timezone.utc) - timedelta(hours=4)).strftime("%Y-%m-%dT%H:%M:%S")

    async with db.execute(
        f"""SELECT n.*, 
            a.id as analysis_id, a.title_zh, a.headline_summary, a.overall_sentiment, a.classification, 
            a.confidence, a.affected_stocks, a.affected_sectors, a.affected_commodities,
            a.logic_chain, a.key_factors, a.llm_provider, a.llm_model, a.analyzed_at,
            CASE 
                WHEN a.id IS NOT NULL 
                     AND ABS(a.overall_sentiment) >= 50 
                     AND a.confidence >= 70 
                     AND datetime(n.published_at) >= datetime(?)
                THEN 1 ELSE 0 
            END as is_pinned
        FROM news_items n
        LEFT JOIN analyses a ON a.news_id = n.id
        {where} ORDER BY is_pinned DESC, n.published_at DESC LIMIT ? OFFSET ?""",
        [pin_cutoff, *filter_params, page_size, offset],
    ) as cur:
        rows = await cur.fetchall()

    items = []
    for r in rows:
        d = row_to_dict(r)
        pinned = bool(d.pop("is_pinned", 0))
        # Extract analysis fields into nested object
        if d.get("analysis_id"):
            analysis = {
                "id": d.pop("analysis_id"),
                "news_id": d["id"],
                "title_zh": d.pop("title_zh", ""),
                "headline_summary": d.pop("headline_summary", ""),
                "overall_sentiment": d.pop("overall_sentiment", 0),
                "classification": d.pop("classification", "neutral"),
                "confidence": d.pop("confidence", 0),
                "affected_stocks": d.pop("affected_stocks", "[]"),
                "affected_sectors": d.pop("affected_sectors", "[]"),
                "affected_commodities": d.pop("affected_commodities", "[]"),
                "logic_chain": d.pop("logic_chain", ""),
                "key_factors": d.pop("key_factors", "[]"),
                "llm_provider": d.pop("llm_provider", ""),
                "llm_model": d.pop("llm_model", ""),
                "analyzed_at": d.pop("analyzed_at", ""),
            }
            parse_json_fields(analysis, ["affected_stocks", "affected_sectors", "affected_commodities", "key_factors"])
            d["analysis"] = analysis
        else:
            # Remove None analysis columns
            for k in ["analysis_id", "title_zh", "headline_summary", "overall_sentiment", "classification",
                       "confidence", "affected_stocks", "affected_sectors", "affected_commodities",
                       "logic_chain", "key_factors", "llm_provider", "llm_model", "analyzed_at"]:
                d.pop(k, None)
            d["analysis"] = None
        d["is_pinned"] = pinned
        items.append(_without_internal_news_fields(d))
    return total, items


async def get_news_item_by_id(
    db: aiosqlite.Connection,
    news_id: int,
    *,
    include_internal: bool = True,
) -> Optional[dict]:
    async with db.execute("SELECT * FROM news_items WHERE id = ?", (news_id,)) as cur:
        row = await cur.fetchone()
    if not row:
        return None
    item = row_to_dict(row)
    return item if include_internal else _without_internal_news_fields(item)


MAX_ANALYZABLE = 50  # Only analyze the 50 most recent news items

async def skip_old_news(db: aiosqlite.Connection) -> int:
    """Mark news outside the top-50 window as 'skipped' to save LLM tokens."""
    cursor = await db.execute(
        """UPDATE news_items SET analysis_status = 'skipped'
           WHERE analysis_status = 'pending'
           AND id NOT IN (
               SELECT id FROM news_items ORDER BY published_at DESC LIMIT ?
           )""",
        (MAX_ANALYZABLE,),
    )
    await db.commit()
    return cursor.rowcount

async def get_unanalyzed_news(db: aiosqlite.Connection, limit: int = 5) -> list[dict]:
    recovered = await recover_stale_analysis_leases(db)
    if recovered:
        logger.warning("Recovered %s expired analysis leases", recovered)

    # First skip any old news outside the analysis window
    skipped = await skip_old_news(db)
    if skipped:
        logger.info(f"Skipped {skipped} old news items (outside top-{MAX_ANALYZABLE} window)")

    async with db.execute(
        "SELECT * FROM news_items WHERE analysis_status = 'pending' ORDER BY published_at DESC LIMIT ?",
        (limit,),
    ) as cur:
        rows = await cur.fetchall()
    return [row_to_dict(r) for r in rows]


async def claim_news_for_analysis(db: aiosqlite.Connection, news_id: int) -> bool:
    """Claim an item with a renewable-by-retry lease to avoid permanent processing rows."""
    now = datetime.now(timezone.utc)
    lease_expires_at = now + timedelta(seconds=DEFAULT_ANALYSIS_LEASE_SECONDS)
    cursor = await db.execute(
        """UPDATE news_items
           SET analysis_status = 'processing',
               analysis_attempts = analysis_attempts + 1,
               analysis_claimed_at = ?,
               analysis_lease_expires_at = ?,
               analysis_error = ''
           WHERE id = ?
             AND (
                 analysis_status = 'pending'
                 OR (
                     analysis_status = 'processing'
                     AND datetime(analysis_lease_expires_at) <= datetime(?)
                 )
             )""",
        (now.isoformat(), lease_expires_at.isoformat(), news_id, now.isoformat()),
    )
    await db.commit()
    return cursor.rowcount > 0


async def recover_stale_analysis_leases(db: aiosqlite.Connection) -> int:
    now = datetime.now(timezone.utc).isoformat()
    cursor = await db.execute(
        """UPDATE news_items
           SET analysis_status = CASE
                   WHEN analysis_attempts >= ? THEN 'failed'
                   ELSE 'pending'
               END,
               analysis_error = 'Analysis lease expired; item requeued',
               analysis_claimed_at = NULL,
               analysis_lease_expires_at = NULL
           WHERE analysis_status = 'processing'
             AND (
                 analysis_lease_expires_at IS NULL
                 OR datetime(analysis_lease_expires_at) <= datetime(?)
             )""",
        (MAX_ANALYSIS_ATTEMPTS, now),
    )
    await db.commit()
    return cursor.rowcount


async def mark_analysis_completed(db: aiosqlite.Connection, news_id: int) -> None:
    await db.execute(
        """UPDATE news_items
           SET analysis_status = 'completed', analysis_error = '',
               analysis_claimed_at = NULL, analysis_lease_expires_at = NULL
           WHERE id = ?""",
        (news_id,),
    )
    await db.commit()


async def mark_analysis_failed(db: aiosqlite.Connection, news_id: int, error: str) -> None:
    await db.execute(
        """UPDATE news_items
           SET analysis_status = CASE WHEN analysis_attempts >= ? THEN 'failed' ELSE 'pending' END,
               analysis_error = ?, analysis_claimed_at = NULL, analysis_lease_expires_at = NULL
           WHERE id = ?""",
        (MAX_ANALYSIS_ATTEMPTS, error[:500], news_id),
    )
    await db.commit()


async def requeue_failed_analyses(db: aiosqlite.Connection, news_id: Optional[int] = None) -> int:
    where = "analysis_status = 'failed'"
    params: list[Any] = []
    if news_id is not None:
        where += " AND id = ?"
        params.append(news_id)
    cursor = await db.execute(
        f"""UPDATE news_items
            SET analysis_status = 'pending', analysis_attempts = 0, analysis_error = '',
                analysis_claimed_at = NULL, analysis_lease_expires_at = NULL
            WHERE {where}""",
        params,
    )
    await db.commit()
    return cursor.rowcount


# --- analyses ---

async def insert_analysis(db: aiosqlite.Connection, analysis: dict) -> int:
    cursor = await db.execute(
        """INSERT OR IGNORE INTO analyses
           (news_id, title_zh, headline_summary, overall_sentiment, classification, confidence, affected_stocks,
            affected_sectors, affected_commodities, logic_chain, key_factors,
            llm_provider, llm_model, analyzed_at)
           VALUES (:news_id, :title_zh, :headline_summary, :overall_sentiment, :classification, :confidence,
                   :affected_stocks, :affected_sectors, :affected_commodities,
                   :logic_chain, :key_factors, :llm_provider, :llm_model, :analyzed_at)""",
        analysis,
    )
    await db.commit()
    analysis_id = cursor.lastrowid
    try:
        await cleanup_retained_data(db)
    except Exception:
        logger.exception("Post-analysis retention cleanup failed")
    return analysis_id


async def save_analysis_result(db: aiosqlite.Connection, analysis: dict) -> int:
    """Upsert an analysis and mark its news item complete in one transaction."""
    try:
        await db.execute(
            """INSERT INTO analyses
               (news_id, title_zh, headline_summary, overall_sentiment, classification, confidence,
                affected_stocks, affected_sectors, affected_commodities, logic_chain, key_factors,
                llm_provider, llm_model, analyzed_at)
               VALUES (:news_id, :title_zh, :headline_summary, :overall_sentiment, :classification,
                       :confidence, :affected_stocks, :affected_sectors, :affected_commodities,
                       :logic_chain, :key_factors, :llm_provider, :llm_model, :analyzed_at)
               ON CONFLICT(news_id) DO UPDATE SET
                   title_zh=excluded.title_zh,
                   headline_summary=excluded.headline_summary,
                   overall_sentiment=excluded.overall_sentiment,
                   classification=excluded.classification,
                   confidence=excluded.confidence,
                   affected_stocks=excluded.affected_stocks,
                   affected_sectors=excluded.affected_sectors,
                   affected_commodities=excluded.affected_commodities,
                   logic_chain=excluded.logic_chain,
                   key_factors=excluded.key_factors,
                   llm_provider=excluded.llm_provider,
                   llm_model=excluded.llm_model,
                   analyzed_at=excluded.analyzed_at""",
            analysis,
        )
        await db.execute(
            """UPDATE news_items
               SET analysis_status = 'completed', analysis_error = '',
                   analysis_claimed_at = NULL, analysis_lease_expires_at = NULL
               WHERE id = :news_id""",
            analysis,
        )
        async with db.execute("SELECT id FROM analyses WHERE news_id = ?", (analysis["news_id"],)) as cur:
            row = await cur.fetchone()
        if not row:
            raise RuntimeError("analysis upsert did not produce a row")
        await db.commit()
        try:
            await cleanup_retained_data(db)
        except Exception:
            logger.exception("Post-analysis retention cleanup failed")
        return int(row[0])
    except Exception:
        await db.rollback()
        raise


async def get_analyses(
    db: aiosqlite.Connection,
    page: int = 1,
    page_size: int = 20,
    classification: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> tuple[int, list[dict]]:
    offset = (page - 1) * page_size
    conditions = []
    params: list[Any] = []

    if classification:
        conditions.append("a.classification = ?")
        params.append(classification)
    if date_from:
        conditions.append("a.analyzed_at >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("a.analyzed_at <= ?")
        params.append(date_to)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    async with db.execute(
        f"SELECT COUNT(*) FROM analyses a {where}", params
    ) as cur:
        row = await cur.fetchone()
        total = row[0] if row else 0

    async with db.execute(
        f"""SELECT a.*, n.title as news_title, n.source as news_source, n.url as news_url
            FROM analyses a
            JOIN news_items n ON n.id = a.news_id
            {where}
            ORDER BY a.analyzed_at DESC
            LIMIT ? OFFSET ?""",
        params + [page_size, offset],
    ) as cur:
        rows = await cur.fetchall()

    items = [parse_json_fields(row_to_dict(r), ["affected_stocks", "affected_sectors", "affected_commodities", "key_factors"]) for r in rows]
    return total, items


async def get_latest_analyses(db: aiosqlite.Connection, limit: int = 10) -> list[dict]:
    async with db.execute(
        """SELECT a.*, n.title as news_title, n.source as news_source, n.url as news_url
           FROM analyses a
           JOIN news_items n ON n.id = a.news_id
           ORDER BY a.analyzed_at DESC
           LIMIT ?""",
        (limit,),
    ) as cur:
        rows = await cur.fetchall()
    return [parse_json_fields(row_to_dict(r), ["affected_stocks", "affected_sectors", "affected_commodities", "key_factors"]) for r in rows]


async def get_analysis_for_news(db: aiosqlite.Connection, news_id: int) -> Optional[dict]:
    async with db.execute(
        "SELECT * FROM analyses WHERE news_id = ?", (news_id,)
    ) as cur:
        row = await cur.fetchone()
    if not row:
        return None
    return parse_json_fields(row_to_dict(row), ["affected_stocks", "affected_sectors", "affected_commodities", "key_factors"])


async def get_analysis_stats(db: aiosqlite.Connection, days: int = 7) -> dict:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")
    async with db.execute(
        """SELECT
               COUNT(*) as total,
               AVG(overall_sentiment) as avg_sentiment,
               SUM(CASE WHEN classification='bullish' THEN 1 ELSE 0 END) as bullish,
               SUM(CASE WHEN classification='bearish' THEN 1 ELSE 0 END) as bearish,
               SUM(CASE WHEN classification='neutral' THEN 1 ELSE 0 END) as neutral
           FROM analyses
           WHERE analyzed_at >= ?""",
        (cutoff,),
    ) as cur:
        row = await cur.fetchone()
    stats = row_to_dict(row) if row else {}

    # Sector breakdown with per-sector sentiment
    async with db.execute(
        """SELECT affected_sectors, overall_sentiment, classification
           FROM analyses WHERE analyzed_at >= ?""",
        (cutoff,),
    ) as cur:
        rows = await cur.fetchall()

    sector_data: dict[str, dict] = {}
    for r in rows:
        sectors = parse_json_fields({"affected_sectors": r[0]}, ["affected_sectors"])["affected_sectors"]
        sentiment = r[1] or 0
        cls = r[2] or "neutral"
        if cls not in {"bullish", "bearish", "neutral"}:
            cls = "neutral"
        for s in sectors:
            if not isinstance(s, str):
                continue
            if s not in sector_data:
                sector_data[s] = {"count": 0, "total_sentiment": 0, "bullish": 0, "bearish": 0, "neutral": 0}
            sector_data[s]["count"] += 1
            sector_data[s]["total_sentiment"] += sentiment
            sector_data[s][cls] += 1

    sector_counts: dict[str, int] = {k: v["count"] for k, v in sector_data.items()}

    # Build sector_sentiment with per-sector avg score
    sector_sentiment = {
        name: {
            "count": data["count"],
            "avg_sentiment": round(data["total_sentiment"] / max(data["count"], 1), 1),
            "bullish": data["bullish"],
            "bearish": data["bearish"],
            "neutral": data["neutral"],
        }
        for name, data in sector_data.items()
    }

    # Top stocks
    async with db.execute(
        "SELECT affected_stocks FROM analyses WHERE analyzed_at >= ?",
        (cutoff,),
    ) as cur:
        rows = await cur.fetchall()

    stock_scores: dict[str, list[int]] = {}
    for r in rows:
        stocks = parse_json_fields({"affected_stocks": r[0]}, ["affected_stocks"])["affected_stocks"]
        for s in stocks:
            if not isinstance(s, dict):
                continue
            ticker = s.get("ticker", "")
            if ticker:
                stock_scores.setdefault(ticker, []).append(_bounded_score(s.get("impact_score")))

    top_stocks = sorted(
        [{"ticker": t, "avg_impact": sum(v) / len(v), "mention_count": len(v)} for t, v in stock_scores.items()],
        key=lambda x: x["mention_count"],
        reverse=True,
    )[:10]

    async with db.execute(
        "SELECT COUNT(*) FROM news_items WHERE analysis_status IN ('pending', 'processing')"
    ) as cur:
        row = await cur.fetchone()
        pending_count = row[0] if row else 0

    return {
        "window_days": days,
        "total_analyzed": stats.get("total", 0) or 0,
        "avg_sentiment": round(stats.get("avg_sentiment") or 0, 2),
        "bullish_count": stats.get("bullish", 0) or 0,
        "bearish_count": stats.get("bearish", 0) or 0,
        "neutral_count": stats.get("neutral", 0) or 0,
        "pending_count": pending_count,
        "sector_breakdown": sector_counts,
        "sector_sentiment": sector_sentiment,
        "top_affected_stocks": top_stocks,
    }


# --- x_sentiments ---

async def insert_x_sentiment(db: aiosqlite.Connection, sentiment: dict) -> int:
    cursor = await db.execute(
        """INSERT INTO x_sentiments
           (query, trending_tickers, retail_sentiment_score, key_narratives, meme_stocks, raw_analysis, fear_greed_estimate, analyzed_at)
           VALUES (:query, :trending_tickers, :retail_sentiment_score, :key_narratives, :meme_stocks, :raw_analysis, :fear_greed_estimate, :analyzed_at)""",
        sentiment,
    )
    await db.commit()
    sentiment_id = cursor.lastrowid
    try:
        await cleanup_retained_data(db)
    except Exception:
        logger.exception("Post-sentiment retention cleanup failed")
    return sentiment_id


async def get_latest_x_sentiment(db: aiosqlite.Connection) -> Optional[dict]:
    async with db.execute(
        "SELECT * FROM x_sentiments ORDER BY analyzed_at DESC LIMIT 1"
    ) as cur:
        row = await cur.fetchone()
    if not row:
        return None
    return parse_json_fields(row_to_dict(row), ["trending_tickers", "key_narratives", "meme_stocks"])


async def get_x_sentiment_history(
    db: aiosqlite.Connection, page: int = 1, page_size: int = 20
) -> tuple[int, list[dict]]:
    offset = (page - 1) * page_size

    async with db.execute("SELECT COUNT(*) FROM x_sentiments") as cur:
        row = await cur.fetchone()
        total = row[0] if row else 0

    async with db.execute(
        "SELECT * FROM x_sentiments ORDER BY analyzed_at DESC LIMIT ? OFFSET ?",
        (page_size, offset),
    ) as cur:
        rows = await cur.fetchall()

    items = [parse_json_fields(row_to_dict(r), ["trending_tickers", "key_narratives", "meme_stocks"]) for r in rows]
    return total, items


# --- settings ---

async def get_setting(db: aiosqlite.Connection, key: str) -> Optional[Any]:
    async with db.execute("SELECT value FROM settings WHERE key = ?", (key,)) as cur:
        row = await cur.fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        return row[0]


async def set_setting(db: aiosqlite.Connection, key: str, value: Any) -> None:
    serialized = json.dumps(value)
    await db.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, serialized),
    )
    await db.commit()


async def get_all_settings(db: aiosqlite.Connection) -> dict[str, Any]:
    async with db.execute("SELECT key, value FROM settings") as cur:
        rows = await cur.fetchall()
    result = {}
    for row in rows:
        try:
            result[row[0]] = json.loads(row[1])
        except (json.JSONDecodeError, TypeError):
            result[row[0]] = row[1]
    return result


# --- Asset Sentiment Aggregation ---

# Direct aliases only; constituent or ETF proxies are deliberately excluded.
_ASSET_TICKER_ALIASES: dict[str, set[str]] = {
    "^IXIC": {"^IXIC", "IXIC", "NASDAQ", "NASDAQCOMPOSITE"},
    "^GSPC": {"^GSPC", "GSPC", "SPX", "SP500", "S&P500"},
    "^N225": {"^N225", "N225", "NIKKEI", "NIKKEI225"},
    "000001.SS": {"000001.SS", "SHCOMP", "SSECOMPOSITE"},
}

_COMMODITY_NAMES: dict[str, set[str]] = {
    "GC=F": {"gold", "黄金"},
    "SI=F": {"silver", "白银"},
    "CL=F": {"oil", "crude", "crude oil", "原油", "石油"},
}


async def get_asset_sentiment(db: aiosqlite.Connection, symbol: str, days: int = 7) -> dict:
    """Aggregate sentiment for a given asset from recent analyses."""
    from datetime import datetime, timedelta, timezone
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")

    async with db.execute(
        """SELECT a.affected_stocks, a.affected_commodities,
                  a.overall_sentiment, a.classification, a.confidence, a.analyzed_at
           FROM analyses a
           WHERE a.analyzed_at >= ?
           ORDER BY a.analyzed_at DESC""",
        (cutoff,),
    ) as cur:
        rows = await cur.fetchall()

    if not rows:
        return {"score": None, "total": 0, "bullish": 0, "bearish": 0, "neutral": 0,
                "signal": None, "description": None, "tags": []}

    target_tickers = _ASSET_TICKER_ALIASES.get(symbol, {symbol})
    target_commodities = _COMMODITY_NAMES.get(symbol, set())

    weighted_sum = 0.0
    weight_total = 0.0
    bullish = bearish = neutral = 0

    for r in rows:
        stocks = parse_json_fields({"affected_stocks": r[0]}, ["affected_stocks"])["affected_stocks"]
        commodities = parse_json_fields(
            {"affected_commodities": r[1]},
            ["affected_commodities"],
        )["affected_commodities"]
        sentiment = r[2] or 0
        cls = r[3] or "neutral"
        confidence = r[4] if r[4] is not None else 50

        relevance = 0.0

        # Check ticker overlap
        row_tickers = {
            str(item.get("ticker") or "").upper().lstrip("$")
            for item in stocks
            if isinstance(item, dict)
        }
        ticker_hits = row_tickers & target_tickers
        if ticker_hits:
            relevance += len(ticker_hits) * 3.0

        # Check commodity name overlap
        if target_commodities:
            com_names = set()
            for c in commodities:
                if isinstance(c, dict):
                    com_names.add(str(c.get("name") or "").strip().lower())
                elif isinstance(c, str):
                    com_names.add(c.strip().lower())
            if com_names & target_commodities:
                relevance += 3.0

        if relevance <= 0:
            continue

        w = relevance * (confidence / 100.0)
        weighted_sum += sentiment * w
        weight_total += w

        if cls == "bullish":
            bullish += 1
        elif cls == "bearish":
            bearish += 1
        else:
            neutral += 1

    total = bullish + bearish + neutral
    if total == 0 or weight_total == 0:
        return {"score": None, "total": 0, "bullish": 0, "bearish": 0, "neutral": 0,
                "signal": None, "description": None, "tags": []}

    avg_sentiment = weighted_sum / weight_total  # -100 to 100
    # Normalise to 0–100 scale
    score = max(0, min(100, round((avg_sentiment + 100) / 2)))

    if score >= 60:
        signal = "bullish"
    elif score <= 40:
        signal = "bearish"
    else:
        signal = "neutral"

    bull_ratio = bullish / total
    bear_ratio = bearish / total
    neutral_ratio = neutral / total
    desc_parts = []
    if bull_ratio > 0.6:
        desc_parts.append(f"过去 {days} 天内 {total} 条相关新闻中，{round(bull_ratio * 100)}% 偏多。")
    elif bear_ratio > 0.6:
        desc_parts.append(f"过去 {days} 天内 {total} 条相关新闻中，{round(bear_ratio * 100)}% 偏空。")
    elif neutral_ratio > 0.6:
        desc_parts.append(f"过去 {days} 天内 {total} 条相关新闻中，{round(neutral_ratio * 100)}% 为中性。")
    else:
        desc_parts.append(f"过去 {days} 天内 {total} 条相关新闻，多空分歧较大。")

    if avg_sentiment > 30:
        desc_parts.append("这些新闻的模型平均情绪分数偏正。")
    elif avg_sentiment < -30:
        desc_parts.append("这些新闻的模型平均情绪分数偏负。")
    else:
        desc_parts.append("这些新闻的模型平均情绪分数接近中性。")

    return {
        "score": score,
        "total": total,
        "bullish": bullish,
        "bearish": bearish,
        "neutral": neutral,
        "signal": signal,
        "description": " ".join(desc_parts),
        "tags": [],
    }
