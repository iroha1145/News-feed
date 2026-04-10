import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import aiosqlite

from app.config import settings

logger = logging.getLogger(__name__)


def _resolve_db_path() -> str:
    parsed = urlparse(settings.database_url)
    if parsed.scheme != "sqlite+aiosqlite":
        raise ValueError(f"Unsupported database_url scheme: {settings.database_url}")

    db_path = parsed.path.lstrip("/")
    if parsed.netloc:
        db_path = f"{parsed.netloc}/{db_path}" if db_path else parsed.netloc

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
    analysis_error TEXT DEFAULT ''
)
"""

CREATE_ANALYSES = """
CREATE TABLE IF NOT EXISTS analyses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    news_id INTEGER NOT NULL REFERENCES news_items(id),
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


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    return db


async def init_db() -> None:
    logger.info("Initializing database tables...")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_NEWS_ITEMS)
        await db.execute(CREATE_ANALYSES)
        await db.execute(CREATE_X_SENTIMENTS)
        await db.execute(CREATE_SETTINGS)
        # Centralized migration list: (table, column, definition)
        migrations = [
            ("news_items", "analysis_status", "TEXT DEFAULT 'pending'"),
            ("news_items", "analysis_attempts", "INTEGER DEFAULT 0"),
            ("news_items", "analysis_error", "TEXT DEFAULT ''"),
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
        await db.commit()
    logger.info("Database tables initialized successfully")


def row_to_dict(row: aiosqlite.Row) -> dict:
    return dict(row)


def parse_json_fields(d: dict, fields: list[str]) -> dict:
    for field in fields:
        if field in d and isinstance(d[field], str):
            try:
                d[field] = json.loads(d[field])
            except (json.JSONDecodeError, TypeError):
                d[field] = []
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
        return None  # duplicate


async def get_news_items(
    db: aiosqlite.Connection,
    page: int = 1,
    page_size: int = 20,
    source: Optional[str] = None,
) -> tuple[int, list[dict]]:
    offset = (page - 1) * page_size
    where = "WHERE source = ?" if source else ""
    params_count: list[Any] = [source] if source else []
    params_list: list[Any] = [source] if source else []
    params_list += [page_size, offset]

    async with db.execute(f"SELECT COUNT(*) FROM news_items {where}", params_count) as cur:
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
                     AND n.published_at >= ?
                THEN 1 ELSE 0 
            END as is_pinned
        FROM news_items n
        LEFT JOIN analyses a ON a.news_id = n.id
        {where} ORDER BY is_pinned DESC, n.published_at DESC LIMIT ? OFFSET ?""",
        [pin_cutoff] + params_list,
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
        items.append(d)
    return total, items


async def get_news_item_by_id(db: aiosqlite.Connection, news_id: int) -> Optional[dict]:
    async with db.execute("SELECT * FROM news_items WHERE id = ?", (news_id,)) as cur:
        row = await cur.fetchone()
    return row_to_dict(row) if row else None


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
    """Mark a news item as 'processing' to prevent duplicate LLM calls. Returns True if claim succeeded."""
    cursor = await db.execute(
        "UPDATE news_items SET analysis_status = 'processing', analysis_attempts = analysis_attempts + 1 WHERE id = ? AND analysis_status IN ('pending', 'failed')",
        (news_id,),
    )
    await db.commit()
    return cursor.rowcount > 0


async def mark_analysis_completed(db: aiosqlite.Connection, news_id: int) -> None:
    await db.execute("UPDATE news_items SET analysis_status = 'completed' WHERE id = ?", (news_id,))
    await db.commit()


async def mark_analysis_failed(db: aiosqlite.Connection, news_id: int, error: str) -> None:
    await db.execute(
        "UPDATE news_items SET analysis_status = CASE WHEN analysis_attempts >= 3 THEN 'failed' ELSE 'pending' END, analysis_error = ? WHERE id = ?",
        (error[:500], news_id),
    )
    await db.commit()


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
    return cursor.lastrowid


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


async def get_analysis_stats(db: aiosqlite.Connection) -> dict:
    async with db.execute(
        """SELECT
               COUNT(*) as total,
               AVG(overall_sentiment) as avg_sentiment,
               SUM(CASE WHEN classification='bullish' THEN 1 ELSE 0 END) as bullish,
               SUM(CASE WHEN classification='bearish' THEN 1 ELSE 0 END) as bearish,
               SUM(CASE WHEN classification='neutral' THEN 1 ELSE 0 END) as neutral
           FROM analyses"""
    ) as cur:
        row = await cur.fetchone()
    stats = row_to_dict(row) if row else {}

    # Sector breakdown with per-sector sentiment
    async with db.execute("SELECT affected_sectors, overall_sentiment, classification FROM analyses") as cur:
        rows = await cur.fetchall()

    sector_data: dict[str, dict] = {}
    for r in rows:
        sectors = json.loads(r[0]) if r[0] else []
        sentiment = r[1] or 0
        cls = r[2] or "neutral"
        for s in sectors:
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
    async with db.execute("SELECT affected_stocks FROM analyses") as cur:
        rows = await cur.fetchall()

    stock_scores: dict[str, list[int]] = {}
    for r in rows:
        stocks = json.loads(r[0]) if r[0] else []
        for s in stocks:
            ticker = s.get("ticker", "")
            if ticker:
                stock_scores.setdefault(ticker, []).append(s.get("impact_score", 0))

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
    return cursor.lastrowid


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

# Map index symbols → representative tickers & sectors
_INDEX_MAPPING: dict[str, dict] = {
    "^IXIC":    {"sectors": ["Technology", "Tech", "Semiconductor", "Software", "Internet"],
                 "tickers": ["AAPL", "MSFT", "GOOG", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "NFLX", "AMD", "INTC", "QCOM"]},
    "^GSPC":    {"sectors": ["Technology", "Finance", "Healthcare", "Energy", "Consumer"],
                 "tickers": ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "JPM", "V", "UNH", "JNJ", "XOM"]},
    "^N225":    {"sectors": ["Technology", "Automotive", "Manufacturing", "Finance"],
                 "tickers": ["TM", "SONY", "HMC", "NTDOY", "MUFG"]},
    "000001.SS": {"sectors": ["Finance", "Technology", "Energy", "Consumer"],
                  "tickers": ["BABA", "JD", "PDD", "BIDU", "NIO"]},
}

_COMMODITY_MAPPING: dict[str, dict] = {
    "GC=F": {"commodities": ["Gold", "gold", "黄金"], "sectors": ["Mining", "Precious Metals"]},
    "SI=F": {"commodities": ["Silver", "silver", "白银"], "sectors": ["Mining", "Precious Metals"]},
    "CL=F": {"commodities": ["Oil", "oil", "Crude", "crude", "原油", "石油"], "sectors": ["Energy", "Oil"]},
}


async def get_asset_sentiment(db: aiosqlite.Connection, symbol: str, days: int = 7) -> dict:
    """Aggregate sentiment for a given asset from recent analyses."""
    from datetime import datetime, timedelta, timezone
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")

    async with db.execute(
        """SELECT a.affected_stocks, a.affected_sectors, a.affected_commodities,
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

    idx_map = _INDEX_MAPPING.get(symbol, {})
    com_map = _COMMODITY_MAPPING.get(symbol, {})
    target_tickers = set(idx_map.get("tickers", []))
    target_sectors = set(idx_map.get("sectors", []))
    target_commodities = set(com_map.get("commodities", []))
    target_com_sectors = set(com_map.get("sectors", []))

    weighted_sum = 0.0
    weight_total = 0.0
    bullish = bearish = neutral = 0

    for r in rows:
        stocks = json.loads(r[0]) if r[0] else []
        sectors = json.loads(r[1]) if r[1] else []
        commodities = json.loads(r[2]) if r[2] else []
        sentiment = r[3] or 0
        cls = r[4] or "neutral"
        confidence = r[5] or 50

        relevance = 0.0

        # Check ticker overlap
        row_tickers = {s.get("ticker", "") for s in stocks if isinstance(s, dict)}
        ticker_hits = row_tickers & target_tickers
        if ticker_hits:
            relevance += len(ticker_hits) * 2.0

        # Check sector overlap
        sector_set = set(sectors) if isinstance(sectors, list) else set()
        sector_hits = sector_set & (target_sectors | target_com_sectors)
        if sector_hits:
            relevance += len(sector_hits) * 1.0

        # Check commodity name overlap
        if target_commodities:
            com_names = set()
            for c in commodities:
                if isinstance(c, dict):
                    com_names.add(c.get("name", ""))
                elif isinstance(c, str):
                    com_names.add(c)
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

    if score >= 65:
        signal = "Bullish Divergence"
        tags = ["Momentum Long", "Low Risk"] if score >= 75 else ["Accumulation", "Medium Risk"]
    elif score <= 35:
        signal = "Bearish Divergence"
        tags = ["Risk Off", "High Risk"] if score <= 25 else ["Distribution", "Medium Risk"]
    else:
        signal = "Neutral Range"
        tags = ["Consolidation", "Neutral"]

    bull_ratio = bullish / total
    desc_parts = []
    if bull_ratio > 0.6:
        desc_parts.append(f"过去 {days} 天内 {total} 条相关新闻中，{round(bull_ratio * 100)}% 偏多。")
    elif bull_ratio < 0.4:
        desc_parts.append(f"过去 {days} 天内 {total} 条相关新闻中，{round((1 - bull_ratio) * 100)}% 偏空。")
    else:
        desc_parts.append(f"过去 {days} 天内 {total} 条相关新闻，多空分歧较大。")

    if avg_sentiment > 30:
        desc_parts.append("整体情绪积极，资金面或有积累信号。")
    elif avg_sentiment < -30:
        desc_parts.append("整体情绪偏负面，注意风险控制。")
    else:
        desc_parts.append("市场情绪震荡，建议观望。")

    return {
        "score": score,
        "total": total,
        "bullish": bullish,
        "bearish": bearish,
        "neutral": neutral,
        "signal": signal,
        "description": " ".join(desc_parts),
        "tags": tags,
    }
