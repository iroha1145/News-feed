from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import aiosqlite

logger = logging.getLogger(__name__)


CREATE_ANALYSIS_JOBS = """
CREATE TABLE IF NOT EXISTS analysis_jobs (
    job_id TEXT PRIMARY KEY,
    news_id INTEGER NOT NULL REFERENCES news_items(id) ON DELETE CASCADE,
    input_hash TEXT NOT NULL,
    source_input_hash TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    change_sequence INTEGER CHECK(change_sequence IS NULL OR change_sequence >= 1),
    retry_of_job_id TEXT,
    execution_number INTEGER NOT NULL DEFAULT 1 CHECK(execution_number >= 1),
    status TEXT NOT NULL CHECK(status IN (
        'pending','queued','in_progress','completed','failed','cancelled',
        'insufficient_context','budget_blocked'
    )),
    priority INTEGER NOT NULL DEFAULT 0,
    provider TEXT NOT NULL DEFAULT 'openai',
    model TEXT NOT NULL,
    reasoning_effort TEXT NOT NULL CHECK(reasoning_effort IN ('none','low','medium','high','xhigh','max')),
    execution_mode TEXT NOT NULL DEFAULT 'background' CHECK(execution_mode IN ('background','worker_sync')),
    max_output_tokens INTEGER NOT NULL DEFAULT 16384 CHECK(max_output_tokens >= 256),
    prompt_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    openai_response_id TEXT,
    submitted_at TEXT,
    last_polled_at TEXT,
    completed_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    retrieve_error_count INTEGER NOT NULL DEFAULT 0 CHECK(retrieve_error_count >= 0),
    cancel_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(cancel_attempt_count >= 0),
    next_attempt_at TEXT,
    error_code TEXT,
    cancel_requested_at TEXT,
    usage_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_input_tokens >= 0),
    usage_cached_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_cached_input_tokens >= 0),
    usage_output_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_output_tokens >= 0),
    lease_owner TEXT,
    lease_expires_at TEXT,
    fencing_token INTEGER NOT NULL DEFAULT 0 CHECK(fencing_token >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(news_id, input_hash, model, prompt_version, schema_version)
)
"""

CREATE_ANALYSIS_REVISIONS = """
CREATE TABLE IF NOT EXISTS analysis_revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    news_id INTEGER NOT NULL REFERENCES news_items(id) ON DELETE CASCADE,
    job_id TEXT REFERENCES analysis_jobs(job_id) ON DELETE SET NULL,
    revision INTEGER NOT NULL CHECK(revision >= 1),
    input_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    reasoning_effort TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    analyzed_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    usage_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_input_tokens >= 0),
    usage_cached_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_cached_input_tokens >= 0),
    usage_output_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_output_tokens >= 0),
    is_legacy INTEGER NOT NULL DEFAULT 0 CHECK(is_legacy IN (0,1)),
    created_at TEXT NOT NULL,
    UNIQUE(news_id, revision)
)
"""

CREATE_ANALYSIS_STOCK_IMPACTS = """
CREATE TABLE IF NOT EXISTS analysis_stock_impacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_id INTEGER NOT NULL REFERENCES analysis_revisions(id) ON DELETE CASCADE,
    news_id INTEGER NOT NULL REFERENCES news_items(id) ON DELETE CASCADE,
    ticker TEXT NOT NULL CHECK(length(ticker) BETWEEN 1 AND 20),
    company TEXT NOT NULL CHECK(length(company) BETWEEN 1 AND 200),
    impact_score INTEGER NOT NULL CHECK(impact_score BETWEEN -100 AND 100),
    confidence INTEGER NOT NULL CHECK(confidence BETWEEN 0 AND 100),
    horizon TEXT NOT NULL CHECK(horizon IN ('intraday','days','weeks','uncertain')),
    mechanism TEXT NOT NULL CHECK(mechanism IN (
        'direct_company','supplier_customer','sector_readthrough','macro_rate',
        'commodity_input','regulatory','competitive','other'
    )),
    reason TEXT NOT NULL CHECK(length(reason) BETWEEN 1 AND 2000),
    source TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    published_at TEXT,
    fetched_at TEXT NOT NULL,
    analyzed_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    model TEXT NOT NULL,
    reasoning_effort TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    UNIQUE(analysis_id, ticker)
)
"""

CREATE_CALENDAR_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS calendar_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_token TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    source_fetched_at TEXT NOT NULL,
    data_through TEXT,
    is_stale INTEGER NOT NULL DEFAULT 0 CHECK(is_stale IN (0,1)),
    created_at TEXT NOT NULL
)
"""

CREATE_CALENDAR_EVENT_REVISIONS = """
CREATE TABLE IF NOT EXISTS calendar_event_revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER NOT NULL REFERENCES calendar_snapshots(id) ON DELETE CASCADE,
    event_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(revision >= 1),
    currency TEXT NOT NULL CHECK(length(currency) = 3),
    title TEXT NOT NULL,
    impact TEXT NOT NULL CHECK(impact IN ('low','medium','high','holiday')),
    scheduled_at TEXT NOT NULL,
    forecast TEXT,
    previous TEXT,
    actual TEXT,
    content_hash TEXT NOT NULL,
    is_stale INTEGER NOT NULL DEFAULT 0 CHECK(is_stale IN (0,1)),
    source_fetched_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(event_id, revision),
    UNIQUE(event_id, content_hash)
)
"""

CREATE_INTEGRATION_CHANGES = """
CREATE TABLE IF NOT EXISTS integration_changes (
    change_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL CHECK(entity_type IN ('news','analysis','calendar','source_health')),
    entity_id TEXT NOT NULL,
    operation TEXT NOT NULL CHECK(operation IN ('upsert','delete')),
    payload_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""

CREATE_INTEGRATION_NONCES = """
CREATE TABLE IF NOT EXISTS integration_nonces (
    key_id TEXT NOT NULL,
    nonce TEXT NOT NULL,
    received_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    PRIMARY KEY(key_id, nonce)
)
"""

CREATE_SOURCE_HEALTH = """
CREATE TABLE IF NOT EXISTS source_health (
    source TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK(status IN ('ok','degraded','unavailable','not_configured','disabled')),
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

CREATE_ANALYSIS_WORKER_STATE = """
CREATE TABLE IF NOT EXISTS analysis_worker_state (
    worker_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('idle','working','stopping','failed')),
    last_job_id TEXT,
    error_code TEXT
)
"""

TABLES = [
    CREATE_ANALYSIS_JOBS,
    CREATE_ANALYSIS_REVISIONS,
    CREATE_ANALYSIS_STOCK_IMPACTS,
    CREATE_CALENDAR_SNAPSHOTS,
    CREATE_CALENDAR_EVENT_REVISIONS,
    CREATE_INTEGRATION_CHANGES,
    CREATE_INTEGRATION_NONCES,
    CREATE_SOURCE_HEALTH,
    CREATE_ANALYSIS_WORKER_STATE,
]

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_analysis_jobs_ready ON analysis_jobs(status, next_attempt_at, priority DESC, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_analysis_jobs_lease ON analysis_jobs(status, lease_expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_analysis_jobs_news ON analysis_jobs(news_id, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_analysis_jobs_idempotency ON analysis_jobs(news_id, source_input_hash, model, reasoning_effort, prompt_version, schema_version, execution_number DESC)",
    "CREATE INDEX IF NOT EXISTS idx_analysis_revisions_news_available ON analysis_revisions(news_id, available_at DESC, revision DESC)",
    "CREATE INDEX IF NOT EXISTS idx_analysis_revisions_analyzed ON analysis_revisions(analyzed_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_stock_impacts_ticker_available ON analysis_stock_impacts(ticker, available_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_stock_impacts_content_hash ON analysis_stock_impacts(content_hash)",
    "CREATE INDEX IF NOT EXISTS idx_stock_impacts_news ON analysis_stock_impacts(news_id)",
    "CREATE INDEX IF NOT EXISTS idx_stock_impacts_analyzed ON analysis_stock_impacts(analyzed_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_calendar_events_scheduled_available ON calendar_event_revisions(scheduled_at, available_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_integration_changes_updated ON integration_changes(updated_at, change_sequence)",
    "CREATE INDEX IF NOT EXISTS idx_integration_changes_entity ON integration_changes(entity_type, entity_id, change_sequence DESC)",
    "CREATE INDEX IF NOT EXISTS idx_integration_nonces_expires ON integration_nonces(expires_at)",
]

TRIGGERS = [
    """
    CREATE TRIGGER IF NOT EXISTS trg_news_integration_insert
    AFTER INSERT ON news_items
    BEGIN
      INSERT INTO integration_changes(entity_type, entity_id, operation, payload_hash, updated_at)
      VALUES('news', CAST(NEW.id AS TEXT), 'upsert', NEW.content_hash, COALESCE(NEW.updated_at, NEW.fetched_at));
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_analysis_revision_integration_insert
    AFTER INSERT ON analysis_revisions
    BEGIN
      INSERT INTO integration_changes(entity_type, entity_id, operation, payload_hash, updated_at)
      VALUES('analysis', CAST(NEW.news_id AS TEXT), 'upsert', NEW.input_hash, NEW.available_at);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_calendar_revision_integration_insert
    AFTER INSERT ON calendar_event_revisions
    BEGIN
      INSERT INTO integration_changes(entity_type, entity_id, operation, payload_hash, updated_at)
      VALUES('calendar', NEW.event_id, 'upsert', NEW.content_hash, NEW.available_at);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_source_health_integration_insert
    AFTER INSERT ON source_health
    BEGIN
      INSERT INTO integration_changes(entity_type, entity_id, operation, payload_hash, updated_at)
      VALUES('source_health', NEW.source, 'upsert', NEW.status, NEW.updated_at);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_source_health_integration_update
    AFTER UPDATE ON source_health
    BEGIN
      INSERT INTO integration_changes(entity_type, entity_id, operation, payload_hash, updated_at)
      VALUES('source_health', NEW.source, 'upsert', NEW.status, NEW.updated_at);
    END
    """,
]


async def _add_column(db: aiosqlite.Connection, table: str, column: str, definition: str) -> None:
    try:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    except aiosqlite.OperationalError as exc:
        if "duplicate column" not in str(exc).lower():
            raise


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _legacy_payload(row: aiosqlite.Row, stocks: list[dict[str, Any]]) -> dict[str, Any]:
    try:
        sectors = json.loads(row[9] or "[]")
    except (TypeError, json.JSONDecodeError):
        sectors = []
    try:
        commodities = json.loads(row[10] or "[]")
    except (TypeError, json.JSONDecodeError):
        commodities = []
    try:
        factors = json.loads(row[12] or "[]")
    except (TypeError, json.JSONDecodeError):
        factors = []
    normalized_stocks = []
    for stock in stocks:
        if not isinstance(stock, dict):
            continue
        ticker = str(stock.get("ticker") or "").strip().upper().lstrip("$")[:20]
        if not ticker:
            continue
        normalized_stocks.append(
            {
                "ticker": ticker,
                "company": str(stock.get("company") or ticker).strip()[:200] or ticker,
                "impact_score": max(-100, min(100, int(stock.get("impact_score") or 0))),
                "confidence": 0,
                "horizon": "uncertain",
                "mechanism": "other",
                "reason": str(stock.get("reason") or "Legacy analysis").strip()[:2000] or "Legacy analysis",
            }
        )
    return {
        "title_zh": str(row[3] or "历史新闻")[:500] or "历史新闻",
        "headline_summary": str(row[4] or "历史分析记录")[:2000] or "历史分析记录",
        "overall_sentiment": max(-100, min(100, int(row[5] or 0))),
        "classification": row[6] if row[6] in {"bullish", "bearish", "neutral"} else "neutral",
        "confidence": max(0, min(100, int(row[7] or 0))),
        "market_relevance": 0,
        "affected_stocks": normalized_stocks,
        "affected_sectors": [str(value)[:500] for value in sectors if isinstance(value, str) and value.strip()][:50],
        "affected_commodities": [
            {
                "name": str(value.get("name") or "Other")[:500],
                "impact_score": max(-100, min(100, int(value.get("impact_score") or 0))),
                "reason": str(value.get("reason") or "Legacy analysis")[:2000] or "Legacy analysis",
            }
            for value in commodities
            if isinstance(value, dict)
        ][:30],
        # Legacy logic_chain may contain verbose private reasoning. It is never
        # copied into the new public contract without a trusted summarization.
        "causal_summary": "旧版分析未保存可安全公开的因果摘要。",
        "key_factors": [str(value)[:500] for value in factors if isinstance(value, str) and value.strip()][:30],
        "uncertainty_notes": ["该记录由旧版结构迁移，缺少部分新字段。"],
        "insufficient_context": False,
    }


async def _backfill_legacy_analyses(db: aiosqlite.Connection) -> None:
    async with db.execute(
        """SELECT a.id, a.news_id, n.fetched_at, a.title_zh, a.headline_summary,
                  a.overall_sentiment, a.classification, a.confidence, a.affected_stocks,
                  a.affected_sectors, a.affected_commodities, a.logic_chain, a.key_factors,
                  a.llm_provider, a.llm_model, a.analyzed_at, n.source, n.content_hash, n.published_at
           FROM analyses a JOIN news_items n ON n.id = a.news_id
           WHERE NOT EXISTS (SELECT 1 FROM analysis_revisions r WHERE r.news_id = a.news_id)
           ORDER BY a.id"""
    ) as cursor:
        rows = await cursor.fetchall()

    for row in rows:
        try:
            parsed_stocks = json.loads(row[8] or "[]")
            if not isinstance(parsed_stocks, list):
                raise ValueError("affected_stocks is not a list")
        except (TypeError, ValueError, json.JSONDecodeError):
            logger.warning("Skipping malformed legacy stock projection for analysis_id=%s", row[0])
            parsed_stocks = []
        payload = _legacy_payload(row, parsed_stocks)
        analyzed_at = str(row[15])
        fetched_at = str(row[2])
        available_at = max(fetched_at, analyzed_at)
        cursor = await db.execute(
            """INSERT OR IGNORE INTO analysis_revisions
               (news_id, job_id, revision, input_hash, payload_json, provider, model,
                reasoning_effort, prompt_version, schema_version, fetched_at, analyzed_at,
                available_at, is_legacy, created_at)
               VALUES (?, NULL, 1, ?, ?, ?, ?, 'none', 'legacy-v1', 'legacy-v1', ?, ?, ?, 1, ?)""",
            (
                row[1], f"legacy:{row[0]}", json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                row[13] or "legacy", row[14] or "legacy", fetched_at, analyzed_at,
                available_at, _utc_now(),
            ),
        )
        if cursor.rowcount == 0:
            continue
        revision_id = cursor.lastrowid
        seen: set[str] = set()
        for stock in payload["affected_stocks"]:
            ticker = stock["ticker"]
            if ticker in seen:
                continue
            seen.add(ticker)
            await db.execute(
                """INSERT OR IGNORE INTO analysis_stock_impacts
                   (analysis_id, news_id, ticker, company, impact_score, confidence, horizon,
                    mechanism, reason, source, content_hash, published_at, fetched_at,
                    analyzed_at, available_at, model, reasoning_effort, prompt_version, schema_version)
                   VALUES (?, ?, ?, ?, ?, 0, 'uncertain', 'other', ?, ?, ?, ?, ?, ?, ?, ?,
                           'none', 'legacy-v1', 'legacy-v1')""",
                (
                    revision_id, row[1], ticker, stock["company"], stock["impact_score"],
                    stock["reason"], row[16], row[17], row[18], fetched_at, analyzed_at,
                    available_at, row[14] or "legacy",
                ),
            )


async def init_catalyst_schema(db: aiosqlite.Connection) -> None:
    """Apply additive, idempotent integration migrations without rebuilding legacy tables."""
    await _add_column(db, "news_items", "updated_at", "TEXT")
    await _add_column(db, "news_items", "source_tickers", "TEXT NOT NULL DEFAULT '[]'")
    await db.execute("UPDATE news_items SET updated_at = COALESCE(updated_at, fetched_at) WHERE updated_at IS NULL")

    for statement in TABLES:
        await db.execute(statement)
    await _add_column(db, "analysis_jobs", "retrieve_error_count", "INTEGER NOT NULL DEFAULT 0 CHECK(retrieve_error_count >= 0)")
    await _add_column(db, "analysis_jobs", "cancel_attempt_count", "INTEGER NOT NULL DEFAULT 0 CHECK(cancel_attempt_count >= 0)")
    await _add_column(db, "analysis_jobs", "source_input_hash", "TEXT")
    await _add_column(db, "analysis_jobs", "content_hash", "TEXT")
    await _add_column(db, "analysis_jobs", "change_sequence", "INTEGER CHECK(change_sequence IS NULL OR change_sequence >= 1)")
    await _add_column(db, "analysis_jobs", "retry_of_job_id", "TEXT")
    await _add_column(db, "analysis_jobs", "execution_number", "INTEGER NOT NULL DEFAULT 1 CHECK(execution_number >= 1)")
    await _add_column(db, "analysis_jobs", "execution_mode", "TEXT NOT NULL DEFAULT 'background' CHECK(execution_mode IN ('background','worker_sync'))")
    await _add_column(db, "analysis_jobs", "max_output_tokens", "INTEGER NOT NULL DEFAULT 16384 CHECK(max_output_tokens >= 256)")
    await db.execute(
        "UPDATE analysis_jobs SET source_input_hash=input_hash WHERE source_input_hash IS NULL"
    )
    await db.execute(
        """UPDATE analysis_jobs SET content_hash=(
             SELECT n.content_hash FROM news_items n WHERE n.id=analysis_jobs.news_id
           ) WHERE content_hash IS NULL"""
    )
    await _add_column(db, "source_health", "raw_count", "INTEGER CHECK(raw_count IS NULL OR raw_count >= 0)")
    await _add_column(db, "source_health", "inserted_count", "INTEGER CHECK(inserted_count IS NULL OR inserted_count >= 0)")
    await _add_column(db, "source_health", "duplicates_count", "INTEGER CHECK(duplicates_count IS NULL OR duplicates_count >= 0)")
    for statement in INDEXES:
        await db.execute(statement)
    for statement in TRIGGERS:
        await db.execute(statement)
    await db.execute(
        """INSERT OR IGNORE INTO source_health
           (source,status,consecutive_failures,updated_at)
           VALUES ('faireconomy','unavailable',0,?)""",
        (_utc_now(),),
    )

    # A legacy in-flight row has no durable upstream response identifier. It is
    # safe to return it to pending, but it is never submitted during migration.
    await db.execute(
        """UPDATE news_items
           SET analysis_status='pending', analysis_claimed_at=NULL,
               analysis_lease_expires_at=NULL,
               analysis_error='Migrated legacy processing row; no durable response id'
           WHERE analysis_status='processing'"""
    )

    # Terra queue identity is process-wide because the web and worker run in
    # separate containers.  Remove legacy UI overrides so Calendar, News, and
    # Integration Health cannot silently disagree after a restart.
    await db.execute(
        "DELETE FROM settings WHERE key IN ('default_llm_provider','default_llm_model')"
    )

    await _backfill_legacy_analyses(db)

    # Existing news predates the insert trigger. One seed change per news item
    # gives incremental clients a complete first snapshot without rewriting it.
    await db.execute(
        """INSERT INTO integration_changes(entity_type, entity_id, operation, payload_hash, updated_at)
           SELECT 'news', CAST(n.id AS TEXT), 'upsert', n.content_hash, n.updated_at
           FROM news_items n
           WHERE NOT EXISTS (
             SELECT 1 FROM integration_changes c
             WHERE c.entity_type='news' AND c.entity_id=CAST(n.id AS TEXT)
           )"""
    )
    await db.commit()
