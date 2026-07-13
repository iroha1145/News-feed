from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

import aiosqlite

logger = logging.getLogger(__name__)

CATALYST_SCHEMA_MIGRATION = 3


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
        'insufficient_context','budget_blocked','incomplete_output'
    )),
    priority INTEGER NOT NULL DEFAULT 0,
    provider TEXT NOT NULL DEFAULT 'openai',
    model TEXT NOT NULL,
    reasoning_effort TEXT NOT NULL CHECK(reasoning_effort IN ('none','low','medium','high','xhigh','max')),
    execution_mode TEXT NOT NULL DEFAULT 'background' CHECK(execution_mode IN ('background','worker_sync')),
    max_output_tokens INTEGER NOT NULL DEFAULT 32768 CHECK(max_output_tokens BETWEEN 256 AND 128000),
    task_type TEXT NOT NULL DEFAULT 'news_item',
    request_origin TEXT NOT NULL DEFAULT 'manual' CHECK(request_origin IN ('manual','automatic')),
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
    usage_cache_write_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_cache_write_tokens >= 0),
    usage_reasoning_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_reasoning_tokens >= 0),
    usage_output_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_output_tokens >= 0),
    usage_total_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_total_tokens >= 0),
    latency_ms INTEGER CHECK(latency_ms IS NULL OR latency_ms >= 0),
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
    validation_status TEXT NOT NULL DEFAULT 'unverified' CHECK(validation_status IN (
        'canonical','valid_external','ambiguous','invalid','unverified'
    )),
    validated_at TEXT,
    focus_revision INTEGER,
    universe_version TEXT,
    association_method TEXT NOT NULL DEFAULT 'llm_inference' CHECK(association_method='llm_inference'),
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
    source_fetch_status TEXT NOT NULL DEFAULT 'unavailable' CHECK(source_fetch_status IN (
        'ok','degraded','unavailable','not_configured','disabled'
    )),
    news_persistence_status TEXT NOT NULL DEFAULT 'unavailable' CHECK(news_persistence_status IN (
        'ok','degraded','unavailable','not_configured','disabled'
    )),
    event_projection_status TEXT NOT NULL DEFAULT 'unavailable' CHECK(event_projection_status IN (
        'ok','degraded','unavailable','not_configured','disabled'
    )),
    updated_at TEXT NOT NULL
)
"""

CREATE_PROJECTION_SAFETY_COUNTERS = """
CREATE TABLE IF NOT EXISTS projection_safety_counters (
    counter_key TEXT PRIMARY KEY,
    count INTEGER NOT NULL DEFAULT 0 CHECK(count >= 0),
    updated_at TEXT NOT NULL
)
"""

CREATE_EVENT_PROJECTION_RETRIES = """
CREATE TABLE IF NOT EXISTS event_projection_retries (
    retry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    payload_hash TEXT NOT NULL UNIQUE,
    news_id INTEGER REFERENCES news_items(id) ON DELETE SET NULL,
    source TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','in_progress','completed','failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    next_attempt_at TEXT,
    last_error_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT
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

CREATE_FOCUS_CONTEXT_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS focus_context_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    revision INTEGER NOT NULL UNIQUE CHECK(revision >= 1),
    schema_version TEXT NOT NULL,
    as_of TEXT NOT NULL,
    data_through TEXT,
    market_session TEXT NOT NULL,
    universe_version TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('current','stale')),
    fetched_at TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""

CREATE_NEWS_TICKER_MENTIONS = """
CREATE TABLE IF NOT EXISTS news_ticker_mentions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    news_id INTEGER NOT NULL REFERENCES news_items(id) ON DELETE CASCADE,
    ticker TEXT NOT NULL CHECK(length(ticker) BETWEEN 1 AND 20),
    association_method TEXT NOT NULL CHECK(association_method IN (
        'provider_tag','company_endpoint','exact_alias','event_propagation','llm_inference'
    )),
    association_confidence REAL NOT NULL CHECK(association_confidence BETWEEN 0 AND 1),
    validation_status TEXT NOT NULL CHECK(validation_status IN (
        'canonical','valid_external','ambiguous','invalid','unverified'
    )),
    validated_at TEXT,
    focus_revision INTEGER,
    universe_version TEXT,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""

CREATE_NEWS_EVENT_GROUPS = """
CREATE TABLE IF NOT EXISTS news_event_groups (
    event_group_id TEXT PRIMARY KEY,
    representative_news_id INTEGER REFERENCES news_items(id) ON DELETE SET NULL,
    representative_title TEXT NOT NULL,
    event_type TEXT NOT NULL,
    first_published_at TEXT,
    last_published_at TEXT,
    first_fetched_at TEXT NOT NULL,
    last_fetched_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    member_count INTEGER NOT NULL DEFAULT 0 CHECK(member_count >= 0),
    source_count INTEGER NOT NULL DEFAULT 0 CHECK(source_count >= 0),
    source_names_json TEXT NOT NULL DEFAULT '[]',
    source_tickers_json TEXT NOT NULL DEFAULT '[]',
    validated_tickers_json TEXT NOT NULL DEFAULT '[]',
    novelty_score REAL NOT NULL DEFAULT 85 CHECK(novelty_score BETWEEN 0 AND 100),
    evidence_fingerprint TEXT NOT NULL DEFAULT '',
    market_confirmation_score REAL CHECK(
        market_confirmation_score IS NULL OR market_confirmation_score BETWEEN 0 AND 100
    ),
    last_hot_score REAL CHECK(last_hot_score IS NULL OR last_hot_score BETWEEN 0 AND 100),
    status TEXT NOT NULL CHECK(status IN ('CLUSTERED','GATED','STORED','PREPARED','LEASED','CONSUMED')),
    version INTEGER NOT NULL DEFAULT 1 CHECK(version >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""

CREATE_NEWS_EVENT_MEMBERS = """
CREATE TABLE IF NOT EXISTS news_event_members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_group_id TEXT NOT NULL REFERENCES news_event_groups(event_group_id) ON DELETE CASCADE,
    news_id INTEGER REFERENCES news_items(id) ON DELETE SET NULL,
    source TEXT NOT NULL,
    normalized_url TEXT NOT NULL,
    title TEXT NOT NULL,
    published_at TEXT,
    fetched_at TEXT NOT NULL,
    source_tickers_json TEXT NOT NULL DEFAULT '[]',
    validated_tickers_json TEXT NOT NULL DEFAULT '[]',
    publisher_identity TEXT NOT NULL DEFAULT 'unknown',
    event_type TEXT NOT NULL DEFAULT 'other',
    evidence_fingerprint TEXT NOT NULL DEFAULT '',
    content_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(event_group_id, source, normalized_url, content_hash)
)
"""

# A preparation row is one monotonic revision. analysis_revisions remains the
# single-news analysis history and is deliberately unrelated to this table.
CREATE_HOTSPOT_PREPARATION_SETS = """
CREATE TABLE IF NOT EXISTS hotspot_preparation_sets (
    prepared_revision INTEGER PRIMARY KEY AUTOINCREMENT,
    event_group_id TEXT NOT NULL REFERENCES news_event_groups(event_group_id) ON DELETE CASCADE,
    event_group_version INTEGER NOT NULL CHECK(event_group_version >= 1),
    gate_version TEXT NOT NULL,
    hot_score REAL NOT NULL CHECK(hot_score BETWEEN 0 AND 100),
    component_scores_json TEXT NOT NULL,
    active_weights_json TEXT NOT NULL,
    reasons_json TEXT NOT NULL,
    event_snapshot_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('PREPARED','LEASED','CONSUMED')),
    prepared_at TEXT NOT NULL,
    leased_cycle_id TEXT,
    consumed_cycle_id TEXT,
    consumed_at TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(event_group_id, event_group_version, gate_version)
)
"""

CREATE_HOTSPOT_PREPARATION_STATE = """
CREATE TABLE IF NOT EXISTS hotspot_preparation_state (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id=1),
    prepared_revision INTEGER NOT NULL DEFAULT 0 CHECK(prepared_revision >= 0),
    last_consumed_revision INTEGER NOT NULL DEFAULT 0 CHECK(last_consumed_revision >= 0),
    last_cycle_at TEXT,
    active_cycle_id TEXT,
    cooldown_until TEXT,
    updated_at TEXT NOT NULL
)
"""

CREATE_MARKET_FOCUS_CYCLES = """
CREATE TABLE IF NOT EXISTS market_focus_cycles (
    cycle_id TEXT PRIMARY KEY,
    scheduled_slot TEXT UNIQUE,
    idempotency_key TEXT NOT NULL UNIQUE,
    retry_of_cycle_id TEXT REFERENCES market_focus_cycles(cycle_id) ON DELETE SET NULL,
    execution_number INTEGER NOT NULL DEFAULT 1 CHECK(execution_number >= 1),
    trigger_type TEXT NOT NULL CHECK(trigger_type IN ('manual','scheduled_0800','scheduled_1200','scheduled_1600','scheduled_2000')),
    status TEXT NOT NULL CHECK(status IN (
        'pending','queued','in_progress','completed','failed','cancelled',
        'budget_blocked','incomplete_output','insufficient_context'
    )),
    no_new_hot_events INTEGER NOT NULL DEFAULT 0 CHECK(no_new_hot_events IN (0,1)),
    prepared_revision INTEGER NOT NULL DEFAULT 0 CHECK(prepared_revision >= 0),
    last_consumed_revision_at_start INTEGER NOT NULL DEFAULT 0 CHECK(last_consumed_revision_at_start >= 0),
    consumes_through_revision INTEGER CHECK(consumes_through_revision IS NULL OR consumes_through_revision >= 1),
    focus_revision INTEGER,
    snapshot_as_of TEXT NOT NULL,
    input_schema_version TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    input_json TEXT NOT NULL,
    event_group_count INTEGER NOT NULL CHECK(event_group_count >= 0),
    focus_symbol_count INTEGER NOT NULL CHECK(focus_symbol_count >= 0),
    provider TEXT NOT NULL DEFAULT 'openai',
    model TEXT NOT NULL,
    reasoning_effort TEXT NOT NULL,
    execution_mode TEXT NOT NULL,
    max_output_tokens INTEGER NOT NULL CHECK(max_output_tokens >= 256),
    prompt_version TEXT NOT NULL,
    output_schema_version TEXT NOT NULL,
    prompt_cache_key TEXT NOT NULL,
    openai_response_id TEXT,
    result_json TEXT,
    error_code TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    retrieve_error_count INTEGER NOT NULL DEFAULT 0 CHECK(retrieve_error_count >= 0),
    cancel_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(cancel_attempt_count >= 0),
    next_attempt_at TEXT,
    cancel_requested_at TEXT,
    lease_owner TEXT,
    lease_expires_at TEXT,
    fencing_token INTEGER NOT NULL DEFAULT 0 CHECK(fencing_token >= 0),
    latency_ms INTEGER CHECK(latency_ms IS NULL OR latency_ms >= 0),
    usage_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_input_tokens >= 0),
    usage_cached_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_cached_input_tokens >= 0),
    usage_cache_write_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_cache_write_tokens >= 0),
    usage_reasoning_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_reasoning_tokens >= 0),
    usage_output_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_output_tokens >= 0),
    usage_total_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_total_tokens >= 0),
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    updated_at TEXT NOT NULL
)
"""

CREATE_MARKET_FOCUS_CYCLE_EVENTS = """
CREATE TABLE IF NOT EXISTS market_focus_cycle_events (
    cycle_id TEXT NOT NULL REFERENCES market_focus_cycles(cycle_id) ON DELETE CASCADE,
    prepared_revision INTEGER NOT NULL REFERENCES hotspot_preparation_sets(prepared_revision) ON DELETE RESTRICT,
    event_group_id TEXT NOT NULL REFERENCES news_event_groups(event_group_id) ON DELETE RESTRICT,
    event_group_version INTEGER NOT NULL,
    snapshot_json TEXT NOT NULL,
    PRIMARY KEY(cycle_id, prepared_revision)
)
"""

CREATE_MARKET_FOCUS_CYCLE_ARCHIVES = """
CREATE TABLE IF NOT EXISTS market_focus_cycle_archives (
    cycle_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    completed_at TEXT,
    cycle_json TEXT NOT NULL,
    event_snapshots_json TEXT NOT NULL,
    result_json TEXT,
    archived_at TEXT NOT NULL
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
    CREATE_PROJECTION_SAFETY_COUNTERS,
    CREATE_EVENT_PROJECTION_RETRIES,
    CREATE_ANALYSIS_WORKER_STATE,
    CREATE_FOCUS_CONTEXT_SNAPSHOTS,
    CREATE_NEWS_TICKER_MENTIONS,
    CREATE_NEWS_EVENT_GROUPS,
    CREATE_NEWS_EVENT_MEMBERS,
    CREATE_HOTSPOT_PREPARATION_SETS,
    CREATE_HOTSPOT_PREPARATION_STATE,
    CREATE_MARKET_FOCUS_CYCLES,
    CREATE_MARKET_FOCUS_CYCLE_EVENTS,
    CREATE_MARKET_FOCUS_CYCLE_ARCHIVES,
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
    "CREATE INDEX IF NOT EXISTS idx_stock_impacts_validated ON analysis_stock_impacts(ticker,validation_status,available_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_calendar_events_scheduled_available ON calendar_event_revisions(scheduled_at, available_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_integration_changes_updated ON integration_changes(updated_at, change_sequence)",
    "CREATE INDEX IF NOT EXISTS idx_integration_changes_entity ON integration_changes(entity_type, entity_id, change_sequence DESC)",
    "CREATE INDEX IF NOT EXISTS idx_integration_nonces_expires ON integration_nonces(expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_focus_context_latest ON focus_context_snapshots(revision DESC)",
    "CREATE INDEX IF NOT EXISTS idx_ticker_mentions_news ON news_ticker_mentions(news_id, ticker, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_ticker_mentions_validated ON news_ticker_mentions(ticker, validation_status, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_event_groups_available ON news_event_groups(available_at DESC, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_event_members_group ON news_event_members(event_group_id, fetched_at)",
    "CREATE INDEX IF NOT EXISTS idx_event_members_retention ON news_event_members(created_at,event_group_id)",
    "CREATE INDEX IF NOT EXISTS idx_projection_retries_ready ON event_projection_retries(status,next_attempt_at,retry_id)",
    "CREATE INDEX IF NOT EXISTS idx_projection_retries_retention ON event_projection_retries(updated_at,status)",
    "CREATE INDEX IF NOT EXISTS idx_hotspot_prepared_status ON hotspot_preparation_sets(status, prepared_revision)",
    "CREATE INDEX IF NOT EXISTS idx_market_focus_cycles_status ON market_focus_cycles(status, created_at DESC)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_market_focus_single_active ON market_focus_cycles((1)) WHERE status IN ('pending','queued','in_progress')",
    "CREATE INDEX IF NOT EXISTS idx_market_focus_cycle_ready ON market_focus_cycles(status,next_attempt_at,created_at)",
    "CREATE INDEX IF NOT EXISTS idx_market_focus_archive_completed ON market_focus_cycle_archives(completed_at,status)",
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


async def _migrate_analysis_job_status_constraint(db: aiosqlite.Connection) -> None:
    async with db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='analysis_jobs'"
    ) as cursor:
        row = await cursor.fetchone()
    if row is None or "incomplete_output" in str(row[0] or ""):
        return
    # SQLite cannot alter a CHECK constraint. Rebuild only this table while
    # retaining its name so analysis_revisions keeps its original FK target.
    await db.execute("DROP TABLE IF EXISTS analysis_jobs_status_v2")
    statement = CREATE_ANALYSIS_JOBS.replace(
        "CREATE TABLE IF NOT EXISTS analysis_jobs",
        "CREATE TABLE analysis_jobs_status_v2",
        1,
    )
    await db.execute(statement)
    async with db.execute("PRAGMA table_info(analysis_jobs)") as cursor:
        old_columns = {str(value[1]) for value in await cursor.fetchall()}
    async with db.execute("PRAGMA table_info(analysis_jobs_status_v2)") as cursor:
        new_columns = [str(value[1]) for value in await cursor.fetchall()]
    shared = [column for column in new_columns if column in old_columns]
    columns = ",".join(f'"{column}"' for column in shared)
    await db.execute(
        f"INSERT INTO analysis_jobs_status_v2 ({columns}) SELECT {columns} FROM analysis_jobs"
    )
    await db.execute("DROP TABLE analysis_jobs")
    await db.execute("ALTER TABLE analysis_jobs_status_v2 RENAME TO analysis_jobs")


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


async def _increment_safety_counter(
    db: aiosqlite.Connection,
    counter_key: str,
    amount: int = 1,
) -> None:
    await db.execute(
        """INSERT INTO projection_safety_counters(counter_key,count,updated_at)
           VALUES (?,?,?)
           ON CONFLICT(counter_key) DO UPDATE SET
             count=projection_safety_counters.count+excluded.count,
             updated_at=excluded.updated_at""",
        (counter_key[:100], max(1, amount), _utc_now()),
    )


async def _backfill_stock_impact_validation(db: aiosqlite.Connection) -> None:
    """Mark historical model projections honestly without inventing canonical membership."""

    from app.services.market_focus import _focus_symbol_sets, validate_ticker_association

    async with db.execute(
        "SELECT revision,universe_version,payload_json FROM focus_context_snapshots "
        "ORDER BY revision DESC LIMIT 1"
    ) as cursor:
        focus_row = await cursor.fetchone()
    focus_revision = int(focus_row[0]) if focus_row else None
    universe_version = str(focus_row[1]) if focus_row else None
    focus_symbols: set[str] = set()
    focus_external_symbols: set[str] = set()
    if focus_row:
        try:
            payload = json.loads(focus_row[2])
        except (TypeError, json.JSONDecodeError):
            payload = {}
        focus_symbols, focus_external_symbols = _focus_symbol_sets(payload)

    async with db.execute(
        "SELECT id,news_id,ticker FROM analysis_stock_impacts ORDER BY id"
    ) as cursor:
        rows = await cursor.fetchall()
    now = _utc_now()
    for row in rows:
        async with db.execute(
            """SELECT DISTINCT ticker FROM news_ticker_mentions
               WHERE news_id=? AND validation_status IN ('canonical','valid_external')
                 AND association_method<>'llm_inference'""",
            (row[1],),
        ) as trusted_cursor:
            trusted_external = {str(value[0]) for value in await trusted_cursor.fetchall()}
        state = validate_ticker_association(
            str(row[2]),
            association_method="llm_inference",
            focus_symbols=focus_symbols,
            trusted_external_symbols=trusted_external | focus_external_symbols,
        )
        if state == "invalid":
            await db.execute("DELETE FROM analysis_stock_impacts WHERE id=?", (row[0],))
            await _increment_safety_counter(db, "invalid_historical_model_ticker")
            continue
        await db.execute(
            """UPDATE analysis_stock_impacts SET validation_status=?,validated_at=?,
               focus_revision=?,universe_version=?,association_method='llm_inference'
               WHERE id=?""",
            (state, now, focus_revision, universe_version, row[0]),
        )
        await db.execute(
            """INSERT INTO news_ticker_mentions
               (news_id,ticker,association_method,association_confidence,
                validation_status,validated_at,focus_revision,universe_version,source,created_at)
               SELECT ?,?,'llm_inference',0.5,?,?,?,?, 'historical_projection',?
               WHERE NOT EXISTS (
                 SELECT 1 FROM news_ticker_mentions
                 WHERE news_id=? AND ticker=? AND association_method='llm_inference'
               )""",
            (
                row[1], str(row[2]), state, now, focus_revision, universe_version, now,
                row[1], str(row[2]),
            ),
        )


async def _sync_legacy_trusted_stocks(db: aiosqlite.Connection) -> None:
    """Keep legacy dashboards from exposing untrusted model ticker guesses."""

    async with db.execute("SELECT news_id FROM analyses ORDER BY news_id") as cursor:
        news_ids = [int(row[0]) for row in await cursor.fetchall()]
    for news_id in news_ids:
        async with db.execute(
            """SELECT si.ticker,si.company,si.impact_score,si.reason
               FROM analysis_stock_impacts si
               WHERE si.analysis_id=(
                 SELECT r.id FROM analysis_revisions r
                 WHERE r.news_id=? ORDER BY r.revision DESC,r.id DESC LIMIT 1
               )
                 AND si.validation_status IN ('canonical','valid_external')
               ORDER BY si.ticker""",
            (news_id,),
        ) as cursor:
            rows = await cursor.fetchall()
        trusted = [
            {
                "ticker": str(row[0]),
                "company": str(row[1]),
                "impact_score": int(row[2]),
                "reason": str(row[3]),
            }
            for row in rows
        ]
        await db.execute(
            "UPDATE analyses SET affected_stocks=? WHERE news_id=?",
            (json.dumps(trusted, ensure_ascii=False), news_id),
        )


async def _backfill_event_evidence_fingerprints(db: aiosqlite.Connection) -> None:
    """Establish a stable baseline without manufacturing a new event revision."""

    from app.services.market_focus import (
        event_evidence_fingerprint,
        event_group_evidence_state,
        normalize_source_identity,
    )

    async with db.execute(
        """SELECT m.id,m.source,m.title,m.source_tickers_json,m.validated_tickers_json,
                  m.event_type,m.evidence_fingerprint,n.summary,m.news_id,
                  g.event_type,g.validated_tickers_json,m.content_hash
           FROM news_event_members m
           JOIN news_event_groups g ON g.event_group_id=m.event_group_id
           LEFT JOIN news_items n ON n.id=m.news_id
           WHERE m.evidence_fingerprint='' OR m.publisher_identity='unknown'"""
    ) as cursor:
        rows = await cursor.fetchall()
    for row in rows:
        event_type = str(row[5] or "other")
        if event_type == "other" and row[9]:
            event_type = str(row[9])
        trusted: list[str] = []
        if row[8] is not None:
            async with db.execute(
                """SELECT DISTINCT ticker FROM news_ticker_mentions
                   WHERE news_id=? AND validation_status IN ('canonical','valid_external')
                   ORDER BY ticker""",
                (row[8],),
            ) as ticker_cursor:
                trusted = [str(value[0]) for value in await ticker_cursor.fetchall()]
        try:
            member_validated = json.loads(row[4] or "[]")
        except (TypeError, json.JSONDecodeError):
            member_validated = []
        try:
            group_validated = json.loads(row[10] or "[]")
        except (TypeError, json.JSONDecodeError):
            group_validated = []
        validated = trusted or member_validated or group_validated
        fingerprint = event_evidence_fingerprint(
            title=str(row[2]),
            summary=str(row[7] or ""),
            event_type=event_type,
            validated_tickers={str(value) for value in validated},
        )
        content_hash = str(row[11] or "")
        if not row[6]:
            content_hash = hashlib.sha256(
                f"{content_hash}\n{fingerprint}".encode()
            ).hexdigest()
        await db.execute(
            """UPDATE news_event_members SET publisher_identity=?,event_type=?,
               validated_tickers_json=?,evidence_fingerprint=?,content_hash=? WHERE id=?""",
            (
                normalize_source_identity(str(row[1])),
                event_type,
                json.dumps(sorted({str(value) for value in validated})),
                fingerprint,
                content_hash,
                row[0],
            ),
        )
    async with db.execute(
        "SELECT event_group_id FROM news_event_groups WHERE evidence_fingerprint=''"
    ) as cursor:
        group_ids = [str(row[0]) for row in await cursor.fetchall()]
    for event_group_id in group_ids:
        state = await event_group_evidence_state(db, event_group_id)
        await db.execute(
            "UPDATE news_event_groups SET evidence_fingerprint=? WHERE event_group_id=?",
            (state["evidence_fingerprint"], event_group_id),
        )


async def _init_catalyst_schema(db: aiosqlite.Connection) -> None:
    """Apply additive, idempotent integration migrations without rebuilding legacy tables."""
    async with db.execute("PRAGMA user_version") as cursor:
        row = await cursor.fetchone()
    current_migration = int(row[0] if row else 0)
    if current_migration > CATALYST_SCHEMA_MIGRATION:
        raise RuntimeError("database_schema_is_newer_than_application")
    migration_lock_held = False
    if current_migration < CATALYST_SCHEMA_MIGRATION:
        # Web and worker may start together against the same SQLite file. Hold
        # one exclusive migration transaction, then re-read the version after
        # waiting so a second initializer does not replay the heavy backfill.
        await db.commit()
        await db.execute("PRAGMA foreign_keys=OFF")
        await db.execute("BEGIN EXCLUSIVE")
        async with db.execute("PRAGMA user_version") as cursor:
            locked_row = await cursor.fetchone()
        locked_version = int(locked_row[0] if locked_row else 0)
        if locked_version >= CATALYST_SCHEMA_MIGRATION:
            await db.commit()
            await db.execute("PRAGMA foreign_keys=ON")
            current_migration = locked_version
        else:
            current_migration = locked_version
            migration_lock_held = True
    await _add_column(db, "news_items", "updated_at", "TEXT")
    await _add_column(db, "news_items", "source_tickers", "TEXT NOT NULL DEFAULT '[]'")
    await db.execute("UPDATE news_items SET updated_at = COALESCE(updated_at, fetched_at) WHERE updated_at IS NULL")

    for statement in TABLES:
        await db.execute(statement)
    await db.execute(
        """INSERT OR IGNORE INTO hotspot_preparation_state
           (singleton_id,prepared_revision,last_consumed_revision,updated_at)
           VALUES (1,0,0,?)""",
        (_utc_now(),),
    )
    await _add_column(db, "market_focus_cycles", "retry_of_cycle_id", "TEXT")
    await _add_column(db, "market_focus_cycles", "execution_number", "INTEGER NOT NULL DEFAULT 1 CHECK(execution_number >= 1)")
    await _add_column(db, "analysis_jobs", "retrieve_error_count", "INTEGER NOT NULL DEFAULT 0 CHECK(retrieve_error_count >= 0)")
    await _add_column(db, "analysis_jobs", "cancel_attempt_count", "INTEGER NOT NULL DEFAULT 0 CHECK(cancel_attempt_count >= 0)")
    await _add_column(db, "analysis_jobs", "source_input_hash", "TEXT")
    await _add_column(db, "analysis_jobs", "content_hash", "TEXT")
    await _add_column(db, "analysis_jobs", "change_sequence", "INTEGER CHECK(change_sequence IS NULL OR change_sequence >= 1)")
    await _add_column(db, "analysis_jobs", "retry_of_job_id", "TEXT")
    await _add_column(db, "analysis_jobs", "execution_number", "INTEGER NOT NULL DEFAULT 1 CHECK(execution_number >= 1)")
    await _add_column(db, "analysis_jobs", "execution_mode", "TEXT NOT NULL DEFAULT 'background' CHECK(execution_mode IN ('background','worker_sync'))")
    await _add_column(db, "analysis_jobs", "max_output_tokens", "INTEGER NOT NULL DEFAULT 32768 CHECK(max_output_tokens >= 256)")
    await _add_column(db, "analysis_jobs", "task_type", "TEXT NOT NULL DEFAULT 'news_item'")
    await _add_column(db, "analysis_jobs", "request_origin", "TEXT NOT NULL DEFAULT 'manual'")
    await _add_column(db, "analysis_jobs", "usage_cache_write_tokens", "INTEGER NOT NULL DEFAULT 0 CHECK(usage_cache_write_tokens >= 0)")
    await _add_column(db, "analysis_jobs", "usage_reasoning_tokens", "INTEGER NOT NULL DEFAULT 0 CHECK(usage_reasoning_tokens >= 0)")
    await _add_column(db, "analysis_jobs", "usage_total_tokens", "INTEGER NOT NULL DEFAULT 0 CHECK(usage_total_tokens >= 0)")
    await _add_column(db, "analysis_jobs", "latency_ms", "INTEGER CHECK(latency_ms IS NULL OR latency_ms >= 0)")
    await _add_column(
        db,
        "analysis_stock_impacts",
        "validation_status",
        "TEXT NOT NULL DEFAULT 'unverified' CHECK(validation_status IN ('canonical','valid_external','ambiguous','invalid','unverified'))",
    )
    await _add_column(db, "analysis_stock_impacts", "validated_at", "TEXT")
    await _add_column(db, "analysis_stock_impacts", "focus_revision", "INTEGER")
    await _add_column(db, "analysis_stock_impacts", "universe_version", "TEXT")
    await _add_column(
        db,
        "analysis_stock_impacts",
        "association_method",
        "TEXT NOT NULL DEFAULT 'llm_inference' CHECK(association_method='llm_inference')",
    )
    if current_migration < CATALYST_SCHEMA_MIGRATION:
        # Populate newly added nullable compatibility columns before rebuilding
        # the table with its final NOT NULL constraints.
        await db.execute(
            "UPDATE analysis_jobs SET source_input_hash=input_hash WHERE source_input_hash IS NULL"
        )
        await db.execute(
            """UPDATE analysis_jobs SET content_hash=COALESCE((
                 SELECT n.content_hash FROM news_items n WHERE n.id=analysis_jobs.news_id
               ),input_hash) WHERE content_hash IS NULL"""
        )
        await _migrate_analysis_job_status_constraint(db)
    await _add_column(db, "source_health", "raw_count", "INTEGER CHECK(raw_count IS NULL OR raw_count >= 0)")
    await _add_column(db, "source_health", "inserted_count", "INTEGER CHECK(inserted_count IS NULL OR inserted_count >= 0)")
    await _add_column(db, "source_health", "duplicates_count", "INTEGER CHECK(duplicates_count IS NULL OR duplicates_count >= 0)")
    await _add_column(
        db,
        "source_health",
        "source_fetch_status",
        "TEXT NOT NULL DEFAULT 'unavailable' CHECK(source_fetch_status IN ('ok','degraded','unavailable','not_configured','disabled'))",
    )
    await _add_column(
        db,
        "source_health",
        "news_persistence_status",
        "TEXT NOT NULL DEFAULT 'unavailable' CHECK(news_persistence_status IN ('ok','degraded','unavailable','not_configured','disabled'))",
    )
    await _add_column(
        db,
        "source_health",
        "event_projection_status",
        "TEXT NOT NULL DEFAULT 'unavailable' CHECK(event_projection_status IN ('ok','degraded','unavailable','not_configured','disabled'))",
    )
    await _add_column(db, "news_event_groups", "evidence_fingerprint", "TEXT NOT NULL DEFAULT ''")
    await _add_column(
        db,
        "news_event_groups",
        "market_confirmation_score",
        "REAL CHECK(market_confirmation_score IS NULL OR market_confirmation_score BETWEEN 0 AND 100)",
    )
    await _add_column(
        db,
        "news_event_groups",
        "last_hot_score",
        "REAL CHECK(last_hot_score IS NULL OR last_hot_score BETWEEN 0 AND 100)",
    )
    await _add_column(db, "news_event_members", "validated_tickers_json", "TEXT NOT NULL DEFAULT '[]'")
    await _add_column(db, "news_event_members", "publisher_identity", "TEXT NOT NULL DEFAULT 'unknown'")
    await _add_column(db, "news_event_members", "event_type", "TEXT NOT NULL DEFAULT 'other'")
    await _add_column(db, "news_event_members", "evidence_fingerprint", "TEXT NOT NULL DEFAULT ''")
    await db.execute(
        """UPDATE source_health SET
             source_fetch_status=CASE
               WHEN source_fetch_status='unavailable' AND status<>'unavailable' THEN status
               ELSE source_fetch_status END,
             news_persistence_status=CASE
               WHEN news_persistence_status='unavailable' AND status IN ('ok','disabled','not_configured') THEN status
               ELSE news_persistence_status END,
             event_projection_status=CASE
               WHEN event_projection_status='unavailable' AND status IN ('ok','disabled','not_configured') THEN status
               ELSE event_projection_status END"""
    )
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

    if current_migration < CATALYST_SCHEMA_MIGRATION:
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
        # separate containers. Remove legacy UI overrides during the versioned migration.
        await db.execute(
            "DELETE FROM settings WHERE key IN ('default_llm_provider','default_llm_model')"
        )

        await _backfill_legacy_analyses(db)
        await _backfill_stock_impact_validation(db)
        await _sync_legacy_trusted_stocks(db)
        await _backfill_event_evidence_fingerprints(db)

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
        await db.execute(f"PRAGMA user_version={CATALYST_SCHEMA_MIGRATION}")
    await db.commit()
    if migration_lock_held:
        await db.execute("PRAGMA foreign_keys=ON")


async def init_catalyst_schema(db: aiosqlite.Connection) -> None:
    """Apply the Catalyst schema and leave the connection safe after failure."""
    try:
        await _init_catalyst_schema(db)
    except BaseException:
        # A failed exclusive migration must not leak an open transaction or a
        # connection with foreign-key enforcement disabled into startup retry
        # logic. Preserve the original failure if best-effort cleanup also
        # encounters a database error.
        try:
            await db.rollback()
        finally:
            try:
                await db.execute("PRAGMA foreign_keys=ON")
            except Exception:
                pass
        raise
