from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import secrets
import socket
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import aiosqlite
from pydantic import ValidationError

from app.config import settings
from app.models.catalysts import NewsImpactAnalysis
from app.models.database import get_db
from app.services.responses_runtime import (
    OpenAIResponsesProvider,
    ResponseResult,
    ResponsesProvider,
    build_model_input,
    validate_output,
)

logger = logging.getLogger(__name__)

ACTIVE_JOB_STATUSES = {"pending", "queued", "in_progress"}
TERMINAL_JOB_STATUSES = {
    "completed", "failed", "cancelled", "insufficient_context", "budget_blocked",
    "incomplete_output",
}
LOW_CONTEXT_MODEL = "low-context-neutral-v2"
MARKET_TERMS = {
    "stock", "stocks", "share", "shares", "equity", "market", "nasdaq", "s&p",
    "earnings", "revenue", "profit", "guidance", "merger", "acquisition", "ipo",
    "federal reserve", "fed", "interest rate", "inflation", "cpi", "payroll",
    "economy", "economic", "recession", "tariff", "trade", "regulation",
    "oil", "gold", "silver", "commodity", "bond", "yield", "dollar",
    "股票", "股市", "财报", "营收", "利润", "通胀", "利率", "美联储", "经济",
}


@dataclass(frozen=True)
class CreateJobResult:
    job: dict[str, Any]
    created: bool
    retry_after: int | None = None


class InputVersionConflict(Exception):
    """The caller tried to enqueue a news revision that is no longer current."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_text(value: datetime | None = None) -> str:
    return (value or utc_now()).astimezone(timezone.utc).isoformat()


def parse_utc(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("database timestamp is missing timezone")
    return parsed.astimezone(timezone.utc)


def _source_tickers(value: Any) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return []
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        ticker = str(item or "").strip().upper().lstrip("$")[:20]
        if ticker and ticker not in result:
            result.append(ticker)
    return result[:100]


def has_sufficient_context(news: dict[str, Any]) -> bool:
    title = " ".join(str(news.get("title") or "").split())
    summary = " ".join(str(news.get("summary") or "").split())
    return len(title) + len(summary) >= settings.news_llm_min_context_chars


def deterministic_market_relevance(news: dict[str, Any]) -> int:
    title = " ".join(str(news.get("title") or "").lower().split())
    summary = " ".join(str(news.get("summary") or "").lower().split())
    tickers = _source_tickers(news.get("source_tickers"))
    if tickers:
        source = str(news.get("source") or "").lower()
        native_source = any(name in source for name in ("finnhub", "massive", "seekingalpha"))
        alias_match = any(re.search(rf"(?<![A-Za-z]){re.escape(ticker.lower())}(?![A-Za-z])", title) for ticker in tickers)
        if native_source and alias_match:
            return 85
        if native_source:
            return 70
        if alias_match:
            return 65
        return 45
    if any(term in title for term in MARKET_TERMS):
        return 70
    if any(term in summary for term in MARKET_TERMS):
        return 45
    return 0


def input_hash(news: dict[str, Any]) -> str:
    payload = {
        "content_hash": news.get("content_hash"),
        "title": str(news.get("title") or ""),
        "summary": str(news.get("summary") or ""),
        "source": str(news.get("source") or ""),
        "published_at": news.get("published_at"),
        "source_tickers": _source_tickers(news.get("source_tickers")),
    }
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def execution_input_hash(
    source_digest: str,
    *,
    provider: str,
    model: str,
    reasoning_effort: str,
    prompt_version: str,
    schema_version: str,
    execution_number: int,
    retry_of_job_id: str | None,
    request_origin: Literal["manual", "automatic"],
) -> str:
    """Build a durable execution key while retaining the source digest separately.

    The original table has a uniqueness constraint on ``input_hash``.  A forced
    retry must get a new Job without rewriting the failed execution, so the
    execution key includes its generation and parent while ``source_input_hash``
    remains the hash of the exact news input used by every revision.
    """

    raw = "\n".join(
        (
            source_digest,
            provider,
            model,
            reasoning_effort,
            prompt_version,
            schema_version,
            str(execution_number),
            retry_of_job_id or "initial",
            request_origin,
        )
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def low_context_analysis() -> NewsImpactAnalysis:
    return NewsImpactAnalysis.model_validate(
        {
            "title_zh": "信息不足",
            "headline_summary": "原文信息不足，系统保留新闻，但不生成方向性判断。",
            "overall_sentiment": 0,
            "classification": "neutral",
            "confidence": 0,
            "market_relevance": 0,
            "affected_stocks": [],
            "affected_sectors": [],
            "affected_commodities": [],
            "causal_summary": "现有标题与摘要不足以支持可靠的市场因果判断。",
            "key_factors": ["上下文不足"],
            "uncertainty_notes": ["未调用外部模型。"],
            "insufficient_context": True,
        }
    )


async def _fetch_news(db: aiosqlite.Connection, news_id: int) -> dict[str, Any] | None:
    async with db.execute("SELECT * FROM news_items WHERE id=?", (news_id,)) as cursor:
        row = await cursor.fetchone()
    return dict(row) if row else None


async def _fetch_job(db: aiosqlite.Connection, job_id: str) -> dict[str, Any] | None:
    async with db.execute("SELECT * FROM analysis_jobs WHERE job_id=?", (job_id,)) as cursor:
        row = await cursor.fetchone()
    return dict(row) if row else None


async def _budget_error(
    db: aiosqlite.Connection,
    now: datetime,
    *,
    request_origin: Literal["manual", "automatic"],
) -> str | None:
    capability = (
        settings.automatic_news_analysis_capability
        if request_origin == "automatic"
        else settings.manual_news_analysis_capability
    )
    if capability != "enabled":
        return capability
    async with db.execute(
        "SELECT COUNT(*) FROM analysis_jobs WHERE status IN ('pending','queued','in_progress')"
    ) as cursor:
        queued = int((await cursor.fetchone())[0])
    if queued >= settings.news_llm_max_queued:
        return "queue_capacity_reached"

    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    job_limit = (
        settings.news_llm_daily_job_limit
        if request_origin == "automatic"
        else settings.news_llm_manual_daily_job_limit
    )
    output_limit = (
        settings.news_llm_daily_output_token_limit
        if request_origin == "automatic"
        else settings.news_llm_manual_daily_output_token_limit
    )
    if job_limit is not None:
        async with db.execute(
            """SELECT COUNT(*) FROM analysis_jobs
               WHERE provider='openai' AND created_at >= ?
                 AND request_origin=?
                 AND status NOT IN ('insufficient_context','budget_blocked')""",
            (day_start, request_origin),
        ) as cursor:
            if int((await cursor.fetchone())[0]) >= job_limit:
                return "daily_job_limit_reached"
    if output_limit is not None:
        async with db.execute(
            """SELECT COALESCE(SUM(
                   CASE
                     WHEN status IN ('pending','queued','in_progress')
                       OR error_code='submission_outcome_unknown'
                       OR (status='cancelled' AND error_code IN (
                         'upstream_cancel_pending','upstream_cancel_observe'
                       ))
                     THEN max_output_tokens
                     ELSE usage_output_tokens
                   END
                 ),0)
               FROM analysis_jobs
               WHERE provider='openai' AND created_at >= ? AND request_origin=?""",
            (day_start, request_origin),
        ) as cursor:
            reserved_or_used = int((await cursor.fetchone())[0])
            if (
                reserved_or_used + settings.news_item_max_output_tokens
                > output_limit
            ):
                return "daily_output_token_limit_reached"
    return None


async def _publish_analysis_locked(
    db: aiosqlite.Connection,
    *,
    job: dict[str, Any],
    news: dict[str, Any],
    payload: NewsImpactAnalysis,
    provider: str,
    model: str,
    reasoning_effort: str,
    prompt_version: str,
    schema_version: str,
    usage_input_tokens: int = 0,
    usage_cached_input_tokens: int = 0,
    usage_cache_write_tokens: int = 0,
    usage_reasoning_tokens: int = 0,
    usage_output_tokens: int = 0,
    usage_total_tokens: int = 0,
    latency_ms: int | None = None,
    terminal_status: str = "completed",
) -> int:
    now = utc_now()
    analyzed_at = utc_text(now)
    fetched = parse_utc(str(news["fetched_at"]))
    available_at = utc_text(max(fetched, now))
    async with db.execute(
        "SELECT COALESCE(MAX(revision),0)+1 FROM analysis_revisions WHERE news_id=?",
        (news["id"],),
    ) as cursor:
        revision = int((await cursor.fetchone())[0])
    payload_dict = payload.model_dump(mode="json")
    payload_json = json.dumps(payload_dict, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    cursor = await db.execute(
        """INSERT INTO analysis_revisions
           (news_id, job_id, revision, input_hash, payload_json, provider, model,
            reasoning_effort, prompt_version, schema_version, fetched_at, analyzed_at,
            available_at, usage_input_tokens, usage_cached_input_tokens,
            usage_output_tokens, is_legacy, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)""",
        (
            news["id"], job["job_id"], revision,
            job.get("source_input_hash") or job["input_hash"], payload_json,
            provider, model, reasoning_effort, prompt_version, schema_version,
            str(news["fetched_at"]), analyzed_at, available_at, usage_input_tokens,
            usage_cached_input_tokens, usage_output_tokens, analyzed_at,
        ),
    )
    analysis_id = int(cursor.lastrowid)

    from app.services.market_focus import record_ticker_mentions

    source_tickers = _source_tickers(news.get("source_tickers"))
    if source_tickers:
        await record_ticker_mentions(
            db,
            news_id=int(news["id"]),
            tickers=source_tickers,
            association_method="provider_tag",
            source=str(news["source"]),
        )
    async with db.execute(
        """SELECT DISTINCT ticker FROM news_ticker_mentions
           WHERE news_id=? AND association_method<>'llm_inference'
             AND validation_status IN ('canonical','valid_external')""",
        (news["id"],),
    ) as trusted_cursor:
        trusted_external = {str(row[0]) for row in await trusted_cursor.fetchall()}
    # A new analysis revision replaces the current model-derived associations.
    # Provider/company evidence remains append-only and the raw model output is
    # still retained in analysis_revisions for audit.
    await db.execute(
        "DELETE FROM news_ticker_mentions WHERE news_id=? AND association_method='llm_inference'",
        (news["id"],),
    )
    model_mentions = await record_ticker_mentions(
        db,
        news_id=int(news["id"]),
        tickers=[stock.ticker for stock in payload.affected_stocks],
        association_method="llm_inference",
        source="model_output",
        trusted_external_symbols=trusted_external,
    )
    validation_by_ticker = {
        row["ticker"]: row["validation_status"] for row in model_mentions if row["ticker"]
    }
    async with db.execute(
        "SELECT revision,universe_version FROM focus_context_snapshots ORDER BY revision DESC LIMIT 1"
    ) as focus_cursor:
        focus_row = await focus_cursor.fetchone()

    for stock in payload.affected_stocks:
        validation_status = validation_by_ticker.get(stock.ticker, "unverified")
        if validation_status not in {"canonical", "valid_external"}:
            continue
        await db.execute(
            """INSERT INTO analysis_stock_impacts
               (analysis_id, news_id, ticker, company, impact_score, confidence, horizon,
                mechanism, reason, source, content_hash, published_at, fetched_at,
                analyzed_at, available_at, model, reasoning_effort, prompt_version, schema_version,
                validation_status,validated_at,focus_revision,universe_version,association_method)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'llm_inference')""",
            (
                analysis_id, news["id"], stock.ticker, stock.company, stock.impact_score,
                stock.confidence, stock.horizon.value, stock.mechanism.value, stock.reason,
                str(news["source"]), str(news["content_hash"]), news.get("published_at"),
                str(news["fetched_at"]), analyzed_at, available_at, model,
                reasoning_effort, prompt_version, schema_version, validation_status, analyzed_at,
                int(focus_row[0]) if focus_row else None,
                str(focus_row[1]) if focus_row else None,
            ),
        )

    from app.services.market_focus import refresh_event_groups_for_news

    await refresh_event_groups_for_news(db, int(news["id"]))

    legacy_stocks = [
        {
            "ticker": stock.ticker,
            "company": stock.company,
            "impact_score": stock.impact_score,
            "reason": stock.reason,
        }
        for stock in payload.affected_stocks
        if validation_by_ticker.get(stock.ticker) in {"canonical", "valid_external"}
    ]
    legacy_commodities = [
        {"name": value.name, "impact_score": value.impact_score, "reason": value.reason}
        for value in payload.affected_commodities
    ]
    await db.execute(
        """INSERT INTO analyses
           (news_id, title_zh, headline_summary, overall_sentiment, classification, confidence,
            affected_stocks, affected_sectors, affected_commodities, logic_chain, key_factors,
            llm_provider, llm_model, analyzed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(news_id) DO UPDATE SET
             title_zh=excluded.title_zh, headline_summary=excluded.headline_summary,
             overall_sentiment=excluded.overall_sentiment, classification=excluded.classification,
             confidence=excluded.confidence, affected_stocks=excluded.affected_stocks,
             affected_sectors=excluded.affected_sectors,
             affected_commodities=excluded.affected_commodities,
             logic_chain=excluded.logic_chain, key_factors=excluded.key_factors,
             llm_provider=excluded.llm_provider, llm_model=excluded.llm_model,
             analyzed_at=excluded.analyzed_at""",
        (
            news["id"], payload.title_zh, payload.headline_summary,
            payload.overall_sentiment, payload.classification.value, payload.confidence,
            json.dumps(legacy_stocks, ensure_ascii=False),
            json.dumps(payload.affected_sectors, ensure_ascii=False),
            json.dumps(legacy_commodities, ensure_ascii=False), payload.causal_summary,
            json.dumps(payload.key_factors, ensure_ascii=False), provider, model, analyzed_at,
        ),
    )
    await db.execute(
        """UPDATE news_items
           SET analysis_status='completed', analysis_error='', analysis_claimed_at=NULL,
               analysis_lease_expires_at=NULL, updated_at=? WHERE id=?""",
        (available_at, news["id"]),
    )
    await db.execute(
        """UPDATE analysis_jobs
           SET status=?, completed_at=?, updated_at=?, error_code=NULL,
               usage_input_tokens=?, usage_cached_input_tokens=?, usage_output_tokens=?,
               usage_cache_write_tokens=?,usage_reasoning_tokens=?,usage_total_tokens=?,latency_ms=?,
               next_attempt_at=NULL, lease_owner=NULL, lease_expires_at=NULL
           WHERE job_id=?""",
        (
            terminal_status, analyzed_at, analyzed_at, usage_input_tokens,
            usage_cached_input_tokens, usage_output_tokens, usage_cache_write_tokens,
            usage_reasoning_tokens, usage_total_tokens, latency_ms, job["job_id"],
        ),
    )
    return analysis_id


async def create_or_get_job(
    db: aiosqlite.Connection,
    news_id: int,
    *,
    force: bool = False,
    priority: int = 100,
    expected_content_hash: str | None = None,
    expected_change_sequence: int | None = None,
    request_origin: Literal["manual", "automatic"] = "manual",
) -> CreateJobResult:
    now = utc_now()
    await db.execute("BEGIN IMMEDIATE")
    try:
        news = await _fetch_news(db, news_id)
        if news is None:
            raise LookupError("news_not_found")
        async with db.execute(
            """SELECT MAX(change_sequence) FROM integration_changes
               WHERE entity_id=? AND entity_type IN ('news','analysis')""",
            (str(news_id),),
        ) as cursor:
            sequence_row = await cursor.fetchone()
        current_change_sequence = int(sequence_row[0]) if sequence_row and sequence_row[0] else None
        if expected_content_hash is not None and not secrets.compare_digest(
            str(news["content_hash"]), str(expected_content_hash)
        ):
            raise InputVersionConflict("content_hash_mismatch")
        if (
            expected_change_sequence is not None
            and current_change_sequence != expected_change_sequence
        ):
            raise InputVersionConflict("change_sequence_mismatch")
        sufficient = has_sufficient_context(news)
        selected_provider = settings.default_llm_provider if sufficient else "system"
        provider_supported = not sufficient or selected_provider == "openai"
        selected_model = settings.default_llm_model if sufficient else LOW_CONTEXT_MODEL
        selected_reasoning = (
            settings.openai_reasoning
            if sufficient and selected_provider == "openai"
            else "none"
        )
        digest = input_hash(news)
        async with db.execute(
            """SELECT * FROM analysis_jobs
               WHERE news_id=? AND COALESCE(source_input_hash,input_hash)=?
                 AND provider=? AND model=? AND reasoning_effort=?
                 AND request_origin=?
                 AND prompt_version=? AND schema_version=?
               ORDER BY execution_number DESC, datetime(created_at) DESC, job_id DESC
               LIMIT 1""",
            (
                news_id, digest, selected_provider, selected_model, selected_reasoning, request_origin,
                settings.news_impact_prompt_version,
                settings.news_impact_schema_version,
            ),
        ) as cursor:
            existing_row = await cursor.fetchone()
        retry_of_job_id: str | None = None
        execution_number = 1
        if existing_row:
            existing = dict(existing_row)
            if existing["status"] in ACTIVE_JOB_STATUSES or existing["status"] in {
                "completed", "insufficient_context",
            }:
                await db.commit()
                return CreateJobResult(existing, created=False)
            if existing["status"] == "failed" and existing.get("error_code") == "submission_outcome_unknown":
                await db.commit()
                return CreateJobResult(existing, created=False)
            if not force or not sufficient:
                retry_after = None
                if existing.get("next_attempt_at"):
                    retry_at = parse_utc(existing["next_attempt_at"])
                    if retry_at > now:
                        retry_after = max(1, int((retry_at - now).total_seconds()))
                await db.commit()
                return CreateJobResult(existing, created=False, retry_after=retry_after)
            # Explicit retries are append-only executions.  Never reset the old
            # Job: its response id, error, usage, and cost audit remain intact.
            retry_of_job_id = str(existing["job_id"])
            execution_number = max(1, int(existing.get("execution_number") or 1)) + 1

        budget_error = (
            await _budget_error(db, now, request_origin=request_origin)
            if provider_supported and (sufficient or request_origin == "manual")
            else None
        )
        if existing_row and dict(existing_row)["status"] == "budget_blocked" and budget_error:
            existing = dict(existing_row)
            await db.commit()
            return CreateJobResult(existing, created=False)
        if request_origin == "manual" and budget_error:
            status = "budget_blocked"
            job_error = budget_error
        elif not sufficient:
            status = "insufficient_context"
            job_error = None
        elif not provider_supported:
            status = "failed"
            job_error = "unsupported_analysis_provider"
        else:
            status = "budget_blocked" if budget_error else "pending"
            job_error = budget_error
        job_id = f"mlj_{uuid.uuid4().hex}"
        durable_input_hash = execution_input_hash(
            digest,
            provider=selected_provider,
            model=selected_model,
            reasoning_effort=selected_reasoning,
            prompt_version=settings.news_impact_prompt_version,
            schema_version=settings.news_impact_schema_version,
            execution_number=execution_number,
            retry_of_job_id=retry_of_job_id,
            request_origin=request_origin,
        )
        await db.execute(
            """INSERT INTO analysis_jobs
               (job_id, news_id, input_hash, source_input_hash, content_hash,
                change_sequence, retry_of_job_id, execution_number, status, priority, provider, model,
                reasoning_effort, execution_mode, max_output_tokens,request_origin,
                prompt_version, schema_version, next_attempt_at,
                error_code, completed_at, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                job_id, news_id, durable_input_hash, digest, str(news["content_hash"]),
                current_change_sequence, retry_of_job_id, execution_number, status,
                max(-1000, min(1000, priority)), selected_provider, selected_model,
                selected_reasoning, settings.openai_execution_mode,
                settings.news_item_max_output_tokens, request_origin, settings.news_impact_prompt_version,
                settings.news_impact_schema_version, utc_text(now) if status == "pending" else None,
                job_error, utc_text(now) if status == "failed" else None,
                utc_text(now), utc_text(now),
            ),
        )
        await db.execute(
            """UPDATE news_items SET analysis_status=?, analysis_error=?,
               analysis_claimed_at=NULL, analysis_lease_expires_at=NULL, updated_at=?
               WHERE id=?""",
            (status, job_error or "", utc_text(now), news_id),
        )
        job = await _fetch_job(db, job_id)
        if job is None:
            raise RuntimeError("job insert did not produce a row")
        if status == "insufficient_context":
            await _publish_analysis_locked(
                db,
                job=job,
                news=news,
                payload=low_context_analysis(),
                provider="system",
                model=LOW_CONTEXT_MODEL,
                reasoning_effort="none",
                prompt_version=settings.news_impact_prompt_version,
                schema_version=settings.news_impact_schema_version,
                terminal_status="insufficient_context",
            )
        await db.commit()
        refreshed = await _fetch_job(db, job_id)
        return CreateJobResult(refreshed or job, created=True)
    except Exception:
        await db.rollback()
        raise


async def recover_expired_job_leases(db: aiosqlite.Connection) -> int:
    now = utc_text()
    interrupted = await db.execute(
        """UPDATE analysis_jobs
           SET status='failed', error_code=CASE
                 WHEN error_code='submission_in_progress' THEN 'submission_outcome_unknown'
                 ELSE 'worker_interrupted' END,
               next_attempt_at=NULL, lease_owner=NULL, lease_expires_at=NULL, updated_at=?
           WHERE status='in_progress' AND openai_response_id IS NULL
             AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?""",
        (now, now),
    )
    recovered = await db.execute(
        """UPDATE analysis_jobs
           SET status='queued', error_code=NULL, lease_owner=NULL, lease_expires_at=NULL,
               next_attempt_at=?, updated_at=?
           WHERE status IN ('queued','in_progress') AND openai_response_id IS NOT NULL
             AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?""",
        (now, now, now),
    )
    await db.commit()
    return max(0, interrupted.rowcount) + max(0, recovered.rowcount)


async def claim_next_job(db: aiosqlite.Connection, worker_id: str) -> dict[str, Any] | None:
    now = utc_now()
    unknown_submission_cutoff = utc_text(
        now - timedelta(seconds=settings.openai_background_poll_timeout_seconds)
    )
    await recover_expired_job_leases(db)
    await db.execute("BEGIN IMMEDIATE")
    try:
        concurrency_limit = min(
            settings.news_llm_max_inflight,
            settings.openai_max_concurrency,
        )
        # Existing Background responses remain in-flight even when an operator
        # changes the process default to worker_sync.  They are always claimed
        # first for retrieve/cancel and never consume a second submission slot.
        async with db.execute(
            """SELECT COUNT(*) FROM analysis_jobs
               WHERE (
                   openai_response_id IS NOT NULL
                   AND (
                       status IN ('queued','in_progress')
                       OR (status='cancelled' AND error_code IN (
                           'upstream_cancel_pending','upstream_cancel_observe'
                       ))
                   )
               ) OR (
                   openai_response_id IS NULL
                   AND lease_expires_at > ? AND lease_owner IS NOT NULL
                   AND status IN ('pending','queued','in_progress')
               ) OR (
                   openai_response_id IS NULL
                   AND status='failed'
                   AND error_code='submission_outcome_unknown'
                   AND execution_mode='background'
                   AND submitted_at IS NOT NULL
                   AND submitted_at >= ?
               )""",
            (utc_text(now), unknown_submission_cutoff),
        ) as cursor:
            inflight = int((await cursor.fetchone())[0])
        async with db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='calendar_analysis_jobs'"
        ) as cursor:
            calendar_table = await cursor.fetchone()
        if calendar_table is not None:
            async with db.execute(
                """SELECT COUNT(*) FROM calendar_analysis_jobs
                   WHERE (
                     openai_response_id IS NOT NULL
                     AND status IN ('queued','in_progress')
                   ) OR (
                     openai_response_id IS NULL AND lease_expires_at > ?
                     AND lease_owner IS NOT NULL
                     AND status IN ('pending','queued','in_progress')
                   ) OR (
                     openai_response_id IS NULL
                     AND status='failed'
                     AND error_code='submission_outcome_unknown'
                     AND execution_mode='background'
                     AND submitted_at IS NOT NULL
                     AND submitted_at >= ?
                   )""",
                (utc_text(now), unknown_submission_cutoff),
            ) as cursor:
                inflight += int((await cursor.fetchone())[0])
        submission_clause = (
            "" if inflight < concurrency_limit else "AND openai_response_id IS NOT NULL"
        )
        cost_gate_clause = """AND (
            openai_response_id IS NOT NULL
            OR (request_origin='manual' AND ?)
            OR (request_origin='automatic' AND ?)
        )"""
        response_priority = "CASE WHEN openai_response_id IS NOT NULL THEN 0 ELSE 1 END,"
        async with db.execute(
            f"""SELECT * FROM analysis_jobs
               WHERE (
                   status IN ('pending','queued','in_progress')
                   OR (status='cancelled' AND openai_response_id IS NOT NULL
                       AND error_code IN ('upstream_cancel_pending','upstream_cancel_observe'))
               )
               AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
               AND (lease_expires_at IS NULL OR lease_expires_at <= ?)
               {cost_gate_clause}
               {submission_clause}
               ORDER BY CASE WHEN status='cancelled' THEN 1 ELSE 0 END DESC,
                        {response_priority}
                        priority DESC, created_at, job_id
               LIMIT 1""",
            (
                utc_text(now),
                utc_text(now),
                settings.manual_news_analysis_capability == "enabled",
                settings.automatic_news_analysis_capability == "enabled",
            ),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            await db.commit()
            return None
        job = dict(row)
        fence = int(job["fencing_token"]) + 1
        lease_expires = utc_text(now + timedelta(seconds=settings.analysis_worker_lease_seconds))
        cursor = await db.execute(
            """UPDATE analysis_jobs SET lease_owner=?, lease_expires_at=?, fencing_token=?, updated_at=?
               WHERE job_id=? AND fencing_token=?""",
            (worker_id, lease_expires, fence, utc_text(now), job["job_id"], job["fencing_token"]),
        )
        if cursor.rowcount != 1:
            await db.rollback()
            return None
        await db.commit()
        job.update(lease_owner=worker_id, lease_expires_at=lease_expires, fencing_token=fence)
        return job
    except Exception:
        await db.rollback()
        raise


async def _update_claimed(
    db: aiosqlite.Connection,
    job: dict[str, Any],
    assignments: str,
    values: tuple[Any, ...],
) -> bool:
    cursor = await db.execute(
        f"UPDATE analysis_jobs SET {assignments} WHERE job_id=? AND fencing_token=? AND lease_owner=?",
        (*values, job["job_id"], job["fencing_token"], job["lease_owner"]),
    )
    await db.commit()
    return cursor.rowcount == 1


async def _fail_claimed(
    db: aiosqlite.Connection,
    job: dict[str, Any],
    error_code: str,
    result: ResponseResult | None = None,
) -> None:
    now = utc_now()
    usage_input = max(0, int(result.usage_input_tokens)) if result else 0
    usage_cached = max(0, int(result.usage_cached_input_tokens)) if result else 0
    usage_output = max(0, int(result.usage_output_tokens)) if result else 0
    next_attempt_at = (
        None
        if error_code == "submission_outcome_unknown"
        else utc_text(now + timedelta(seconds=settings.analysis_job_retry_cooldown_seconds))
    )
    updated = await _update_claimed(
        db,
        job,
        "status='failed', error_code=?, next_attempt_at=?, completed_at=COALESCE(completed_at,?), updated_at=?,"
        "usage_input_tokens=MAX(usage_input_tokens,?),"
        "usage_cached_input_tokens=MAX(usage_cached_input_tokens,?),"
        "usage_cache_write_tokens=MAX(usage_cache_write_tokens,?),"
        "usage_reasoning_tokens=MAX(usage_reasoning_tokens,?),"
        "usage_output_tokens=MAX(usage_output_tokens,?),"
        "usage_total_tokens=MAX(usage_total_tokens,?),latency_ms=COALESCE(?,latency_ms),"
        "lease_owner=NULL, lease_expires_at=NULL",
        (
            error_code[:100],
            next_attempt_at,
            utc_text(now),
            utc_text(now),
            usage_input,
            usage_cached,
            result.usage_cache_write_tokens if result else 0,
            result.usage_reasoning_tokens if result else 0,
            usage_output,
            result.usage_total_tokens if result else 0,
            result.latency_ms if result else None,
        ),
    )
    if updated:
        await db.execute(
            """UPDATE news_items SET analysis_status='failed', analysis_error=?, updated_at=?
               WHERE id=?""",
            (error_code[:100], utc_text(now), job["news_id"]),
        )
        await db.commit()


async def _retry_claimed(db: aiosqlite.Connection, job: dict[str, Any], error_code: str) -> None:
    now = utc_now()
    error_count = max(0, int(job.get("retrieve_error_count") or 0))
    delay = min(
        settings.openai_background_max_poll_seconds,
        settings.openai_background_initial_poll_seconds * (2 ** min(error_count, 6)),
    )
    await _update_claimed(
        db,
        job,
        "status='queued', error_code=?, attempt_count=attempt_count+1, "
        "retrieve_error_count=retrieve_error_count+1, next_attempt_at=?, "
        "updated_at=?, lease_owner=NULL, lease_expires_at=NULL",
        (error_code[:100], utc_text(now + timedelta(seconds=delay)), utc_text(now)),
    )


def _next_poll(job: dict[str, Any], now: datetime) -> datetime:
    attempts = max(0, int(job.get("attempt_count") or 0))
    delay = min(
        settings.openai_background_max_poll_seconds,
        settings.openai_background_initial_poll_seconds * (2 ** min(attempts, 6)),
    )
    return now + timedelta(seconds=delay)


async def _record_cancel_result(
    db: aiosqlite.Connection,
    job: dict[str, Any],
    result: ResponseResult,
) -> None:
    """Persist the upstream cancellation outcome without dropping its response id."""

    now = utc_now()
    status = str(result.status or "").lower()
    if status in {"cancelled", "failed", "incomplete", "expired"}:
        await _update_claimed(
            db,
            job,
            "status='cancelled',error_code=NULL,next_attempt_at=NULL,"
            "cancel_attempt_count=cancel_attempt_count+1,completed_at=COALESCE(completed_at,?),"
            "usage_input_tokens=MAX(usage_input_tokens,?),"
            "usage_cached_input_tokens=MAX(usage_cached_input_tokens,?),"
            "usage_output_tokens=MAX(usage_output_tokens,?),updated_at=?,"
            "lease_owner=NULL,lease_expires_at=NULL",
            (
                utc_text(now), result.usage_input_tokens,
                result.usage_cached_input_tokens, result.usage_output_tokens,
                utc_text(now),
            ),
        )
        return
    await _update_claimed(
        db,
        job,
        "status='cancelled',error_code='upstream_cancel_observe',"
        "cancel_attempt_count=cancel_attempt_count+1,next_attempt_at=?,"
        "completed_at=COALESCE(completed_at,?),updated_at=?,"
        "usage_input_tokens=MAX(usage_input_tokens,?),"
        "usage_cached_input_tokens=MAX(usage_cached_input_tokens,?),"
        "usage_output_tokens=MAX(usage_output_tokens,?),"
        "lease_owner=NULL,lease_expires_at=NULL",
        (
            utc_text(_next_poll(job, now)), utc_text(now), utc_text(now),
            result.usage_input_tokens, result.usage_cached_input_tokens,
            result.usage_output_tokens,
        ),
    )


async def _record_cancel_failure(db: aiosqlite.Connection, job: dict[str, Any]) -> None:
    attempts = max(0, int(job.get("cancel_attempt_count") or 0)) + 1
    error_code = "upstream_cancel_observe" if attempts >= 3 else "upstream_cancel_pending"
    delay = min(
        settings.openai_background_max_poll_seconds,
        settings.openai_background_initial_poll_seconds * (2 ** min(attempts - 1, 6)),
    )
    await _update_claimed(
        db,
        job,
        "status='cancelled',error_code=?,cancel_attempt_count=cancel_attempt_count+1,"
        "next_attempt_at=?,completed_at=COALESCE(completed_at,?),updated_at=?,"
        "lease_owner=NULL,lease_expires_at=NULL",
        (error_code, utc_text(utc_now() + timedelta(seconds=delay)), utc_text(), utc_text()),
    )


async def _defer_completed_cancel_observation(
    db: aiosqlite.Connection,
    job: dict[str, Any],
    result: ResponseResult,
) -> None:
    """Keep a provider-completed cancellation race recoverable until output is retrievable."""

    now = utc_now()
    await _update_claimed(
        db,
        job,
        "status='cancelled',error_code='upstream_cancel_observe',"
        "cancel_attempt_count=cancel_attempt_count+1,retrieve_error_count=retrieve_error_count+1,"
        "next_attempt_at=?,completed_at=COALESCE(completed_at,?),updated_at=?,"
        "usage_input_tokens=MAX(usage_input_tokens,?),"
        "usage_cached_input_tokens=MAX(usage_cached_input_tokens,?),"
        "usage_output_tokens=MAX(usage_output_tokens,?),"
        "lease_owner=NULL,lease_expires_at=NULL",
        (
            utc_text(_next_poll(job, now)), utc_text(now), utc_text(now),
            result.usage_input_tokens, result.usage_cached_input_tokens,
            result.usage_output_tokens,
        ),
    )


async def _process_cancel_response(
    db: aiosqlite.Connection,
    job: dict[str, Any],
    news: dict[str, Any],
    provider: ResponsesProvider,
    result: ResponseResult,
) -> None:
    """Resolve cancellation races without discarding an already-completed analysis."""

    if str(result.status or "").lower() != "completed":
        await _record_cancel_result(db, job, result)
        return
    completed = result
    if not completed.output_text:
        try:
            completed = await provider.retrieve(str(job["openai_response_id"]))
        except Exception:
            await _defer_completed_cancel_observation(db, job, result)
            return
    if str(completed.status or "").lower() == "completed":
        await _handle_provider_result(db, job, news, completed)
        return
    await _record_cancel_result(db, job, completed)


async def _handle_provider_result(
    db: aiosqlite.Connection,
    job: dict[str, Any],
    news: dict[str, Any],
    result: ResponseResult,
) -> None:
    status = result.status.lower()
    now = utc_now()
    if result.model is not None and result.model != str(job["model"]):
        await _fail_claimed(db, job, "provider_model_mismatch", result)
        return
    if (
        result.reasoning_effort is not None
        and result.reasoning_effort != str(job["reasoning_effort"])
    ):
        await _fail_claimed(db, job, "provider_reasoning_mismatch", result)
        return
    if status in {"queued", "in_progress"}:
        submitted = parse_utc(job["submitted_at"]) if job.get("submitted_at") else now
        poll_elapsed = (now - submitted).total_seconds() > settings.openai_background_poll_timeout_seconds
        await _update_claimed(
            db,
            job,
            "status=?, last_polled_at=?, attempt_count=attempt_count+1, retrieve_error_count=0, next_attempt_at=?, "
            "error_code=?, updated_at=?, lease_owner=NULL, lease_expires_at=NULL",
            (
                status, utc_text(now), utc_text(_next_poll(job, now)),
                "poll_window_elapsed" if poll_elapsed else None, utc_text(now),
            ),
        )
        return
    if status == "cancelled":
        await _update_claimed(
            db,
            job,
            "status='cancelled', completed_at=?, error_code=NULL, updated_at=?, lease_owner=NULL, lease_expires_at=NULL",
            (utc_text(now), utc_text(now)),
        )
        return
    if status == "incomplete" or result.error_code in {"max_output_tokens", "incomplete_max_output_tokens"}:
        updated = await _update_claimed(
            db,
            job,
            "status='incomplete_output',error_code='incomplete_output',next_attempt_at=NULL,"
            "completed_at=?,updated_at=?,usage_input_tokens=MAX(usage_input_tokens,?),"
            "usage_cached_input_tokens=MAX(usage_cached_input_tokens,?),"
            "usage_cache_write_tokens=MAX(usage_cache_write_tokens,?),"
            "usage_reasoning_tokens=MAX(usage_reasoning_tokens,?),"
            "usage_output_tokens=MAX(usage_output_tokens,?),"
            "usage_total_tokens=MAX(usage_total_tokens,?),latency_ms=COALESCE(?,latency_ms),"
            "lease_owner=NULL,lease_expires_at=NULL",
            (
                utc_text(now), utc_text(now), result.usage_input_tokens,
                result.usage_cached_input_tokens, result.usage_cache_write_tokens,
                result.usage_reasoning_tokens, result.usage_output_tokens,
                result.usage_total_tokens, result.latency_ms,
            ),
        )
        if updated:
            await db.execute(
                "UPDATE news_items SET analysis_status='failed',analysis_error='incomplete_output',updated_at=? WHERE id=?",
                (utc_text(now), job["news_id"]),
            )
            await db.commit()
        return
    if status != "completed":
        await _fail_claimed(db, job, result.error_code or "provider_response_failed", result)
        return
    try:
        payload = validate_output(result.output_text or "")
    except (ValueError, ValidationError):
        invalid_tickers = 0
        try:
            raw_payload = json.loads(result.output_text or "")
            stocks = raw_payload.get("affected_stocks", []) if isinstance(raw_payload, dict) else []
            if isinstance(stocks, list):
                from app.utils.tickers import normalize_ticker

                invalid_tickers = sum(
                    1
                    for stock in stocks
                    if isinstance(stock, dict) and not normalize_ticker(stock.get("ticker"))
                )
        except (TypeError, ValueError, json.JSONDecodeError):
            invalid_tickers = 0
        if invalid_tickers:
            await db.execute(
                """INSERT INTO projection_safety_counters(counter_key,count,updated_at)
                   VALUES ('invalid_model_ticker',?,?)
                   ON CONFLICT(counter_key) DO UPDATE SET
                     count=projection_safety_counters.count+excluded.count,
                     updated_at=excluded.updated_at""",
                (invalid_tickers, utc_text()),
            )
        await _fail_claimed(db, job, "invalid_structured_output", result)
        return

    await db.execute("BEGIN IMMEDIATE")
    try:
        current = await _fetch_job(db, job["job_id"])
        if (
            current is None
            or current["fencing_token"] != job["fencing_token"]
            or current["lease_owner"] != job["lease_owner"]
        ):
            await db.rollback()
            return
        await _publish_analysis_locked(
            db,
            job=current,
            news=news,
            payload=payload,
            provider="openai",
            model=current["model"],
            reasoning_effort=current["reasoning_effort"],
            prompt_version=current["prompt_version"],
            schema_version=current["schema_version"],
            usage_input_tokens=result.usage_input_tokens,
            usage_cached_input_tokens=result.usage_cached_input_tokens,
            usage_cache_write_tokens=result.usage_cache_write_tokens,
            usage_reasoning_tokens=result.usage_reasoning_tokens,
            usage_output_tokens=result.usage_output_tokens,
            usage_total_tokens=result.usage_total_tokens,
            latency_ms=result.latency_ms,
        )
        await db.commit()
    except Exception:
        await db.rollback()
        raise


async def process_claimed_job(
    db: aiosqlite.Connection,
    job: dict[str, Any],
    provider: ResponsesProvider,
) -> None:
    news = await _fetch_news(db, int(job["news_id"]))
    if news is None:
        await _fail_claimed(db, job, "news_not_found")
        return
    if (
        str(news.get("content_hash") or "") != str(job.get("content_hash") or "")
        or input_hash(news) != str(job.get("source_input_hash") or job.get("input_hash") or "")
    ):
        await _fail_claimed(db, job, "news_version_changed")
        return
    if job["status"] == "cancelled":
        if job.get("error_code") == "upstream_cancel_observe":
            try:
                observed = await provider.retrieve(str(job["openai_response_id"]))
            except Exception:
                error_count = max(0, int(job.get("retrieve_error_count") or 0))
                delay = min(
                    settings.openai_background_max_poll_seconds,
                    settings.openai_background_initial_poll_seconds * (2 ** min(error_count, 6)),
                )
                await _update_claimed(
                    db,
                    job,
                    "retrieve_error_count=retrieve_error_count+1,next_attempt_at=?,updated_at=?,"
                    "lease_owner=NULL,lease_expires_at=NULL",
                    (utc_text(utc_now() + timedelta(seconds=delay)), utc_text()),
                )
                return
            observed_status = str(observed.status or "").lower()
            if observed_status == "completed":
                await _handle_provider_result(db, job, news, observed)
                return
            if observed_status not in {
                "cancelled", "failed", "incomplete", "expired"
            }:
                await _update_claimed(
                    db,
                    job,
                    "retrieve_error_count=0,last_polled_at=?,next_attempt_at=?,updated_at=?,"
                    "lease_owner=NULL,lease_expires_at=NULL",
                    (utc_text(), utc_text(_next_poll(job, utc_now())), utc_text()),
                )
            else:
                await _update_claimed(
                    db,
                    job,
                    "error_code=NULL,retrieve_error_count=0,next_attempt_at=NULL,"
                    "completed_at=COALESCE(completed_at,?),updated_at=?,"
                    "usage_input_tokens=MAX(usage_input_tokens,?),"
                    "usage_cached_input_tokens=MAX(usage_cached_input_tokens,?),"
                    "usage_output_tokens=MAX(usage_output_tokens,?),"
                    "lease_owner=NULL,lease_expires_at=NULL",
                    (
                        utc_text(), utc_text(), observed.usage_input_tokens,
                        observed.usage_cached_input_tokens, observed.usage_output_tokens,
                    ),
                )
            return
        try:
            result = await provider.cancel(str(job["openai_response_id"]))
            await _process_cancel_response(db, job, news, provider, result)
        except Exception:
            await _record_cancel_failure(db, job)
        return

    model_input = build_model_input({**news, "source_tickers": _source_tickers(news.get("source_tickers"))})
    if job.get("openai_response_id"):
        try:
            result = await provider.retrieve(str(job["openai_response_id"]))
        except Exception:
            await _retry_claimed(db, job, "provider_retrieve_failed")
            return
        await _handle_provider_result(db, job, news, result)
        return

    execution_mode = str(job.get("execution_mode") or settings.openai_execution_mode)
    max_output_tokens = int(job.get("max_output_tokens") or settings.openai_max_output_tokens)
    if execution_mode == "worker_sync":
        try:
            await _update_claimed(
                db, job,
                "status='in_progress',error_code='submission_in_progress',"
                "submitted_at=COALESCE(submitted_at,?),attempt_count=attempt_count+1,updated_at=?",
                (utc_text(), utc_text()),
            )
            request_started = time.monotonic()
            result = await provider.create_sync(
                model_input,
                model=str(job["model"]),
                reasoning_effort=str(job["reasoning_effort"]),
                max_output_tokens=max_output_tokens,
            )
            if result.latency_ms is None:
                result = replace(result, latency_ms=round((time.monotonic() - request_started) * 1000))
        except Exception:
            await _fail_claimed(db, job, "submission_outcome_unknown")
            return
        await _handle_provider_result(db, job, news, result)
        return

    # The marker is durable before submission. If the process dies after the
    # provider accepted the request but before its id is saved, lease recovery
    # reports submission_outcome_unknown and never creates a duplicate request.
    marked = await _update_claimed(
        db,
        job,
        "status='in_progress', error_code='submission_in_progress', submitted_at=COALESCE(submitted_at,?), "
        "attempt_count=attempt_count+1, updated_at=?",
        (utc_text(), utc_text()),
    )
    if not marked:
        return
    try:
        request_started = time.monotonic()
        result = await provider.create_background(
            model_input,
            model=str(job["model"]),
            reasoning_effort=str(job["reasoning_effort"]),
            max_output_tokens=max_output_tokens,
        )
        if result.latency_ms is None:
            result = replace(result, latency_ms=round((time.monotonic() - request_started) * 1000))
    except Exception:
        await _fail_claimed(db, job, "submission_outcome_unknown")
        return
    if not result.response_id:
        await _fail_claimed(db, job, "provider_missing_response_id", result)
        return
    persisted = await _update_claimed(
        db,
        job,
        "openai_response_id=?, status=?, error_code=NULL, last_polled_at=?, next_attempt_at=?, updated_at=?",
        (
            result.response_id,
            # Provider completion is not the local terminal state.  Keep the
            # Job recoverable until validated output and projections commit.
            "queued" if result.status == "queued" else "in_progress",
            utc_text(), utc_text(_next_poll(job, utc_now())), utc_text(),
        ),
    )
    if not persisted:
        # Do not retry submission: the upstream request exists but local durable
        # linkage failed. The lease marker leaves an auditable failure path.
        return
    job["openai_response_id"] = result.response_id
    async with db.execute(
        "SELECT cancel_requested_at FROM analysis_jobs WHERE job_id=?",
        (job["job_id"],),
    ) as cursor:
        cancellation = await cursor.fetchone()
    if cancellation and cancellation[0]:
        try:
            cancel_result = await provider.cancel(result.response_id)
            await _process_cancel_response(db, job, news, provider, cancel_result)
        except Exception:
            await _record_cancel_failure(db, job)
        return
    await _handle_provider_result(db, job, news, result)


async def _renew_worker_lease(job: dict[str, Any], stop: asyncio.Event) -> None:
    interval = max(5, min(15, settings.analysis_worker_lease_seconds // 3))
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
            return
        except asyncio.TimeoutError:
            pass
        renewal_db = None
        try:
            renewal_db = await get_db()
            now = utc_now()
            await renewal_db.execute(
                """UPDATE analysis_jobs SET lease_expires_at=?,updated_at=?
                   WHERE job_id=? AND fencing_token=? AND lease_owner=?""",
                (
                    utc_text(now + timedelta(seconds=settings.analysis_worker_lease_seconds)),
                    utc_text(now), job["job_id"], job["fencing_token"], job["lease_owner"],
                ),
            )
            await renewal_db.execute(
                "UPDATE analysis_worker_state SET heartbeat_at=? WHERE worker_id=?",
                (utc_text(now), job["lease_owner"]),
            )
            await renewal_db.commit()
        except Exception:
            if renewal_db is not None:
                await renewal_db.rollback()
            logger.warning("Analysis worker lease renewal was deferred")
        finally:
            if renewal_db is not None:
                await renewal_db.close()


async def run_worker_once(
    *,
    provider: ResponsesProvider | None = None,
    worker_id: str | None = None,
) -> bool:
    owned_provider = provider is None
    provider = provider or OpenAIResponsesProvider()
    worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    db = await get_db()
    try:
        now = utc_text()
        await db.execute(
            """INSERT INTO analysis_worker_state(worker_id,started_at,heartbeat_at,status)
               VALUES (?,?,?,'idle')
               ON CONFLICT(worker_id) DO UPDATE SET heartbeat_at=excluded.heartbeat_at,
                 status='idle', error_code=NULL""",
            (worker_id, now, now),
        )
        await db.commit()
        if provider.capabilities().status != "ok":
            return False
        job = await claim_next_job(db, worker_id)
        if job is None:
            return False
        await db.execute(
            "UPDATE analysis_worker_state SET heartbeat_at=?,status='working',last_job_id=? WHERE worker_id=?",
            (utc_text(), job["job_id"], worker_id),
        )
        await db.commit()
        renewal_stop = asyncio.Event()
        renewal_task = asyncio.create_task(_renew_worker_lease(job, renewal_stop))
        try:
            await process_claimed_job(db, job, provider)
        finally:
            renewal_stop.set()
            await renewal_task
        await db.execute(
            "UPDATE analysis_worker_state SET heartbeat_at=?,status='idle',error_code=NULL WHERE worker_id=?",
            (utc_text(), worker_id),
        )
        await db.commit()
        return True
    except Exception:
        await db.execute(
            "UPDATE analysis_worker_state SET heartbeat_at=?,status='failed',error_code='worker_iteration_failed' WHERE worker_id=?",
            (utc_text(), worker_id),
        )
        await db.commit()
        raise
    finally:
        await db.close()
        if owned_provider and callable(getattr(provider, "close", None)):
            await provider.close()


async def request_cancel(db: aiosqlite.Connection, job_id: str) -> dict[str, Any] | None:
    now = utc_text()
    await db.execute("BEGIN IMMEDIATE")
    try:
        job = await _fetch_job(db, job_id)
        if job is None:
            await db.commit()
            return None
        if job["status"] in TERMINAL_JOB_STATUSES:
            await db.commit()
            return job
        if (
            job["status"] == "in_progress"
            and not job.get("openai_response_id")
            and job.get("error_code") == "submission_in_progress"
        ):
            await db.execute(
                "UPDATE analysis_jobs SET cancel_requested_at=?,updated_at=? WHERE job_id=?",
                (now, now, job_id),
            )
            await db.commit()
            return await _fetch_job(db, job_id)
        upstream_pending = bool(job.get("openai_response_id"))
        await db.execute(
            """UPDATE analysis_jobs SET status='cancelled', cancel_requested_at=?, completed_at=?,
               error_code=?, next_attempt_at=?, cancel_attempt_count=0,
               retrieve_error_count=0, updated_at=?, lease_owner=NULL, lease_expires_at=NULL
               WHERE job_id=?""",
            (
                now, now, "upstream_cancel_pending" if upstream_pending else None,
                now if upstream_pending else None, now, job_id,
            ),
        )
        await db.commit()
        return await _fetch_job(db, job_id)
    except Exception:
        await db.rollback()
        raise


async def _enqueue_jobs(
    limit: int | None,
    *,
    request_origin: Literal["manual", "automatic"],
) -> int:
    capability = (
        settings.automatic_news_analysis_capability
        if request_origin == "automatic"
        else settings.manual_news_analysis_capability
    )
    if capability != "enabled":
        return 0
    db = await get_db()
    try:
        limit = min(limit or settings.analysis_batch_size, settings.news_llm_max_queued)
        async with db.execute(
            """SELECT n.* FROM news_items n
               WHERE n.analysis_status='pending'
                 AND NOT EXISTS (SELECT 1 FROM analysis_jobs j WHERE j.news_id=n.id)
               ORDER BY CASE WHEN n.source_tickers!='[]' THEN 1 ELSE 0 END DESC,
                        COALESCE(n.published_at,n.fetched_at) DESC, n.id DESC LIMIT ?""",
            (min(settings.news_llm_max_queued, limit * 5),),
        ) as cursor:
            candidates = [dict(row) for row in await cursor.fetchall()]
        count = 0
        for candidate in candidates:
            if count >= limit:
                break
            news_id = int(candidate["id"])
            has_ticker = bool(_source_tickers(candidate.get("source_tickers")))
            if (
                request_origin == "automatic"
                and has_sufficient_context(candidate)
                and deterministic_market_relevance(candidate) < settings.news_llm_min_market_relevance
            ):
                await db.execute(
                    """UPDATE news_items SET analysis_status='skipped',
                       analysis_error='deterministic_market_relevance_below_threshold'
                       WHERE id=? AND analysis_status='pending'""",
                    (news_id,),
                )
                await db.commit()
                continue
            priority = 100 if request_origin == "manual" else (50 if has_ticker else 0)
            result = await create_or_get_job(
                db,
                news_id,
                priority=priority,
                request_origin=request_origin,
            )
            if result.created:
                count += 1
        return count
    finally:
        await db.close()


async def enqueue_auto_jobs(limit: int | None = None) -> int:
    # Automatic paid work is fail-closed. Both daily limits must be explicit;
    # absent limits never mean unlimited.
    return await _enqueue_jobs(limit, request_origin="automatic")


async def enqueue_manual_jobs(limit: int | None = None) -> int:
    # The manual batch path has separate switches and budgets. It only persists
    # Jobs; the dedicated worker owns every provider request.
    return await _enqueue_jobs(limit, request_origin="manual")


async def retry_failed_jobs(
    db: aiosqlite.Connection,
    *,
    news_id: int | None = None,
) -> list[dict[str, Any]]:
    """Create append-only retry Jobs for the latest retryable failed executions."""

    params: list[Any] = []
    news_filter = ""
    if news_id is not None:
        news_filter = "AND n.id=?"
        params.append(news_id)
    async with db.execute(
        f"""WITH ranked AS (
              SELECT j.*, ROW_NUMBER() OVER (
                PARTITION BY j.news_id
                ORDER BY execution_number DESC, datetime(created_at) DESC, job_id DESC
              ) AS rn
              FROM analysis_jobs j
            )
            SELECT n.id
            FROM news_items n
            LEFT JOIN ranked j ON j.news_id=n.id AND j.rn=1
            WHERE (j.status='failed' OR (j.job_id IS NULL AND n.analysis_status='failed'))
              AND COALESCE(j.error_code,'')!='submission_outcome_unknown'
              {news_filter}
            ORDER BY n.id""",
        params,
    ) as cursor:
        candidates = [int(row[0]) for row in await cursor.fetchall()]

    created_jobs: list[dict[str, Any]] = []
    for candidate in candidates:
        result = await create_or_get_job(db, candidate, force=True, priority=100)
        if result.created:
            created_jobs.append(result.job)
    return created_jobs
