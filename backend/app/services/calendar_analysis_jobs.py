from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import socket
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import aiosqlite
from pydantic import ValidationError

from app.config import settings
from app.models.database import get_db
from app.services.openai_capacity import openai_inflight_counts
from app.services.calendar_analyzer import (
    CALENDAR_ANALYSIS_INSTRUCTIONS,
    CalendarAnalysisPayload,
    build_calendar_model_input,
    prepare_calendar_events,
    validate_calendar_output,
)
from app.services.responses_runtime import (
    OpenAIResponsesProvider,
    ResponseResult,
    ResponsesProvider,
    structured_output_format,
)

logger = logging.getLogger(__name__)

ACTIVE_STATUSES = {"pending", "queued", "in_progress"}

CREATE_CALENDAR_ANALYSIS_JOBS = """
CREATE TABLE IF NOT EXISTS calendar_analysis_jobs (
    job_id TEXT PRIMARY KEY,
    input_hash TEXT NOT NULL UNIQUE,
    snapshot_hash TEXT NOT NULL,
    retry_of_job_id TEXT,
    execution_number INTEGER NOT NULL DEFAULT 1 CHECK(execution_number >= 1),
    status TEXT NOT NULL CHECK(status IN (
        'pending','queued','in_progress','completed','failed',
        'insufficient_context','budget_blocked'
    )),
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    reasoning_effort TEXT NOT NULL CHECK(reasoning_effort IN (
        'none','low','medium','high','xhigh','max'
    )),
    execution_mode TEXT NOT NULL DEFAULT 'background' CHECK(execution_mode IN ('background','worker_sync')),
    max_output_tokens INTEGER NOT NULL DEFAULT 16384 CHECK(max_output_tokens >= 256),
    prompt_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    events_json TEXT NOT NULL,
    result_json TEXT,
    event_count INTEGER NOT NULL CHECK(event_count >= 0),
    openai_response_id TEXT,
    budgeted_at TEXT,
    submitted_at TEXT,
    last_polled_at TEXT,
    completed_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    retrieve_error_count INTEGER NOT NULL DEFAULT 0 CHECK(retrieve_error_count >= 0),
    next_attempt_at TEXT,
    error_code TEXT,
    usage_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_input_tokens >= 0),
    usage_cached_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_cached_input_tokens >= 0),
    usage_output_tokens INTEGER NOT NULL DEFAULT 0 CHECK(usage_output_tokens >= 0),
    lease_owner TEXT,
    lease_expires_at TEXT,
    fencing_token INTEGER NOT NULL DEFAULT 0 CHECK(fencing_token >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""

CALENDAR_ANALYSIS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_calendar_analysis_jobs_ready "
    "ON calendar_analysis_jobs(status, next_attempt_at, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_calendar_analysis_jobs_lease "
    "ON calendar_analysis_jobs(status, lease_expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_calendar_analysis_jobs_completed "
    "ON calendar_analysis_jobs(completed_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_calendar_analysis_jobs_snapshot "
    "ON calendar_analysis_jobs(snapshot_hash,execution_number DESC,created_at DESC)",
]


@dataclass(frozen=True)
class CreateCalendarJobResult:
    job: dict[str, Any]
    created: bool


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


async def init_calendar_analysis_schema(db: aiosqlite.Connection) -> None:
    await db.execute(CREATE_CALENDAR_ANALYSIS_JOBS)
    for column, definition in (
        ("snapshot_hash", "TEXT"),
        ("retry_of_job_id", "TEXT"),
        ("execution_number", "INTEGER NOT NULL DEFAULT 1 CHECK(execution_number >= 1)"),
        ("execution_mode", "TEXT NOT NULL DEFAULT 'background' CHECK(execution_mode IN ('background','worker_sync'))"),
        ("max_output_tokens", "INTEGER NOT NULL DEFAULT 16384 CHECK(max_output_tokens >= 256)"),
    ):
        try:
            await db.execute(
                f"ALTER TABLE calendar_analysis_jobs ADD COLUMN {column} {definition}"
            )
        except aiosqlite.OperationalError as exc:
            if "duplicate column" not in str(exc).lower():
                raise
    await db.execute(
        "UPDATE calendar_analysis_jobs SET snapshot_hash=input_hash WHERE snapshot_hash IS NULL"
    )
    for statement in CALENDAR_ANALYSIS_INDEXES:
        await db.execute(statement)


def _calendar_snapshot(
    events: list[dict[str, Any]],
    *,
    provider: str,
    model: str,
) -> tuple[list[dict[str, str]], str, str]:
    prepared = prepare_calendar_events(events)
    events_json = json.dumps(
        prepared,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    identity = {
        "events": prepared,
        "provider": provider,
        "model": model,
        "reasoning_effort": settings.openai_reasoning,
        "prompt_version": settings.calendar_analysis_prompt_version,
        "schema_version": settings.calendar_analysis_schema_version,
    }
    encoded = json.dumps(
        identity,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return prepared, events_json, hashlib.sha256(encoded).hexdigest()


async def _fetch_job(
    db: aiosqlite.Connection,
    job_id: str,
) -> dict[str, Any] | None:
    async with db.execute(
        "SELECT * FROM calendar_analysis_jobs WHERE job_id=?",
        (job_id,),
    ) as cursor:
        row = await cursor.fetchone()
    return dict(row) if row else None


async def get_calendar_job(
    db: aiosqlite.Connection,
    job_id: str,
) -> dict[str, Any] | None:
    return await _fetch_job(db, job_id)


async def _budget_error(db: aiosqlite.Connection, now: datetime) -> str | None:
    capability = settings.manual_calendar_analysis_capability
    if capability != "enabled":
        return capability
    async with db.execute(
        "SELECT COUNT(*) FROM calendar_analysis_jobs "
        "WHERE status IN ('pending','queued','in_progress')"
    ) as cursor:
        active_count = int((await cursor.fetchone())[0])
    if active_count >= settings.calendar_llm_max_queued:
        return "calendar_queue_capacity_reached"

    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    async with db.execute(
        "SELECT COUNT(*) FROM calendar_analysis_jobs WHERE budgeted_at >= ?",
        (day_start,),
    ) as cursor:
        job_limit = settings.calendar_llm_daily_job_limit
        if job_limit is None:
            return "budget_configuration_required"
        if int((await cursor.fetchone())[0]) >= job_limit:
            return "calendar_daily_job_limit_reached"
    async with db.execute(
        """SELECT COALESCE(SUM(
               CASE
                 WHEN status IN ('pending','queued','in_progress')
                   OR error_code='submission_outcome_unknown'
                 THEN max_output_tokens
                 ELSE usage_output_tokens
               END
             ),0)
           FROM calendar_analysis_jobs WHERE budgeted_at >= ?""",
        (day_start,),
    ) as cursor:
        reserved_or_used = int((await cursor.fetchone())[0])
        output_limit = settings.calendar_llm_daily_output_token_limit
        if output_limit is None:
            return "budget_configuration_required"
        if (
            reserved_or_used + settings.calendar_max_output_tokens
            > output_limit
        ):
            return "calendar_daily_output_token_limit_reached"
    return None


async def create_or_get_calendar_job(
    db: aiosqlite.Connection,
    events: list[dict[str, Any]],
    *,
    provider: str,
    model: str,
    force: bool = False,
) -> CreateCalendarJobResult:
    prepared, events_json, snapshot_hash = _calendar_snapshot(
        events,
        provider=provider,
        model=model,
    )
    now = utc_now()
    now_text = utc_text(now)
    await db.execute("BEGIN IMMEDIATE")
    try:
        async with db.execute(
            """SELECT * FROM calendar_analysis_jobs
               WHERE COALESCE(snapshot_hash,input_hash)=?
               ORDER BY execution_number DESC,datetime(created_at) DESC,job_id DESC
               LIMIT 1""",
            (snapshot_hash,),
        ) as cursor:
            existing_row = await cursor.fetchone()
        retry_of_job_id = None
        execution_number = 1
        if existing_row is not None:
            existing = dict(existing_row)
            if existing["status"] in ACTIVE_STATUSES or existing["status"] in {
                "completed", "insufficient_context"
            }:
                await db.commit()
                return CreateCalendarJobResult(existing, created=False)
            if (
                existing["status"] == "failed"
                and existing.get("error_code") == "submission_outcome_unknown"
            ):
                await db.commit()
                return CreateCalendarJobResult(existing, created=False)
            if existing["status"] == "budget_blocked":
                budget_error = await _budget_error(db, now)
                if budget_error is not None:
                    if existing.get("error_code") != budget_error:
                        await db.execute(
                            "UPDATE calendar_analysis_jobs SET error_code=?,updated_at=? WHERE job_id=?",
                            (budget_error, now_text, existing["job_id"]),
                        )
                        await db.commit()
                        existing["error_code"] = budget_error
                        existing["updated_at"] = now_text
                    else:
                        await db.commit()
                    return CreateCalendarJobResult(existing, created=False)
                retry_of_job_id = existing["job_id"]
                execution_number = int(existing.get("execution_number") or 1) + 1
            elif existing["status"] == "failed" and force:
                retry_of_job_id = existing["job_id"]
                execution_number = int(existing.get("execution_number") or 1) + 1
            else:
                await db.commit()
                return CreateCalendarJobResult(existing, created=False)

        if not prepared:
            status = "insufficient_context"
            error_code = "calendar_has_no_analyzable_events"
            result_json = CalendarAnalysisPayload(events=[]).model_dump_json()
            completed_at = now_text
            budgeted_at = None
            next_attempt_at = None
        elif provider != "openai":
            status = "failed"
            error_code = "unsupported_calendar_provider"
            result_json = None
            completed_at = now_text
            budgeted_at = None
            next_attempt_at = None
        else:
            error_code = await _budget_error(db, now)
            status = "budget_blocked" if error_code else "pending"
            result_json = None
            completed_at = now_text if status == "budget_blocked" else None
            budgeted_at = None if error_code else now_text
            next_attempt_at = None if error_code else now_text

        job_id = f"calj_{uuid.uuid4().hex}"
        input_hash = hashlib.sha256(
            f"{snapshot_hash}:{execution_number}:{retry_of_job_id or 'initial'}".encode("utf-8")
        ).hexdigest()
        await db.execute(
            """INSERT INTO calendar_analysis_jobs
               (job_id,input_hash,snapshot_hash,retry_of_job_id,execution_number,
                status,provider,model,reasoning_effort,execution_mode,max_output_tokens,
                prompt_version,schema_version,events_json,result_json,event_count,
                budgeted_at,completed_at,next_attempt_at,error_code,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                job_id,
                input_hash,
                snapshot_hash,
                retry_of_job_id,
                execution_number,
                status,
                provider,
                model,
                settings.openai_reasoning,
                settings.openai_execution_mode,
                settings.calendar_max_output_tokens,
                settings.calendar_analysis_prompt_version,
                settings.calendar_analysis_schema_version,
                events_json,
                result_json,
                len(prepared),
                budgeted_at,
                completed_at,
                next_attempt_at,
                error_code,
                now_text,
                now_text,
            ),
        )
        await db.commit()
        job = await _fetch_job(db, job_id)
        if job is None:
            raise RuntimeError("calendar job insert did not produce a row")
        return CreateCalendarJobResult(job, created=True)
    except Exception:
        await db.rollback()
        raise


async def load_completed_calendar_analysis(
    db: aiosqlite.Connection,
    events: list[dict[str, Any]],
    *,
    provider: str,
    model: str,
) -> list[dict[str, Any]] | None:
    _prepared, _events_json, snapshot_hash = _calendar_snapshot(
        events,
        provider=provider,
        model=model,
    )
    async with db.execute(
        """SELECT result_json FROM calendar_analysis_jobs
           WHERE COALESCE(snapshot_hash,input_hash)=? AND status='completed'
           ORDER BY execution_number DESC,datetime(completed_at) DESC LIMIT 1""",
        (snapshot_hash,),
    ) as cursor:
        row = await cursor.fetchone()
    if row is None or not row[0]:
        return None
    try:
        payload = CalendarAnalysisPayload.model_validate_json(row[0])
    except (ValidationError, ValueError, TypeError):
        logger.error("Stored calendar analysis failed validation for snapshot_hash=%s", snapshot_hash)
        return None
    return [item.model_dump(mode="json") for item in payload.events]


def public_calendar_job(job: dict[str, Any]) -> dict[str, Any]:
    result = None
    if job.get("result_json"):
        try:
            payload = CalendarAnalysisPayload.model_validate_json(job["result_json"])
            result = [item.model_dump(mode="json") for item in payload.events]
        except (ValidationError, ValueError, TypeError):
            result = None
    retry_after = None
    if job.get("next_attempt_at") and job["status"] in ACTIVE_STATUSES:
        retry_after = max(
            0,
            int((parse_utc(job["next_attempt_at"]) - utc_now()).total_seconds()),
        )
    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "model": job["model"],
        "reasoning": job["reasoning_effort"],
        "event_count": int(job["event_count"]),
        "analyzed": len(result) if result is not None else 0,
        "submitted_at": job.get("submitted_at"),
        "updated_at": job["updated_at"],
        "completed_at": job.get("completed_at"),
        "error_code": job.get("error_code"),
        "retry_after": retry_after,
        "result": result,
    }


async def recover_expired_calendar_job_leases(db: aiosqlite.Connection) -> int:
    now = utc_text()
    interrupted = await db.execute(
        """UPDATE calendar_analysis_jobs
           SET status='failed',error_code=CASE
                 WHEN error_code='submission_in_progress' THEN 'submission_outcome_unknown'
                 ELSE 'worker_interrupted' END,next_attempt_at=NULL,
               completed_at=?,lease_owner=NULL,lease_expires_at=NULL,updated_at=?
           WHERE status='in_progress' AND openai_response_id IS NULL
             AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?""",
        (now, now, now),
    )
    recovered = await db.execute(
        """UPDATE calendar_analysis_jobs
           SET status='queued',error_code=NULL,next_attempt_at=?,
               lease_owner=NULL,lease_expires_at=NULL,updated_at=?
           WHERE status IN ('queued','in_progress') AND openai_response_id IS NOT NULL
             AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?""",
        (now, now, now),
    )
    await db.commit()
    return max(0, interrupted.rowcount) + max(0, recovered.rowcount)


async def claim_next_calendar_job(
    db: aiosqlite.Connection,
    worker_id: str,
    *,
    allow_new_submissions: bool | None = None,
) -> dict[str, Any] | None:
    await recover_expired_calendar_job_leases(db)
    now = utc_now()
    now_text = utc_text(now)
    unknown_submission_cutoff = utc_text(
        now - timedelta(seconds=settings.openai_background_poll_timeout_seconds)
    )
    await db.execute("BEGIN IMMEDIATE")
    try:
        counts = await openai_inflight_counts(
            db,
            now_text=now_text,
            unknown_submission_cutoff=unknown_submission_cutoff,
        )
        if allow_new_submissions is None:
            allow_new_submissions = (
                settings.manual_calendar_analysis_capability == "enabled"
            )
        allow_submission = (
            allow_new_submissions
            and counts.calendar < settings.calendar_llm_max_inflight
            and counts.total < settings.openai_max_concurrency
        )
        submission_clause = "" if allow_submission else "AND openai_response_id IS NOT NULL"
        response_priority = "CASE WHEN openai_response_id IS NOT NULL THEN 0 ELSE 1 END,"

        async with db.execute(
            f"""SELECT * FROM calendar_analysis_jobs
                WHERE status IN ('pending','queued','in_progress')
                  AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                  AND (lease_expires_at IS NULL OR lease_expires_at <= ?)
                  {submission_clause}
                ORDER BY {response_priority} created_at,job_id
                LIMIT 1""",
            (now_text, now_text),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            await db.commit()
            return None
        job = dict(row)
        fence = int(job["fencing_token"]) + 1
        lease_expires_at = utc_text(
            now + timedelta(seconds=settings.analysis_worker_lease_seconds)
        )
        cursor = await db.execute(
            """UPDATE calendar_analysis_jobs
               SET lease_owner=?,lease_expires_at=?,fencing_token=?,updated_at=?
               WHERE job_id=? AND fencing_token=?""",
            (
                worker_id,
                lease_expires_at,
                fence,
                now_text,
                job["job_id"],
                job["fencing_token"],
            ),
        )
        if cursor.rowcount != 1:
            await db.rollback()
            return None
        await db.commit()
        job.update(
            lease_owner=worker_id,
            lease_expires_at=lease_expires_at,
            fencing_token=fence,
        )
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
        f"UPDATE calendar_analysis_jobs SET {assignments} "
        "WHERE job_id=? AND fencing_token=? AND lease_owner=? "
        "AND input_hash=? AND provider=? AND model=? AND reasoning_effort=? "
        "AND prompt_version=? AND schema_version=? AND events_json=?",
        (
            *values,
            job["job_id"],
            job["fencing_token"],
            job["lease_owner"],
            job["input_hash"],
            job["provider"],
            job["model"],
            job["reasoning_effort"],
            job["prompt_version"],
            job["schema_version"],
            job["events_json"],
        ),
    )
    await db.commit()
    return cursor.rowcount == 1


async def _fail_claimed(
    db: aiosqlite.Connection,
    job: dict[str, Any],
    error_code: str,
    result: ResponseResult | None = None,
) -> None:
    now = utc_text()
    usage_input = max(0, int(result.usage_input_tokens)) if result else 0
    usage_cached = max(0, int(result.usage_cached_input_tokens)) if result else 0
    usage_output = max(0, int(result.usage_output_tokens)) if result else 0
    await _update_claimed(
        db,
        job,
        "status='failed',error_code=?,next_attempt_at=NULL,completed_at=?,updated_at=?,"
        "usage_input_tokens=MAX(usage_input_tokens,?),"
        "usage_cached_input_tokens=MAX(usage_cached_input_tokens,?),"
        "usage_output_tokens=MAX(usage_output_tokens,?),"
        "lease_owner=NULL,lease_expires_at=NULL",
        (error_code[:100], now, now, usage_input, usage_cached, usage_output),
    )


def _next_poll(job: dict[str, Any], now: datetime) -> datetime:
    attempts = max(0, int(job.get("attempt_count") or 0))
    delay = min(
        settings.openai_background_max_poll_seconds,
        settings.openai_background_initial_poll_seconds * (2 ** min(attempts, 6)),
    )
    return now + timedelta(seconds=delay)


async def _retry_poll(
    db: aiosqlite.Connection,
    job: dict[str, Any],
    error_code: str,
) -> None:
    now = utc_now()
    errors = max(0, int(job.get("retrieve_error_count") or 0))
    delay = min(
        settings.openai_background_max_poll_seconds,
        settings.openai_background_initial_poll_seconds * (2 ** min(errors, 6)),
    )
    await _update_claimed(
        db,
        job,
        "status='queued',error_code=?,retrieve_error_count=retrieve_error_count+1,"
        "next_attempt_at=?,updated_at=?,lease_owner=NULL,lease_expires_at=NULL",
        (error_code[:100], utc_text(now + timedelta(seconds=delay)), utc_text(now)),
    )


async def _handle_provider_result(
    db: aiosqlite.Connection,
    job: dict[str, Any],
    events: list[dict[str, Any]],
    result: ResponseResult,
) -> None:
    status = result.status.lower()
    now = utc_now()
    if result.model is not None and result.model != job["model"]:
        await _fail_claimed(db, job, "calendar_provider_model_mismatch", result)
        return
    if (
        result.reasoning_effort is not None
        and result.reasoning_effort != job["reasoning_effort"]
    ):
        await _fail_claimed(db, job, "calendar_provider_reasoning_mismatch", result)
        return
    if status in {"queued", "in_progress"}:
        submitted = parse_utc(job["submitted_at"]) if job.get("submitted_at") else now
        poll_elapsed = (
            now - submitted
        ).total_seconds() > settings.openai_background_poll_timeout_seconds
        await _update_claimed(
            db,
            job,
            "status=?,last_polled_at=?,attempt_count=attempt_count+1,"
            "retrieve_error_count=0,next_attempt_at=?,error_code=?,updated_at=?,"
            "lease_owner=NULL,lease_expires_at=NULL",
            (
                status,
                utc_text(now),
                utc_text(_next_poll(job, now)),
                "poll_window_elapsed" if poll_elapsed else None,
                utc_text(now),
            ),
        )
        return
    if status != "completed":
        await _fail_claimed(db, job, result.error_code or "provider_response_failed", result)
        return
    try:
        payload = validate_calendar_output(result.output_text or "", events)
    except (ValidationError, ValueError, TypeError):
        await _fail_claimed(db, job, "invalid_calendar_structured_output", result)
        return
    serialized = payload.model_dump_json()
    await _update_claimed(
        db,
        job,
        "status='completed',result_json=?,completed_at=?,updated_at=?,error_code=NULL,"
        "usage_input_tokens=?,usage_cached_input_tokens=?,usage_output_tokens=?,"
        "next_attempt_at=NULL,lease_owner=NULL,lease_expires_at=NULL",
        (
            serialized,
            utc_text(now),
            utc_text(now),
            max(0, int(result.usage_input_tokens)),
            max(0, int(result.usage_cached_input_tokens)),
            max(0, int(result.usage_output_tokens)),
        ),
    )


async def process_claimed_calendar_job(
    db: aiosqlite.Connection,
    job: dict[str, Any],
    provider: ResponsesProvider,
) -> None:
    try:
        events = json.loads(job["events_json"])
    except (TypeError, json.JSONDecodeError):
        await _fail_claimed(db, job, "invalid_calendar_job_snapshot")
        return
    if not isinstance(events, list) or len(events) != int(job["event_count"]):
        await _fail_claimed(db, job, "invalid_calendar_job_snapshot")
        return

    model_input = build_calendar_model_input(events)
    output_format = structured_output_format(
        schema=CalendarAnalysisPayload.model_json_schema(mode="validation"),
        name="calendar_analysis",
    )
    request_options = {
        "model": str(job["model"]),
        "reasoning_effort": str(job["reasoning_effort"]),
        "max_output_tokens": int(
            job.get("max_output_tokens") or settings.calendar_max_output_tokens
        ),
        "output_format": output_format,
        "instructions": CALENDAR_ANALYSIS_INSTRUCTIONS,
    }

    if job.get("openai_response_id"):
        try:
            result = await provider.retrieve(str(job["openai_response_id"]))
        except Exception:
            await _retry_poll(db, job, "calendar_provider_retrieve_failed")
            return
        await _handle_provider_result(db, job, events, result)
        return

    execution_mode = str(job.get("execution_mode") or settings.openai_execution_mode)
    execution_marker = "submission_in_progress"
    marked = await _update_claimed(
        db,
        job,
        "status='in_progress',error_code=?,"
        "submitted_at=COALESCE(submitted_at,?),attempt_count=attempt_count+1,updated_at=?",
        (execution_marker, utc_text(), utc_text()),
    )
    if not marked:
        return

    if execution_mode == "worker_sync":
        try:
            result = await provider.create_sync(model_input, **request_options)
        except Exception:
            await _fail_claimed(db, job, "submission_outcome_unknown")
            return
        await _handle_provider_result(db, job, events, result)
        return

    try:
        result = await provider.create_background(model_input, **request_options)
    except Exception:
        await _fail_claimed(db, job, "submission_outcome_unknown")
        return
    if not result.response_id:
        await _fail_claimed(db, job, "provider_missing_response_id", result)
        return
    persisted = await _update_claimed(
        db,
        job,
        "openai_response_id=?,status='in_progress',error_code=NULL,last_polled_at=?,"
        "next_attempt_at=?,updated_at=?",
        (
            result.response_id,
            utc_text(),
            utc_text(_next_poll(job, utc_now())),
            utc_text(),
        ),
    )
    if not persisted:
        return
    job["openai_response_id"] = result.response_id
    await _handle_provider_result(db, job, events, result)


async def _renew_calendar_lease(job: dict[str, Any], stop: asyncio.Event) -> None:
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
                """UPDATE calendar_analysis_jobs SET lease_expires_at=?,updated_at=?
                   WHERE job_id=? AND fencing_token=? AND lease_owner=?""",
                (
                    utc_text(now + timedelta(seconds=settings.analysis_worker_lease_seconds)),
                    utc_text(now),
                    job["job_id"],
                    job["fencing_token"],
                    job["lease_owner"],
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
            logger.warning("Calendar analysis worker lease renewal was deferred")
        finally:
            if renewal_db is not None:
                await renewal_db.close()


async def run_calendar_worker_once(
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
                 status='idle',error_code=NULL""",
            (worker_id, now, now),
        )
        await db.commit()
        if provider.capabilities().status != "ok":
            return False
        job = await claim_next_calendar_job(db, worker_id)
        if job is None:
            return False
        await db.execute(
            "UPDATE analysis_worker_state SET heartbeat_at=?,status='working',last_job_id=? "
            "WHERE worker_id=?",
            (utc_text(), job["job_id"], worker_id),
        )
        await db.commit()
        renewal_stop = asyncio.Event()
        renewal_task = asyncio.create_task(_renew_calendar_lease(job, renewal_stop))
        try:
            await process_claimed_calendar_job(db, job, provider)
        finally:
            renewal_stop.set()
            await renewal_task
        await db.execute(
            "UPDATE analysis_worker_state SET heartbeat_at=?,status='idle',error_code=NULL "
            "WHERE worker_id=?",
            (utc_text(), worker_id),
        )
        await db.commit()
        return True
    except Exception:
        await db.rollback()
        try:
            await db.execute(
                "UPDATE analysis_worker_state SET heartbeat_at=?,status='failed',"
                "error_code='calendar_worker_iteration_failed' WHERE worker_id=?",
                (utc_text(), worker_id),
            )
            await db.commit()
        except Exception:
            await db.rollback()
            logger.warning("Calendar worker failure-state update was deferred")
        raise
    finally:
        await db.close()
        if owned_provider and callable(getattr(provider, "close", None)):
            await provider.close()
