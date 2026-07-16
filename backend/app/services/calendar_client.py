from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from app.config import settings
from app.models.database import (
    get_db,
    parse_utc,
    record_calendar_snapshot,
    upsert_source_health,
)
from app.utils.http import safe_exception_message

logger = logging.getLogger(__name__)

CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
CACHE_TTL = settings.calendar.interval_seconds
STALE_RETRY_TTL = 300
MAX_STALE_CACHE_AGE = timedelta(days=7)
CACHE_FILE = settings.calendar_cache_path

COUNTRY_MAP = {
    "USD": "🇺🇸 美国", "EUR": "🇪🇺 欧元区", "GBP": "🇬🇧 英国",
    "JPY": "🇯🇵 日本", "CNY": "🇨🇳 中国", "AUD": "🇦🇺 澳大利亚",
    "CAD": "🇨🇦 加拿大", "CHF": "🇨🇭 瑞士", "NZD": "🇳🇿 新西兰",
}
IMPACT_MAP = {"high": "高", "medium": "中", "low": "低", "holiday": "假日"}
MAJOR_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CNY", "AUD", "CAD", "CHF"}

_calendar_cache: list[dict] = []
_cache_time = 0.0
_cache_ttl = CACHE_TTL
_calendar_status: dict[str, Any] = {
    "source": "faireconomy",
    "stale": False,
    "as_of": None,
    "last_attempt": None,
    "last_success": None,
    "last_error": None,
    "error_code": None,
}


class CalendarPersistenceError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def get_calendar_status() -> dict[str, Any]:
    return dict(_calendar_status)


def _atomic_write_cache(raw_events: list[dict], fetched_at: str) -> None:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {"fetched_at": fetched_at, "events": raw_events}
    temporary_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=CACHE_FILE.parent,
            prefix="calendar-",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = handle.name
            json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, CACHE_FILE)
    finally:
        if temporary_path and os.path.exists(temporary_path):
            os.unlink(temporary_path)


def _load_last_known_good() -> tuple[list[dict], str | None]:
    with CACHE_FILE.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if isinstance(payload, list):
        events = payload
        fetched_at = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime, tz=timezone.utc).isoformat()
    elif isinstance(payload, dict) and isinstance(payload.get("events"), list):
        events = payload["events"]
        fetched_at = payload.get("fetched_at")
    else:
        raise ValueError("Calendar cache has an invalid schema")

    if not fetched_at:
        raise ValueError("Calendar cache has no freshness timestamp")
    try:
        fetched = datetime.fromisoformat(str(fetched_at).replace("Z", "+00:00"))
        if fetched.tzinfo is None:
            fetched = fetched.replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError("Calendar cache has an invalid freshness timestamp") from exc
    if datetime.now(timezone.utc) - fetched.astimezone(timezone.utc) > MAX_STALE_CACHE_AGE:
        raise ValueError("Calendar cache is too old to display")
    return events, str(fetched_at)


def _normalize_events(raw_events: list[dict], *, stale: bool, fetched_at: str | None) -> list[dict]:
    events: list[dict] = []
    for raw in raw_events:
        if not isinstance(raw, dict):
            continue
        country_code = str(raw.get("country") or "").upper()
        impact = str(raw.get("impact", "") or "").lower()
        date = str(raw.get("date") or "").strip()
        title = str(raw.get("title") or "").strip()
        if not date or not title:
            continue
        event = {
            "date": date,
            "title": title,
            "country_code": country_code,
            "country": COUNTRY_MAP.get(country_code, country_code),
            "impact": impact,
            "impact_zh": IMPACT_MAP.get(impact, impact),
            "forecast": str(raw.get("forecast") or ""),
            "previous": str(raw.get("previous") or ""),
            "actual": str(raw.get("actual") or ""),
            "is_stale": stale,
            "source_fetched_at": fetched_at,
        }
        if country_code in MAJOR_CURRENCIES and (impact in {"high", "medium"} or event["actual"]):
            events.append(event)
    events.sort(key=lambda event: str(event.get("date", "")))
    return events


def _validate_event_times(events: list[dict]) -> None:
    for event in events:
        parse_utc(str(event.get("date") or ""), field="calendar.date")


def _retry_at(attempted_at: str) -> str:
    return (
        datetime.fromisoformat(attempted_at.replace("Z", "+00:00"))
        + timedelta(seconds=STALE_RETRY_TTL)
    ).isoformat()


async def _read_prior_health(db: Any) -> tuple[str | None, str | None, int]:
    async with db.execute(
        """SELECT last_success_at,data_through,consecutive_failures
           FROM source_health WHERE source='faireconomy'"""
    ) as cursor:
        prior = await cursor.fetchone()
    return (
        prior[0] if prior else None,
        prior[1] if prior else None,
        int(prior[2] or 0) if prior else 0,
    )


async def _persist_unavailable_health(attempted_at: str, error_code: str) -> None:
    db = await get_db()
    try:
        prior_success, prior_data_through, prior_failures = await _read_prior_health(db)
        await upsert_source_health(
            db,
            source="faireconomy",
            status="degraded" if prior_success else "unavailable",
            last_attempt_at=attempted_at,
            last_success_at=prior_success,
            data_through=prior_data_through,
            consecutive_failures=prior_failures + 1,
            next_attempt_at=_retry_at(attempted_at),
            raw_count=0,
            inserted_count=None,
            duplicates_count=None,
            error_code=error_code,
        )
    finally:
        await db.close()


async def _persist_calendar_refresh(
    events: list[dict],
    *,
    attempted_at: str,
    fetched_at: str,
    observed_at: str,
    stale: bool,
    source_error_code: str | None,
) -> str:
    db = await get_db()
    stage = "snapshot"
    try:
        await db.execute("BEGIN IMMEDIATE")
        prior_success, prior_data_through, prior_failures = await _read_prior_health(db)
        await record_calendar_snapshot(
            db,
            events,
            source_fetched_at=fetched_at,
            stale=stale,
            observed_at=observed_at,
            commit=False,
        )
        stage = "source_health"
        reported_success = fetched_at if not stale else (prior_success or fetched_at)
        await upsert_source_health(
            db,
            source="faireconomy",
            status="degraded" if stale else "ok",
            last_attempt_at=attempted_at,
            last_success_at=reported_success,
            data_through=(prior_data_through or fetched_at) if stale else fetched_at,
            consecutive_failures=prior_failures + 1 if stale else 0,
            next_attempt_at=_retry_at(observed_at) if stale else None,
            raw_count=0 if stale else len(events),
            inserted_count=None,
            duplicates_count=None,
            error_code=source_error_code if stale else None,
            commit=False,
        )
        stage = "commit"
        await db.commit()
        return reported_success
    except Exception as exc:
        try:
            await db.rollback()
        except Exception:
            pass
        code = (
            "calendar_source_health_persistence_failed"
            if stage == "source_health"
            else "calendar_persistence_failed"
        )
        raise CalendarPersistenceError(code) from exc
    finally:
        await db.close()


async def fetch_economic_calendar(*, force: bool = False) -> list[dict]:
    """Publish calendar state only after its SQLite transaction commits."""
    global _calendar_cache, _cache_time, _cache_ttl

    if not force and _calendar_cache and time.monotonic() - _cache_time < _cache_ttl:
        return [dict(event) for event in _calendar_cache]

    attempted_at = _utc_now()
    _calendar_status["last_attempt"] = attempted_at
    raw_events: list[dict] | None = None
    fetched_at: str | None = None
    source_error_code: str | None = None

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                CALENDAR_URL,
                headers={"User-Agent": "MacroLens/1.0 (economic calendar reader)"},
            )
            response.raise_for_status()
    except Exception as exc:
        logger.warning(
            "Economic calendar upstream failed error_type=%s",
            type(exc).__name__,
        )
        source_error_code = "calendar_upstream_failed"
    else:
        try:
            payload = response.json()
            if not isinstance(payload, list) or not payload:
                raise ValueError("Calendar source returned no events")
            raw_events = payload
            fetched_at = _utc_now()
        except Exception as exc:
            logger.warning(
                "Economic calendar response could not be parsed error_type=%s",
                type(exc).__name__,
            )
            source_error_code = "calendar_parse_failed"

    events: list[dict] | None = None
    if raw_events is not None:
        try:
            events = _normalize_events(raw_events, stale=False, fetched_at=fetched_at)
            _validate_event_times(events)
        except Exception as exc:
            logger.warning(
                "Economic calendar normalization failed error_type=%s",
                type(exc).__name__,
            )
            raw_events = None
            fetched_at = None
            source_error_code = "calendar_parse_failed"

    stale = raw_events is None
    if raw_events is None:
        try:
            raw_events, fetched_at = _load_last_known_good()
            logger.warning("Economic calendar is using stale last-known-good data")
        except Exception as exc:
            logger.warning(
                "Economic calendar fallback is unavailable error_type=%s",
                type(exc).__name__,
            )
            error_code = source_error_code or "calendar_parse_failed"
            _calendar_status.update(
                stale=True,
                last_attempt=attempted_at,
                last_error=error_code,
                error_code=error_code,
            )
            _cache_ttl = STALE_RETRY_TTL
            try:
                await _persist_unavailable_health(attempted_at, error_code)
            except Exception as health_error:
                logger.error(
                    "Economic calendar failure health could not be persisted error_type=%s",
                    type(health_error).__name__,
                )
            return [dict(event) for event in _calendar_cache]

    # Point-in-time availability starts when the response/fallback decision is
    # complete, never when the request began.
    observed_at = _utc_now()
    if events is None:
        try:
            events = _normalize_events(raw_events, stale=stale, fetched_at=fetched_at)
            _validate_event_times(events)
        except Exception as exc:
            logger.warning(
                "Economic calendar fallback normalization failed error_type=%s",
                type(exc).__name__,
            )
            error_code = "calendar_parse_failed"
            _calendar_status.update(
                stale=True,
                last_attempt=attempted_at,
                last_error=error_code,
                error_code=error_code,
            )
            _cache_ttl = STALE_RETRY_TTL
            try:
                await _persist_unavailable_health(attempted_at, error_code)
            except Exception as health_error:
                logger.error(
                    "Economic calendar parse health could not be persisted error_type=%s",
                    type(health_error).__name__,
                )
            return [dict(event) for event in _calendar_cache]

    try:
        reported_success = await _persist_calendar_refresh(
            events,
            attempted_at=attempted_at,
            fetched_at=str(fetched_at),
            observed_at=observed_at,
            stale=stale,
            source_error_code=source_error_code,
        )
    except CalendarPersistenceError as exc:
        logger.error(
            "Economic calendar persistence failed code=%s error_type=%s",
            exc.code,
            type(exc.__cause__).__name__ if exc.__cause__ else "UnknownError",
        )
        _calendar_status.update(
            stale=True,
            last_attempt=attempted_at,
            last_error=exc.code,
            error_code=exc.code,
        )
        _cache_ttl = STALE_RETRY_TTL
        try:
            await _persist_unavailable_health(attempted_at, exc.code)
        except Exception as health_error:
            logger.error(
                "Economic calendar persistence health could not be recorded error_type=%s",
                type(health_error).__name__,
            )
        return [dict(event) for event in _calendar_cache]

    if not stale:
        try:
            _atomic_write_cache(raw_events, str(fetched_at))
        except Exception as exc:
            logger.warning("Economic calendar cache write failed: %s", safe_exception_message(exc))
    _calendar_status.update(
        stale=stale,
        as_of=fetched_at,
        last_success=reported_success,
        last_error=source_error_code if stale else None,
        error_code=source_error_code if stale else None,
    )
    _cache_ttl = STALE_RETRY_TTL if stale else CACHE_TTL
    _calendar_cache = events
    _cache_time = time.monotonic()
    logger.info("Economic calendar: %s events (stale=%s)", len(events), stale)
    return [dict(event) for event in events]
