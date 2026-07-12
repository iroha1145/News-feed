from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from app.utils.http import log_http_failure, safe_exception_message

logger = logging.getLogger(__name__)

CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
CACHE_TTL = 3600
STALE_RETRY_TTL = 300
MAX_STALE_CACHE_AGE = timedelta(days=7)
CACHE_FILE = Path(__file__).resolve().parents[2] / "data" / "calendar_cache.json"

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
}


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


async def fetch_economic_calendar(*, force: bool = False) -> list[dict]:
    """Fetch the calendar, atomically persisting and exposing a stale last-known-good fallback."""
    global _calendar_cache, _cache_time, _cache_ttl

    if not force and _calendar_cache and time.monotonic() - _cache_time < _cache_ttl:
        return [dict(event) for event in _calendar_cache]

    attempted_at = _utc_now()
    _calendar_status["last_attempt"] = attempted_at
    raw_events: list[dict] | None = None
    fetched_at: str | None = None
    upstream_error: str | None = None

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                CALENDAR_URL,
                headers={"User-Agent": "MacroLens/1.0 (economic calendar reader)"},
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list) or not payload:
                raise ValueError("Calendar source returned no events")
            raw_events = payload
            fetched_at = _utc_now()
    except Exception as exc:
        upstream_error = log_http_failure(logger, "Economic calendar", exc, endpoint=CALENDAR_URL)

    stale = raw_events is None
    if raw_events is not None:
        try:
            _atomic_write_cache(raw_events, fetched_at or attempted_at)
        except Exception as exc:
            logger.warning("Economic calendar cache write failed: %s", safe_exception_message(exc))
        _calendar_status.update(
            stale=False,
            as_of=fetched_at,
            last_success=fetched_at,
            last_error=None,
        )
        _cache_ttl = CACHE_TTL
    else:
        try:
            raw_events, fetched_at = _load_last_known_good()
            logger.warning("Economic calendar is using stale last-known-good data")
        except Exception as exc:
            cache_error = safe_exception_message(exc)
            _calendar_status.update(stale=True, as_of=None, last_error=upstream_error or cache_error)
            _calendar_cache = []
            _cache_time = time.monotonic()
            _cache_ttl = STALE_RETRY_TTL
            return []
        _calendar_status.update(stale=True, as_of=fetched_at, last_error=upstream_error)
        _cache_ttl = STALE_RETRY_TTL

    # Point-in-time availability starts when the response/fallback decision is
    # complete, never when the request began.
    observed_at = _utc_now()
    events = _normalize_events(raw_events, stale=stale, fetched_at=fetched_at)
    if fetched_at:
        try:
            from app.integrations.option_pro.repository import (
                record_calendar_snapshot,
                upsert_source_health,
            )
            from app.models.database import get_db

            db = await get_db()
            try:
                async with db.execute(
                    """SELECT last_success_at,consecutive_failures FROM source_health
                       WHERE source='faireconomy'"""
                ) as cursor:
                    prior_health = await cursor.fetchone()
                prior_success = prior_health[0] if prior_health else None
                prior_failures = int(prior_health[1] or 0) if prior_health else 0
                if stale and not _calendar_status.get("last_success"):
                    _calendar_status["last_success"] = prior_success or fetched_at
                await record_calendar_snapshot(
                    db,
                    events,
                    source_fetched_at=fetched_at,
                    stale=stale,
                    observed_at=observed_at,
                )
                await upsert_source_health(
                    db,
                    source="faireconomy",
                    status="degraded" if stale else "ok",
                    last_attempt_at=attempted_at,
                    last_success_at=(fetched_at if not stale else (prior_success or fetched_at)),
                    data_through=fetched_at,
                    consecutive_failures=prior_failures + 1 if stale else 0,
                    next_attempt_at=(
                        (
                            datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
                            + timedelta(seconds=STALE_RETRY_TTL)
                        ).isoformat()
                        if stale else None
                    ),
                    raw_count=0 if stale else len(events),
                    inserted_count=None,
                    duplicates_count=None,
                    error_code="calendar_source_failed" if stale else None,
                )
            finally:
                await db.close()
        except Exception:
            # Calendar display remains available from its last-known-good file
            # even if the additive Integration projection cannot be published.
            logger.exception("Economic calendar integration projection failed")
    _calendar_cache = events
    _cache_time = time.monotonic()
    logger.info("Economic calendar: %s events (stale=%s)", len(events), stale)
    return [dict(event) for event in events]
