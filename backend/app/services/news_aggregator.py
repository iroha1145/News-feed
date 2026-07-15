from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

from app.config import settings
from app.models.database import (
    get_db,
    get_source_health,
    insert_news_items_batch,
    upsert_source_health,
)
from app.services.finnhub_client import fetch_finnhub_news
from app.services.gnews_client import fetch_gnews_news
from app.services.googlenews_client import fetch_google_news
from app.services.massive_client import fetch_massive_news
from app.services.newsapi_client import fetch_newsapi_news
from app.services.seekingalpha_client import fetch_seekingalpha_breaking, fetch_seekingalpha_daily
from app.utils.dedup import compute_content_hash, compute_legacy_content_hash
from app.utils.http import safe_exception_message

logger = logging.getLogger(__name__)

Fetcher = Callable[..., Awaitable[list[dict]]]


@dataclass(frozen=True)
class SourceDefinition:
    name: str
    fetcher: Fetcher
    requires_api_key: bool = False
    group: str | None = None


SOURCE_DEFINITIONS: dict[str, SourceDefinition] = {
    "finnhub": SourceDefinition("finnhub", fetch_finnhub_news, True),
    "massive": SourceDefinition("massive", fetch_massive_news, True),
    "google": SourceDefinition("google", fetch_google_news),
    "seekingalpha_breaking": SourceDefinition(
        "seekingalpha_breaking", fetch_seekingalpha_breaking, group="seekingalpha"
    ),
    "seekingalpha_daily": SourceDefinition(
        "seekingalpha_daily", fetch_seekingalpha_daily, group="seekingalpha"
    ),
    "newsapi": SourceDefinition("newsapi", fetch_newsapi_news, True),
    "gnews": SourceDefinition("gnews", fetch_gnews_news, True),
}

if set(SOURCE_DEFINITIONS) != set(settings.sources):
    missing = sorted(set(SOURCE_DEFINITIONS) ^ set(settings.sources))
    raise RuntimeError(f"source configuration mismatch: {', '.join(missing)}")

_status_lock = asyncio.Lock()
_source_status: dict[str, dict] = {}
_backoff_until: dict[str, float] = {}
_BACKOFF_CAP_SECONDS = 21_600


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def is_source_enabled(name: str) -> bool:
    return settings.source(name).enabled


def get_source_interval(name: str) -> int:
    return settings.source(name).interval_seconds


def get_enabled_sources() -> list[str]:
    return [name for name in SOURCE_DEFINITIONS if is_source_enabled(name)]


def _source_snapshot(name: str) -> dict:
    definition = SOURCE_DEFINITIONS[name]
    api_key = settings.api_key(name)
    snapshot = {
        "source": name,
        "group": definition.group,
        "enabled": is_source_enabled(name),
        "configured": not definition.requires_api_key or bool(api_key),
        "interval_seconds": get_source_interval(name),
        "last_attempt": None,
        "last_success": None,
        "data_through": None,
        "last_error": None,
        "duration_ms": None,
        "raw": 0,
        "inserted": 0,
        "duplicates": 0,
        "consecutive_failures": 0,
        "next_attempt_at": None,
        "source_fetch_status": "unavailable",
        "news_persistence_status": "unavailable",
        "last_error_code": None,
    }
    snapshot.update(_source_status.get(name, {}))
    snapshot["enabled"] = is_source_enabled(name)
    snapshot["configured"] = not definition.requires_api_key or bool(api_key)
    snapshot["interval_seconds"] = get_source_interval(name)
    return snapshot


def get_source_statuses() -> list[dict]:
    return [_source_snapshot(name) for name in SOURCE_DEFINITIONS]


async def _update_status(name: str, **changes: object) -> None:
    async with _status_lock:
        current = dict(_source_status.get(name, {}))
        current.update(changes)
        _source_status[name] = current
        snapshot = _source_snapshot(name)

    if not snapshot["enabled"]:
        status = "disabled"
    elif not snapshot["configured"]:
        status = "not_configured"
    elif snapshot["source_fetch_status"] not in {"ok"}:
        status = "degraded" if snapshot.get("last_success") else "unavailable"
    elif snapshot["news_persistence_status"] != "ok":
        status = "degraded"
    else:
        status = "ok"

    db = await get_db()
    try:
        await upsert_source_health(
            db,
            source=name,
            status=status,
            last_attempt_at=snapshot.get("last_attempt"),
            last_success_at=snapshot.get("last_success"),
            data_through=snapshot.get("data_through"),
            consecutive_failures=int(snapshot.get("consecutive_failures") or 0),
            next_attempt_at=snapshot.get("next_attempt_at"),
            raw_count=int(snapshot.get("raw") or 0) if snapshot.get("last_attempt") else None,
            inserted_count=int(snapshot.get("inserted") or 0) if snapshot.get("last_attempt") else None,
            duplicates_count=int(snapshot.get("duplicates") or 0) if snapshot.get("last_attempt") else None,
            error_code=snapshot.get("last_error_code"),
        )
    finally:
        await db.close()


async def initialize_source_health() -> None:
    db = await get_db()
    try:
        persisted = {row["source"]: row for row in await get_source_health(db)}
    finally:
        await db.close()
    async with _status_lock:
        for name, row in persisted.items():
            if name not in SOURCE_DEFINITIONS:
                continue
            _source_status[name] = {
                "last_attempt": row.get("last_attempt_at"),
                "last_success": row.get("last_success_at"),
                "data_through": row.get("data_through"),
                "last_error": row.get("error_code"),
                "raw": row.get("raw_count") or 0,
                "inserted": row.get("inserted_count") or 0,
                "duplicates": row.get("duplicates_count") or 0,
                "consecutive_failures": row.get("consecutive_failures") or 0,
                "next_attempt_at": row.get("next_attempt_at"),
                "source_fetch_status": (
                    "ok" if row.get("status") == "ok" else row.get("status") or "unavailable"
                ),
                "news_persistence_status": (
                    "ok" if row.get("status") == "ok" else row.get("status") or "unavailable"
                ),
                "last_error_code": row.get("error_code"),
            }
    for name in SOURCE_DEFINITIONS:
        await _update_status(name)


async def aggregate_source(name: str, *, force: bool = False) -> dict:
    if name == "seekingalpha":
        results = await asyncio.gather(
            aggregate_source("seekingalpha_breaking", force=force),
            aggregate_source("seekingalpha_daily", force=force),
        )
        return {
            "source": name,
            "inserted": sum(result.get("inserted", 0) for result in results),
            "duplicates": sum(result.get("duplicates", 0) for result in results),
            "results": results,
        }
    if name not in SOURCE_DEFINITIONS:
        raise ValueError(f"unknown news source: {name}")

    definition = SOURCE_DEFINITIONS[name]
    started = time.monotonic()
    attempted_at = _now()
    await _update_status(name, last_attempt=_iso(attempted_at))
    if not force and not is_source_enabled(name):
        return {"source": name, "status": "disabled", "inserted": 0, "duplicates": 0}

    if not force and time.monotonic() < _backoff_until.get(name, 0):
        return {
            "source": name,
            "status": "backoff",
            "next_attempt_at": _source_snapshot(name).get("next_attempt_at"),
            "inserted": 0,
            "duplicates": 0,
        }

    api_key = settings.api_key(name)
    if definition.requires_api_key and not api_key:
        await _update_status(
            name,
            last_error="API key not configured",
            source_fetch_status="not_configured",
            news_persistence_status="not_configured",
            last_error_code="source_not_configured",
        )
        return {"source": name, "status": "not_configured", "inserted": 0, "duplicates": 0}

    try:
        fetched = (
            await definition.fetcher(api_key)
            if definition.requires_api_key
            else await definition.fetcher()
        )
    except Exception as exc:
        failures = int(_source_snapshot(name).get("consecutive_failures") or 0) + 1
        base_delay = get_source_interval(name) * (2 ** min(failures, 6))
        delay = min(_BACKOFF_CAP_SECONDS, max(60, int(base_delay * random.uniform(0.85, 1.15))))
        _backoff_until[name] = time.monotonic() + delay
        error = safe_exception_message(exc, secrets=(api_key,))
        await _update_status(
            name,
            last_error=error,
            duration_ms=round((time.monotonic() - started) * 1000, 1),
            raw=0,
            inserted=0,
            duplicates=0,
            consecutive_failures=failures,
            next_attempt_at=_iso(attempted_at + timedelta(seconds=delay)),
            source_fetch_status=(
                "degraded" if _source_snapshot(name).get("last_success") else "unavailable"
            ),
            last_error_code="source_fetch_failed",
        )
        logger.error("News source %s failed; retry deferred for %ss: %s", name, delay, error)
        return {"source": name, "status": "error", "error": error, "inserted": 0, "duplicates": 0}

    _backoff_until.pop(name, None)
    fetched_at = _iso(_now())
    records = [
        {
            **item,
            "fetched_at": fetched_at,
            "content_hash": compute_content_hash(
                str(item.get("title") or ""),
                str(item.get("url") or ""),
                item.get("published_at"),
            ),
            "legacy_content_hash": compute_legacy_content_hash(
                str(item.get("title") or ""), str(item.get("url") or "")
            ),
        }
        for item in fetched
    ]
    db = await get_db()
    try:
        result = await insert_news_items_batch(db, records)
    except Exception as exc:
        error = safe_exception_message(exc, secrets=(api_key,))
        await _update_status(
            name,
            last_success=fetched_at,
            # Persistence waterline deliberately stays unchanged.
            last_error=error,
            duration_ms=round((time.monotonic() - started) * 1000, 1),
            raw=len(fetched),
            inserted=0,
            duplicates=0,
            consecutive_failures=0,
            next_attempt_at=None,
            source_fetch_status="ok",
            news_persistence_status="degraded",
            last_error_code="news_persistence_failed",
        )
        return {
            "source": name,
            "status": "degraded",
            "error": "news_persistence_failed",
            "inserted": 0,
            "duplicates": 0,
        }
    finally:
        await db.close()

    duration_ms = round((time.monotonic() - started) * 1000, 1)
    await _update_status(
        name,
        last_success=fetched_at,
        data_through=fetched_at,
        last_error=None,
        duration_ms=duration_ms,
        raw=len(fetched),
        inserted=result["inserted"],
        duplicates=result["duplicates"],
        consecutive_failures=0,
        next_attempt_at=None,
        source_fetch_status="ok",
        news_persistence_status="ok",
        last_error_code=None,
    )
    return {
        "source": name,
        "status": "ok",
        "raw": len(fetched),
        "inserted": result["inserted"],
        "duplicates": result["duplicates"],
        "duration_ms": duration_ms,
    }


async def aggregate_enabled(*, force: bool = False) -> dict:
    results = await asyncio.gather(
        *(aggregate_source(name, force=force) for name in get_enabled_sources())
    )
    return {
        "sources": results,
        "inserted": sum(result.get("inserted", 0) for result in results),
        "duplicates": sum(result.get("duplicates", 0) for result in results),
    }


async def aggregate_all_news() -> int:
    return int((await aggregate_enabled())["inserted"])
