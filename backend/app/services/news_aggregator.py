from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Optional

from app.config import settings as app_settings
from app.models import database as database_module
from app.models.database import get_db, get_setting, insert_news_item
from app.services.finnhub_client import fetch_finnhub_news
from app.services.gnews_client import fetch_gnews_news
from app.services.googlenews_client import fetch_google_news
from app.services.massive_client import fetch_massive_news
from app.services.newsapi_client import fetch_newsapi_news
from app.services.seekingalpha_client import (
    fetch_seekingalpha_breaking,
    fetch_seekingalpha_daily,
)
from app.utils.dedup import compute_content_hash, compute_legacy_content_hash, deduplicate_batch
from app.utils.http import safe_exception_message

logger = logging.getLogger(__name__)

Fetcher = Callable[[str], Awaitable[list[dict]]]
KeylessFetcher = Callable[[], Awaitable[list[dict]]]


@dataclass(frozen=True)
class SourceDefinition:
    name: str
    fetcher: Fetcher | KeylessFetcher
    default_enabled: bool
    default_interval: int
    settings_prefix: str
    api_key_setting: Optional[str] = None
    group: Optional[str] = None


SOURCE_DEFINITIONS: dict[str, SourceDefinition] = {
    "finnhub": SourceDefinition("finnhub", fetch_finnhub_news, True, 300, "finnhub_news", "finnhub_api_key"),
    "massive": SourceDefinition("massive", fetch_massive_news, True, 300, "massive_news", "massive_api_key"),
    "google": SourceDefinition("google", fetch_google_news, True, 900, "google_news"),
    "seekingalpha_breaking": SourceDefinition(
        "seekingalpha_breaking", fetch_seekingalpha_breaking, True, 300,
        "seekingalpha_breaking", group="seekingalpha",
    ),
    "seekingalpha_daily": SourceDefinition(
        "seekingalpha_daily", fetch_seekingalpha_daily, True, 21600,
        "seekingalpha_daily", group="seekingalpha",
    ),
    "newsapi": SourceDefinition("newsapi", fetch_newsapi_news, False, 1800, "newsapi_news", "newsapi_api_key"),
    "gnews": SourceDefinition("gnews", fetch_gnews_news, False, 1800, "gnews_news", "gnews_api_key"),
}

_status_lock = asyncio.Lock()
_source_status: dict[str, dict] = {}
_backoff_until: dict[str, float] = {}
_BACKOFF_CAP_SECONDS = 21600


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _read_bool(value: object, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "off", "disabled"}:
        return False
    logger.warning("Invalid boolean source setting; using default=%s", default)
    return default


def is_source_enabled(name: str) -> bool:
    definition = SOURCE_DEFINITIONS[name]
    env_name = f"{definition.settings_prefix.upper()}_ENABLED"
    env_value = os.getenv(env_name)
    setting_value = getattr(app_settings, f"{definition.settings_prefix}_enabled", definition.default_enabled)
    return _read_bool(env_value if env_value is not None else setting_value, definition.default_enabled)


def get_source_interval(name: str) -> int:
    definition = SOURCE_DEFINITIONS[name]
    env_name = f"{definition.settings_prefix.upper()}_INTERVAL"
    raw_value = os.getenv(
        env_name,
        str(getattr(app_settings, f"{definition.settings_prefix}_interval", definition.default_interval)),
    )
    try:
        return max(30, int(raw_value))
    except (TypeError, ValueError):
        return definition.default_interval


def get_enabled_sources() -> list[str]:
    return [name for name in SOURCE_DEFINITIONS if is_source_enabled(name)]


def _source_snapshot(name: str) -> dict:
    definition = SOURCE_DEFINITIONS[name]
    status = {
        "source": name,
        "group": definition.group,
        "enabled": is_source_enabled(name),
        "configured": (
            definition.api_key_setting is None
            or bool(getattr(app_settings, definition.api_key_setting, ""))
        ),
        "interval_seconds": get_source_interval(name),
        "last_attempt": None,
        "last_success": None,
        "last_error": None,
        "duration_ms": None,
        "raw": 0,
        "inserted": 0,
        "duplicates": 0,
        "consecutive_failures": 0,
        "next_attempt_at": None,
    }
    status.update(_source_status.get(name, {}))
    status["enabled"] = is_source_enabled(name)
    status["interval_seconds"] = get_source_interval(name)
    return status


def get_source_statuses() -> list[dict]:
    return [_source_snapshot(name) for name in SOURCE_DEFINITIONS]


async def _api_key_for(definition: SourceDefinition) -> str:
    if definition.api_key_setting is None:
        return ""
    db = await get_db()
    try:
        override = await get_setting(db, definition.api_key_setting)
    finally:
        await db.close()
    return str(override or getattr(app_settings, definition.api_key_setting, "") or "")


def _batch_insert_function():
    for name in ("insert_news_items_batch", "bulk_insert_news_items", "insert_news_items_bulk"):
        function = getattr(database_module, name, None)
        if callable(function):
            return function
    return None


def _interpret_batch_result(result: object, total: int) -> tuple[int, int]:
    if isinstance(result, dict):
        inserted = int(result.get("inserted", result.get("inserted_count", 0)))
        duplicates = int(result.get("duplicates", result.get("duplicate_count", total - inserted)))
        return inserted, max(0, duplicates)
    if isinstance(result, tuple) and len(result) >= 2 and all(isinstance(value, int) for value in result[:2]):
        return int(result[0]), int(result[1])
    if isinstance(result, list):
        inserted = sum(value is not None for value in result)
        return inserted, total - inserted
    if isinstance(result, int):
        return result, total - result
    raise TypeError("Unsupported batch insert result")


async def _insert_records(records: list[dict]) -> tuple[int, int]:
    if not records:
        return 0, 0
    db = await get_db()
    try:
        batch_insert = _batch_insert_function()
        if batch_insert is not None:
            result = await batch_insert(db, records)
            return _interpret_batch_result(result, len(records))

        inserted = 0
        for record in records:
            if await insert_news_item(db, record) is not None:
                inserted += 1
        return inserted, len(records) - inserted
    finally:
        await db.close()


async def _update_status(name: str, **changes: object) -> None:
    async with _status_lock:
        current = dict(_source_status.get(name, {}))
        current.update(changes)
        _source_status[name] = current
        snapshot = _source_snapshot(name)
    try:
        from app.integrations.option_pro.repository import upsert_source_health

        if not snapshot.get("enabled"):
            persistent_status = "disabled"
        elif not snapshot.get("configured"):
            persistent_status = "not_configured"
        elif snapshot.get("last_error"):
            persistent_status = "degraded" if snapshot.get("last_success") else "unavailable"
        elif snapshot.get("last_success"):
            persistent_status = "ok"
        else:
            persistent_status = "unavailable"
        neutral_counts = persistent_status in {"disabled", "not_configured"} or not snapshot.get("last_attempt")
        db = await get_db()
        try:
            await upsert_source_health(
                db,
                source=name,
                status=persistent_status,
                last_attempt_at=snapshot.get("last_attempt"),
                last_success_at=snapshot.get("last_success"),
                data_through=snapshot.get("last_success"),
                consecutive_failures=int(snapshot.get("consecutive_failures") or 0),
                next_attempt_at=snapshot.get("next_attempt_at"),
                raw_count=None if neutral_counts else int(snapshot.get("raw") or 0),
                inserted_count=None if neutral_counts else int(snapshot.get("inserted") or 0),
                duplicates_count=None if neutral_counts else int(snapshot.get("duplicates") or 0),
                error_code="source_fetch_failed" if snapshot.get("last_error") else None,
            )
        finally:
            await db.close()
    except Exception:
        # Source ingestion remains isolated from the Integration projection.
        logger.exception("Unable to persist health for news source %s", name)


async def initialize_source_health() -> None:
    """Restore durable health before applying current configured/disabled states."""
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM source_health") as cursor:
            persisted = {str(row["source"]): dict(row) for row in await cursor.fetchall()}
    finally:
        await db.close()
    async with _status_lock:
        for name, row in persisted.items():
            if name not in SOURCE_DEFINITIONS:
                continue
            _source_status[name] = {
                "last_attempt": row.get("last_attempt_at"),
                "last_success": row.get("last_success_at"),
                "last_error": row.get("error_code"),
                "raw": row.get("raw_count") or 0,
                "inserted": row.get("inserted_count") or 0,
                "duplicates": row.get("duplicates_count") or 0,
                "consecutive_failures": row.get("consecutive_failures") or 0,
                "next_attempt_at": row.get("next_attempt_at"),
            }
    for name in SOURCE_DEFINITIONS:
        await _update_status(name)


async def aggregate_source(name: str, *, force: bool = False) -> dict:
    """Fetch, normalize, deduplicate, and store one independently scheduled source."""
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
        raise ValueError(f"Unknown news source: {name}")

    definition = SOURCE_DEFINITIONS[name]
    started = time.monotonic()
    attempted_at = _utc_now()
    await _update_status(name, last_attempt=_iso(attempted_at), enabled=is_source_enabled(name))

    if not force and not is_source_enabled(name):
        return {"source": name, "status": "disabled", "inserted": 0, "duplicates": 0}

    backoff_until = _backoff_until.get(name, 0.0)
    if not force and time.monotonic() < backoff_until:
        status = _source_snapshot(name)
        return {
            "source": name,
            "status": "backoff",
            "next_attempt_at": status.get("next_attempt_at"),
            "inserted": 0,
            "duplicates": 0,
        }

    api_key = ""
    try:
        api_key = await _api_key_for(definition)
        if definition.api_key_setting and not api_key:
            duration_ms = round((time.monotonic() - started) * 1000, 1)
            await _update_status(
                name,
                configured=False,
                last_error="API key not configured",
                duration_ms=duration_ms,
                raw=0,
                inserted=0,
                duplicates=0,
                next_attempt_at=None,
            )
            return {"source": name, "status": "not_configured", "inserted": 0, "duplicates": 0}

        if definition.api_key_setting:
            fetched = await definition.fetcher(api_key)  # type: ignore[misc]
        else:
            fetched = await definition.fetcher()  # type: ignore[call-arg]

        raw_count = len(fetched)
        unique_items, in_batch_duplicates = deduplicate_batch(fetched)
        fetched_at = _iso(_utc_now())
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
                    str(item.get("title") or ""),
                    str(item.get("url") or ""),
                ),
            }
            for item in unique_items
        ]
        inserted, database_duplicates = await _insert_records(records)
        duplicates = in_batch_duplicates + database_duplicates
    except Exception as exc:
        prior_failures = int(_source_snapshot(name).get("consecutive_failures") or 0)
        failures = prior_failures + 1
        base_delay = get_source_interval(name) * (2 ** min(failures, 6))
        delay = min(_BACKOFF_CAP_SECONDS, max(60, int(base_delay * random.uniform(0.85, 1.15))))
        _backoff_until[name] = time.monotonic() + delay
        next_attempt = attempted_at + timedelta(seconds=delay)
        error = safe_exception_message(exc, secrets=(api_key,))
        duration_ms = round((time.monotonic() - started) * 1000, 1)
        await _update_status(
            name,
            configured=not bool(definition.api_key_setting) or bool(api_key),
            last_error=error,
            duration_ms=duration_ms,
            raw=0,
            inserted=0,
            duplicates=0,
            consecutive_failures=failures,
            next_attempt_at=_iso(next_attempt),
        )
        logger.error("News source %s failed; retry deferred for %ss: %s", name, delay, error)
        return {"source": name, "status": "error", "error": error, "inserted": 0, "duplicates": 0}

    _backoff_until.pop(name, None)
    duration_ms = round((time.monotonic() - started) * 1000, 1)
    await _update_status(
        name,
        configured=True,
        last_success=fetched_at,
        last_error=None,
        duration_ms=duration_ms,
        raw=raw_count,
        inserted=inserted,
        duplicates=duplicates,
        consecutive_failures=0,
        next_attempt_at=None,
    )
    logger.info(
        "News source %s: raw=%s inserted=%s duplicates=%s duration_ms=%s",
        name, raw_count, inserted, duplicates, duration_ms,
    )
    return {
        "source": name,
        "status": "ok",
        "raw": raw_count,
        "inserted": inserted,
        "duplicates": duplicates,
        "duration_ms": duration_ms,
    }


async def aggregate_enabled(*, force: bool = False) -> dict:
    """Fetch all enabled sources concurrently while preserving per-source isolation."""
    names = get_enabled_sources()
    results = await asyncio.gather(*(aggregate_source(name, force=force) for name in names))
    return {
        "sources": results,
        "inserted": sum(result.get("inserted", 0) for result in results),
        "duplicates": sum(result.get("duplicates", 0) for result in results),
    }


async def aggregate_all_news() -> int:
    """Backward-compatible entry point; only enabled sources are fetched."""
    result = await aggregate_enabled()
    return int(result["inserted"])
