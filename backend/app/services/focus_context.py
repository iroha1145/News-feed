from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import secrets
import socket
import ssl
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Literal, Optional

import aiosqlite
import httpx
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
)

from app.config import settings
from app.integrations.option_pro.auth import (
    EMPTY_BODY_SHA256,
    HEADER_CONTENT_HASH,
    HEADER_KEY_ID,
    HEADER_NONCE,
    HEADER_SIGNATURE,
    HEADER_TIMESTAMP,
    calculate_signature,
    canonical_string,
)
from app.models.database import get_db


FOCUS_CONTEXT_PATH = "/api/integrations/macrolens/v1/focus-context"
FOCUS_SCHEMA_VERSION = "option-pro-macrolens-focus-v2"
FOCUS_SCHEMA_SHA256 = "fbc646433375bc5657ec1dcaf0f980c14191390dabe8468129fdf71f78d5cade"

logger = logging.getLogger(__name__)


class FocusClientError(RuntimeError):
    """A redacted failure safe to expose in worker logs and health details."""

    def __init__(self, category: str, *, retryable: bool = False) -> None:
        self.category = category
        self.retryable = retryable
        super().__init__(f"focus_context_{category}")


@dataclass
class _CircuitState:
    failures: int = 0
    opened_at: float | None = None


_CIRCUIT_LOCK = threading.Lock()
_CIRCUITS: dict[str, _CircuitState] = {}


class FocusSymbol(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    ticker: str = Field(pattern=r"^[A-Z0-9][A-Z0-9.^/_-]{0,19}$")
    validation_status: Literal["canonical", "valid_external", "unverified"]
    universe_reasons: list[str] = Field(min_length=1, max_length=12)
    dollar_volume_rank: Optional[int] = Field(default=None, ge=1)
    dollar_volume: Optional[float] = Field(default=None, ge=0)
    dollar_volume_basis: Literal[
        "intraday_completed_bars",
        "previous_complete_session",
        "adv20_completed_sessions",
        "unavailable",
    ] = "unavailable"
    session_change_pct: Optional[float] = None
    rvol_time_of_day: Optional[float] = Field(default=None, ge=0)
    breakout_state: Optional[str] = Field(default=None, max_length=60)
    sector_id: Optional[str] = Field(default=None, max_length=120)
    as_of: AwareDatetime
    data_through: Optional[AwareDatetime] = None
    data_quality: Optional[float] = Field(default=None, ge=0, le=1)
    data_status: Literal["active", "stale"] = "active"
    source_status: Literal[
        "active", "degraded", "fallback", "unavailable", "stale"
    ] = "unavailable"
    data_source: Optional[str] = Field(default=None, max_length=80)


class FocusContext(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    schema_version: Literal["option-pro-macrolens-focus-v2"] = FOCUS_SCHEMA_VERSION
    schema_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    revision: int = Field(ge=1)
    as_of: AwareDatetime
    data_through: Optional[AwareDatetime] = None
    market_session: Literal["premarket", "regular", "after_hours", "closed", "unknown"]
    universe_version: str = Field(min_length=1, max_length=200)
    symbols: list[FocusSymbol] = Field(default_factory=list, max_length=200)
    major_market_symbols: list[str] = Field(default_factory=list, max_length=20)
    warnings: list[str] = Field(default_factory=list, max_length=50)

    @field_validator("major_market_symbols")
    @classmethod
    def normalize_major_symbols(cls, value: list[str]) -> list[str]:
        normalized: list[str] = []
        for item in value:
            ticker = item.strip().upper().lstrip("$")
            if not ticker or len(ticker) > 20:
                raise ValueError("major market symbol is invalid")
            if ticker not in normalized:
                normalized.append(ticker)
        return normalized

    @field_validator("schema_sha256")
    @classmethod
    def require_committed_schema(cls, value: str) -> str:
        if not secrets.compare_digest(value, FOCUS_SCHEMA_SHA256):
            raise ValueError("focus_schema_sha256_mismatch")
        return value


def focus_capability() -> str:
    if not settings.option_pro_focus_base_url:
        return "not_configured"
    if not settings.option_pro_focus_key_id or not settings.option_pro_focus_secret:
        return "not_configured"
    return "enabled"


def _signed_headers(*, timestamp: int | None = None, nonce: str | None = None) -> dict[str, str]:
    timestamp_text = str(timestamp or int(time.time()))
    nonce_text = nonce or secrets.token_urlsafe(24).replace("=", "")
    canonical = canonical_string(
        "GET", FOCUS_CONTEXT_PATH, "", timestamp_text, nonce_text, EMPTY_BODY_SHA256
    )
    return {
        HEADER_KEY_ID: settings.option_pro_focus_key_id,
        HEADER_TIMESTAMP: timestamp_text,
        HEADER_NONCE: nonce_text,
        HEADER_CONTENT_HASH: EMPTY_BODY_SHA256,
        HEADER_SIGNATURE: calculate_signature(settings.option_pro_focus_secret, canonical),
    }


def _focus_origin() -> str:
    # Settings validation guarantees that this is a credential-free HTTPS
    # origin. Keeping URL construction here prevents redirects or request data
    # from changing the configured destination.
    return settings.option_pro_focus_base_url.rstrip("/")


def _tls_context() -> ssl.SSLContext:
    if not settings.option_pro_focus_verify_tls:
        raise FocusClientError("tls_configuration")
    try:
        return ssl.create_default_context(
            cafile=settings.option_pro_focus_ca_bundle or None
        )
    except (OSError, ssl.SSLError, ValueError) as exc:
        raise FocusClientError("tls_configuration") from None


def _create_focus_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=_focus_origin(),
        timeout=httpx.Timeout(
            connect=settings.option_pro_focus_connect_timeout_seconds,
            read=settings.option_pro_focus_read_timeout_seconds,
            write=settings.option_pro_focus_connect_timeout_seconds,
            pool=settings.option_pro_focus_connect_timeout_seconds,
        ),
        verify=_tls_context(),
        trust_env=False,
        follow_redirects=False,
    )


def _exception_chain_contains(exc: BaseException, kind: type[BaseException]) -> bool:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, kind):
            return True
        current = current.__cause__ or current.__context__
    return False


def _transport_error(exc: BaseException) -> FocusClientError:
    if _exception_chain_contains(exc, ssl.SSLError):
        return FocusClientError("tls", retryable=True)
    if _exception_chain_contains(exc, socket.gaierror):
        return FocusClientError("dns", retryable=True)
    if isinstance(exc, httpx.TimeoutException):
        return FocusClientError("timeout", retryable=True)
    if isinstance(exc, httpx.ConnectError):
        return FocusClientError("connect", retryable=True)
    if isinstance(exc, httpx.RequestError):
        return FocusClientError("transport", retryable=True)
    return FocusClientError("transport")


def _status_error(status_code: int) -> FocusClientError | None:
    if 300 <= status_code < 400:
        return FocusClientError("redirect")
    if status_code == 401:
        return FocusClientError("auth_401")
    if status_code == 403:
        return FocusClientError("auth_403")
    if status_code == 429:
        return FocusClientError("rate_limited", retryable=True)
    if 500 <= status_code < 600:
        return FocusClientError("upstream_5xx", retryable=True)
    if status_code != 200:
        return FocusClientError("http_4xx")
    return None


async def _read_focus_response(
    client: httpx.AsyncClient,
) -> bytes:
    timeout = httpx.Timeout(
        connect=settings.option_pro_focus_connect_timeout_seconds,
        read=settings.option_pro_focus_read_timeout_seconds,
        write=settings.option_pro_focus_connect_timeout_seconds,
        pool=settings.option_pro_focus_connect_timeout_seconds,
    )
    try:
        async with client.stream(
            "GET",
            f"{_focus_origin()}{FOCUS_CONTEXT_PATH}",
            headers=_signed_headers(),
            timeout=timeout,
            follow_redirects=False,
        ) as response:
            if error := _status_error(response.status_code):
                raise error
            media_type = response.headers.get("content-type", "").split(";", 1)[0]
            if media_type.strip().lower() != "application/json":
                raise FocusClientError("content_type")
            content_length = response.headers.get("content-length")
            if content_length:
                try:
                    declared_length = int(content_length)
                except ValueError:
                    raise FocusClientError("content_length") from None
                if declared_length < 0:
                    raise FocusClientError("content_length")
                if declared_length > settings.option_pro_focus_max_response_bytes:
                    raise FocusClientError("oversized_response")
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > settings.option_pro_focus_max_response_bytes:
                    raise FocusClientError("oversized_response")
            return bytes(body)
    except FocusClientError:
        raise
    except (httpx.RequestError, httpx.TimeoutException) as exc:
        raise _transport_error(exc) from None


async def _fetch_focus_context(
    client: httpx.AsyncClient,
    *,
    sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> FocusContext:
    async def attempts() -> FocusContext:
        last_error: FocusClientError | None = None
        for attempt in range(1, settings.option_pro_focus_max_attempts + 1):
            try:
                body = await _read_focus_response(client)
                try:
                    return FocusContext.model_validate_json(body)
                except (ValidationError, ValueError):
                    raise FocusClientError("schema") from None
            except FocusClientError as exc:
                last_error = exc
                if (
                    not exc.retryable
                    or attempt >= settings.option_pro_focus_max_attempts
                ):
                    raise
                logger.warning(
                    "focus_context_pull_retry category=%s attempt=%d",
                    exc.category,
                    attempt,
                )
                delay = settings.option_pro_focus_retry_backoff_seconds * (
                    2 ** (attempt - 1)
                )
                if delay:
                    await sleeper(delay)
        raise last_error or FocusClientError("transport")

    try:
        return await asyncio.wait_for(
            attempts(),
            timeout=settings.option_pro_focus_timeout_seconds,
        )
    except asyncio.TimeoutError:
        raise FocusClientError("total_timeout", retryable=True) from None


def _circuit_permits(origin: str, *, now: float | None = None) -> bool:
    observed = time.monotonic() if now is None else now
    with _CIRCUIT_LOCK:
        state = _CIRCUITS.get(origin)
        if state is None or state.opened_at is None:
            return True
        if observed - state.opened_at < settings.option_pro_focus_circuit_reset_seconds:
            return False
        state.failures = 0
        state.opened_at = None
        return True


def _record_circuit_failure(origin: str, *, now: float | None = None) -> None:
    observed = time.monotonic() if now is None else now
    with _CIRCUIT_LOCK:
        state = _CIRCUITS.setdefault(origin, _CircuitState())
        state.failures += 1
        if state.failures >= settings.option_pro_focus_circuit_failure_threshold:
            state.opened_at = observed


def _record_circuit_success(origin: str) -> None:
    with _CIRCUIT_LOCK:
        _CIRCUITS.pop(origin, None)


def _reset_focus_circuits() -> None:
    """Reset process state for isolated tests."""

    with _CIRCUIT_LOCK:
        _CIRCUITS.clear()


async def latest_focus_context(db: aiosqlite.Connection) -> dict[str, Any] | None:
    async with db.execute(
        "SELECT * FROM focus_context_snapshots ORDER BY revision DESC LIMIT 1"
    ) as cursor:
        row = await cursor.fetchone()
    if row is None:
        return None
    result = dict(row)
    result["payload"] = json.loads(result.pop("payload_json"))
    return result


async def mark_focus_context_stale(db: aiosqlite.Connection) -> None:
    await db.execute(
        "UPDATE focus_context_snapshots SET status='stale' WHERE status='current'"
    )
    await db.commit()


async def persist_focus_context(
    db: aiosqlite.Connection,
    context: FocusContext,
    *,
    fetched_at: datetime | None = None,
) -> bool:
    payload = context.model_dump(mode="json")
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    now = (fetched_at or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
    inserted = False
    await db.execute("BEGIN IMMEDIATE")
    try:
        async with db.execute(
            "SELECT revision,payload_hash FROM focus_context_snapshots ORDER BY revision DESC LIMIT 1"
        ) as cursor:
            previous = await cursor.fetchone()
        if previous and int(previous[0]) > context.revision:
            await db.commit()
            return False
        if previous and int(previous[0]) == context.revision:
            if not secrets.compare_digest(str(previous[1]), digest):
                raise ValueError("focus_revision_payload_conflict")
            await db.execute(
                "UPDATE focus_context_snapshots SET status='current' WHERE revision=?",
                (context.revision,),
            )
            await db.commit()
        else:
            await db.execute("UPDATE focus_context_snapshots SET status='stale' WHERE status='current'")
            await db.execute(
                """INSERT INTO focus_context_snapshots
                   (revision,schema_version,as_of,data_through,market_session,universe_version,
                    payload_json,payload_hash,status,fetched_at,created_at)
                   VALUES (?,?,?,?,?,?,?,?,'current',?,?)""",
                (
                    context.revision,
                    context.schema_version,
                    context.as_of.astimezone(timezone.utc).isoformat(),
                    context.data_through.astimezone(timezone.utc).isoformat()
                    if context.data_through
                    else None,
                    context.market_session,
                    context.universe_version,
                    encoded,
                    digest,
                    now,
                    now,
                ),
            )
            await db.commit()
            inserted = True
    except Exception:
        await db.rollback()
        raise
    # Event and model-association revalidation may touch many historical rows.
    # It runs only after the small snapshot transaction has released its write
    # lock, and commits bounded batches inside the service.
    try:
        from app.services.market_focus import revalidate_events_for_focus_context

        await revalidate_events_for_focus_context(db, payload)
    except Exception as exc:
        await db.rollback()
        await db.execute(
            "UPDATE focus_context_snapshots SET status='stale' WHERE revision=?",
            (context.revision,),
        )
        await db.commit()
        logger.exception("Focus context persisted but association revalidation failed")
        raise RuntimeError("focus_association_revalidation_failed") from exc
    return inserted


async def pull_focus_context(*, client: httpx.AsyncClient | None = None) -> dict[str, Any]:
    """Pull a bounded read-only snapshot; failures leave the last snapshot readable."""

    if focus_capability() != "enabled":
        return {"status": "not_configured", "updated": False}
    owned = client is None
    origin = _focus_origin()
    db = await get_db()
    try:
        try:
            if not _circuit_permits(origin):
                raise FocusClientError("circuit_open")
            if client is None:
                client = _create_focus_client()
            context = await _fetch_focus_context(client)
        except FocusClientError as exc:
            if exc.category != "circuit_open":
                _record_circuit_failure(origin)
            await mark_focus_context_stale(db)
            logger.error("focus_context_pull_failed category=%s", exc.category)
            raise
        _record_circuit_success(origin)
        try:
            changed = await persist_focus_context(db, context)
        except Exception:
            await mark_focus_context_stale(db)
            raise
        return {"status": "ok", "updated": changed, "revision": context.revision}
    finally:
        await db.close()
        if owned and client is not None:
            await client.aclose()


async def resume_focus_revalidation() -> dict[str, Any]:
    """Advance one local revalidation slice without another remote request."""

    db = await get_db()
    try:
        snapshot = await latest_focus_context(db)
        if snapshot is None:
            return {"status": "idle", "pending": False}
        from app.services.market_focus import (
            TICKER_VALIDATION_RULES_VERSION,
            revalidate_events_for_focus_context,
        )

        regated = await revalidate_events_for_focus_context(db, snapshot["payload"])
        async with db.execute(
            """SELECT pending_run_key,pending_focus_revision,pending_phase,
                      pending_mention_cursor,pending_group_cursor,
                      last_focus_revision,validation_rules_version
               FROM focus_validation_state WHERE singleton_id=1"""
        ) as cursor:
            row = await cursor.fetchone()
        async with db.execute(
            "SELECT COALESCE(MAX(revision),0) FROM focus_context_snapshots"
        ) as cursor:
            max_revision_row = await cursor.fetchone()
        max_revision = int(max_revision_row[0] if max_revision_row else 0)
        backlog = bool(
            row
            and (
                max_revision > int(row[5] or 0)
                or str(row[6] or "") != TICKER_VALIDATION_RULES_VERSION
            )
        )
        pending = bool(row and row[0]) or backlog
        return {
            "status": "pending" if pending else "complete",
            "pending": pending,
            "focus_revision": (
                int(row[1])
                if row and row[1] is not None
                else int(row[5] or 0) + 1 if backlog else None
            ),
            "phase": str(row[2]) if row and row[2] else "queued" if backlog else None,
            "mention_cursor": int(row[3]) if row else 0,
            "group_cursor": str(row[4]) if row else "",
            "event_groups_regated": regated,
        }
    finally:
        await db.close()
