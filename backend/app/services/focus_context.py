from __future__ import annotations

import hashlib
import json
import secrets
import time
from datetime import datetime, timezone
from typing import Any, Literal, Optional, Union

import aiosqlite
import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

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
FOCUS_SCHEMA_VERSION = "option-pro-macrolens-focus-v1"
FOCUS_SCHEMA_SHA256 = "43e3e90b8436cc4dff54262222ec3bf4655c2357273cd01ba2f5a3305a889a19"


class FocusSymbol(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    ticker: str = Field(pattern=r"^[A-Z0-9][A-Z0-9.^/_-]{0,19}$")
    validation_status: Literal["canonical", "valid_external", "unverified"]
    universe_reasons: list[str] = Field(min_length=1, max_length=12)
    dollar_volume_rank: Optional[int] = Field(default=None, ge=1)
    session_change_pct: Optional[float] = None
    rvol_time_of_day: Optional[float] = Field(default=None, ge=0)
    breakout_state: Optional[str] = Field(default=None, max_length=60)
    sector_id: Optional[str] = Field(default=None, max_length=120)
    as_of: datetime
    data_quality: Optional[float] = Field(default=None, ge=0, le=1)
    data_status: Literal["active", "stale"] = "active"


class FocusContext(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["option-pro-macrolens-focus-v1"] = FOCUS_SCHEMA_VERSION
    schema_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    revision: int = Field(ge=1)
    as_of: datetime
    data_through: Optional[datetime] = None
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
                "UPDATE focus_context_snapshots SET status='current',fetched_at=? WHERE revision=?",
                (now, context.revision),
            )
            await db.commit()
            return False
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
        return True
    except Exception:
        await db.rollback()
        raise


async def pull_focus_context(*, client: httpx.AsyncClient | None = None) -> dict[str, Any]:
    """Pull a bounded read-only snapshot; failures leave the last snapshot readable."""

    if focus_capability() != "enabled":
        return {"status": "not_configured", "updated": False}
    owned = client is None
    if client is None:
        verify: Union[bool, str] = settings.option_pro_focus_verify_tls
        if settings.option_pro_focus_ca_bundle:
            verify = settings.option_pro_focus_ca_bundle
        client = httpx.AsyncClient(
            base_url=settings.option_pro_focus_base_url.rstrip("/"),
            timeout=settings.option_pro_focus_timeout_seconds,
            verify=verify,
        )
    db = await get_db()
    try:
        response = await client.get(FOCUS_CONTEXT_PATH, headers=_signed_headers())
        response.raise_for_status()
        context = FocusContext.model_validate_json(response.content)
        changed = await persist_focus_context(db, context)
        return {"status": "ok", "updated": changed, "revision": context.revision}
    except Exception:
        await mark_focus_context_stale(db)
        raise
    finally:
        await db.close()
        if owned:
            await client.aclose()
