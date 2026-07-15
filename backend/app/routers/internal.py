from __future__ import annotations

import base64
import binascii
import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.deps.bearer import require_owner_token
from app.models.database import (
    calendar_watermark,
    get_db,
    get_news_as_of,
    get_source_health,
    news_change_watermark,
    parse_utc,
    query_calendar_page,
    query_news_changes,
    utc_now,
    utc_text,
)
from app.utils.scheduler import get_scheduler

router = APIRouter(prefix="/internal/v1", dependencies=[Depends(require_owner_token)])
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _filter_digest(
    kind: str,
    updated_after: datetime,
    as_of: datetime,
    after_sequence: int | None = None,
) -> str:
    filters: dict[str, Any] = {
        "kind": kind,
        "updated_after": utc_text(updated_after),
        "as_of": utc_text(as_of),
    }
    if after_sequence is not None:
        filters["after_sequence"] = after_sequence
    material = json.dumps(filters, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(material.encode()).hexdigest()


def _encode_cursor(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return base64.urlsafe_b64encode(encoded).rstrip(b"=").decode()


def _decode_cursor(value: str, *, kind: str) -> dict[str, Any]:
    if not value or len(value) > 2_048:
        raise HTTPException(status_code=400, detail={"code": "invalid_cursor"})
    try:
        padding = "=" * (-len(value) % 4)
        decoded = base64.b64decode(value + padding, altchars=b"-_", validate=True)
        payload = json.loads(decoded)
        if not isinstance(payload, dict) or payload.get("v") != 1 or payload.get("kind") != kind:
            raise ValueError("cursor shape")
        parse_utc(payload["updated_after"], field="cursor.updated_after")
        parse_utc(payload["as_of"], field="cursor.as_of")
        checkpoint = payload.get("after_sequence")
        if checkpoint is not None and (
            isinstance(checkpoint, bool) or not isinstance(checkpoint, int) or checkpoint < 0
        ):
            raise ValueError("cursor checkpoint")
    except (
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        binascii.Error,
    ) as exc:
        raise HTTPException(status_code=400, detail={"code": "invalid_cursor"}) from exc
    return payload


def _parse_query_time(value: str | None, *, field: str, default: datetime) -> datetime:
    if value is None:
        return default
    try:
        parsed = parse_utc(value, field=field)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail={"code": f"invalid_{field}"}) from exc
    now = utc_now()
    if parsed - now > timedelta(minutes=5):
        raise HTTPException(status_code=422, detail={"code": f"future_{field}"})
    return parsed


def _frozen_window(
    *,
    kind: str,
    cursor: str | None,
    updated_after: str | None,
    as_of: str | None,
    after_sequence: int | None,
) -> tuple[datetime, datetime, int | None, dict[str, Any] | None]:
    now = utc_now()
    if cursor is None:
        after = _parse_query_time(updated_after, field="updated_after", default=_EPOCH)
        cutoff = _parse_query_time(as_of, field="as_of", default=now)
        if after > cutoff:
            raise HTTPException(status_code=422, detail={"code": "invalid_time_window"})
        return after, cutoff, after_sequence, None

    payload = _decode_cursor(cursor, kind=kind)
    after = parse_utc(payload["updated_after"], field="cursor.updated_after")
    cutoff = parse_utc(payload["as_of"], field="cursor.as_of")
    frozen_sequence = payload.get("after_sequence")
    if after > cutoff or cutoff - utc_now() > timedelta(minutes=5):
        raise HTTPException(status_code=400, detail={"code": "invalid_cursor"})
    if updated_after is not None or as_of is not None or after_sequence is not None:
        raise HTTPException(status_code=400, detail={"code": "cursor_filter_mismatch"})
    if payload.get("filter") != _filter_digest(kind, after, cutoff, frozen_sequence):
        raise HTTPException(status_code=400, detail={"code": "invalid_cursor"})
    return after, cutoff, frozen_sequence, payload


@router.get("/health")
async def internal_health() -> dict[str, Any]:
    db = await get_db()
    try:
        async with db.execute("SELECT 1") as cursor:
            row = await cursor.fetchone()
        if row is None or row[0] != 1:
            raise RuntimeError("database probe failed")
        sources = await get_source_health(db)
        async with db.execute("SELECT COALESCE(MAX(change_sequence),0) FROM news_changes") as cursor:
            news_sequence = int((await cursor.fetchone())[0])
        async with db.execute(
            "SELECT COALESCE(MAX(snapshot_sequence),0) FROM etl_calendar_snapshots"
        ) as cursor:
            calendar_sequence = int((await cursor.fetchone())[0])
    finally:
        await db.close()
    scheduler = get_scheduler()
    data_through = max(
        (str(item["data_through"]) for item in sources if item.get("data_through")),
        default=None,
    )
    return {
        "status": "ok" if scheduler is not None and scheduler.running else "degraded",
        "service": "macrolens-etl",
        "as_of": utc_text(),
        "data_through": data_through,
        "database": "ok",
        "scheduler": "running" if scheduler is not None and scheduler.running else "stopped",
        "watermarks": {"news_sequence": news_sequence, "calendar_sequence": calendar_sequence},
        "sources": sources,
    }


@router.get("/news/changes")
async def news_changes(
    updated_after: str | None = Query(default=None),
    after_sequence: int | None = Query(default=None, ge=0),
    cursor: str | None = Query(default=None, max_length=2_048),
    limit: int = Query(default=50, ge=1, le=50),
    as_of: str | None = Query(default=None),
) -> dict[str, Any]:
    after, cutoff, checkpoint, payload = _frozen_window(
        kind="news_changes",
        cursor=cursor,
        updated_after=updated_after,
        as_of=as_of,
        after_sequence=after_sequence,
    )
    db = await get_db()
    try:
        if payload is None:
            watermark = await news_change_watermark(db, as_of=cutoff)
            if checkpoint is not None and checkpoint > watermark:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={"code": "sequence_checkpoint_ahead"},
                )
            last_sequence = 0
        else:
            try:
                watermark = int(payload["watermark_sequence"])
                last_sequence = int(payload["last_sequence"])
                if (
                    watermark < 0
                    or not 0 <= last_sequence <= watermark
                    or (checkpoint is not None and checkpoint > watermark)
                ):
                    raise ValueError("cursor sequence")
            except (KeyError, TypeError, ValueError) as exc:
                raise HTTPException(status_code=400, detail={"code": "invalid_cursor"}) from exc
        items, has_more = await query_news_changes(
            db,
            updated_after=after,
            as_of=cutoff,
            after_sequence=last_sequence,
            checkpoint_sequence=checkpoint,
            watermark_sequence=watermark,
            limit=limit,
        )
    finally:
        await db.close()

    next_cursor = None
    if has_more and items:
        cursor_payload: dict[str, Any] = {
            "v": 1,
            "kind": "news_changes",
            "updated_after": utc_text(after),
            "as_of": utc_text(cutoff),
            "filter": _filter_digest("news_changes", after, cutoff, checkpoint),
            "watermark_sequence": watermark,
            "last_sequence": items[-1]["sequence"],
        }
        if checkpoint is not None:
            cursor_payload["after_sequence"] = checkpoint
        next_cursor = _encode_cursor(cursor_payload)
    return {
        "items": items,
        "has_more": has_more,
        "next_cursor": next_cursor,
        "watermark": {"sequence": watermark, "as_of": utc_text(cutoff)},
        "next_updated_after": None if has_more else utc_text(cutoff),
        "next_after_sequence": None if has_more else watermark,
    }


@router.get("/news/{news_id}")
async def news_item(
    news_id: int,
    as_of: str | None = Query(default=None),
) -> dict[str, Any]:
    if news_id < 1:
        raise HTTPException(status_code=404, detail={"code": "news_not_found"})
    cutoff = _parse_query_time(as_of, field="as_of", default=utc_now())
    db = await get_db()
    try:
        result = await get_news_as_of(db, news_id, as_of=cutoff)
    finally:
        await db.close()
    if result is None:
        raise HTTPException(status_code=404, detail={"code": "news_not_found"})
    item, sequence, available_at = result
    return {
        "item": item,
        "watermark": {"sequence": sequence, "as_of": utc_text(cutoff)},
        "available_at": available_at,
    }


@router.get("/calendar")
async def calendar(
    updated_after: str | None = Query(default=None),
    after_sequence: int | None = Query(default=None, ge=0),
    cursor: str | None = Query(default=None, max_length=2_048),
    limit: int = Query(default=200, ge=1, le=500),
    as_of: str | None = Query(default=None),
) -> dict[str, Any]:
    after, cutoff, checkpoint, payload = _frozen_window(
        kind="calendar",
        cursor=cursor,
        updated_after=updated_after,
        as_of=as_of,
        after_sequence=after_sequence,
    )
    db = await get_db()
    try:
        if payload is None:
            snapshot = await calendar_watermark(db, updated_after=after, as_of=cutoff)
            sequence = int(snapshot["snapshot_sequence"]) if snapshot else 0
            if checkpoint is not None and checkpoint > sequence:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={"code": "sequence_checkpoint_ahead"},
                )
            token = str(snapshot["snapshot_token"]) if snapshot else None
            after_ordinal = 0
            include_items = bool(
                snapshot
                and (
                    sequence > checkpoint
                    if checkpoint is not None
                    else snapshot["has_changes"]
                )
            )
        else:
            try:
                sequence = int(payload["snapshot_sequence"])
                after_ordinal = int(payload["last_ordinal"])
                token = payload.get("snapshot_token")
                if (
                    sequence < 0
                    or after_ordinal < 0
                    or (checkpoint is not None and checkpoint > sequence)
                    or (sequence > 0 and (
                        not isinstance(token, str)
                        or not token.startswith("cal_")
                        or len(token) != 44
                    ))
                ):
                    raise ValueError("cursor sequence")
            except (KeyError, TypeError, ValueError) as exc:
                raise HTTPException(status_code=400, detail={"code": "invalid_cursor"}) from exc
            snapshot = None
            include_items = bool(sequence)
            if sequence:
                async with db.execute(
                    "SELECT * FROM etl_calendar_snapshots WHERE snapshot_sequence=? AND snapshot_token=?",
                    (sequence, token),
                ) as query:
                    row = await query.fetchone()
                if row is None:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={"code": "calendar_snapshot_expired"},
                    )
                snapshot = dict(row)
        items, has_more = (
            await query_calendar_page(
                db, snapshot_sequence=sequence, after_ordinal=after_ordinal, limit=limit
            )
            if sequence and include_items
            else ([], False)
        )
    finally:
        await db.close()

    next_cursor = None
    if has_more and items:
        cursor_payload = {
            "v": 1,
            "kind": "calendar",
            "updated_after": utc_text(after),
            "as_of": utc_text(cutoff),
            "filter": _filter_digest("calendar", after, cutoff, checkpoint),
            "snapshot_sequence": sequence,
            "snapshot_token": token,
            "last_ordinal": items[-1]["ordinal"],
        }
        if checkpoint is not None:
            cursor_payload["after_sequence"] = checkpoint
        next_cursor = _encode_cursor(cursor_payload)
    return {
        "items": items,
        "has_more": has_more,
        "next_cursor": next_cursor,
        "watermark": {
            "sequence": sequence,
            "snapshot_token": token,
            "as_of": utc_text(cutoff),
        },
        "data_through": snapshot.get("data_through") if snapshot else None,
        "is_stale": bool(snapshot.get("is_stale")) if snapshot else False,
        "next_updated_after": None if has_more else utc_text(cutoff),
        "next_after_sequence": None if has_more else sequence,
    }
