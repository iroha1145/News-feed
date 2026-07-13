from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import secrets
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable
from urllib.parse import urlparse

import aiosqlite

from app.config import settings
from app.integrations.option_pro.auth import IntegrationAPIError
from app.models.catalysts import (
    AnalysisStatus,
    CatalystItem,
    CalendarEvent,
    NewsImpactAnalysis,
    PublicAnalysis,
)
from app.services.analysis_jobs import parse_utc, utc_now, utc_text

logger = logging.getLogger(__name__)


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except (TypeError, json.JSONDecodeError):
            return []
    return []


def _safe_url(value: str) -> str:
    parsed = urlparse(str(value or ""))
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        raise ValueError("unsupported news URL")
    return str(value)[:4096]


def _cursor_secrets() -> tuple[str, ...]:
    values = (
        settings.option_pro_read_secret,
        settings.option_pro_action_secret,
        settings.option_pro_previous_read_secret,
        settings.option_pro_previous_action_secret,
    )
    unique = []
    for value in values:
        if value and value not in unique:
            unique.append(value)
    if not unique:
        raise IntegrationAPIError(503, "integration_not_configured", "Integration credentials are not configured.")
    return tuple(unique)


def encode_cursor(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    encoded = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
    signature = hmac.new(_cursor_secrets()[0].encode(), encoded.encode(), hashlib.sha256).hexdigest()
    return f"{encoded}.{signature}"


def _decode_cursor_payload(cursor: str, *, kind: str) -> dict[str, Any]:
    try:
        encoded, signature = cursor.split(".", 1)
        if len(encoded) > 4096 or len(signature) != 64:
            raise ValueError
        if not any(
            secrets.compare_digest(
                hmac.new(secret.encode(), encoded.encode(), hashlib.sha256).hexdigest(),
                signature,
            )
            for secret in _cursor_secrets()
        ):
            raise ValueError
        raw = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        payload = json.loads(raw)
        if not isinstance(payload, dict) or payload.get("v") != 1:
            raise ValueError
        if payload.get("kind") != kind:
            raise ValueError
        return payload
    except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise IntegrationAPIError(400, "invalid_cursor", "The pagination cursor is invalid.") from exc


def decode_cursor(cursor: str, *, kind: str, filter_digest: str) -> dict[str, Any]:
    payload = _decode_cursor_payload(cursor, kind=kind)
    if payload.get("filter") != filter_digest:
        raise IntegrationAPIError(400, "invalid_cursor", "The pagination cursor is invalid.")
    return payload


def filter_digest(kind: str, values: dict[str, Any]) -> str:
    normalized = {key: (value.isoformat() if isinstance(value, (date, datetime)) else value) for key, value in values.items()}
    raw = json.dumps({"kind": kind, **normalized}, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(raw).hexdigest()


VISIBLE_CTE = """
WITH visible_revisions AS (
  SELECT r.*, ROW_NUMBER() OVER (
    PARTITION BY r.news_id ORDER BY datetime(r.available_at) DESC, r.revision DESC
  ) AS rn
  FROM analysis_revisions r
  WHERE datetime(r.available_at) <= datetime(:as_of)
),
visible_jobs AS (
  SELECT j.*, ROW_NUMBER() OVER (
    PARTITION BY j.news_id ORDER BY datetime(j.updated_at) DESC, j.job_id DESC
  ) AS rn
  FROM analysis_jobs j
  WHERE datetime(j.created_at) <= datetime(:as_of)
    AND datetime(j.updated_at) <= datetime(:as_of)
)
"""

ITEM_SELECT = """
SELECT n.id AS news_id, n.content_hash, n.source, n.title, n.summary, n.url,
       n.published_at, n.fetched_at, n.updated_at AS news_updated_at, n.source_tickers,
       r.id AS revision_id, r.revision, r.payload_json, r.model AS revision_model,
       r.reasoning_effort AS revision_reasoning, r.prompt_version AS revision_prompt,
       r.schema_version AS revision_schema, r.analyzed_at, r.available_at,
       COALESCE((
         SELECT json_group_array(json_object(
           'ticker',latest.ticker,
           'validation_status',latest.validation_status,
           'validated_at',latest.validated_at,
           'focus_revision',latest.focus_revision,
           'universe_version',latest.universe_version,
           'association_method',latest.association_method
         ))
         FROM (
           SELECT m.ticker,m.validation_status,m.validated_at,m.focus_revision,
                  m.universe_version,m.association_method
           FROM news_ticker_mentions m
           WHERE m.news_id=n.id AND m.association_method='llm_inference'
             AND m.id=(
               SELECT MAX(newest.id) FROM news_ticker_mentions newest
               WHERE newest.news_id=m.news_id AND newest.ticker=m.ticker
                 AND newest.association_method='llm_inference'
             )
           ORDER BY m.ticker
         ) latest
       ),'[]') AS stock_validations_json,
       j.status AS job_status,
       sh.status AS source_health_status,
       sh.last_success_at AS source_last_success_at,
       :as_of AS query_as_of,
       COALESCE((
         SELECT MAX(c.change_sequence) FROM integration_changes c
         WHERE c.entity_id=CAST(n.id AS TEXT)
           AND c.entity_type IN ('news','analysis')
           AND datetime(c.updated_at) <= datetime(:as_of)
       ), n.id) AS change_sequence,
       COALESCE((
         SELECT c.updated_at FROM integration_changes c
         WHERE c.entity_id=CAST(n.id AS TEXT)
           AND c.entity_type IN ('news','analysis')
           AND datetime(c.updated_at) <= datetime(:as_of)
         ORDER BY c.change_sequence DESC LIMIT 1
       ), n.updated_at, n.fetched_at) AS effective_updated_at
FROM news_items n
LEFT JOIN visible_revisions r ON r.news_id=n.id AND r.rn=1
LEFT JOIN visible_jobs j ON j.news_id=n.id AND j.rn=1
LEFT JOIN source_health sh ON sh.source=CASE
  WHEN n.source LIKE 'seekingalpha/breaking%' THEN 'seekingalpha_breaking'
  WHEN n.source LIKE 'seekingalpha/daily%' THEN 'seekingalpha_daily'
  WHEN instr(n.source,'/')>0 THEN substr(n.source,1,instr(n.source,'/')-1)
  ELSE n.source
END
"""


def _item_from_row(row: aiosqlite.Row | dict[str, Any]) -> CatalystItem | None:
    record = dict(row)
    try:
        url = _safe_url(record["url"])
    except ValueError:
        logger.warning("Omitting news_id=%s with an unsupported URL scheme", record.get("news_id"))
        return None

    public_analysis = None
    analysis_status = AnalysisStatus.not_requested
    if record.get("revision_id"):
        try:
            payload = NewsImpactAnalysis.model_validate_json(record["payload_json"])
        except Exception:
            logger.warning("Omitting invalid analysis revision_id=%s from integration output", record.get("revision_id"))
            payload = None
        if payload is not None:
            public_analysis = PublicAnalysis(
                **payload.model_dump(),
                analysis_id=record["revision_id"],
                revision=record["revision"],
                model=record["revision_model"],
                reasoning=record["revision_reasoning"],
                prompt_version=record["revision_prompt"],
                schema_version=record["revision_schema"],
                analyzed_at=record["analyzed_at"],
                available_at=record["available_at"],
                stock_validations=_json_list(record.get("stock_validations_json")),
            )
            analysis_status = (
                AnalysisStatus.insufficient_context
                if payload.insufficient_context else AnalysisStatus.completed
            )
    elif record.get("job_status") in AnalysisStatus._value2member_map_:
        analysis_status = AnalysisStatus(record["job_status"])

    tickers: list[str] = []
    for value in _json_list(record.get("source_tickers")):
        ticker = str(value or "").strip().upper().lstrip("$")[:20]
        if ticker and ticker not in tickers:
            tickers.append(ticker)

    source_stale = record.get("source_health_status") in {"degraded", "unavailable"}
    if record.get("source_last_success_at") and record.get("query_as_of"):
        source_stale = source_stale or (
            parse_utc(record["source_last_success_at"])
            < parse_utc(record["query_as_of"])
            - timedelta(seconds=settings.option_pro_source_stale_after_seconds)
        )

    return CatalystItem(
        news_id=record["news_id"],
        content_hash=record["content_hash"],
        source=str(record["source"])[:500],
        title=str(record["title"])[:2000],
        summary=str(record["summary"])[:20_000] if record.get("summary") is not None else None,
        url=url,
        published_at=record.get("published_at"),
        fetched_at=record["fetched_at"],
        updated_at=record.get("effective_updated_at") or record.get("news_updated_at") or record["fetched_at"],
        change_sequence=max(1, int(record.get("change_sequence") or record["news_id"])),
        source_tickers=tickers,
        analysis_status=analysis_status,
        analysis=public_analysis,
        analyzed_at=record.get("analyzed_at") if public_analysis else None,
        available_at=record.get("available_at") if public_analysis else None,
        is_stale=source_stale,
    )


async def query_feed(
    db: aiosqlite.Connection,
    *,
    as_of: datetime,
    window_hours: int,
    limit: int,
    cursor: str | None,
    source: str | None,
    classification: str | None,
    min_confidence: int,
    min_abs_impact: int,
    analysis_status: str | None,
    include_unanalyzed: bool = True,
) -> tuple[list[CatalystItem], str | None, bool, datetime | None]:
    filters = {
        "as_of": as_of,
        "window_hours": window_hours,
        "source": source,
        "classification": classification,
        "min_confidence": min_confidence,
        "min_abs_impact": min_abs_impact,
        "analysis_status": analysis_status,
        "include_unanalyzed": include_unanalyzed,
    }
    digest = filter_digest("feed", filters)
    cursor_payload = decode_cursor(cursor, kind="feed", filter_digest=digest) if cursor else None
    conditions = [
        "datetime(n.fetched_at) <= datetime(:as_of)",
        "(n.published_at IS NULL OR datetime(n.published_at) <= datetime(:as_of))",
        "datetime(COALESCE(n.published_at,n.fetched_at)) >= datetime(:window_start)",
    ]
    params: dict[str, Any] = {
        "as_of": utc_text(as_of),
        "window_start": utc_text(as_of - timedelta(hours=window_hours)),
        "limit": limit + 1,
    }
    if source:
        conditions.append("n.source=:source")
        params["source"] = source
    if classification:
        conditions.append("r.id IS NOT NULL AND json_extract(r.payload_json,'$.classification')=:classification")
        params["classification"] = classification
    if min_confidence:
        conditions.append("r.id IS NOT NULL AND CAST(json_extract(r.payload_json,'$.confidence') AS INTEGER)>=:min_confidence")
        params["min_confidence"] = min_confidence
    elif not include_unanalyzed:
        conditions.append("r.id IS NOT NULL")
    if min_abs_impact:
        conditions.append(
            "r.id IS NOT NULL AND EXISTS (SELECT 1 FROM analysis_stock_impacts si "
            "WHERE si.analysis_id=r.id AND si.validation_status IN ('canonical','valid_external') "
            "AND ABS(si.impact_score)>=:min_abs_impact)"
        )
        params["min_abs_impact"] = min_abs_impact
    if analysis_status:
        if analysis_status == "completed":
            conditions.append("r.id IS NOT NULL AND json_extract(r.payload_json,'$.insufficient_context')=0")
        elif analysis_status == "insufficient_context":
            conditions.append("r.id IS NOT NULL AND json_extract(r.payload_json,'$.insufficient_context')=1")
        elif analysis_status == "not_requested":
            conditions.append("r.id IS NULL AND j.status IS NULL")
        else:
            conditions.append("r.id IS NULL AND j.status=:analysis_status")
            params["analysis_status"] = analysis_status
    if cursor_payload:
        conditions.append(
            "(datetime(COALESCE(n.published_at,n.fetched_at)) < datetime(:cursor_time) "
            "OR (datetime(COALESCE(n.published_at,n.fetched_at)) = datetime(:cursor_time) AND n.id < :cursor_id))"
        )
        params["cursor_time"] = cursor_payload["time"]
        params["cursor_id"] = int(cursor_payload["id"])

    sql = (
        VISIBLE_CTE + ITEM_SELECT + " WHERE " + " AND ".join(conditions)
        + " ORDER BY datetime(COALESCE(n.published_at,n.fetched_at)) DESC, n.id DESC LIMIT :limit"
    )
    async with db.execute(sql, params) as query:
        rows = await query.fetchall()
    has_more = len(rows) > limit
    page_rows = rows[:limit]
    items = [item for row in page_rows if (item := _item_from_row(row)) is not None]
    next_cursor = None
    if has_more and page_rows:
        last = dict(page_rows[-1])
        next_cursor = encode_cursor(
            {
                "v": 1,
                "kind": "feed",
                "filter": digest,
                "time": last.get("published_at") or last["fetched_at"],
                "id": last["news_id"],
            }
        )
    data_through = max((item.updated_at for item in items), default=None)
    return items, next_cursor, has_more, data_through


async def get_news_item(db: aiosqlite.Connection, news_id: int, as_of: datetime) -> CatalystItem | None:
    sql = (
        VISIBLE_CTE + ITEM_SELECT
        + " WHERE n.id=:news_id AND datetime(n.fetched_at)<=datetime(:as_of)"
        + " AND (n.published_at IS NULL OR datetime(n.published_at)<=datetime(:as_of))"
    )
    async with db.execute(sql, {"news_id": news_id, "as_of": utc_text(as_of)}) as cursor:
        row = await cursor.fetchone()
    return _item_from_row(row) if row else None


async def query_ticker(
    db: aiosqlite.Connection,
    *,
    ticker: str,
    as_of: datetime,
    window_hours: int,
    limit: int,
    cursor: str | None,
    min_confidence: int,
    include_neutral: bool,
    include_unanalyzed: bool = True,
) -> tuple[list[CatalystItem], str | None, bool, datetime | None]:
    filters = {
        "ticker": ticker,
        "as_of": as_of,
        "window_hours": window_hours,
        "min_confidence": min_confidence,
        "include_neutral": include_neutral,
        "include_unanalyzed": include_unanalyzed,
    }
    digest = filter_digest("ticker", filters)
    cursor_payload = decode_cursor(cursor, kind="ticker", filter_digest=digest) if cursor else None
    conditions = [
        "datetime(n.fetched_at)<=datetime(:as_of)",
        "(n.published_at IS NULL OR datetime(n.published_at)<=datetime(:as_of))",
        "datetime(COALESCE(n.published_at,n.fetched_at))>=datetime(:window_start)",
        "(EXISTS (SELECT 1 FROM news_ticker_mentions tm WHERE tm.news_id=n.id "
        "AND tm.ticker=:ticker AND tm.validation_status IN ('canonical','valid_external')) "
        "OR EXISTS (SELECT 1 FROM analysis_stock_impacts si WHERE si.analysis_id=r.id "
        "AND si.ticker=:ticker AND si.validation_status IN ('canonical','valid_external')))",
    ]
    params: dict[str, Any] = {
        "as_of": utc_text(as_of),
        "window_start": utc_text(as_of - timedelta(hours=window_hours)),
        "ticker": ticker,
        "limit": limit + 1,
    }
    if min_confidence:
        conditions.append(
            "(r.id IS NOT NULL AND EXISTS (SELECT 1 FROM analysis_stock_impacts si "
            "WHERE si.analysis_id=r.id AND si.ticker=:ticker "
            "AND si.validation_status IN ('canonical','valid_external') "
            "AND si.confidence>=:min_confidence))"
        )
        params["min_confidence"] = min_confidence
    elif not include_unanalyzed:
        conditions.append("r.id IS NOT NULL")
    if not include_neutral:
        conditions.append("(r.id IS NULL OR json_extract(r.payload_json,'$.classification')!='neutral')")
    if cursor_payload:
        conditions.append(
            "(datetime(COALESCE(n.published_at,n.fetched_at))<datetime(:cursor_time) "
            "OR (datetime(COALESCE(n.published_at,n.fetched_at))=datetime(:cursor_time) AND n.id<:cursor_id))"
        )
        params["cursor_time"] = cursor_payload["time"]
        params["cursor_id"] = int(cursor_payload["id"])
    sql = (
        VISIBLE_CTE + ITEM_SELECT + " WHERE " + " AND ".join(conditions)
        + " ORDER BY datetime(COALESCE(n.published_at,n.fetched_at)) DESC,n.id DESC LIMIT :limit"
    )
    async with db.execute(sql, params) as query:
        rows = await query.fetchall()
    has_more = len(rows) > limit
    page_rows = rows[:limit]
    items = [item for row in page_rows if (item := _item_from_row(row)) is not None]
    next_cursor = None
    if has_more and page_rows:
        last = dict(page_rows[-1])
        next_cursor = encode_cursor(
            {"v": 1, "kind": "ticker", "filter": digest,
             "time": last.get("published_at") or last["fetched_at"], "id": last["news_id"]}
        )
    data_through = max((item.updated_at for item in items), default=None)
    return items, next_cursor, has_more, data_through


async def query_latest(
    db: aiosqlite.Connection,
    *,
    updated_after: datetime | None,
    limit: int,
    cursor: str | None,
) -> tuple[str, list[CatalystItem], datetime | None, str | None, bool, datetime | None]:
    as_of = utc_now()
    cursor_payload = _decode_cursor_payload(cursor, kind="latest") if cursor else None
    if updated_after is None:
        if cursor_payload is None:
            updated_after = as_of - timedelta(days=1)
        else:
            # Freeze the implicit first-page boundary for the whole snapshot.
            try:
                updated_after = parse_utc(cursor_payload["updated_after"])
            except (KeyError, TypeError, ValueError) as exc:
                raise IntegrationAPIError(
                    400, "invalid_cursor", "The pagination cursor is invalid."
                ) from exc
    if as_of - updated_after > timedelta(days=7, seconds=5):
        raise IntegrationAPIError(
            400,
            "updated_after_too_old",
            "updated_after may cover at most seven days.",
            retryable=True,
            resync_from=as_of - timedelta(days=7),
            server_time=as_of,
            latest_window_days=7,
        )
    digest = filter_digest("latest", {"updated_after": updated_after})
    if cursor_payload is not None and cursor_payload.get("filter") != digest:
        raise IntegrationAPIError(400, "invalid_cursor", "The pagination cursor is invalid.")
    if cursor_payload:
        as_of = parse_utc(cursor_payload["snapshot_as_of"])
        snapshot_max = int(cursor_payload["snapshot_max"])
        last_sequence = int(cursor_payload["last_sequence"])
        snapshot_token = str(cursor_payload["snapshot_token"])
    else:
        async with db.execute(
            """SELECT COALESCE(MAX(change_sequence),0) FROM integration_changes
               WHERE entity_type IN ('news','analysis') AND datetime(updated_at)<=datetime(?)""",
            (utc_text(as_of),),
        ) as query:
            snapshot_max = int((await query.fetchone())[0])
        last_sequence = 0
        snapshot_token = "chg_" + hashlib.sha256(f"{snapshot_max}:{utc_text(as_of)}".encode()).hexdigest()[:32]

    async with db.execute(
        """SELECT CAST(entity_id AS INTEGER) AS news_id, MAX(change_sequence) AS sequence,
                  MAX(updated_at) AS changed_at
           FROM integration_changes
           WHERE entity_type IN ('news','analysis')
             AND julianday(updated_at)>julianday(?)
             AND change_sequence>? AND change_sequence<=?
           GROUP BY entity_id
           ORDER BY sequence LIMIT ?""",
        (utc_text(updated_after), last_sequence, snapshot_max, limit + 1),
    ) as query:
        changes = [dict(row) for row in await query.fetchall()]
    has_more = len(changes) > limit
    page = changes[:limit]
    items: list[CatalystItem] = []
    for change in page:
        item = await get_news_item(db, int(change["news_id"]), as_of)
        if item is not None:
            # The page's frozen sequence is authoritative for this snapshot.
            item.change_sequence = int(change["sequence"])
            item.updated_at = parse_utc(change["changed_at"])
            items.append(item)
    next_cursor = None
    if has_more and page:
        next_cursor = encode_cursor(
            {
                "v": 1,
                "kind": "latest",
                "filter": digest,
                "snapshot_max": snapshot_max,
                "snapshot_token": snapshot_token,
                "snapshot_as_of": utc_text(as_of),
                "updated_after": utc_text(updated_after),
                "last_sequence": int(page[-1]["sequence"]),
            }
        )
    next_updated_after = (
        max(parse_utc(change["changed_at"]) for change in page)
        if has_more and page
        else as_of
    )
    data_through = max((item.updated_at for item in items), default=None)
    return snapshot_token, items, next_updated_after, next_cursor, has_more, data_through


def _calendar_event_id(event: dict[str, Any]) -> str:
    identity = "\n".join(
        (
            str(event.get("date") or event.get("scheduled_at") or ""),
            str(event.get("country_code") or event.get("currency") or "").upper(),
            str(event.get("title") or "").strip(),
        )
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


async def record_calendar_snapshot(
    db: aiosqlite.Connection,
    events: list[dict[str, Any]],
    *,
    source_fetched_at: str,
    stale: bool,
    source: str = "faireconomy",
    observed_at: str | None = None,
) -> str:
    fetched = parse_utc(source_fetched_at)
    observed = max(fetched, parse_utc(observed_at) if observed_at else fetched)
    canonical = json.dumps(events, ensure_ascii=False, separators=(",", ":"), sort_keys=True, default=str)
    token = "cal_" + hashlib.sha256(
        f"{utc_text(observed)}:{int(stale)}:{source_fetched_at}:{canonical}".encode()
    ).hexdigest()
    await db.execute("BEGIN IMMEDIATE")
    try:
        cursor = await db.execute(
            """INSERT OR IGNORE INTO calendar_snapshots
               (snapshot_token,source,source_fetched_at,data_through,is_stale,created_at)
               VALUES (?,?,?,?,?,?)""",
            (token, source, utc_text(fetched), utc_text(fetched), 1 if stale else 0, utc_text(observed)),
        )
        if cursor.rowcount == 0:
            await db.commit()
            return token
        snapshot_id = int(cursor.lastrowid)
        for event in events:
            event_id = _calendar_event_id(event)
            currency = str(event.get("country_code") or event.get("currency") or "").upper()
            title = str(event.get("title") or "").strip()
            scheduled = event.get("date") or event.get("scheduled_at")
            impact = str(event.get("impact") or "low").lower()
            if len(currency) != 3 or not title or not scheduled or impact not in {"low", "medium", "high", "holiday"}:
                continue
            scheduled_at = parse_utc(str(scheduled))
            version_payload = {
                "event_id": event_id,
                "currency": currency,
                "title": title,
                "impact": impact,
                "scheduled_at": utc_text(scheduled_at),
                "forecast": str(event.get("forecast") or "") or None,
                "previous": str(event.get("previous") or "") or None,
                "actual": str(event.get("actual") or "") or None,
                # Freshness is part of the point-in-time state.  The same
                # economic values can become stale after an upstream failure
                # and fresh again after recovery without overwriting history.
                "is_stale": bool(stale),
                "source_fetched_at": utc_text(fetched),
            }
            content_hash = hashlib.sha256(
                json.dumps(version_payload, separators=(",", ":"), sort_keys=True).encode()
            ).hexdigest()
            async with db.execute(
                "SELECT COALESCE(MAX(revision),0)+1 FROM calendar_event_revisions WHERE event_id=?",
                (event_id,),
            ) as query:
                revision = int((await query.fetchone())[0])
            await db.execute(
                """INSERT OR IGNORE INTO calendar_event_revisions
                   (snapshot_id,event_id,revision,currency,title,impact,scheduled_at,forecast,
                    previous,actual,content_hash,is_stale,source_fetched_at,available_at,created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    snapshot_id, event_id, revision, currency, title, impact, utc_text(scheduled_at),
                    version_payload["forecast"], version_payload["previous"], version_payload["actual"],
                    content_hash, 1 if stale else 0, utc_text(fetched),
                    utc_text(observed), utc_text(observed),
                ),
            )
        await db.commit()
        return token
    except Exception:
        await db.rollback()
        raise


async def query_calendar(
    db: aiosqlite.Connection,
    *,
    date_from: date,
    date_to: date,
    as_of: datetime,
    currencies: Iterable[str],
    min_impact: str,
) -> tuple[list[CalendarEvent], datetime | None]:
    ranks = {"low": 1, "medium": 2, "high": 3, "holiday": 0}
    currency_values = sorted(set(currencies))
    placeholders = ",".join("?" for _ in currency_values)
    currency_clause = f"AND currency IN ({placeholders})" if currency_values else ""
    params: list[Any] = [utc_text(as_of), date_from.isoformat(), date_to.isoformat(), *currency_values]
    sql = f"""
      WITH visible AS (
        SELECT e.*, ROW_NUMBER() OVER (
          PARTITION BY event_id ORDER BY datetime(available_at) DESC, revision DESC
        ) AS rn
        FROM calendar_event_revisions e
        WHERE datetime(available_at)<=datetime(?)
      )
      SELECT * FROM visible
      WHERE rn=1 AND date(scheduled_at)>=date(?) AND date(scheduled_at)<=date(?)
      {currency_clause}
      ORDER BY datetime(scheduled_at), event_id
    """
    async with db.execute(sql, params) as cursor:
        rows = await cursor.fetchall()
    minimum = ranks[min_impact]
    items = [
        CalendarEvent(
            event_id=row["event_id"], currency=row["currency"], title=row["title"],
            impact=row["impact"], scheduled_at=row["scheduled_at"], forecast=row["forecast"],
            previous=row["previous"], actual=row["actual"], is_stale=bool(row["is_stale"]),
            source_fetched_at=row["source_fetched_at"], available_at=row["available_at"],
        )
        for row in rows if ranks.get(row["impact"], 0) >= minimum
    ]
    data_through = max((item.source_fetched_at for item in items), default=None)
    return items, data_through


async def upsert_source_health(
    db: aiosqlite.Connection,
    *,
    source: str,
    status: str,
    last_attempt_at: str | None,
    last_success_at: str | None,
    data_through: str | None,
    consecutive_failures: int,
    next_attempt_at: str | None,
    raw_count: int | None,
    inserted_count: int | None,
    duplicates_count: int | None,
    error_code: str | None,
    source_fetch_status: str | None = None,
    news_persistence_status: str | None = None,
    event_projection_status: str | None = None,
) -> None:
    source_fetch_status = source_fetch_status or status
    news_persistence_status = news_persistence_status or status
    event_projection_status = event_projection_status or status
    await db.execute(
        """INSERT INTO source_health
           (source,status,last_attempt_at,last_success_at,data_through,consecutive_failures,
            next_attempt_at,raw_count,inserted_count,duplicates_count,error_code,
            source_fetch_status,news_persistence_status,event_projection_status,updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(source) DO UPDATE SET status=excluded.status,
             last_attempt_at=excluded.last_attempt_at,last_success_at=excluded.last_success_at,
             data_through=excluded.data_through,consecutive_failures=excluded.consecutive_failures,
             next_attempt_at=excluded.next_attempt_at,raw_count=excluded.raw_count,
             inserted_count=excluded.inserted_count,duplicates_count=excluded.duplicates_count,
             error_code=excluded.error_code,
             source_fetch_status=excluded.source_fetch_status,
             news_persistence_status=excluded.news_persistence_status,
             event_projection_status=excluded.event_projection_status,
             updated_at=excluded.updated_at""",
        (
            source, status, last_attempt_at, last_success_at, data_through,
            max(0, consecutive_failures), next_attempt_at,
            max(0, raw_count) if raw_count is not None else None,
            max(0, inserted_count) if inserted_count is not None else None,
            max(0, duplicates_count) if duplicates_count is not None else None,
            error_code[:100] if error_code else None,
            source_fetch_status,
            news_persistence_status,
            event_projection_status,
            utc_text(),
        ),
    )
    await db.commit()


async def catalyst_result_status(db: aiosqlite.Connection, items: list[CatalystItem]) -> str:
    if items:
        return "stale" if any(item.is_stale for item in items) else "active"
    async with db.execute("SELECT COUNT(*) FROM news_items") as cursor:
        news_count = int((await cursor.fetchone())[0])
    async with db.execute("SELECT COUNT(*) FROM source_health WHERE last_success_at IS NOT NULL") as cursor:
        successful_sources = int((await cursor.fetchone())[0])
    return "empty" if news_count or successful_sources else "unavailable"
