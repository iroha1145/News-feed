from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Iterable

import aiosqlite


VALIDATION_STATUSES = frozenset(
    {"canonical", "valid_external", "ambiguous", "invalid", "unverified"}
)
TRUSTED_VALIDATION_STATUSES = frozenset({"canonical", "valid_external"})
NON_LLM_ASSOCIATION_METHODS = frozenset(
    {"provider_tag", "company_endpoint", "exact_alias", "event_propagation"}
)
VALIDATION_RULES_VERSION = "ticker-validation-v1"


def utc_text(value: datetime | str | None = None) -> str:
    if value is None:
        parsed = datetime.now(timezone.utc)
    elif isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("validation_available_at_timezone_required")
    # A fixed-width UTC representation is intentional. SQLite's datetime()
    # truncates fractional seconds, while these values participate in strict
    # point-in-time ordering and visibility checks.
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")


def build_validation_basis_hash(
    *,
    canonical_symbols: Iterable[str],
    external_symbols: Iterable[str],
    universe_version: str | None,
    rules_version: str = VALIDATION_RULES_VERSION,
) -> str:
    payload = {
        "canonical_symbols": sorted(set(canonical_symbols)),
        "external_symbols": sorted(set(external_symbols)),
        "rules_version": rules_version,
        "universe_version": universe_version or "",
    }
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(raw).hexdigest()


def legacy_validation_basis_hash(
    *,
    mention_id: int,
    validation_status: str,
    focus_revision: int | None,
    universe_version: str | None,
) -> str:
    raw = json.dumps(
        {
            "focus_revision": focus_revision,
            "mention_id": mention_id,
            "status": validation_status,
            "universe_version": universe_version or "",
            "version": "legacy-backfill-v4",
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(raw).hexdigest()


async def _refresh_current_cache(
    db: aiosqlite.Connection,
    mention_id: int,
    *,
    checked_at: str,
) -> dict[str, Any]:
    async with db.execute(
        """SELECT id,validation_status,available_at,focus_revision,universe_version
           FROM ticker_validation_revisions
           WHERE mention_id=?
           ORDER BY available_at DESC,id DESC LIMIT 1""",
        (mention_id,),
    ) as cursor:
        latest = await cursor.fetchone()
    if latest is None:
        status = "unverified"
        revision_id = None
        validated_at = None
        focus_revision = None
        universe_version = None
    else:
        status = str(latest[1])
        revision_id = int(latest[0])
        validated_at = str(latest[2])
        focus_revision = latest[3]
        universe_version = latest[4]
    await db.execute(
        """UPDATE news_ticker_mentions SET
             current_validation_status=?,validation_status=?,
             current_validation_revision_id=?,validated_at=COALESCE(validated_at,?),
             focus_revision=?,universe_version=?,last_checked_at=CASE
               WHEN last_checked_at IS NULL
                 OR REPLACE(last_checked_at,'Z','+00:00')<? THEN ?
               ELSE last_checked_at END
           WHERE id=?""",
        (
            status,
            status,
            revision_id,
            validated_at,
            focus_revision,
            universe_version,
            checked_at,
            checked_at,
            mention_id,
        ),
    )
    return {
        "validation_status": status,
        "validated_at": validated_at,
        "focus_revision": focus_revision,
        "universe_version": universe_version,
        "validation_revision_id": revision_id,
    }


async def append_validation_revision(
    db: aiosqlite.Connection,
    *,
    mention_id: int,
    validation_status: str,
    available_at: datetime | str,
    focus_revision: int | None,
    universe_version: str | None,
    reason_code: str,
    validation_basis_hash: str,
    legacy_backfill: bool = False,
    observed_at: datetime | str | None = None,
) -> tuple[dict[str, Any], bool]:
    if validation_status not in VALIDATION_STATUSES:
        raise ValueError("unsupported_ticker_validation_status")
    if len(validation_basis_hash) != 64:
        raise ValueError("validation_basis_hash_required")
    effective_at = utc_text(available_at)
    # available_at answers "when did this state become effective?" while
    # observed_at/created_at answers "when did this database learn it?".  A
    # delayed historical repair must therefore remain visible to incremental
    # consumers whose watermark already passed the effective timestamp.
    observed_text = utc_text(observed_at)
    async with db.execute(
        """SELECT validation_status FROM ticker_validation_revisions
           WHERE mention_id=? AND validation_basis_hash=?
           ORDER BY id LIMIT 1""",
        (mention_id, validation_basis_hash),
    ) as cursor:
        prior_basis = await cursor.fetchone()
    if prior_basis is not None and str(prior_basis[0]) != validation_status:
        raise ValueError("validation_basis_status_conflict")
    async with db.execute(
        """SELECT validation_status,validation_basis_hash
           FROM ticker_validation_revisions
           WHERE mention_id=? AND available_at<=?
           ORDER BY available_at DESC,id DESC LIMIT 1""",
        (mention_id, effective_at),
    ) as cursor:
        point_in_time_prior = await cursor.fetchone()
    if point_in_time_prior is not None and str(point_in_time_prior[0]) == validation_status:
        await db.execute(
            """UPDATE news_ticker_mentions SET last_checked_at=CASE
                 WHEN last_checked_at IS NULL
                   OR REPLACE(last_checked_at,'Z','+00:00')<? THEN ?
                 ELSE last_checked_at END WHERE id=?""",
            (observed_text, observed_text, mention_id),
        )
        state = await _refresh_current_cache(
            db,
            mention_id,
            checked_at=observed_text,
        )
        return state, False
    if (
        point_in_time_prior is not None
        and str(point_in_time_prior[1]) == validation_basis_hash
    ):
        raise ValueError("validation_basis_status_conflict")

    cursor = await db.execute(
        """INSERT OR IGNORE INTO ticker_validation_revisions
           (mention_id,validation_status,available_at,focus_revision,universe_version,
            reason_code,created_at,legacy_backfill,validation_basis_hash)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            mention_id,
            validation_status,
            effective_at,
            focus_revision,
            universe_version,
            reason_code[:100],
            observed_text,
            1 if legacy_backfill else 0,
            validation_basis_hash,
        ),
    )
    created = cursor.rowcount == 1
    state = await _refresh_current_cache(
        db,
        mention_id,
        checked_at=observed_text,
    )
    return state, created


async def record_ticker_mention(
    db: aiosqlite.Connection,
    *,
    news_id: int,
    ticker: str,
    association_method: str,
    association_confidence: float,
    source: str,
    validation_status: str,
    available_at: datetime | str,
    focus_revision: int | None,
    universe_version: str | None,
    validation_basis_hash: str,
    analysis_revision_id: int | None = None,
    reason_code: str = "association_observed",
    legacy_association: bool = False,
) -> dict[str, Any]:
    if validation_status == "invalid":
        raise ValueError("invalid_ticker_mentions_are_not_persisted")
    if validation_status not in VALIDATION_STATUSES:
        raise ValueError("unsupported_ticker_validation_status")
    if association_method == "llm_inference" and analysis_revision_id is None and not legacy_association:
        raise ValueError("llm_ticker_mention_requires_analysis_revision")
    if association_method != "llm_inference" and analysis_revision_id is not None:
        raise ValueError("non_llm_ticker_mention_cannot_reference_analysis_revision")
    checked_at = utc_text(available_at)
    confidence = max(0.0, min(1.0, float(association_confidence)))
    source = str(source)[:200]
    await db.execute(
        """INSERT OR IGNORE INTO news_ticker_mentions
           (news_id,ticker,association_method,association_confidence,validation_status,
            validated_at,focus_revision,universe_version,source,created_at,
            analysis_revision_id,last_checked_at,current_validation_status,
            current_validation_revision_id,legacy_association)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?)""",
        (
            news_id,
            ticker,
            association_method,
            confidence,
            validation_status,
            checked_at,
            focus_revision,
            universe_version,
            source,
            checked_at,
            analysis_revision_id,
            checked_at,
            validation_status,
            1 if legacy_association else 0,
        ),
    )
    async with db.execute(
        """SELECT id,association_confidence,created_at FROM news_ticker_mentions
           WHERE news_id=? AND ticker=? AND association_method=? AND source=?
             AND analysis_revision_id IS ?
           ORDER BY id LIMIT 1""",
        (news_id, ticker, association_method, source, analysis_revision_id),
    ) as cursor:
        mention = await cursor.fetchone()
    if mention is None:
        raise RuntimeError("ticker_mention_identity_not_found")
    mention_id = int(mention[0])
    await db.execute(
        """UPDATE news_ticker_mentions SET
             association_confidence=MAX(association_confidence,?),last_checked_at=CASE
               WHEN last_checked_at IS NULL
                 OR REPLACE(last_checked_at,'Z','+00:00')<? THEN ?
               ELSE last_checked_at END
           WHERE id=?""",
        (confidence, checked_at, checked_at, mention_id),
    )
    state, created_revision = await append_validation_revision(
        db,
        mention_id=mention_id,
        validation_status=validation_status,
        available_at=checked_at,
        focus_revision=focus_revision,
        universe_version=universe_version,
        reason_code=reason_code,
        validation_basis_hash=validation_basis_hash,
        legacy_backfill=legacy_association,
    )
    return {
        "mention_id": mention_id,
        "ticker": ticker,
        "association_method": association_method,
        "association_confidence": max(float(mention[1]), confidence),
        "analysis_revision_id": analysis_revision_id,
        "validation_revision_created": created_revision,
        **state,
    }


async def validation_as_of(
    db: aiosqlite.Connection,
    *,
    mention_id: int,
    as_of: datetime | str,
) -> dict[str, Any]:
    cutoff = utc_text(as_of)
    async with db.execute(
        """SELECT id,validation_status,available_at,focus_revision,universe_version,
                  reason_code,legacy_backfill,validation_basis_hash
           FROM ticker_validation_revisions
           WHERE mention_id=? AND available_at<=?
           ORDER BY available_at DESC,id DESC LIMIT 1""",
        (mention_id, cutoff),
    ) as cursor:
        row = await cursor.fetchone()
    if row is None:
        return {
            "validation_status": "unverified",
            "validated_at": None,
            "focus_revision": None,
            "universe_version": None,
            "validation_revision_id": None,
        }
    return {
        "validation_revision_id": int(row[0]),
        "validation_status": str(row[1]),
        "validated_at": str(row[2]),
        "focus_revision": row[3],
        "universe_version": row[4],
        "reason_code": str(row[5]),
        "legacy_backfill": bool(row[6]),
        "validation_basis_hash": str(row[7]),
    }


async def trusted_tickers_for_news_as_of(
    db: aiosqlite.Connection,
    *,
    news_id: int,
    as_of: datetime | str,
) -> dict[str, Any]:
    """Return the trusted point-in-time projection for one news item.

    Non-model associations retain their own lineage. Model associations are
    visible only from the newest analysis revision available at ``as_of``;
    older model revisions remain queryable at their historical boundaries but
    cannot leak into the current event projection.
    """

    cutoff = utc_text(as_of)
    async with db.execute(
        """SELECT id,revision,available_at
           FROM analysis_revisions
           WHERE news_id=? AND REPLACE(available_at,'Z','+00:00')<=?
           ORDER BY REPLACE(available_at,'Z','+00:00') DESC,
                    revision DESC,id DESC
           LIMIT 1""",
        (news_id, cutoff),
    ) as cursor:
        analysis_row = await cursor.fetchone()
    latest_analysis_revision_id = int(analysis_row[0]) if analysis_row else None

    async def visible_mentions(
        *,
        llm: bool,
    ) -> list[dict[str, Any]]:
        if llm and latest_analysis_revision_id is None:
            return []
        method_predicate = (
            "m.association_method='llm_inference' AND m.analysis_revision_id=?"
            if llm
            else "m.association_method IN ('provider_tag','company_endpoint','exact_alias','event_propagation')"
        )
        params: list[Any] = [cutoff, news_id]
        if llm:
            params.append(latest_analysis_revision_id)
        params.append(cutoff)
        async with db.execute(
            f"""SELECT m.id,m.ticker,m.association_method,m.association_confidence,
                       m.analysis_revision_id,v.id,v.validation_status,v.available_at,
                       v.focus_revision,v.universe_version,v.reason_code
                FROM news_ticker_mentions m
                LEFT JOIN ticker_validation_revisions v ON v.id=(
                  SELECT latest.id FROM ticker_validation_revisions latest
                  WHERE latest.mention_id=m.id
                    AND REPLACE(latest.available_at,'Z','+00:00')<=?
                  ORDER BY REPLACE(latest.available_at,'Z','+00:00') DESC,
                           latest.id DESC LIMIT 1
                )
                WHERE m.news_id=? AND {method_predicate}
                  AND REPLACE(m.created_at,'Z','+00:00')<=?
                ORDER BY m.ticker,m.id""",
            tuple(params),
        ) as cursor:
            rows = await cursor.fetchall()
        return [
            {
                "mention_id": int(row[0]),
                "ticker": str(row[1]),
                "association_method": str(row[2]),
                "association_confidence": float(row[3]),
                "analysis_revision_id": int(row[4]) if row[4] is not None else None,
                "validation_revision_id": int(row[5]) if row[5] is not None else None,
                "validation_status": str(row[6] or "unverified"),
                "validated_at": str(row[7]) if row[7] is not None else None,
                "focus_revision": int(row[8]) if row[8] is not None else None,
                "universe_version": str(row[9]) if row[9] is not None else None,
                "reason_code": str(row[10]) if row[10] is not None else None,
                "projection_scope": "llm" if llm else "provider",
            }
            for row in rows
        ]

    provider_provenance = await visible_mentions(llm=False)
    llm_provenance = await visible_mentions(llm=True)
    provider_tickers = sorted(
        {
            row["ticker"]
            for row in provider_provenance
            if row["validation_status"] in TRUSTED_VALIDATION_STATUSES
        }
    )
    llm_tickers = sorted(
        {
            row["ticker"]
            for row in llm_provenance
            if row["validation_status"] in TRUSTED_VALIDATION_STATUSES
        }
    )
    return {
        "as_of": cutoff,
        "provider_tickers": provider_tickers,
        "latest_analysis_revision_id": latest_analysis_revision_id,
        "llm_tickers": llm_tickers,
        "trusted_tickers": sorted(set(provider_tickers) | set(llm_tickers)),
        "validation_provenance": provider_provenance + llm_provenance,
    }
