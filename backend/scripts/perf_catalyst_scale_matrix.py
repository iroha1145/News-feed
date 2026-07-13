#!/usr/bin/env python3
"""Run the combined Catalyst scale matrix against one isolated SQLite file.

The fixture deliberately keeps every requested population in the same database:
20,000 news items, 100,000 ticker mentions, 20,000 validation revisions,
5,000 event groups, and 90 days of half-hour focus snapshots.  Writes are
chunked so the harness itself does not hide an unbounded-memory implementation.

This is an opt-in performance harness, not part of the ordinary unit-test run.
It never contacts a market-data or model provider.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import platform
import resource
import sqlite3
import sys
import time
import tracemalloc
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence


NEWS_COUNT = 20_000
MENTIONS_PER_NEWS = 5
MENTION_COUNT = NEWS_COUNT * MENTIONS_PER_NEWS
VALIDATION_REVISION_COUNT = 20_000
EVENT_GROUP_COUNT = 5_000
EVENT_MEMBERS_PER_GROUP = NEWS_COUNT // EVENT_GROUP_COUNT
FOCUS_DAYS = 90
FOCUS_SNAPSHOTS_PER_DAY = 48
FOCUS_SNAPSHOT_COUNT = FOCUS_DAYS * FOCUS_SNAPSHOTS_PER_DAY

SEED_BATCH_SIZE = 1_000
REVALIDATION_MAX_ROWS = 250
REVALIDATION_BATCH_SIZE = 100
RETENTION_BATCH_SIZE = 200
LOCK_WAIT_MS = 300


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _chunks(values: Iterable[Sequence[Any]], size: int = SEED_BATCH_SIZE):
    chunk: list[Sequence[Any]] = []
    for value in values:
        chunk.append(value)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


async def _insert_chunks(
    db: Any,
    sql: str,
    values: Iterable[Sequence[Any]],
) -> None:
    for chunk in _chunks(values):
        await db.executemany(sql, chunk)
        await db.commit()


def _focus_payload(
    *,
    revision: int,
    as_of: datetime,
    include_hot: bool = False,
) -> dict[str, Any]:
    symbols = [
        {
            "ticker": "BASE",
            "validation_status": "canonical",
            "data_through": _utc_text(as_of - timedelta(minutes=1)),
            "data_status": "active",
            "source_status": "active",
            "data_quality": 1.0,
            "session_change_pct": 0.5,
            "rvol_time_of_day": 1.1,
            "breakout_state": "forming",
        }
    ]
    if include_hot:
        symbols.append(
            {
                "ticker": "HOT",
                "validation_status": "canonical",
                "data_through": _utc_text(as_of - timedelta(minutes=1)),
                "data_status": "active",
                "source_status": "active",
                "data_quality": 1.0,
                "session_change_pct": 1.2,
                "rvol_time_of_day": 1.7,
                "breakout_state": "confirmed",
            }
        )
    return {
        "schema_version": "option-pro-macrolens-focus-v2",
        "revision": revision,
        "as_of": _utc_text(as_of),
        "data_through": _utc_text(as_of - timedelta(minutes=1)),
        "market_session": "regular",
        "universe_version": "scale-matrix-v1",
        "major_market_symbols": [],
        "symbols": symbols,
    }


async def _seed_matrix(db: Any, now: datetime) -> dict[str, float]:
    started = time.perf_counter()

    def news_rows():
        for news_id in range(1, NEWS_COUNT + 1):
            observed_at = now - timedelta(seconds=news_id % 7_200)
            observed_text = _utc_text(observed_at)
            yield (
                f"scale-source-{news_id % 16}",
                f"Scale matrix news {news_id}",
                "Synthetic fixture for bounded Catalyst performance checks.",
                f"https://scale.invalid/news/{news_id}",
                observed_text,
                observed_text,
                hashlib.sha256(f"news:{news_id}".encode()).hexdigest(),
                "pending",
                observed_text,
                "[]",
            )

    await _insert_chunks(
        db,
        """INSERT INTO news_items
           (source,title,summary,url,published_at,fetched_at,content_hash,
            analysis_status,updated_at,source_tickers)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        news_rows(),
    )

    def mention_rows():
        for news_id in range(1, NEWS_COUNT + 1):
            observed_at = now - timedelta(seconds=news_id % 7_200)
            for slot in range(MENTIONS_PER_NEWS):
                if slot == 0 and news_id <= 1_000:
                    ticker = "HOT"
                elif slot == 0:
                    ticker = f"S{news_id % 5_000:04d}"
                else:
                    ticker = f"T{slot}{news_id % 5_000:04d}"
                yield (
                    news_id,
                    ticker,
                    "exact_alias",
                    0.8,
                    "unverified",
                    "scale_matrix",
                    "unverified",
                    _utc_text(observed_at),
                )

    await _insert_chunks(
        db,
        """INSERT INTO news_ticker_mentions
           (news_id,ticker,association_method,association_confidence,
            validation_status,source,current_validation_status,created_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        mention_rows(),
    )

    validation_at = _utc_text(now - timedelta(hours=3))
    basis_hash = hashlib.sha256(b"scale-matrix-initial-unverified").hexdigest()

    def validation_rows():
        for news_id in range(1, VALIDATION_REVISION_COUNT + 1):
            mention_id = ((news_id - 1) * MENTIONS_PER_NEWS) + 1
            yield (
                mention_id,
                "unverified",
                validation_at,
                None,
                "scale-matrix-v1",
                "scale_fixture_initial",
                validation_at,
                0,
                basis_hash,
            )

    await _insert_chunks(
        db,
        """INSERT INTO ticker_validation_revisions
           (mention_id,validation_status,available_at,focus_revision,
            universe_version,reason_code,created_at,legacy_backfill,
            validation_basis_hash)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        validation_rows(),
    )
    await db.execute(
        """UPDATE news_ticker_mentions
           SET current_validation_revision_id=(
                 SELECT v.id FROM ticker_validation_revisions v
                 WHERE v.mention_id=news_ticker_mentions.id
                 ORDER BY v.available_at DESC,v.id DESC LIMIT 1
               ),
               last_checked_at=?
           WHERE id IN (SELECT mention_id FROM ticker_validation_revisions)""",
        (validation_at,),
    )
    await db.commit()

    def event_group_rows():
        for group_number in range(EVENT_GROUP_COUNT):
            representative_news_id = group_number * EVENT_MEMBERS_PER_GROUP + 1
            observed_at = now - timedelta(seconds=group_number % 7_200)
            observed_text = _utc_text(observed_at)
            publishers = [f"publisher-{value}" for value in range(4)]
            yield (
                f"eg-{group_number:05d}",
                representative_news_id,
                f"Scale event group {group_number}",
                "other",
                observed_text,
                observed_text,
                observed_text,
                observed_text,
                observed_text,
                EVENT_MEMBERS_PER_GROUP,
                EVENT_MEMBERS_PER_GROUP,
                json.dumps(publishers),
                "[]",
                "[]",
                85.0,
                hashlib.sha256(f"event:{group_number}".encode()).hexdigest(),
                "GATED",
                1,
                observed_text,
                observed_text,
            )

    await _insert_chunks(
        db,
        """INSERT INTO news_event_groups
           (event_group_id,representative_news_id,representative_title,event_type,
            first_published_at,last_published_at,first_fetched_at,last_fetched_at,
            available_at,member_count,source_count,source_names_json,
            source_tickers_json,validated_tickers_json,novelty_score,
            evidence_fingerprint,status,version,created_at,updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        event_group_rows(),
    )

    def event_member_rows():
        for news_id in range(1, NEWS_COUNT + 1):
            group_number = (news_id - 1) // EVENT_MEMBERS_PER_GROUP
            member_number = (news_id - 1) % EVENT_MEMBERS_PER_GROUP
            observed_at = now - timedelta(seconds=news_id % 7_200)
            observed_text = _utc_text(observed_at)
            yield (
                f"eg-{group_number:05d}",
                news_id,
                f"scale-source-{member_number}",
                f"https://scale.invalid/news/{news_id}",
                f"Scale matrix news {news_id}",
                observed_text,
                observed_text,
                "[]",
                "[]",
                f"publisher-{member_number}",
                "other",
                hashlib.sha256(f"evidence:{news_id}".encode()).hexdigest(),
                hashlib.sha256(f"member:{news_id}".encode()).hexdigest(),
                observed_text,
            )

    await _insert_chunks(
        db,
        """INSERT INTO news_event_members
           (event_group_id,news_id,source,normalized_url,title,published_at,
            fetched_at,source_tickers_json,validated_tickers_json,
            publisher_identity,event_type,evidence_fingerprint,content_hash,
            created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        event_member_rows(),
    )

    first_focus_at = now - timedelta(
        minutes=30 * (FOCUS_SNAPSHOT_COUNT - 1)
    )

    def focus_rows():
        for revision in range(1, FOCUS_SNAPSHOT_COUNT + 1):
            as_of = first_focus_at + timedelta(minutes=30 * (revision - 1))
            payload = _focus_payload(revision=revision, as_of=as_of)
            payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
            yield (
                revision,
                payload["schema_version"],
                payload["as_of"],
                payload["data_through"],
                payload["market_session"],
                payload["universe_version"],
                payload_json,
                hashlib.sha256(payload_json.encode()).hexdigest(),
                "current" if revision == FOCUS_SNAPSHOT_COUNT else "stale",
                payload["as_of"],
                payload["as_of"],
            )

    await _insert_chunks(
        db,
        """INSERT INTO focus_context_snapshots
           (revision,schema_version,as_of,data_through,market_session,
            universe_version,payload_json,payload_hash,status,fetched_at,created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        focus_rows(),
    )
    await db.commit()
    return {"seed_elapsed_seconds": round(time.perf_counter() - started, 6)}


async def _table_counts(db: Any) -> dict[str, int]:
    names = {
        "news_items": "news_items",
        "news_ticker_mentions": "news_ticker_mentions",
        "ticker_validation_revisions": "ticker_validation_revisions",
        "news_event_groups": "news_event_groups",
        "news_event_members": "news_event_members",
        "focus_context_snapshots": "focus_context_snapshots",
    }
    values: dict[str, int] = {}
    for key, table in names.items():
        async with db.execute(f"SELECT COUNT(*) FROM {table}") as cursor:
            values[key] = int((await cursor.fetchone())[0])
    return values


async def _insert_focus_snapshot(
    db: Any,
    *,
    revision: int,
    as_of: datetime,
    include_hot: bool,
) -> dict[str, Any]:
    payload = _focus_payload(
        revision=revision,
        as_of=as_of,
        include_hot=include_hot,
    )
    payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    await db.execute("UPDATE focus_context_snapshots SET status='stale'")
    await db.execute(
        """INSERT INTO focus_context_snapshots
           (revision,schema_version,as_of,data_through,market_session,
            universe_version,payload_json,payload_hash,status,fetched_at,created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (
            revision,
            payload["schema_version"],
            payload["as_of"],
            payload["data_through"],
            payload["market_session"],
            payload["universe_version"],
            payload_json,
            hashlib.sha256(payload_json.encode()).hexdigest(),
            "current",
            payload["as_of"],
            payload["as_of"],
        ),
    )
    await db.commit()
    return payload


async def _query_plan(db: Any, sql: str, params: Sequence[Any]) -> list[str]:
    async with db.execute("EXPLAIN QUERY PLAN " + sql, params) as cursor:
        return [str(row[3]) for row in await cursor.fetchall()]


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return 0


def _database_files(path: Path) -> dict[str, int]:
    return {
        "database_bytes": _file_size(path),
        "wal_bytes": _file_size(Path(str(path) + "-wal")),
        "shared_memory_bytes": _file_size(Path(str(path) + "-shm")),
    }


async def _run_lock_probe(database_path: Path) -> dict[str, Any]:
    import aiosqlite

    owner = await aiosqlite.connect(database_path)
    contender = await aiosqlite.connect(database_path)
    try:
        await owner.execute("BEGIN IMMEDIATE")
        await contender.execute(f"PRAGMA busy_timeout={LOCK_WAIT_MS}")
        started = time.perf_counter()
        error = ""
        try:
            await contender.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            error = str(exc).lower()
        elapsed = time.perf_counter() - started
        assert "locked" in error, f"lock probe unexpectedly acquired the write lock: {error!r}"
        assert elapsed >= (LOCK_WAIT_MS / 1000) * 0.70, elapsed
        assert elapsed <= (LOCK_WAIT_MS / 1000) + 1.0, elapsed
        return {
            "configured_wait_ms": LOCK_WAIT_MS,
            "observed_wait_ms": round(elapsed * 1000, 3),
            "bounded": True,
        }
    finally:
        await owner.rollback()
        await owner.close()
        await contender.close()


async def run_matrix(database_path: Path) -> dict[str, Any]:
    os.environ.update(
        {
            "DATABASE_URL": f"sqlite+aiosqlite:///{database_path}",
            "NEWS_LLM_AUTO_ANALYZE_ENABLED": "false",
            "NEWS_LLM_MANUAL_ENABLED": "false",
            "HOT_CYCLE_ENABLED": "false",
            "HOT_CYCLE_SCHEDULE_ENABLED": "false",
            "HOT_CYCLE_MANUAL_ENABLED": "false",
            "FOCUS_REVALIDATION_MAX_ROWS_PER_RUN": str(REVALIDATION_MAX_ROWS),
            "FOCUS_REVALIDATION_MAX_SECONDS_PER_RUN": "30",
            "FOCUS_REVALIDATION_BATCH_SIZE": str(REVALIDATION_BATCH_SIZE),
            "RETENTION_BATCH_SIZE": str(RETENTION_BATCH_SIZE),
            "FOCUS_SNAPSHOT_RETENTION_DAYS": str(FOCUS_DAYS),
            "FOCUS_SNAPSHOT_FULL_RESOLUTION_DAYS": "30",
            "FOCUS_SNAPSHOT_DAILY_ROLLUP_ENABLED": "true",
        }
    )
    backend_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(backend_root))

    import aiosqlite

    from app.integrations.option_pro.repository import query_ticker
    from app.models.database import get_db, init_db
    from app.services.market_focus import (
        TICKER_VALIDATION_RULES_VERSION,
        _focus_validation_basis,
        revalidate_events_for_focus_context,
    )
    from app.services.retention import cleanup_extended_retention

    await init_db()
    db = await get_db()
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=OFF")
    await db.commit()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    try:
        results: dict[str, Any] = {
            "configuration": {
                "seed_batch_size": SEED_BATCH_SIZE,
                "revalidation_max_rows_per_slice": REVALIDATION_MAX_ROWS,
                "revalidation_batch_size": REVALIDATION_BATCH_SIZE,
                "retention_batch_size": RETENTION_BATCH_SIZE,
            }
        }
        results["timing"] = await _seed_matrix(db, now)
        counts = await _table_counts(db)
        results["fixture_rows"] = counts
        assert counts == {
            "news_items": NEWS_COUNT,
            "news_ticker_mentions": MENTION_COUNT,
            "ticker_validation_revisions": VALIDATION_REVISION_COUNT,
            "news_event_groups": EVENT_GROUP_COUNT,
            "news_event_members": NEWS_COUNT,
            "focus_context_snapshots": FOCUS_SNAPSHOT_COUNT,
        }, counts

        base_payload = _focus_payload(
            revision=FOCUS_SNAPSHOT_COUNT,
            as_of=now - timedelta(minutes=30),
        )
        basis_hash, canonical_hash, external_hash, _, _ = _focus_validation_basis(
            base_payload
        )
        await db.execute(
            """UPDATE focus_validation_state SET
                 last_focus_revision=?,validation_basis_hash=?,
                 canonical_symbols_hash=?,external_symbols_hash=?,
                 universe_version=?,validation_rules_version=?,
                 rows_scanned=0,rows_changed=0,duration_ms=0,
                 validation_revisions_created=0,event_groups_regated=0,
                 pending_run_key=NULL,pending_focus_revision=NULL,pending_phase=NULL
               WHERE singleton_id=1""",
            (
                FOCUS_SNAPSHOT_COUNT,
                basis_hash,
                canonical_hash,
                external_hash,
                base_payload["universe_version"],
                TICKER_VALIDATION_RULES_VERSION,
            ),
        )
        await db.commit()

        same_revision = FOCUS_SNAPSHOT_COUNT + 1
        same_payload = await _insert_focus_snapshot(
            db,
            revision=same_revision,
            as_of=now,
            include_hot=False,
        )
        started = time.perf_counter()
        await revalidate_events_for_focus_context(db, same_payload)
        same_elapsed = time.perf_counter() - started
        async with db.execute(
            """SELECT last_focus_revision,rows_scanned,rows_changed,
                      validation_revisions_created,event_groups_regated,
                      pending_focus_revision
               FROM focus_validation_state WHERE singleton_id=1"""
        ) as cursor:
            same_state = dict(await cursor.fetchone())
        assert int(same_state["last_focus_revision"]) == same_revision, same_state
        assert int(same_state["rows_scanned"]) == 0, same_state
        assert int(same_state["rows_changed"]) == 0, same_state
        assert int(same_state["validation_revisions_created"]) == 0, same_state
        assert same_state["pending_focus_revision"] is None, same_state
        results["same_semantics"] = {
            "rows_scanned": 0,
            "rows_changed": 0,
            "validation_revisions_created": 0,
            "event_groups_regated": int(same_state["event_groups_regated"]),
            "elapsed_seconds": round(same_elapsed, 6),
            "full_scan_avoided": True,
        }

        delta_revision = same_revision + 1
        delta_payload = await _insert_focus_snapshot(
            db,
            revision=delta_revision,
            as_of=now + timedelta(seconds=1),
            include_hot=True,
        )
        before_validation_count = (
            await (await db.execute("SELECT COUNT(*) FROM ticker_validation_revisions")).fetchone()
        )[0]
        started = time.perf_counter()
        await revalidate_events_for_focus_context(db, delta_payload)
        delta_elapsed = time.perf_counter() - started
        async with db.execute(
            """SELECT pending_focus_revision,pending_phase,pending_mention_cursor,
                      pending_rows_scanned,pending_rows_changed,
                      pending_validation_revisions_created,
                      pending_event_groups_regated
               FROM focus_validation_state WHERE singleton_id=1"""
        ) as cursor:
            delta_state = dict(await cursor.fetchone())
        after_validation_count = (
            await (await db.execute("SELECT COUNT(*) FROM ticker_validation_revisions")).fetchone()
        )[0]
        scanned = int(delta_state["pending_rows_scanned"])
        changed = int(delta_state["pending_rows_changed"])
        revisions_created = int(delta_state["pending_validation_revisions_created"])
        assert delta_state["pending_focus_revision"] == delta_revision, delta_state
        assert 0 < scanned <= REVALIDATION_MAX_ROWS, delta_state
        assert changed == scanned, delta_state
        assert revisions_created == scanned, delta_state
        assert after_validation_count - before_validation_count == scanned
        assert scanned < 1_000, "the first slice consumed the complete HOT population"
        assert delta_elapsed < 30, delta_elapsed
        results["incremental_revalidation_slice"] = {
            "candidate_rows": 1_000,
            "rows_scanned": scanned,
            "rows_changed": changed,
            "validation_revisions_created": revisions_created,
            "event_groups_regated": int(delta_state["pending_event_groups_regated"]),
            "cursor": int(delta_state["pending_mention_cursor"]),
            "phase": str(delta_state["pending_phase"]),
            "elapsed_seconds": round(delta_elapsed, 6),
            "budget_respected": True,
        }

        ticker_plan = await _query_plan(
            db,
            """SELECT n.id FROM news_items n
               WHERE EXISTS (
                 SELECT 1 FROM news_ticker_mentions tm
                 JOIN ticker_validation_revisions source_validation
                   ON source_validation.id=(
                     SELECT latest.id FROM ticker_validation_revisions latest
                     WHERE latest.mention_id=tm.id AND latest.available_at<=?
                     ORDER BY latest.available_at DESC,latest.id DESC LIMIT 1
                   )
                 WHERE tm.news_id=n.id AND tm.ticker=?
                   AND tm.association_method<>'llm_inference'
                   AND tm.created_at<=?
                   AND source_validation.validation_status
                       IN ('canonical','valid_external')
               )
               ORDER BY datetime(COALESCE(n.published_at,n.fetched_at)) DESC,n.id DESC
               LIMIT 25""",
            (_utc_text(now + timedelta(seconds=2)), "HOT", _utc_text(now + timedelta(seconds=2))),
        )
        as_of_plan = await _query_plan(
            db,
            """SELECT id FROM ticker_validation_revisions
               WHERE mention_id=? AND available_at<=?
               ORDER BY available_at DESC,id DESC LIMIT 1""",
            (1, _utc_text(now + timedelta(seconds=2))),
        )
        ticker_plan_text = "\n".join(ticker_plan)
        as_of_plan_text = "\n".join(as_of_plan)
        assert (
            "idx_ticker_mentions_news" in ticker_plan_text
            or "idx_ticker_mentions_validated" in ticker_plan_text
        ), ticker_plan_text
        assert "idx_ticker_validation_as_of" in ticker_plan_text, ticker_plan_text
        assert "idx_ticker_validation_as_of" in as_of_plan_text, as_of_plan_text
        assert "SCAN latest" not in ticker_plan_text, ticker_plan_text
        assert "SCAN ticker_validation_revisions" not in as_of_plan_text, as_of_plan_text

        started = time.perf_counter()
        ticker_items, _, _, _ = await query_ticker(
            db,
            ticker="HOT",
            as_of=now + timedelta(seconds=2),
            window_hours=24,
            # Keep the page larger than this deliberately bounded slice.  The
            # scale harness therefore exercises the public query without
            # needing an integration cursor-signing credential.
            limit=1_000,
            cursor=None,
            min_confidence=0,
            include_neutral=True,
            include_unanalyzed=True,
        )
        query_elapsed = time.perf_counter() - started
        assert len(ticker_items) == scanned, len(ticker_items)
        assert query_elapsed < 60, query_elapsed
        results["indexed_queries"] = {
            "query_ticker_result_count": len(ticker_items),
            "query_ticker_elapsed_seconds": round(query_elapsed, 6),
            "query_ticker_plan": ticker_plan,
            "as_of_plan": as_of_plan,
            "ticker_index_hit": True,
            "as_of_index_hit": True,
        }

        files_before_retention = _database_files(database_path)
        retention_batches: list[dict[str, int]] = []
        retention_started = time.perf_counter()
        for batch_number in range(1, 32):
            stats = await cleanup_extended_retention(db)
            deleted = int(stats["focus_snapshots_deleted"])
            assert 0 <= deleted <= RETENTION_BATCH_SIZE, stats
            retention_batches.append(
                {
                    "batch": batch_number,
                    "deleted": deleted,
                    "retained": int(stats["focus_snapshots_retained"]),
                    "rollup_created": int(stats["focus_snapshot_rollup_created"]),
                    "wal_checkpoint_busy": int(stats["wal_checkpoint_busy"]),
                    "wal_pages": int(stats["wal_pages"]),
                    "wal_checkpointed_pages": int(stats["wal_checkpointed_pages"]),
                }
            )
            assert int(stats["wal_checkpoint_busy"]) == 0, stats
            assert int(stats["wal_checkpointed_pages"]) == int(stats["wal_pages"]), stats
            if deleted == 0:
                break
        else:
            raise AssertionError("focus retention did not converge within 31 bounded batches")
        retention_elapsed = time.perf_counter() - retention_started
        nonempty_batches = [item for item in retention_batches if item["deleted"]]
        assert len(nonempty_batches) >= 2, retention_batches
        assert all(item["deleted"] <= RETENTION_BATCH_SIZE for item in nonempty_batches)
        total_deleted = sum(item["deleted"] for item in nonempty_batches)
        async with db.execute("SELECT COUNT(*) FROM focus_context_snapshots") as cursor:
            retained_focus = int((await cursor.fetchone())[0])
        assert retained_focus == FOCUS_SNAPSHOT_COUNT + 2 - total_deleted
        assert retained_focus <= (30 * FOCUS_SNAPSHOTS_PER_DAY) + 64
        async with db.execute("PRAGMA foreign_key_check") as cursor:
            assert await cursor.fetchone() is None

        async with db.execute("PRAGMA wal_checkpoint(TRUNCATE)") as cursor:
            truncate_result = [int(value) for value in await cursor.fetchone()]
        assert truncate_result[0] == 0, truncate_result
        await db.commit()
        files_after_retention = _database_files(database_path)
        assert files_after_retention["wal_bytes"] <= 64 * 1024, files_after_retention
        results["retention"] = {
            "batch_count": len(retention_batches),
            "nonempty_batch_count": len(nonempty_batches),
            "max_deleted_per_batch": max(item["deleted"] for item in retention_batches),
            "total_deleted": total_deleted,
            "retained": retained_focus,
            "elapsed_seconds": round(retention_elapsed, 6),
            "batches": retention_batches,
            "files_before": files_before_retention,
            "files_after": files_after_retention,
            "wal_truncate_result": truncate_result,
            "bounded_and_converged": True,
        }

        await db.close()
        db = None
        results["lock_wait"] = await _run_lock_probe(database_path)

        async with aiosqlite.connect(database_path) as verification_db:
            await verification_db.execute("PRAGMA foreign_keys=ON")
            async with verification_db.execute("PRAGMA integrity_check") as cursor:
                integrity = str((await cursor.fetchone())[0])
            async with verification_db.execute("PRAGMA foreign_key_check") as cursor:
                foreign_key_violations = len(await cursor.fetchall())
        assert integrity.lower() == "ok", integrity
        assert foreign_key_violations == 0, foreign_key_violations
        results["database_verification"] = {
            "integrity_check": integrity,
            "foreign_key_violations": foreign_key_violations,
            **_database_files(database_path),
        }
        return results
    finally:
        if db is not None:
            await db.close()


def _process_peak_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if platform.system() == "Darwin" else value * 1024


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        type=Path,
        default=Path("/tmp/macrolens-catalyst-scale-matrix.db"),
        help="isolated SQLite fixture path",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="remove an existing fixture before the run",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    database_path = args.database.expanduser().resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)
    existing = [
        database_path,
        Path(str(database_path) + "-wal"),
        Path(str(database_path) + "-shm"),
    ]
    if any(path.exists() for path in existing) and not args.replace:
        raise SystemExit("fixture already exists; pass --replace to recreate it")
    if args.replace:
        for path in existing:
            path.unlink(missing_ok=True)

    tracemalloc.start()
    wall_started = time.perf_counter()
    results = asyncio.run(run_matrix(database_path))
    _, traced_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    results["resource_usage"] = {
        "total_elapsed_seconds": round(time.perf_counter() - wall_started, 6),
        "python_tracemalloc_peak_bytes": traced_peak,
        "process_max_rss_bytes": _process_peak_rss_bytes(),
        "database_total_bytes": sum(_database_files(database_path).values()),
    }
    assert traced_peak < 256 * 1024 * 1024, results["resource_usage"]
    print(json.dumps(results, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
