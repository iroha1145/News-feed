from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import aiosqlite

from app.config import settings


def _new_york_trading_date(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return parsed.astimezone(ZoneInfo("America/New_York")).date().isoformat()
    except (TypeError, ValueError):
        return None


async def _delete(
    db: aiosqlite.Connection,
    *,
    table: str,
    id_column: str,
    select_sql: str,
    params: tuple[Any, ...],
) -> int:
    cursor = await db.execute(
        f"DELETE FROM {table} WHERE {id_column} IN ({select_sql} LIMIT ?)",
        (*params, settings.retention_batch_size),
    )
    deleted = max(0, cursor.rowcount)
    await db.commit()
    return deleted


async def _database_page_metrics(db: aiosqlite.Connection) -> dict[str, int]:
    values: dict[str, int] = {}
    for key in ("page_count", "page_size", "freelist_count"):
        async with db.execute(f"PRAGMA {key}") as cursor:
            row = await cursor.fetchone()
        values[key] = int(row[0] if row else 0)
    total = values["page_count"] * values["page_size"]
    free = values["freelist_count"] * values["page_size"]
    return {
        "database_bytes": total,
        "database_free_bytes": free,
        "database_live_bytes": max(0, total - free),
    }


async def _cleanup_focus_snapshots(db: aiosqlite.Connection) -> dict[str, int]:
    """Compact old focus snapshots while preserving current and cycle provenance."""

    await db.create_function("new_york_trading_date", 1, _new_york_trading_date)

    full_days = settings.focus_snapshot_full_resolution_days
    retention_days = settings.focus_snapshot_retention_days
    rollup_enabled = settings.focus_snapshot_daily_rollup_enabled
    rollup_clause = ""
    params: list[Any] = [f"-{retention_days} days"]
    if rollup_enabled:
        rollup_clause = """
            OR (
              datetime(ranked.as_of)>=datetime('now',?)
              AND datetime(ranked.as_of)<datetime('now',?)
              AND ranked.daily_rank>1
            )
        """
        params.extend((f"-{retention_days} days", f"-{full_days} days"))
    params.append(settings.retention_batch_size)
    async with db.execute(
        f"""WITH ranked AS (
               SELECT s.revision,s.as_of,
                      ROW_NUMBER() OVER (
                        PARTITION BY new_york_trading_date(s.as_of)
                        ORDER BY datetime(s.as_of) DESC,s.revision DESC
                      ) AS daily_rank
               FROM focus_context_snapshots s
             )
             SELECT ranked.revision,new_york_trading_date(ranked.as_of) AS snapshot_day,
                    CASE WHEN datetime(ranked.as_of)>=datetime('now',?) THEN 1 ELSE 0 END
                    AS rollup_candidate
             FROM ranked
             WHERE (
                 datetime(ranked.as_of)<datetime('now',?)
                 {rollup_clause}
             )
               AND ranked.revision<>(
                 SELECT MAX(latest.revision) FROM focus_context_snapshots latest
               )
               AND NOT EXISTS (
                 SELECT 1 FROM market_focus_cycles cycle
                 WHERE cycle.focus_revision=ranked.revision
               )
               AND NOT EXISTS (
                 SELECT 1 FROM analysis_stock_impacts impact
                 WHERE impact.focus_revision=ranked.revision
               )
               AND NOT EXISTS (
                 SELECT 1 FROM news_ticker_mentions mention
                 WHERE mention.focus_revision=ranked.revision
               )
               AND NOT EXISTS (
                 SELECT 1 FROM ticker_validation_revisions validation
                 WHERE validation.focus_revision=ranked.revision
               )
               AND NOT EXISTS (
                 SELECT 1 FROM focus_validation_state state
                 WHERE state.last_focus_revision=ranked.revision
                    OR state.pending_focus_revision=ranked.revision
                    OR ranked.revision>COALESCE(state.last_focus_revision,0)
               )
             ORDER BY datetime(ranked.as_of),ranked.revision
             LIMIT ?""",
        (
            f"-{retention_days} days",
            *params,
        ),
    ) as cursor:
        candidates = await cursor.fetchall()

    deleted = 0
    rollup_days: set[str] = set()
    if candidates:
        revisions = [int(row[0]) for row in candidates]
        placeholders = ",".join("?" for _ in revisions)
        await db.execute("BEGIN IMMEDIATE")
        try:
            cursor = await db.execute(
                f"""DELETE FROM focus_context_snapshots
                    WHERE revision IN ({placeholders})
                      AND revision<>(SELECT MAX(revision) FROM focus_context_snapshots)
                      AND NOT EXISTS (
                        SELECT 1 FROM market_focus_cycles cycle
                        WHERE cycle.focus_revision=focus_context_snapshots.revision
                      )
                      AND NOT EXISTS (
                        SELECT 1 FROM analysis_stock_impacts impact
                        WHERE impact.focus_revision=focus_context_snapshots.revision
                      )
                      AND NOT EXISTS (
                        SELECT 1 FROM news_ticker_mentions mention
                        WHERE mention.focus_revision=focus_context_snapshots.revision
                      )
                      AND NOT EXISTS (
                        SELECT 1 FROM ticker_validation_revisions validation
                        WHERE validation.focus_revision=focus_context_snapshots.revision
                      )
                      AND NOT EXISTS (
                        SELECT 1 FROM focus_validation_state state
                        WHERE state.last_focus_revision=focus_context_snapshots.revision
                           OR state.pending_focus_revision=focus_context_snapshots.revision
                           OR focus_context_snapshots.revision>
                              COALESCE(state.last_focus_revision,0)
                      )""",
                tuple(revisions),
            )
            deleted = max(0, cursor.rowcount)
            await db.commit()
        except Exception:
            await db.rollback()
            raise
        if deleted:
            # Candidates were already protected and ordered.  Count each day
            # compacted in this batch once, rather than on every retention run.
            rollup_days = {
                str(row[1]) for row in candidates if int(row[2]) == 1 and row[1]
            }

    async with db.execute("SELECT COUNT(*) FROM focus_context_snapshots") as cursor:
        retained_row = await cursor.fetchone()
    async with db.execute(
        """SELECT COUNT(DISTINCT focus_revision) FROM market_focus_cycles
           WHERE focus_revision IS NOT NULL"""
    ) as cursor:
        protected_row = await cursor.fetchone()
    async with db.execute(
        """SELECT COUNT(*) FROM focus_context_snapshots snapshot
           WHERE EXISTS (
               SELECT 1 FROM analysis_stock_impacts impact
               WHERE impact.focus_revision=snapshot.revision
             ) OR EXISTS (
               SELECT 1 FROM news_ticker_mentions mention
               WHERE mention.focus_revision=snapshot.revision
             ) OR EXISTS (
               SELECT 1 FROM ticker_validation_revisions validation
               WHERE validation.focus_revision=snapshot.revision
             ) OR EXISTS (
               SELECT 1 FROM focus_validation_state state
               WHERE state.last_focus_revision=snapshot.revision
                  OR state.pending_focus_revision=snapshot.revision
                  OR snapshot.revision>COALESCE(state.last_focus_revision,0)
             )"""
    ) as cursor:
        lineage_protected_row = await cursor.fetchone()
    return {
        "focus_snapshots_deleted": deleted,
        "focus_snapshots_retained": int(retained_row[0] if retained_row else 0),
        "focus_snapshot_rollup_created": len(rollup_days),
        "focus_snapshots_cycle_protected": int(protected_row[0] if protected_row else 0),
        "focus_snapshots_lineage_protected": int(
            lineage_protected_row[0] if lineage_protected_row else 0
        ),
    }


async def _passive_wal_checkpoint(db: aiosqlite.Connection) -> dict[str, int]:
    async with db.execute("PRAGMA journal_mode") as cursor:
        mode_row = await cursor.fetchone()
    if not mode_row or str(mode_row[0]).lower() != "wal":
        return {
            "wal_checkpoint_busy": 0,
            "wal_pages": 0,
            "wal_checkpointed_pages": 0,
        }
    async with db.execute("PRAGMA wal_checkpoint(PASSIVE)") as cursor:
        row = await cursor.fetchone()
    return {
        "wal_checkpoint_busy": int(row[0] if row else 0),
        "wal_pages": int(row[1] if row else 0),
        "wal_checkpointed_pages": int(row[2] if row else 0),
    }


async def _archive_terminal_cycles(
    db: aiosqlite.Connection,
    *,
    statuses: tuple[str, ...],
    retention_days: int,
) -> int:
    placeholders = ",".join("?" for _ in statuses)
    async with db.execute(
        f"""SELECT c.* FROM market_focus_cycles c
            WHERE c.status IN ({placeholders})
              AND datetime(COALESCE(c.completed_at,c.updated_at))<datetime('now',?)
              AND NOT EXISTS (
                SELECT 1 FROM market_focus_cycles child
                WHERE child.retry_of_cycle_id=c.cycle_id
              )
              AND COALESCE(c.error_code,'')<>'submission_outcome_unknown'
            ORDER BY datetime(COALESCE(c.completed_at,c.updated_at)),c.cycle_id
            LIMIT ?""",
        (*statuses, f"-{retention_days} days", min(settings.retention_batch_size, 25)),
    ) as cursor:
        cycles = [dict(row) for row in await cursor.fetchall()]

    archived = 0
    for cycle in cycles:
        async with db.execute(
            """SELECT prepared_revision,event_group_id,event_group_version,snapshot_json
               FROM market_focus_cycle_events WHERE cycle_id=?
               ORDER BY prepared_revision""",
            (cycle["cycle_id"],),
        ) as cursor:
            snapshots = [dict(row) for row in await cursor.fetchall()]
        archived_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        await db.execute(
            """INSERT INTO market_focus_cycle_archives
               (cycle_id,status,completed_at,cycle_json,event_snapshots_json,
                result_json,archived_at)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(cycle_id) DO NOTHING""",
            (
                cycle["cycle_id"],
                cycle["status"],
                cycle.get("completed_at"),
                json.dumps(cycle, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
                json.dumps(snapshots, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
                cycle.get("result_json"),
                archived_at,
            ),
        )
        async with db.execute(
            "SELECT 1 FROM market_focus_cycle_archives WHERE cycle_id=?",
            (cycle["cycle_id"],),
        ) as cursor:
            if await cursor.fetchone() is None:
                raise RuntimeError("market_focus_cycle_archive_failed")
        deleted = await db.execute(
            "DELETE FROM market_focus_cycles WHERE cycle_id=?",
            (cycle["cycle_id"],),
        )
        await db.commit()
        archived += max(0, deleted.rowcount)
    return archived


async def cleanup_extended_retention(db: aiosqlite.Connection) -> dict[str, int]:
    """Delete bounded obsolete rows in dependency order without moving revisions backwards."""

    stats: dict[str, int] = {
        "analysis_jobs": 0,
        "analysis_revisions": 0,
        "analysis_stock_impacts": 0,
        "news_event_groups": 0,
        "market_focus_cycles": 0,
        "market_focus_completed_archived": 0,
        "market_focus_failed_archived": 0,
        "hotspot_preparation_sets": 0,
        "news_event_members": 0,
        "event_projection_retries": 0,
        "calendar_event_revisions": 0,
        "integration_changes": 0,
        "news_items": 0,
        "focus_snapshots_deleted": 0,
        "focus_snapshots_retained": 0,
        "focus_snapshot_rollup_created": 0,
        "focus_snapshots_cycle_protected": 0,
        "focus_snapshots_lineage_protected": 0,
    }
    before = await _database_page_metrics(db)
    stats.update({f"{key}_before": value for key, value in before.items()})
    try:
        stats.update(await _cleanup_focus_snapshots(db))
        if settings.analysis_job_retention_days:
            stats["analysis_jobs"] = await _delete(
                db, table="analysis_jobs", id_column="job_id",
                select_sql="""SELECT j.job_id FROM analysis_jobs j
                   WHERE j.status IN ('failed','cancelled','budget_blocked','incomplete_output')
                     AND datetime(j.updated_at)<datetime('now',?)
                     AND j.job_id NOT IN (
                       SELECT job_id FROM analysis_jobs x WHERE x.news_id=j.news_id
                       ORDER BY datetime(x.created_at) DESC LIMIT 1
                     ) ORDER BY datetime(j.updated_at)""",
                params=(f"-{settings.analysis_job_retention_days} days",),
            )
        if settings.analysis_revision_retention_days:
            # Keep the latest valid single-news analysis regardless of age.
            stats["analysis_revisions"] = await _delete(
                db, table="analysis_revisions", id_column="id",
                select_sql="""SELECT r.id FROM analysis_revisions r
                   WHERE datetime(r.created_at)<datetime('now',?)
                     AND r.id<>(SELECT MAX(x.id) FROM analysis_revisions x WHERE x.news_id=r.news_id)
                   ORDER BY datetime(r.created_at)""",
                params=(f"-{settings.analysis_revision_retention_days} days",),
            )
        if settings.stock_impact_retention_days:
            stats["analysis_stock_impacts"] = await _delete(
                db, table="analysis_stock_impacts", id_column="id",
                select_sql="""SELECT si.id FROM analysis_stock_impacts si
                   JOIN analysis_revisions r ON r.id=si.analysis_id
                   WHERE datetime(si.analyzed_at)<datetime('now',?)
                     AND r.id<>(SELECT MAX(x.id) FROM analysis_revisions x WHERE x.news_id=r.news_id)
                   ORDER BY datetime(si.analyzed_at)""",
                params=(f"-{settings.stock_impact_retention_days} days",),
            )
        if settings.event_group_retention_days:
            stats["news_event_groups"] = await _delete(
                db, table="news_event_groups", id_column="event_group_id",
                select_sql="""SELECT g.event_group_id FROM news_event_groups g
                   WHERE datetime(g.updated_at)<datetime('now',?)
                     AND NOT EXISTS (SELECT 1 FROM hotspot_preparation_sets h WHERE h.event_group_id=g.event_group_id)
                   ORDER BY datetime(g.updated_at)""",
                params=(f"-{settings.event_group_retention_days} days",),
            )
        stats["market_focus_completed_archived"] = await _archive_terminal_cycles(
            db,
            statuses=("completed",),
            retention_days=settings.market_focus_completed_retention_days,
        )
        stats["market_focus_failed_archived"] = await _archive_terminal_cycles(
            db,
            statuses=(
                "failed",
                "cancelled",
                "budget_blocked",
                "incomplete_output",
                "insufficient_context",
            ),
            retention_days=settings.market_focus_failed_retention_days,
        )
        stats["market_focus_cycles"] = (
            stats["market_focus_completed_archived"]
            + stats["market_focus_failed_archived"]
        )
        stats["hotspot_preparation_sets"] = await _delete(
            db,
            table="hotspot_preparation_sets",
            id_column="prepared_revision",
            select_sql="""SELECT h.prepared_revision FROM hotspot_preparation_sets h
               WHERE h.status='CONSUMED'
                 AND datetime(COALESCE(h.consumed_at,h.prepared_at))<datetime('now',?)
                 AND NOT EXISTS (
                   SELECT 1 FROM market_focus_cycle_events e
                   WHERE e.prepared_revision=h.prepared_revision
                 )
               ORDER BY h.prepared_revision""",
            params=(f"-{settings.hotspot_preparation_retention_days} days",),
        )
        stats["news_event_members"] = await _delete(
            db,
            table="news_event_members",
            id_column="id",
            select_sql="""SELECT m.id FROM news_event_members m
               WHERE datetime(m.created_at)<datetime('now',?)
                 AND NOT EXISTS (
                   SELECT 1 FROM hotspot_preparation_sets h
                   WHERE h.event_group_id=m.event_group_id
                 )
                 AND NOT EXISTS (
                   SELECT 1 FROM market_focus_cycle_events e
                   WHERE e.event_group_id=m.event_group_id
                 )
               ORDER BY datetime(m.created_at),m.id""",
            params=(f"-{settings.event_member_retention_days} days",),
        )
        stats["event_projection_retries"] = await _delete(
            db,
            table="event_projection_retries",
            id_column="retry_id",
            select_sql="""SELECT retry_id FROM event_projection_retries
               WHERE (status='completed' OR (status='failed' AND next_attempt_at IS NULL))
                 AND datetime(updated_at)<datetime('now',?)
               ORDER BY datetime(updated_at),retry_id""",
            params=(f"-{settings.projection_retry_retention_days} days",),
        )
        if settings.calendar_revision_retention_days:
            stats["calendar_event_revisions"] = await _delete(
                db, table="calendar_event_revisions", id_column="id",
                select_sql="""SELECT r.id FROM calendar_event_revisions r
                   WHERE datetime(r.created_at)<datetime('now',?)
                     AND r.id<>(SELECT MAX(x.id) FROM calendar_event_revisions x WHERE x.event_id=r.event_id)
                   ORDER BY datetime(r.created_at)""",
                params=(f"-{settings.calendar_revision_retention_days} days",),
            )
        if settings.integration_change_retention_days:
            keep_days = max(8, settings.integration_change_retention_days)
            stats["integration_changes"] = await _delete(
                db, table="integration_changes", id_column="change_sequence",
                select_sql="""SELECT c.change_sequence FROM integration_changes c
                   WHERE datetime(c.updated_at)<datetime('now',?)
                     AND c.change_sequence<>(
                       SELECT MAX(x.change_sequence) FROM integration_changes x
                       WHERE x.entity_type=c.entity_type AND x.entity_id=c.entity_id
                     ) ORDER BY c.change_sequence""",
                params=(f"-{keep_days} days",),
            )
        if settings.news_item_retention_days:
            # Deleting a news row must not delete its latest valid analysis.
            stats["news_items"] = await _delete(
                db, table="news_items", id_column="id",
                select_sql="""SELECT n.id FROM news_items n
                   WHERE datetime(COALESCE(n.published_at,n.fetched_at))<datetime('now',?)
                     AND NOT EXISTS (SELECT 1 FROM analysis_revisions r WHERE r.news_id=n.id)
                     AND NOT EXISTS (SELECT 1 FROM analysis_jobs j WHERE j.news_id=n.id AND j.status IN ('pending','queued','in_progress'))
                   ORDER BY datetime(COALESCE(n.published_at,n.fetched_at))""",
                params=(f"-{settings.news_item_retention_days} days",),
            )
        async with db.execute("PRAGMA foreign_key_check") as cursor:
            violations = await cursor.fetchall()
        if violations:
            raise RuntimeError("retention_foreign_key_violation")
        await db.commit()
        stats.update(await _passive_wal_checkpoint(db))
        after = await _database_page_metrics(db)
        stats.update({f"{key}_after": value for key, value in after.items()})
        stats["database_bytes_reclaimed"] = max(
            0,
            stats["database_live_bytes_before"] - stats["database_live_bytes_after"],
        )
        stats["database_bytes"] = stats["database_bytes_after"]
        stats["live_bytes"] = stats["database_live_bytes_after"]
        return stats
    except Exception:
        await db.rollback()
        raise
