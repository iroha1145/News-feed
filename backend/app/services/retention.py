from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import aiosqlite

from app.config import settings


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
    }
    before = await _database_page_metrics(db)
    stats.update({f"{key}_before": value for key, value in before.items()})
    try:
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
        after = await _database_page_metrics(db)
        stats.update({f"{key}_after": value for key, value in after.items()})
        stats["database_bytes_reclaimed"] = max(
            0,
            stats["database_live_bytes_before"] - stats["database_live_bytes_after"],
        )
        return stats
    except Exception:
        await db.rollback()
        raise
