from __future__ import annotations

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
    return max(0, cursor.rowcount)


async def cleanup_extended_retention(db: aiosqlite.Connection) -> dict[str, int]:
    """Delete bounded obsolete rows in dependency order without moving revisions backwards."""

    stats: dict[str, int] = {
        "analysis_jobs": 0,
        "analysis_revisions": 0,
        "analysis_stock_impacts": 0,
        "news_event_groups": 0,
        "market_focus_cycles": 0,
        "calendar_event_revisions": 0,
        "integration_changes": 0,
        "news_items": 0,
    }
    await db.execute("BEGIN IMMEDIATE")
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
        if settings.analysis_cycle_retention_days:
            # Completed cycles are final research records. Only obsolete
            # terminal failures are eligible for removal.
            stats["market_focus_cycles"] = await _delete(
                db, table="market_focus_cycles", id_column="cycle_id",
                select_sql="""SELECT cycle_id FROM market_focus_cycles
                   WHERE status IN ('failed','cancelled','budget_blocked','incomplete_output')
                     AND datetime(updated_at)<datetime('now',?)
                   ORDER BY datetime(updated_at)""",
                params=(f"-{settings.analysis_cycle_retention_days} days",),
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
        return stats
    except Exception:
        await db.rollback()
        raise
