from __future__ import annotations

from dataclasses import dataclass

import aiosqlite


@dataclass(frozen=True)
class OpenAIInflightCounts:
    """Provider work that already owns or may own a remote execution slot."""

    news: int = 0
    calendar: int = 0
    market_focus: int = 0

    @property
    def total(self) -> int:
        return self.news + self.calendar + self.market_focus


async def _table_exists(db: aiosqlite.Connection, table: str) -> bool:
    async with db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ) as cursor:
        return await cursor.fetchone() is not None


async def _count_news(
    db: aiosqlite.Connection,
    *,
    now_text: str,
    unknown_submission_cutoff: str,
) -> int:
    if not await _table_exists(db, "analysis_jobs"):
        return 0
    async with db.execute(
        """SELECT COUNT(*) FROM analysis_jobs
           WHERE (openai_response_id IS NOT NULL AND (
                    status IN ('queued','in_progress')
                    OR (status='cancelled' AND error_code IN (
                        'upstream_cancel_pending','upstream_cancel_observe'
                    ))
                 ))
              OR (openai_response_id IS NULL AND lease_owner IS NOT NULL
                  AND lease_expires_at > ?
                  AND status IN ('pending','queued','in_progress'))
              OR (openai_response_id IS NULL AND status='failed'
                  AND error_code='submission_outcome_unknown'
                  AND execution_mode='background' AND submitted_at IS NOT NULL
                  AND submitted_at >= ?)""",
        (now_text, unknown_submission_cutoff),
    ) as cursor:
        return int((await cursor.fetchone())[0])


async def _count_calendar(
    db: aiosqlite.Connection,
    *,
    now_text: str,
    unknown_submission_cutoff: str,
) -> int:
    if not await _table_exists(db, "calendar_analysis_jobs"):
        return 0
    async with db.execute(
        """SELECT COUNT(*) FROM calendar_analysis_jobs
           WHERE (openai_response_id IS NOT NULL
                  AND status IN ('queued','in_progress'))
              OR (openai_response_id IS NULL AND lease_owner IS NOT NULL
                  AND lease_expires_at > ?
                  AND status IN ('pending','queued','in_progress'))
              OR (openai_response_id IS NULL AND status='failed'
                  AND error_code='submission_outcome_unknown'
                  AND execution_mode='background' AND submitted_at IS NOT NULL
                  AND submitted_at >= ?)""",
        (now_text, unknown_submission_cutoff),
    ) as cursor:
        return int((await cursor.fetchone())[0])


async def _count_market_focus(
    db: aiosqlite.Connection,
    *,
    now_text: str,
    unknown_submission_cutoff: str,
) -> int:
    if not await _table_exists(db, "market_focus_cycles"):
        return 0
    async with db.execute(
        """SELECT COUNT(*) FROM market_focus_cycles
           WHERE (openai_response_id IS NOT NULL AND (
                    status IN ('queued','in_progress')
                    OR (status='cancelled' AND error_code IN (
                        'upstream_cancel_pending','upstream_cancel_observe'
                    ))
                 ))
              OR (openai_response_id IS NULL AND lease_owner IS NOT NULL
                  AND lease_expires_at > ?
                  AND status IN ('pending','queued','in_progress'))
              OR (openai_response_id IS NULL AND status='failed'
                  AND error_code='submission_outcome_unknown'
                  AND execution_mode='background' AND completed_at IS NOT NULL
                  AND completed_at >= ?)""",
        (now_text, unknown_submission_cutoff),
    ) as cursor:
        return int((await cursor.fetchone())[0])


async def openai_inflight_counts(
    db: aiosqlite.Connection,
    *,
    now_text: str,
    unknown_submission_cutoff: str,
) -> OpenAIInflightCounts:
    """Count all durable queues before a worker opens another provider request.

    Callers hold a SQLite write transaction while checking this value and
    claiming work, so workers cannot concurrently observe the same free slot.
    Existing response identities remain countable until they reach a terminal
    state; polling those identities never consumes a second slot.
    """

    return OpenAIInflightCounts(
        news=await _count_news(
            db,
            now_text=now_text,
            unknown_submission_cutoff=unknown_submission_cutoff,
        ),
        calendar=await _count_calendar(
            db,
            now_text=now_text,
            unknown_submission_cutoff=unknown_submission_cutoff,
        ),
        market_focus=await _count_market_focus(
            db,
            now_text=now_text,
            unknown_submission_cutoff=unknown_submission_cutoff,
        ),
    )
