from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import date, datetime, timedelta, timezone

import pytest

from app.config import Settings, settings
from app.models import database
from app.services.analysis_jobs import create_or_get_job, enqueue_auto_jobs
from app.services.focus_context import (
    FOCUS_SCHEMA_SHA256,
    FocusContext,
    persist_focus_context,
)
from app.services.market_focus import (
    CycleConflict,
    calculate_hot_score,
    create_market_focus_cycle,
    get_hotspot_status,
    ingest_event_evidence,
    list_prepared_hotspots,
    request_market_focus_cancel,
    retry_market_focus_cycle,
    run_market_focus_worker_once,
    validate_ticker_association,
)
from app.services.responses_runtime import ProviderCapabilities, ResponseResult
from app.services.retention import cleanup_extended_retention
from app.integrations.option_pro.repository import query_feed, query_ticker
from app.services.finnhub_client import fetch_finnhub_company_news
from app.services.massive_client import fetch_massive_focus_news
from app.utils.dedup import deduplicate_batch
from app.services.market_schedule import (
    EASTERN,
    due_cycle_trigger,
    is_nyse_early_close,
    is_nyse_trading_day,
    scheduled_slots_for_day,
)


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def isolated_market_db(tmp_path, monkeypatch):
    path = tmp_path / "market-focus.db"
    monkeypatch.setattr(database, "DB_PATH", str(path))
    run(database.init_db())
    return path


def news(index: int, *, source: str = "reuters", title: str | None = None) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "source": source,
        "title": title or f"NVDA earnings guidance rises materially {index}",
        "summary": "The company raised revenue guidance after its earnings report.",
        "url": f"https://example.test/{source}/{index}",
        "image_url": None,
        "published_at": now,
        "fetched_at": now,
        "content_hash": hashlib.sha256(f"focus-{index}-{source}".encode()).hexdigest(),
        "source_tickers": ["NVDA"],
        "ticker_association_method": "provider_tag",
    }


def test_cost_defaults_and_named_persistence_tables(isolated_market_db):
    configured = Settings(_env_file=None)
    assert configured.news_llm_auto_analyze_enabled is False
    assert configured.news_item_max_output_tokens == 32768
    assert configured.hot_cycle_max_output_tokens == 49152
    assert configured.openai_max_output_tokens == 128000

    async def scenario():
        db = await database.get_db()
        try:
            names = {
                row[0]
                for row in await (await db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )).fetchall()
            }
            assert "analysis_revisions" in names
            assert "hotspot_preparation_sets" in names
            assert "market_focus_cycles" in names
            assert "news_ticker_mentions" in names
        finally:
            await db.close()

    run(scenario())


def test_auto_analysis_requires_both_budgets_and_never_changes_pending_news(
    isolated_market_db, monkeypatch
):
    async def seed():
        db = await database.get_db()
        try:
            return await database.insert_news_item(db, news(1))
        finally:
            await db.close()

    news_id = run(seed())
    monkeypatch.setattr(settings, "news_llm_auto_analyze_enabled", True)
    monkeypatch.setattr(settings, "news_llm_daily_job_limit", 10)
    monkeypatch.setattr(settings, "news_llm_daily_output_token_limit", None)
    assert settings.automatic_news_analysis_capability == "budget_configuration_required"
    assert run(enqueue_auto_jobs()) == 0

    async def verify():
        db = await database.get_db()
        try:
            row = await (await db.execute(
                "SELECT analysis_status,analysis_error FROM news_items WHERE id=?", (news_id,)
            )).fetchone()
            count = await (await db.execute("SELECT COUNT(*) FROM analysis_jobs")).fetchone()
            assert tuple(row) == ("pending", "")
            assert count[0] == 0
        finally:
            await db.close()

    run(verify())


def test_manual_analysis_budget_is_independent_from_automatic_budget(
    isolated_market_db, monkeypatch
):
    monkeypatch.setattr(settings, "news_llm_daily_job_limit", 1)
    monkeypatch.setattr(settings, "news_llm_daily_output_token_limit", 1_000_000)
    monkeypatch.setattr(settings, "news_llm_manual_daily_job_limit", 10)
    monkeypatch.setattr(settings, "news_llm_manual_daily_output_token_limit", 1_000_000)

    async def scenario():
        db = await database.get_db()
        try:
            first_id = await database.insert_news_item(db, news(2))
            second_id = await database.insert_news_item(db, news(3))
            first = await create_or_get_job(db, first_id, request_origin="automatic")
            automatic_blocked = await create_or_get_job(
                db, second_id, request_origin="automatic"
            )
            manual = await create_or_get_job(db, second_id, request_origin="manual")
            assert first.job["status"] == "pending"
            assert automatic_blocked.job["status"] == "budget_blocked"
            assert manual.job["status"] == "pending"
            assert manual.job["request_origin"] == "manual"
            assert manual.job["job_id"] != automatic_blocked.job["job_id"]
        finally:
            await db.close()

    run(scenario())


def test_hot_score_renormalizes_missing_market_confirmation():
    scored = calculate_hot_score({
        "severity": 80,
        "focus_relevance": 80,
        "novelty": 80,
        "source_diversity": 80,
        "source_quality": 80,
        "market_confirmation": None,
    })
    assert scored.score == 80
    assert "market_confirmation" not in scored.active_weights
    assert pytest.approx(sum(scored.active_weights.values())) == 1


def test_ticker_validation_never_promotes_ambiguous_or_inferred_external():
    assert validate_ticker_association(
        "AI", association_method="exact_alias", focus_symbols=set()
    ) == "ambiguous"
    assert validate_ticker_association(
        "XYZ", association_method="llm_inference", focus_symbols=set()
    ) == "unverified"
    assert validate_ticker_association(
        "XYZ", association_method="provider_tag", focus_symbols=set()
    ) == "valid_external"
    assert validate_ticker_association(
        "AI", association_method="exact_alias", focus_symbols={"AI"}
    ) == "canonical"


def test_cross_source_members_are_preserved_and_revision_is_monotonic(
    isolated_market_db, monkeypatch
):
    async def scenario():
        db = await database.get_db()
        try:
            first = news(10, source="finnhub/Reuters")
            first_id = await database.insert_news_item(db, first)
            first_group = await ingest_event_evidence(db, first, news_id=first_id)
            second = news(11, source="massive/Reuters", title=first["title"])
            second_id = await database.insert_news_item(db, second)
            second_group = await ingest_event_evidence(db, second, news_id=second_id)
            assert first_group == second_group
            group = await (await db.execute(
                "SELECT member_count,source_count FROM news_event_groups WHERE event_group_id=?",
                (first_group,),
            )).fetchone()
            # Two source adapters carrying Reuters are one independent source.
            assert tuple(group) == (2, 1)
            revisions = await (await db.execute(
                "SELECT prepared_revision FROM hotspot_preparation_sets ORDER BY prepared_revision"
            )).fetchall()
            assert [row[0] for row in revisions] == sorted({row[0] for row in revisions})
        finally:
            await db.close()

    run(scenario())


def test_event_members_receive_raw_cross_source_records_before_batch_dedup(
    isolated_market_db,
):
    first = news(12, source="finnhub/Reuters")
    second = news(13, source="massive/Reuters", title=first["title"])
    unique, duplicate_count = deduplicate_batch([first, second])
    assert len(unique) == 1 and duplicate_count == 1

    async def scenario():
        db = await database.get_db()
        try:
            first_id = await database.insert_news_item(db, first)
            group = await ingest_event_evidence(db, first, news_id=first_id)
            # The second source remains evidence even if the representative
            # news table's pre-insert dedup would discard it.
            await ingest_event_evidence(db, second, news_id=None)
            count = await (await db.execute(
                "SELECT COUNT(*) FROM news_event_members WHERE event_group_id=?", (group,)
            )).fetchone()
            assert count[0] == 2
        finally:
            await db.close()

    run(scenario())


def test_hotspot_history_reads_immutable_preparation_snapshot(
    isolated_market_db,
):
    async def scenario():
        db = await database.get_db()
        try:
            item = news(14, source="reuters")
            news_id = await database.insert_news_item(db, item)
            group_id = await ingest_event_evidence(db, item, news_id=news_id)
            cutoff = datetime.now(timezone.utc) + timedelta(minutes=1)
            await db.execute(
                """UPDATE news_event_groups SET source_count=99,
                   source_names_json='[\"future-source\"]',
                   validated_tickers_json='[\"FUTR\"]',available_at=?
                   WHERE event_group_id=?""",
                ((cutoff + timedelta(days=1)).isoformat(), group_id),
            )
            await db.commit()

            rows = await list_prepared_hotspots(db, limit=20, as_of=cutoff)
            assert len(rows) == 1
            assert rows[0]["source_count"] == 1
            assert rows[0]["source_names"] == ["reuters"]
            assert rows[0]["validated_tickers"] == ["NVDA"]
        finally:
            await db.close()

    run(scenario())


def test_manual_cycle_is_idempotent_and_uses_immutable_prepared_snapshot(
    isolated_market_db, monkeypatch
):
    monkeypatch.setattr(settings, "hot_cycle_enabled", True)
    monkeypatch.setattr(settings, "hot_cycle_daily_job_limit", 10)
    monkeypatch.setattr(settings, "hot_cycle_daily_output_token_limit", 1_000_000)
    monkeypatch.setattr(settings, "hot_cycle_manual_cooldown_seconds", 0)

    async def scenario():
        db = await database.get_db()
        try:
            item = news(20)
            news_id = await database.insert_news_item(db, item)
            await ingest_event_evidence(db, item, news_id=news_id)
            status = await get_hotspot_status(db)
            assert status["manual_enabled"] is True
            first = await create_market_focus_cycle(
                db, trigger_type="manual", expected_prepared_revision=status["prepared_revision"]
            )
            replay = await create_market_focus_cycle(
                db, trigger_type="manual", expected_prepared_revision=status["prepared_revision"]
            )
            assert first["cycle_id"] == replay["cycle_id"]
            assert first["event_group_count"] == 1
            snapshot = await (await db.execute(
                "SELECT snapshot_json FROM market_focus_cycle_events WHERE cycle_id=?",
                (first["cycle_id"],),
            )).fetchone()
            assert json.loads(snapshot[0])["representative_title"] == item["title"]
        finally:
            await db.close()

    run(scenario())


def test_focus_contract_hash_and_stale_snapshot_storage(isolated_market_db):
    payload = {
        "schema_version": "option-pro-macrolens-focus-v1",
        "schema_sha256": FOCUS_SCHEMA_SHA256,
        "revision": 1,
        "as_of": "2026-07-13T12:00:00Z",
        "data_through": "2026-07-13T11:59:00Z",
        "market_session": "regular",
        "universe_version": "u1",
        "symbols": [{
            "ticker": "NVDA",
            "validation_status": "canonical",
            "universe_reasons": ["dollar_volume_top20"],
            "as_of": "2026-07-13T12:00:00Z",
            "data_quality": 0.9,
            "data_status": "active",
        }],
        "major_market_symbols": ["SPY"],
        "warnings": [],
    }
    context = FocusContext.model_validate_json(json.dumps(payload))

    async def scenario():
        db = await database.get_db()
        try:
            assert await persist_focus_context(db, context) is True
            row = await (await db.execute(
                "SELECT revision,status FROM focus_context_snapshots"
            )).fetchone()
            assert tuple(row) == (1, "current")
        finally:
            await db.close()

    run(scenario())


def test_nyse_schedule_handles_dst_weekends_holidays_and_early_close(monkeypatch):
    monkeypatch.setattr(settings, "hot_cycle_times_et", "08:00,12:00,16:00")
    monkeypatch.setattr(settings, "hot_cycle_optional_20_et", False)
    assert not is_nyse_trading_day(date(2026, 7, 4))
    assert not scheduled_slots_for_day(date(2026, 7, 4))
    assert is_nyse_early_close(date(2026, 11, 27))
    close_slot = scheduled_slots_for_day(date(2026, 11, 27))[-1]
    assert close_slot[0] == "scheduled_1600"
    assert close_slot[1].hour == 13
    summer = datetime(2026, 7, 13, 12, 0, tzinfo=EASTERN)
    winter = datetime(2026, 12, 14, 12, 0, tzinfo=EASTERN)
    assert due_cycle_trigger(summer) == "scheduled_1200"
    assert due_cycle_trigger(winter) == "scheduled_1200"
    assert summer.utcoffset() != winter.utcoffset()


def _enable_cycles(monkeypatch):
    monkeypatch.setattr(settings, "hot_cycle_enabled", True)
    monkeypatch.setattr(settings, "hot_cycle_daily_job_limit", 100)
    monkeypatch.setattr(settings, "hot_cycle_daily_output_token_limit", 10_000_000)
    monkeypatch.setattr(settings, "hot_cycle_manual_cooldown_seconds", 0)
    monkeypatch.setattr(settings, "openai_execution_mode", "background")


async def _seed_hotspot(db, index: int, *, source: str = "reuters"):
    item = news(index, source=source)
    news_id = await database.insert_news_item(db, item)
    await ingest_event_evidence(db, item, news_id=news_id)
    return item


class BlockingCycleProvider:
    def __init__(self):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.create_calls = 0

    def capabilities(self):
        return ProviderCapabilities("ok", True, True, True, True, True)

    async def create_background(self, model_input, **kwargs):
        self.create_calls += 1
        self.entered.set()
        await self.release.wait()
        return ResponseResult("resp-cycle", "queued")

    async def create_sync(self, model_input, **kwargs):
        return await self.create_background(model_input, **kwargs)

    async def retrieve(self, response_id):
        return ResponseResult(response_id, "queued")

    async def cancel(self, response_id):
        return ResponseResult(response_id, "cancelled")


class BlockingCompletedCycleProvider(BlockingCycleProvider):
    def __init__(self):
        super().__init__()
        self.snapshot: dict = {}

    async def create_background(self, model_input, **kwargs):
        self.create_calls += 1
        self.snapshot = json.loads(model_input.split("\n")[1])
        self.entered.set()
        await self.release.wait()
        payload = {
            "cycle_id": self.snapshot["cycle_id"],
            "as_of": datetime.now(timezone.utc).isoformat(),
            "market_summary": "Bounded completed result.",
            "dominant_events": [],
            "market_uncertainties": [],
            "affected_sectors": [],
            "focus_ticker_assessments": [],
            "no_new_material_catalyst": self.snapshot["no_new_hot_events"],
            "insufficient_context": False,
        }
        return ResponseResult(None, "completed", output_text=json.dumps(payload))


def test_cycle_lease_and_fencing_prevent_two_workers_from_submitting(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)

    async def scenario():
        db = await database.get_db()
        try:
            await _seed_hotspot(db, 30)
            await create_market_focus_cycle(db, trigger_type="manual")
        finally:
            await db.close()
        provider = BlockingCycleProvider()
        first = asyncio.create_task(
            run_market_focus_worker_once(provider=provider, worker_id="cycle-worker-1")
        )
        await provider.entered.wait()
        second = await run_market_focus_worker_once(provider=provider, worker_id="cycle-worker-2")
        assert second is False
        provider.release.set()
        assert await first is True
        assert provider.create_calls == 1
        db = await database.get_db()
        try:
            row = await (await db.execute(
                "SELECT fencing_token,openai_response_id FROM market_focus_cycles"
            )).fetchone()
            assert tuple(row) == (1, "resp-cycle")
        finally:
            await db.close()

    run(scenario())


def test_worker_sync_lease_covers_timeout_and_stale_worker_cannot_publish(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)
    monkeypatch.setattr(settings, "openai_execution_mode", "worker_sync")
    monkeypatch.setattr(settings, "openai_sync_timeout_seconds", 900)
    monkeypatch.setattr(settings, "analysis_worker_lease_seconds", 120)

    async def scenario():
        db = await database.get_db()
        try:
            await _seed_hotspot(db, 301)
            cycle = await create_market_focus_cycle(db, trigger_type="manual")
        finally:
            await db.close()

        provider = BlockingCompletedCycleProvider()
        old_worker = asyncio.create_task(
            run_market_focus_worker_once(provider=provider, worker_id="stale-worker")
        )
        await provider.entered.wait()
        db = await database.get_db()
        try:
            claimed = await (await db.execute(
                """SELECT fencing_token,lease_owner,lease_expires_at
                   FROM market_focus_cycles WHERE cycle_id=?""",
                (cycle["cycle_id"],),
            )).fetchone()
            lease_expires = datetime.fromisoformat(str(claimed[2]).replace("Z", "+00:00"))
            assert (lease_expires - datetime.now(timezone.utc)).total_seconds() > 850
            await db.execute(
                """UPDATE market_focus_cycles SET fencing_token=fencing_token+1,
                   lease_owner='replacement-worker',lease_expires_at=?
                   WHERE cycle_id=?""",
                (
                    (datetime.now(timezone.utc) + timedelta(minutes=20)).isoformat(),
                    cycle["cycle_id"],
                ),
            )
            await db.commit()
        finally:
            await db.close()

        provider.release.set()
        assert await old_worker is True
        db = await database.get_db()
        try:
            row = await (await db.execute(
                "SELECT status,result_json,fencing_token FROM market_focus_cycles WHERE cycle_id=?",
                (cycle["cycle_id"],),
            )).fetchone()
            assert tuple(row) == ("in_progress", None, 2)
            assert (await get_hotspot_status(db))["last_consumed_revision"] == 0
        finally:
            await db.close()

    run(scenario())


class OutcomeUnknownProvider(BlockingCycleProvider):
    async def create_background(self, model_input, **kwargs):
        self.create_calls += 1
        raise RuntimeError("transport outcome unknown")


def test_submission_outcome_unknown_never_retries_or_consumes_revision(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)

    async def scenario():
        db = await database.get_db()
        try:
            await _seed_hotspot(db, 31)
            await create_market_focus_cycle(db, trigger_type="manual")
        finally:
            await db.close()
        provider = OutcomeUnknownProvider()
        assert await run_market_focus_worker_once(provider=provider, worker_id="unknown-1") is True
        assert await run_market_focus_worker_once(provider=provider, worker_id="unknown-2") is False
        db = await database.get_db()
        try:
            cycle = await (await db.execute(
                "SELECT status,error_code FROM market_focus_cycles"
            )).fetchone()
            state = await get_hotspot_status(db)
            assert tuple(cycle) == ("failed", "submission_outcome_unknown")
            assert state["last_consumed_revision"] == 0
            assert state["prepared_hot_count"] == 0
            assert state["active_cycle_id"] is None
            lease = await (await db.execute(
                "SELECT status,leased_cycle_id FROM hotspot_preparation_sets"
            )).fetchone()
            assert lease[0] == "LEASED" and lease[1] is not None
            assert provider.create_calls == 1
        finally:
            await db.close()

    run(scenario())


def test_expired_unlinked_submission_clears_active_without_releasing_or_budgeting_twice(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)

    async def scenario():
        db = await database.get_db()
        try:
            await _seed_hotspot(db, 311)
            cycle = await create_market_focus_cycle(db, trigger_type="manual")
            expired = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
            await db.execute(
                """UPDATE market_focus_cycles SET status='in_progress',attempt_count=1,
                   lease_owner='dead-worker',lease_expires_at=?,fencing_token=1
                   WHERE cycle_id=?""",
                (expired, cycle["cycle_id"]),
            )
            await db.commit()
        finally:
            await db.close()

        provider = OutcomeUnknownProvider()
        assert await run_market_focus_worker_once(
            provider=provider, worker_id="recovery-worker"
        ) is False
        assert provider.create_calls == 0

        db = await database.get_db()
        try:
            row = await (await db.execute(
                """SELECT status,error_code,fencing_token FROM market_focus_cycles
                   WHERE cycle_id=?""",
                (cycle["cycle_id"],),
            )).fetchone()
            assert tuple(row) == ("failed", "submission_outcome_unknown", 2)
            assert (await get_hotspot_status(db))["active_cycle_id"] is None
            lease = await (await db.execute(
                "SELECT status,leased_cycle_id FROM hotspot_preparation_sets"
            )).fetchone()
            assert tuple(lease) == ("LEASED", cycle["cycle_id"])

            await _seed_hotspot(db, 312)
            status = await get_hotspot_status(db)
            assert status["manual_enabled"] is True
            monkeypatch.setattr(
                settings,
                "hot_cycle_daily_output_token_limit",
                settings.hot_cycle_max_output_tokens,
            )
            with pytest.raises(CycleConflict) as caught:
                await create_market_focus_cycle(
                    db,
                    trigger_type="manual",
                    expected_prepared_revision=status["prepared_revision"],
                )
            assert getattr(caught.value, "code", None) == "daily_output_token_limit_reached"
        finally:
            await db.close()

    run(scenario())


class CancelCompletedProvider(BlockingCycleProvider):
    def __init__(self):
        super().__init__()
        self.cycle_id = ""

    async def create_background(self, model_input, **kwargs):
        self.create_calls += 1
        self.cycle_id = json.loads(model_input.split("\n")[1])["cycle_id"]
        return ResponseResult("resp-cancel-race", "queued")

    async def cancel(self, response_id):
        payload = {
            "cycle_id": self.cycle_id,
            "as_of": datetime.now(timezone.utc).isoformat(),
            "market_summary": "The bounded event remains material.",
            "dominant_events": [],
            "market_uncertainties": [],
            "affected_sectors": [],
            "focus_ticker_assessments": [],
            "no_new_material_catalyst": False,
            "insufficient_context": False,
        }
        return ResponseResult(response_id, "completed", output_text=json.dumps(payload))


class BlockingCancelObserveProvider(BlockingCycleProvider):
    def __init__(self):
        super().__init__()
        self.cancel_entered = asyncio.Event()
        self.cancel_release = asyncio.Event()

    async def create_background(self, model_input, **kwargs):
        self.create_calls += 1
        return ResponseResult("resp-cancel-observe", "queued")

    async def cancel(self, response_id):
        self.cancel_entered.set()
        await self.cancel_release.wait()
        return ResponseResult(response_id, "queued")


def test_cancel_completion_race_publishes_completed_result_once(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)

    async def scenario():
        db = await database.get_db()
        try:
            await _seed_hotspot(db, 32)
            cycle = await create_market_focus_cycle(db, trigger_type="manual")
        finally:
            await db.close()
        provider = CancelCompletedProvider()
        assert await run_market_focus_worker_once(provider=provider, worker_id="cancel-1") is True
        db = await database.get_db()
        try:
            requested = await request_market_focus_cancel(db, cycle["cycle_id"])
            assert requested["cancel_requested_at"] is not None
        finally:
            await db.close()
        assert await run_market_focus_worker_once(provider=provider, worker_id="cancel-2") is True
        db = await database.get_db()
        try:
            row = await (await db.execute(
                "SELECT status,result_json FROM market_focus_cycles"
            )).fetchone()
            assert row[0] == "completed"
            assert json.loads(row[1])["cycle_id"] == cycle["cycle_id"]
            assert (await get_hotspot_status(db))["last_consumed_revision"] == 1
        finally:
            await db.close()

    run(scenario())


def test_stale_cancel_observer_cannot_overwrite_replacement_lease(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)

    async def scenario():
        db = await database.get_db()
        try:
            await _seed_hotspot(db, 321)
            cycle = await create_market_focus_cycle(db, trigger_type="manual")
        finally:
            await db.close()

        provider = BlockingCancelObserveProvider()
        assert await run_market_focus_worker_once(
            provider=provider, worker_id="cancel-submit"
        ) is True
        db = await database.get_db()
        try:
            requested = await request_market_focus_cancel(db, cycle["cycle_id"])
            assert requested["cancel_requested_at"] is not None
        finally:
            await db.close()

        old_worker = asyncio.create_task(
            run_market_focus_worker_once(provider=provider, worker_id="cancel-old")
        )
        await provider.cancel_entered.wait()
        db = await database.get_db()
        try:
            await db.execute(
                """UPDATE market_focus_cycles SET fencing_token=fencing_token+1,
                   lease_owner='cancel-replacement',lease_expires_at=?
                   WHERE cycle_id=?""",
                (
                    (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat(),
                    cycle["cycle_id"],
                ),
            )
            await db.commit()
        finally:
            await db.close()

        provider.cancel_release.set()
        assert await old_worker is False
        db = await database.get_db()
        try:
            row = await (await db.execute(
                """SELECT status,error_code,lease_owner,fencing_token
                   FROM market_focus_cycles WHERE cycle_id=?""",
                (cycle["cycle_id"],),
            )).fetchone()
            assert row[0] == "in_progress"
            assert row[1] != "upstream_cancel_observe"
            assert row[2] == "cancel-replacement"
            assert row[3] == 3
        finally:
            await db.close()

    run(scenario())


class IncompleteCycleProvider(BlockingCycleProvider):
    async def create_background(self, model_input, **kwargs):
        return ResponseResult(
            None,
            "incomplete",
            error_code="max_output_tokens",
            usage_input_tokens=100,
            usage_cached_input_tokens=20,
            usage_cache_write_tokens=10,
            usage_reasoning_tokens=40,
            usage_output_tokens=50,
            usage_total_tokens=150,
        )


class ExpiredCycleProvider(BlockingCycleProvider):
    async def create_background(self, model_input, **kwargs):
        return ResponseResult(None, "expired", error_code="provider_expired")


class CompletedCycleProvider(BlockingCycleProvider):
    async def create_background(self, model_input, **kwargs):
        snapshot = json.loads(model_input.split("\n")[1])
        payload = {
            "cycle_id": snapshot["cycle_id"],
            "as_of": datetime.now(timezone.utc).isoformat(),
            "market_summary": "Bounded catalyst summary.",
            "dominant_events": [],
            "market_uncertainties": [],
            "affected_sectors": [],
            "focus_ticker_assessments": [],
            "no_new_material_catalyst": snapshot["no_new_hot_events"],
            "insufficient_context": False,
        }
        return ResponseResult(None, "completed", output_text=json.dumps(payload))


class TickerAssessmentProvider(BlockingCycleProvider):
    async def create_background(self, model_input, **kwargs):
        snapshot = json.loads(model_input.split("\n")[1])
        event_id = snapshot["events"][0]["event_group_id"]
        payload = {
            "cycle_id": snapshot["cycle_id"],
            "as_of": datetime.now(timezone.utc).isoformat(),
            "market_summary": "Bounded catalyst summary.",
            "dominant_events": [{
                "event_group_id": event_id,
                "summary": "Guidance changed.",
                "affected_sectors": ["semiconductors"],
            }],
            "market_uncertainties": [],
            "affected_sectors": ["semiconductors"],
            "focus_ticker_assessments": [{
                "ticker": "NVDA",
                "catalyst_bias": 40,
                "confidence": 80,
                "horizon": "days",
                "supporting_event_ids": [event_id],
                "conflicting_event_ids": [],
                "summary": "The supplied event supports a positive catalyst context.",
                "risks": [],
                "insufficient_evidence": False,
            }],
            "no_new_material_catalyst": False,
            "insufficient_context": False,
        }
        return ResponseResult(None, "completed", output_text=json.dumps(payload))


def test_incomplete_cycle_records_usage_without_publishing_or_consuming(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)

    async def scenario():
        db = await database.get_db()
        try:
            await _seed_hotspot(db, 33)
            await create_market_focus_cycle(db, trigger_type="manual")
        finally:
            await db.close()
        assert await run_market_focus_worker_once(
            provider=IncompleteCycleProvider(), worker_id="incomplete"
        ) is True
        db = await database.get_db()
        try:
            row = await (await db.execute(
                """SELECT status,result_json,usage_cache_write_tokens,
                          usage_reasoning_tokens,usage_total_tokens FROM market_focus_cycles"""
            )).fetchone()
            assert tuple(row) == ("incomplete_output", None, 10, 40, 150)
            state = await get_hotspot_status(db)
            assert state["last_consumed_revision"] == 0
            assert state["prepared_hot_count"] == 1
        finally:
            await db.close()

    run(scenario())


def test_explicit_retry_is_append_only_and_reuses_immutable_event_snapshot(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)

    async def scenario():
        db = await database.get_db()
        try:
            await _seed_hotspot(db, 35)
            parent = await create_market_focus_cycle(db, trigger_type="manual")
        finally:
            await db.close()
        assert await run_market_focus_worker_once(
            provider=IncompleteCycleProvider(), worker_id="retry-parent"
        ) is True
        db = await database.get_db()
        try:
            child = await retry_market_focus_cycle(db, parent["cycle_id"])
            assert child["retry_of_cycle_id"] == parent["cycle_id"]
            assert child["execution_number"] == 2
            rows = await (await db.execute(
                """SELECT cycle_id,snapshot_json FROM market_focus_cycle_events
                   WHERE cycle_id IN (?,?) ORDER BY cycle_id""",
                (parent["cycle_id"], child["cycle_id"]),
            )).fetchall()
            assert len(rows) == 2
            assert rows[0][1] == rows[1][1]
            parent_input = json.loads((await (await db.execute(
                "SELECT input_json FROM market_focus_cycles WHERE cycle_id=?", (parent["cycle_id"],)
            )).fetchone())[0])
            child_input = json.loads(child["input_json"])
            parent_input.pop("cycle_id")
            child_input.pop("cycle_id")
            assert child_input == parent_input
        finally:
            await db.close()

    run(scenario())


def test_expired_cycle_is_failed_not_incomplete(isolated_market_db, monkeypatch):
    _enable_cycles(monkeypatch)

    async def scenario():
        db = await database.get_db()
        try:
            await _seed_hotspot(db, 34)
            await create_market_focus_cycle(db, trigger_type="manual")
        finally:
            await db.close()
        assert await run_market_focus_worker_once(
            provider=ExpiredCycleProvider(), worker_id="expired"
        ) is True
        db = await database.get_db()
        try:
            row = await (await db.execute(
                "SELECT status,error_code,result_json FROM market_focus_cycles"
            )).fetchone()
            assert tuple(row) == ("failed", "provider_expired", None)
            assert (await get_hotspot_status(db))["last_consumed_revision"] == 0
        finally:
            await db.close()

    run(scenario())


def test_fixed_empty_cycle_does_not_consume_revision(isolated_market_db, monkeypatch):
    _enable_cycles(monkeypatch)

    async def scenario():
        db = await database.get_db()
        try:
            cycle = await create_market_focus_cycle(db, trigger_type="scheduled_0800")
            assert cycle["no_new_hot_events"] == 1
            assert cycle["consumes_through_revision"] is None
            assert (await get_hotspot_status(db))["last_consumed_revision"] == 0
        finally:
            await db.close()

    run(scenario())


def test_cycle_consumes_oldest_continuous_revision_without_skipping_ninth(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)
    monkeypatch.setattr(settings, "hot_cycle_max_events", 8)

    async def scenario():
        db = await database.get_db()
        try:
            for index in range(60, 69):
                await _seed_hotspot(db, index, source=f"reuters/{index}")
            state = await get_hotspot_status(db)
            assert state["prepared_revision"] == 9
            first = await create_market_focus_cycle(
                db, trigger_type="manual", expected_prepared_revision=9
            )
        finally:
            await db.close()
        provider = CompletedCycleProvider()
        assert await run_market_focus_worker_once(provider=provider, worker_id="batch-1") is True
        db = await database.get_db()
        try:
            first_revisions = [
                row[0] for row in await (await db.execute(
                    "SELECT prepared_revision FROM market_focus_cycle_events WHERE cycle_id=? ORDER BY prepared_revision",
                    (first["cycle_id"],),
                )).fetchall()
            ]
            assert first_revisions == list(range(1, 9))
            assert (await get_hotspot_status(db))["last_consumed_revision"] == 8
            second = await create_market_focus_cycle(
                db, trigger_type="manual", expected_prepared_revision=9
            )
            assert second["cycle_id"] != first["cycle_id"]
        finally:
            await db.close()
        assert await run_market_focus_worker_once(provider=provider, worker_id="batch-2") is True
        db = await database.get_db()
        try:
            remaining = await (await db.execute(
                "SELECT prepared_revision FROM market_focus_cycle_events WHERE cycle_id=?",
                (second["cycle_id"],),
            )).fetchall()
            assert [row[0] for row in remaining] == [9]
            assert (await get_hotspot_status(db))["last_consumed_revision"] == 9
        finally:
            await db.close()

    run(scenario())


def test_cycle_revalidates_ticker_and_adds_display_only_weighted_context(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)

    async def scenario():
        db = await database.get_db()
        try:
            context = FocusContext.model_validate_json(json.dumps({
                "schema_version": "option-pro-macrolens-focus-v1",
                "schema_sha256": FOCUS_SCHEMA_SHA256,
                "revision": 1,
                "as_of": datetime.now(timezone.utc).isoformat(),
                "data_through": datetime.now(timezone.utc).isoformat(),
                "market_session": "regular",
                "universe_version": "u1",
                "symbols": [{
                    "ticker": "NVDA",
                    "validation_status": "canonical",
                    "universe_reasons": ["dollar_volume_top20"],
                    "as_of": datetime.now(timezone.utc).isoformat(),
                    "data_quality": 0.9,
                    "data_status": "active",
                }],
                "major_market_symbols": ["SPY"],
                "warnings": [],
            }))
            await persist_focus_context(db, context)
            await _seed_hotspot(db, 70)
            await create_market_focus_cycle(db, trigger_type="manual")
        finally:
            await db.close()
        assert await run_market_focus_worker_once(
            provider=TickerAssessmentProvider(), worker_id="weighted"
        ) is True
        db = await database.get_db()
        try:
            raw = await (await db.execute(
                "SELECT result_json,input_json FROM market_focus_cycles"
            )).fetchone()
            result = json.loads(raw[0])
            model_input = json.loads(raw[1])
            assessment = result["focus_ticker_assessments"][0]
            assert result["display_only"] is True
            assert assessment["weighted_catalyst_context"] is not None
            assert set(model_input["focus_symbols"][0]).isdisjoint({
                "intrinsic_strength_score", "ranking_score", "market_fit_score", "option_score"
            })
            assert not any(
                key in json.dumps(result)
                for key in ("buy_signal", "position_size", "stop_loss", "target_price", "win_rate")
            )
        finally:
            await db.close()

    run(scenario())


def test_event_group_material_update_versions_and_out_of_window_news_stays_separate(
    isolated_market_db,
):
    async def scenario():
        db = await database.get_db()
        try:
            first = news(40, source="reuters")
            first_id = await database.insert_news_item(db, first)
            group = await ingest_event_evidence(db, first, news_id=first_id)
            update = dict(first)
            update["url"] = "https://example.test/reuters/update"
            update["content_hash"] = "b" * 64
            update_id = await database.insert_news_item(db, update)
            assert await ingest_event_evidence(db, update, news_id=update_id) == group
            version = await (await db.execute(
                "SELECT version FROM news_event_groups WHERE event_group_id=?", (group,)
            )).fetchone()
            assert version[0] == 2
            old = dict(first)
            old["content_hash"] = "c" * 64
            old["url"] = "https://example.test/reuters/old"
            old["published_at"] = "2026-06-01T12:00:00+00:00"
            old["fetched_at"] = "2026-06-01T12:00:00+00:00"
            old_id = await database.insert_news_item(db, old)
            assert await ingest_event_evidence(db, old, news_id=old_id) != group
        finally:
            await db.close()

    run(scenario())


def test_event_available_at_compares_timezone_aware_instants(isolated_market_db):
    async def scenario():
        db = await database.get_db()
        try:
            item = news(41)
            item["fetched_at"] = "2026-07-13T09:30:00-04:00"  # 13:30 UTC
            item["published_at"] = "2026-07-13T12:00:00+00:00"
            item["content_hash"] = "d" * 64
            news_id = await database.insert_news_item(db, item)
            group = await ingest_event_evidence(db, item, news_id=news_id)
            row = await (await db.execute(
                "SELECT available_at FROM news_event_groups WHERE event_group_id=?", (group,)
            )).fetchone()
            assert row[0] == "2026-07-13T13:30:00+00:00"
        finally:
            await db.close()

    run(scenario())


def test_cycle_output_requires_aware_as_of_not_before_snapshot(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)

    class OldAsOfProvider(CompletedCycleProvider):
        async def create_background(self, model_input, **kwargs):
            snapshot = json.loads(model_input.split("\n")[1])
            payload = {
                "cycle_id": snapshot["cycle_id"],
                "as_of": "2020-01-01T00:00:00+00:00",
                "market_summary": "Stale result.",
                "dominant_events": [],
                "market_uncertainties": [],
                "affected_sectors": [],
                "focus_ticker_assessments": [],
                "no_new_material_catalyst": False,
                "insufficient_context": False,
            }
            return ResponseResult(None, "completed", output_text=json.dumps(payload))

    async def scenario():
        db = await database.get_db()
        try:
            await _seed_hotspot(db, 42)
            await create_market_focus_cycle(db, trigger_type="manual")
        finally:
            await db.close()
        assert await run_market_focus_worker_once(
            provider=OldAsOfProvider(), worker_id="old-as-of"
        ) is True
        db = await database.get_db()
        try:
            row = await (await db.execute(
                "SELECT status,error_code,result_json FROM market_focus_cycles"
            )).fetchone()
            assert tuple(row) == ("failed", "invalid_structured_output", None)
        finally:
            await db.close()

    run(scenario())


def test_positive_confidence_excludes_unanalyzed_and_flag_can_exclude_at_zero(
    isolated_market_db,
):
    async def scenario():
        db = await database.get_db()
        try:
            item = news(50)
            await database.insert_news_item(db, item)
            now = datetime.now(timezone.utc)
            positive, *_ = await query_feed(
                db, as_of=now, window_hours=72, limit=10, cursor=None, source=None,
                classification=None, min_confidence=1, min_abs_impact=0,
                analysis_status=None, include_unanalyzed=True,
            )
            zero_excluded, *_ = await query_feed(
                db, as_of=now, window_hours=72, limit=10, cursor=None, source=None,
                classification=None, min_confidence=0, min_abs_impact=0,
                analysis_status=None, include_unanalyzed=False,
            )
            zero_included, *_ = await query_ticker(
                db, ticker="NVDA", as_of=now, window_hours=72, limit=10,
                cursor=None, min_confidence=0, include_neutral=True,
                include_unanalyzed=True,
            )
            assert positive == []
            assert zero_excluded == []
            assert len(zero_included) == 1
        finally:
            await db.close()

    run(scenario())


def test_retention_is_batched_keeps_latest_and_preserves_foreign_keys(
    isolated_market_db, monkeypatch
):
    monkeypatch.setattr(settings, "integration_change_retention_days", 1)
    monkeypatch.setattr(settings, "retention_batch_size", 1)

    async def scenario():
        db = await database.get_db()
        try:
            await db.execute(
                """INSERT INTO integration_changes(entity_type,entity_id,operation,payload_hash,updated_at)
                   VALUES ('news','old','upsert','a','2026-01-01T00:00:00+00:00'),
                          ('news','old','upsert','b','2026-01-02T00:00:00+00:00'),
                          ('news','latest','upsert','c','2026-01-01T00:00:00+00:00')"""
            )
            await db.commit()
            first = await cleanup_extended_retention(db)
            assert first["integration_changes"] == 1
            await cleanup_extended_retention(db)
            rows = await (await db.execute(
                "SELECT entity_id,payload_hash FROM integration_changes WHERE entity_id IN ('old','latest') ORDER BY entity_id"
            )).fetchall()
            assert [tuple(row) for row in rows] == [("latest", "c"), ("old", "b")]
            assert await (await db.execute("PRAGMA foreign_key_check")).fetchall() == []
        finally:
            await db.close()

    run(scenario())


class FakeFinnhubClient:
    def __init__(self):
        self.calls = []

    async def get(self, url, *, params, headers):
        self.calls.append(params["symbol"])

        class Response:
            def raise_for_status(self):
                return None

            def json(self):
                return []

        return Response()


def test_finnhub_focus_queries_are_deduplicated_and_bounded():
    async def scenario():
        client = FakeFinnhubClient()
        await fetch_finnhub_company_news(
            ["NVDA", "NVDA", "AMD", "AAPL"],
            "2026-07-13",
            "2026-07-13",
            api_key="test-key",
            client=client,
            request_limit=2,
        )
        assert client.calls == ["NVDA", "AMD"]

    run(scenario())


def test_massive_focus_queries_are_deduplicated_and_bounded():
    class Client(FakeFinnhubClient):
        async def get(self, url, *, params, headers):
            self.calls.append(params["ticker"])

            class Response:
                def raise_for_status(self):
                    return None

                def json(self):
                    return {"results": []}

            return Response()

    async def scenario():
        client = Client()
        await fetch_massive_focus_news(
            ["NVDA", "NVDA", "AMD", "AAPL"],
            api_key="test-key",
            client=client,
            request_limit=2,
        )
        assert client.calls == ["NVDA", "AMD"]

    run(scenario())
