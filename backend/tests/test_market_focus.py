from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import date, datetime, timedelta, timezone

import pytest

from app.config import Settings, settings
from app.models import database
from app.services.analysis_jobs import claim_next_job, create_or_get_job, enqueue_auto_jobs
from app.services.focus_context import (
    FOCUS_SCHEMA_SHA256,
    FocusContext,
    persist_focus_context,
    resume_focus_revalidation,
)
from app.services.market_focus import (
    CycleConflict,
    calculate_hot_score,
    calculate_weighted_catalyst_context,
    create_market_focus_cycle,
    get_hotspot_status,
    ingest_event_evidence,
    list_prepared_hotspots,
    request_market_focus_cancel,
    record_ticker_mentions,
    retry_market_focus_cycle,
    run_market_focus_worker_once,
    validate_ticker_association,
    hotspot_qualifies,
)
from app.services.ticker_lineage import (
    append_validation_revision,
    build_validation_basis_hash,
    record_ticker_mention,
    validation_as_of,
)
from app.services.responses_runtime import ProviderCapabilities, ResponseResult
from app.services import market_focus as market_focus_service
from app.services.retention import cleanup_extended_retention, _new_york_trading_date
from app.integrations.option_pro.repository import query_feed, query_ticker
from app.services.finnhub_client import fetch_finnhub_company_news, finnhub_company_news_date
from app.services.massive_client import fetch_massive_focus_news
from app.utils.dedup import deduplicate_batch
from app.services.market_schedule import (
    EASTERN,
    due_cycle_trigger,
    is_nyse_early_close,
    is_nyse_trading_day,
    next_cycle_at,
    scheduled_slots_for_day,
)


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def isolated_market_db(tmp_path, monkeypatch):
    path = tmp_path / "market-focus.db"
    monkeypatch.setattr(database, "DB_PATH", str(path))
    monkeypatch.setattr(settings, "news_llm_manual_enabled", True)
    monkeypatch.setattr(settings, "news_llm_manual_daily_job_limit", 50)
    monkeypatch.setattr(settings, "news_llm_manual_daily_output_token_limit", 1_638_400)
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
    assert configured.news_llm_manual_enabled is False
    assert configured.news_llm_manual_daily_job_limit is None
    assert configured.news_llm_manual_daily_output_token_limit is None
    assert configured.hot_cycle_manual_enabled is False
    assert configured.x_sentiment_enabled is False
    assert configured.calendar_llm_manual_enabled is False
    assert configured.calendar_llm_daily_job_limit is None
    assert configured.calendar_llm_daily_output_token_limit is None
    assert configured.manual_calendar_analysis_capability == "disabled"
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


def test_manual_news_analysis_requires_switch_and_both_budgets(
    isolated_market_db, monkeypatch
):
    async def create(index: int):
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news(index))
            return await create_or_get_job(db, news_id, request_origin="manual")
        finally:
            await db.close()

    monkeypatch.setattr(settings, "news_llm_manual_enabled", False)
    disabled = run(create(901))
    assert disabled.job["status"] == "budget_blocked"
    assert disabled.job["error_code"] == "disabled"

    monkeypatch.setattr(settings, "news_llm_manual_enabled", True)
    monkeypatch.setattr(settings, "news_llm_manual_daily_job_limit", 10)
    monkeypatch.setattr(settings, "news_llm_manual_daily_output_token_limit", None)
    unbudgeted = run(create(902))
    assert unbudgeted.job["status"] == "budget_blocked"
    assert unbudgeted.job["error_code"] == "budget_configuration_required"

    monkeypatch.setattr(settings, "news_llm_manual_daily_output_token_limit", 1_000_000)
    enabled = run(create(903))
    assert enabled.job["status"] == "pending"


def test_worker_cost_gate_blocks_old_unsubmitted_jobs_but_allows_observation(
    isolated_market_db, monkeypatch
):
    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news(920))
            created = await create_or_get_job(db, news_id, request_origin="manual")
            monkeypatch.setattr(settings, "news_llm_manual_enabled", False)
            assert await claim_next_job(db, "cost-gate-blocked") is None

            await db.execute(
                """UPDATE analysis_jobs SET status='queued',openai_response_id='resp-existing'
                   WHERE job_id=?""",
                (created.job["job_id"],),
            )
            await db.commit()
            observed = await claim_next_job(db, "cost-gate-observer")
            assert observed is not None
            assert observed["openai_response_id"] == "resp-existing"
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
    monkeypatch.setattr(settings, "news_llm_auto_analyze_enabled", True)
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


def test_conflicting_evidence_only_reduces_weighted_context():
    assessment = {
        "catalyst_bias": 80,
        "confidence": 75,
        "supporting_event_ids": ["support"],
        "conflicting_event_ids": [],
        "insufficient_evidence": False,
    }
    support_only = calculate_weighted_catalyst_context(
        assessment, {"support": 80, "conflict": 40, "conflict_2": 80}
    )
    one_conflict = calculate_weighted_catalyst_context(
        {**assessment, "conflicting_event_ids": ["conflict"]},
        {"support": 80, "conflict": 40},
    )
    more_conflict = calculate_weighted_catalyst_context(
        {**assessment, "conflicting_event_ids": ["conflict", "conflict_2"]},
        {"support": 80, "conflict": 40, "conflict_2": 80},
    )
    conflict_only = calculate_weighted_catalyst_context(
        {**assessment, "supporting_event_ids": [], "conflicting_event_ids": ["conflict"]},
        {"conflict": 40},
    )
    assert support_only["weighted_catalyst_context"] == 60
    assert 0 < more_conflict["weighted_catalyst_context"] < one_conflict["weighted_catalyst_context"] < 60
    assert one_conflict["supporting_weight"] == 80
    assert one_conflict["conflicting_weight"] == 40
    assert one_conflict["conflict_ratio"] == pytest.approx(1 / 3, abs=1e-6)
    assert conflict_only["weighted_catalyst_context"] is None
    assert conflict_only["effective_reliability"] == 0


def test_support_factor_is_bounded_monotonic_and_deduplicates_reposts(monkeypatch):
    monkeypatch.setattr(settings, "catalyst_context_support_target", 100.0)
    assessment = {
        "catalyst_bias": 80,
        "confidence": 100,
        "supporting_event_ids": ["same_event", "same_event"],
        "conflicting_event_ids": [],
        "insufficient_evidence": False,
    }
    low = calculate_weighted_catalyst_context(assessment, {"same_event": 25})
    medium = calculate_weighted_catalyst_context(
        {**assessment, "supporting_event_ids": ["same_event", "second_event"]},
        {"same_event": 25, "second_event": 25},
    )
    capped = calculate_weighted_catalyst_context(
        {**assessment, "supporting_event_ids": ["same_event", "second_event"]},
        {"same_event": 80, "second_event": 80},
    )
    assert low["supporting_weight"] == 25
    assert low["weighted_catalyst_context"] == 20
    assert medium["weighted_catalyst_context"] == 40
    assert capped["weighted_catalyst_context"] == 80


def test_event_support_score_ignores_syndicated_source_count_and_quality():
    components = {
        "severity": 80,
        "focus_relevance": 100,
        "novelty": 85,
        "source_diversity": 40,
        "source_quality": 55,
        "market_confirmation": 70,
    }
    original = market_focus_service.calculate_event_support_score(components)
    syndicated = market_focus_service.calculate_event_support_score({
        **components,
        "source_diversity": 100,
        "source_quality": 100,
    })
    assert original == syndicated


def test_weighted_context_deduplicates_same_fact_across_event_groups(monkeypatch):
    monkeypatch.setattr(settings, "catalyst_context_support_target", 100.0)
    assessment = {
        "catalyst_bias": 80,
        "confidence": 100,
        "supporting_event_ids": ["event-a", "event-b"],
        "conflicting_event_ids": [],
        "insufficient_evidence": False,
    }
    weighted = calculate_weighted_catalyst_context(
        assessment,
        {"event-a": 30, "event-b": 45},
        event_fingerprints={"event-a": "same-fact", "event-b": "same-fact"},
    )
    assert weighted["supporting_weight"] == 45
    assert weighted["weighted_catalyst_context"] == 36


def test_market_confirmation_requires_post_event_data_through_and_valid_source():
    available_at = "2026-07-13T10:05:00+00:00"
    group = {
        "available_at": available_at,
        "validated_tickers_json": '["NVDA"]',
    }
    symbol = {
        "ticker": "NVDA",
        "validation_status": "canonical",
        "as_of": "2026-07-13T10:30:00+00:00",
        "data_through": "2026-07-13T10:00:00+00:00",
        "data_status": "active",
        "source_status": "active",
        "data_quality": 1.0,
        "session_change_pct": 4.0,
        "rvol_time_of_day": 2.0,
        "breakout_state": "REACCELERATING",
    }
    focus = {"as_of": symbol["as_of"], "symbols": [symbol]}
    assert market_focus_service._market_confirmation(group, focus) is None
    symbol["data_through"] = "2026-07-13T10:10:00+00:00"
    assert market_focus_service._market_confirmation(group, focus) == 87.0
    symbol["source_status"] = "fallback"
    assert market_focus_service._market_confirmation(group, focus) is None
    symbol["source_status"] = "active"
    symbol["data_status"] = "stale"
    assert market_focus_service._market_confirmation(group, focus) is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("data_through", "2026-07-13T10:30:00+00:00"),
        ("data_status", "stale"),
        ("source_status", "fallback"),
        ("data_quality", 0.5),
        ("session_change_pct", 6.0),
        ("rvol_time_of_day", 3.0),
        ("breakout_state", "FAILED"),
    ],
)
def test_confirmation_fingerprint_covers_material_inputs_but_not_observation_clock(
    field, value
):
    payload = {
        "as_of": "2026-07-13T10:20:00+00:00",
        "symbols": [{
            "ticker": "NVDA",
            "validation_status": "canonical",
            "as_of": "2026-07-13T10:20:00+00:00",
            "data_through": "2026-07-13T10:10:00+00:00",
            "data_status": "active",
            "source_status": "active",
            "data_quality": 1.0,
            "session_change_pct": 4.0,
            "rvol_time_of_day": 2.0,
            "breakout_state": "CONFIRMED",
        }],
    }
    baseline = market_focus_service._focus_symbol_confirmation_fingerprints(payload)
    changed = json.loads(json.dumps(payload))
    changed["symbols"][0][field] = value
    assert market_focus_service._focus_symbol_confirmation_fingerprints(changed) != baseline

    clock_only = json.loads(json.dumps(payload))
    clock_only["as_of"] = "2026-07-13T10:50:00+00:00"
    clock_only["symbols"][0]["as_of"] = "2026-07-13T10:50:00+00:00"
    assert market_focus_service._focus_symbol_confirmation_fingerprints(clock_only) == baseline

def test_breakout_confirmation_mapping_covers_complete_lifecycle():
    expected = {
        "DISCOVERED", "WATCHING", "TRIGGERED", "CONFIRMED", "HOLDING",
        "RETESTING", "RETEST_HELD", "REACCELERATING", "EXTENDED", "FAILED",
        "EXPIRED",
    }
    assert market_focus_service.BREAKOUT_CONFIRMATION_MAP_VERSION == (
        "breakout-confirmation-context-v1"
    )
    assert expected <= market_focus_service.BREAKOUT_CONFIRMATION_POINTS.keys()
    assert market_focus_service.BREAKOUT_CONFIRMATION_POINTS["FAILED"] == 0
    assert (
        market_focus_service.BREAKOUT_CONFIRMATION_POINTS["EXTENDED"]
        < market_focus_service.BREAKOUT_CONFIRMATION_POINTS["REACCELERATING"]
    )


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


def test_focus_symbol_validation_states_are_preserved(isolated_market_db):
    async def scenario():
        db = await database.get_db()
        try:
            now = datetime.now(timezone.utc).isoformat()
            context = FocusContext.model_validate_json(json.dumps({
                "schema_version": "option-pro-macrolens-focus-v2",
                "schema_sha256": FOCUS_SCHEMA_SHA256,
                "revision": 1,
                "as_of": now,
                "data_through": now,
                "market_session": "regular",
                "universe_version": "validation-states-v1",
                "symbols": [
                    {
                        "ticker": "NVDA",
                        "validation_status": "canonical",
                        "universe_reasons": ["focus"],
                        "as_of": now,
                    },
                    {
                        "ticker": "AMD",
                        "validation_status": "valid_external",
                        "universe_reasons": ["external"],
                        "as_of": now,
                    },
                    {
                        "ticker": "XYZ",
                        "validation_status": "unverified",
                        "universe_reasons": ["unverified"],
                        "as_of": now,
                    },
                ],
                "major_market_symbols": ["SPY"],
                "warnings": [],
            }))
            await persist_focus_context(db, context)
            news_id = await database.insert_news_item(db, news(922))
            rows = await record_ticker_mentions(
                db,
                news_id=news_id,
                tickers=["NVDA", "AMD", "XYZ", "SPY"],
                association_method="exact_alias",
                source="alias_dictionary",
            )
            assert {
                row["ticker"]: row["validation_status"] for row in rows
            } == {
                "NVDA": "canonical",
                "AMD": "valid_external",
                "XYZ": "unverified",
                "SPY": "canonical",
            }
        finally:
            await db.close()

    run(scenario())


def test_invalid_model_ticker_is_counted_without_storing_raw_value(isolated_market_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news(904))
            rows = await record_ticker_mentions(
                db,
                news_id=news_id,
                tickers=["DROP TABLE news_items"],
                association_method="llm_inference",
                source="model_output",
            )
            assert rows == [{
                "ticker": "",
                "validation_status": "invalid",
                "association_confidence": 0.0,
            }]
            count = await (await db.execute(
                "SELECT count FROM projection_safety_counters WHERE counter_key='invalid_ticker_association'"
            )).fetchone()
            mentions = await (await db.execute(
                "SELECT ticker FROM news_ticker_mentions WHERE news_id=?", (news_id,)
            )).fetchall()
            assert count[0] == 1
            assert mentions == []
        finally:
            await db.close()

    run(scenario())


def test_low_severity_hotspot_requires_two_sources_and_market_confirmation():
    assert hotspot_qualifies(
        99,
        source_count=1,
        has_trusted_ticker=True,
        event_type="ordinary_price_target",
        market_confirmation=None,
    )[0] is False
    assert hotspot_qualifies(
        99,
        source_count=2,
        has_trusted_ticker=True,
        event_type="analyst_action",
        market_confirmation=None,
    )[0] is False
    assert hotspot_qualifies(
        99,
        source_count=2,
        has_trusted_ticker=True,
        event_type="analyst_action",
        market_confirmation=75,
    )[0] is True


def test_ordinary_price_target_does_not_prepare_without_market_confirmation(
    isolated_market_db,
):
    async def scenario():
        db = await database.get_db()
        try:
            first = news(
                905,
                source="finnhub/Reuters",
                title="NVDA analyst raises ordinary price target to $225",
            )
            first_id = await database.insert_news_item(db, first)
            group = await ingest_event_evidence(db, first, news_id=first_id)
            second = news(
                906,
                source="massive/Bloomberg",
                title=first["title"],
            )
            second_id = await database.insert_news_item(db, second)
            assert await ingest_event_evidence(db, second, news_id=second_id) == group
            row = await (await db.execute(
                "SELECT status,source_count FROM news_event_groups WHERE event_group_id=?",
                (group,),
            )).fetchone()
            preparations = await (await db.execute(
                "SELECT COUNT(*) FROM hotspot_preparation_sets WHERE event_group_id=?",
                (group,),
            )).fetchone()
            # A cross-publisher copy with the same fact fingerprint remains
            # lineage, not a second independent source.
            assert tuple(row) == ("GATED", 1)
            assert preparations[0] == 0
        finally:
            await db.close()

    run(scenario())


def test_unverified_focus_symbol_cannot_confirm_low_severity_event(
    isolated_market_db,
):
    async def persist(revision: int, validation_status: str):
        db = await database.get_db()
        try:
            as_of = datetime.now(timezone.utc) + timedelta(minutes=1)
            context = FocusContext.model_validate_json(json.dumps({
                "schema_version": "option-pro-macrolens-focus-v2",
                "schema_sha256": FOCUS_SCHEMA_SHA256,
                "revision": revision,
                "as_of": as_of.isoformat(),
                "data_through": as_of.isoformat(),
                "market_session": "regular",
                "universe_version": f"confirmation-{revision}",
                "symbols": [{
                    "ticker": "NVDA",
                    "validation_status": validation_status,
                    "universe_reasons": ["test"],
                    "session_change_pct": 10.0,
                    "rvol_time_of_day": 2.0,
                    "as_of": as_of.isoformat(),
                    "data_through": as_of.isoformat(),
                    "data_quality": 1.0,
                    "data_status": "active",
                    "source_status": "active",
                }],
                "major_market_symbols": ["SPY"],
                "warnings": [],
            }))
            await persist_focus_context(db, context)
        finally:
            await db.close()

    async def seed():
        db = await database.get_db()
        try:
            first = news(
                923,
                source="finnhub/Reuters",
                title="NVDA analyst raises ordinary price target to $225",
            )
            first_id = await database.insert_news_item(db, first)
            group = await ingest_event_evidence(db, first, news_id=first_id)
            second = news(924, source="massive/Bloomberg", title=first["title"])
            second["summary"] = (
                "A separate filing confirms the target change and adds a new "
                "2027 margin forecast of 41 percent."
            )
            second_id = await database.insert_news_item(db, second)
            await ingest_event_evidence(db, second, news_id=second_id)
            return group
        finally:
            await db.close()

    async def state(group: str):
        db = await database.get_db()
        try:
            row = await (await db.execute(
                "SELECT status,version FROM news_event_groups WHERE event_group_id=?",
                (group,),
            )).fetchone()
            prepared = await (await db.execute(
                "SELECT COUNT(*) FROM hotspot_preparation_sets WHERE event_group_id=?",
                (group,),
            )).fetchone()
            return tuple(row), int(prepared[0])
        finally:
            await db.close()

    group = run(seed())
    run(persist(1, "unverified"))
    assert run(state(group))[0][0] == "GATED"
    assert run(state(group))[1] == 0
    run(persist(2, "valid_external"))
    status, prepared = run(state(group))
    assert status[0] == "PREPARED"
    assert prepared == 1


def test_first_gate_with_existing_confirmation_keeps_initial_event_version(
    isolated_market_db,
):
    async def scenario():
        db = await database.get_db()
        try:
            as_of = datetime.now(timezone.utc) + timedelta(minutes=1)
            context = FocusContext.model_validate_json(json.dumps({
                "schema_version": "option-pro-macrolens-focus-v2",
                "schema_sha256": FOCUS_SCHEMA_SHA256,
                "revision": 1,
                "as_of": as_of.isoformat(),
                "data_through": as_of.isoformat(),
                "market_session": "regular",
                "universe_version": "first-confirmation",
                "symbols": [{
                    "ticker": "NVDA",
                    "validation_status": "canonical",
                    "universe_reasons": ["test"],
                    "session_change_pct": 10.0,
                    "rvol_time_of_day": 2.0,
                    "as_of": as_of.isoformat(),
                    "data_through": as_of.isoformat(),
                    "data_quality": 1.0,
                    "data_status": "active",
                    "source_status": "active",
                }],
                "major_market_symbols": ["SPY"],
                "warnings": [],
            }))
            await persist_focus_context(db, context)
            item = news(925)
            news_id = await database.insert_news_item(db, item)
            group = await ingest_event_evidence(db, item, news_id=news_id)
            row = await (await db.execute(
                "SELECT version,market_confirmation_score FROM news_event_groups WHERE event_group_id=?",
                (group,),
            )).fetchone()
            assert row[0] == 1
            assert float(row[1]) >= 70
        finally:
            await db.close()

    run(scenario())


def test_market_input_change_regates_only_related_recent_groups(
    isolated_market_db,
):
    async def scenario():
        db = await database.get_db()
        try:
            observed = datetime.now(timezone.utc) + timedelta(minutes=5)

            def context(revision: int, *, rvol: float, clock_minutes: int) -> FocusContext:
                clock = observed + timedelta(minutes=clock_minutes)
                symbols = []
                for ticker, ticker_rvol in (("NVDA", rvol), ("MSFT", 2.0)):
                    symbols.append({
                        "ticker": ticker,
                        "validation_status": "canonical",
                        "universe_reasons": ["test"],
                        "session_change_pct": 4.0,
                        "rvol_time_of_day": ticker_rvol,
                        "breakout_state": "CONFIRMED",
                        "as_of": clock,
                        "data_through": observed,
                        "data_quality": 1.0,
                        "data_status": "active",
                        "source_status": "active",
                    })
                return FocusContext.model_validate({
                    "schema_version": "option-pro-macrolens-focus-v2",
                    "schema_sha256": FOCUS_SCHEMA_SHA256,
                    "revision": revision,
                    "as_of": clock,
                    "data_through": observed,
                    "market_session": "regular",
                    "universe_version": "market-trigger-scope",
                    "symbols": symbols,
                    "major_market_symbols": [],
                    "warnings": [],
                })

            await persist_focus_context(db, context(1, rvol=2.0, clock_minutes=0))
            nvda = news(985, title="NVDA raises annual earnings guidance")
            nvda_id = await database.insert_news_item(db, nvda)
            await ingest_event_evidence(db, nvda, news_id=nvda_id)
            msft = news(986, title="MSFT raises annual earnings guidance")
            msft["source_tickers"] = ["MSFT"]
            msft_id = await database.insert_news_item(db, msft)
            await ingest_event_evidence(db, msft, news_id=msft_id)
            old_nvda = news(987, title="NVDA agrees a separate strategic acquisition")
            old_id = await database.insert_news_item(db, old_nvda)
            old_group = await ingest_event_evidence(db, old_nvda, news_id=old_id)
            await db.execute(
                "UPDATE news_event_groups SET available_at=? WHERE event_group_id=?",
                ((datetime.now(timezone.utc) - timedelta(hours=80)).isoformat(), old_group),
            )
            await db.commit()

            # Only observation clocks moved; substantive market data did not.
            await persist_focus_context(db, context(2, rvol=2.0, clock_minutes=30))
            clock_state = await (await db.execute(
                """SELECT rows_scanned,event_groups_regated
                   FROM focus_validation_state WHERE singleton_id=1"""
            )).fetchone()
            assert tuple(clock_state) == (0, 0)

            # NVDA RVOL changed. The recent NVDA group is re-gated; MSFT and
            # the 80-hour-old NVDA group remain outside the work set.
            await persist_focus_context(db, context(3, rvol=3.0, clock_minutes=60))
            changed_state = await (await db.execute(
                """SELECT rows_scanned,event_groups_regated
                   FROM focus_validation_state WHERE singleton_id=1"""
            )).fetchone()
            assert tuple(changed_state) == (0, 1)
        finally:
            await db.close()

    run(scenario())


def test_novelty_compares_same_fact_against_previous_72_hours(isolated_market_db):
    async def scenario():
        db = await database.get_db()
        try:
            baseline = datetime.now(timezone.utc) - timedelta(hours=60)
            first = news(911, source="finnhub/Reuters")
            first["published_at"] = first["fetched_at"] = baseline.isoformat()
            first_id = await database.insert_news_item(db, first)
            first_group = await ingest_event_evidence(db, first, news_id=first_id)

            repeated = news(912, source="massive/Reuters", title=first["title"])
            repeated_at = baseline + timedelta(hours=30)
            repeated["published_at"] = repeated["fetched_at"] = repeated_at.isoformat()
            repeated["summary"] = first["summary"]
            second_id = await database.insert_news_item(db, repeated)
            second_group = await ingest_event_evidence(db, repeated, news_id=second_id)
            assert second_group != first_group
            rows = await (await db.execute(
                """SELECT event_group_id,novelty_score FROM news_event_groups
                   WHERE event_group_id IN (?,?)""",
                (first_group, second_group),
            )).fetchall()
            novelty = {str(row[0]): float(row[1]) for row in rows}
            assert novelty[first_group] == 85
            assert novelty[second_group] <= 20
        finally:
            await db.close()

    run(scenario())


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
                "SELECT member_count,source_count,version FROM news_event_groups WHERE event_group_id=?",
                (first_group,),
            )).fetchone()
            # Two source adapters carrying Reuters are one independent source.
            assert tuple(group) == (2, 1, 1)
            revisions = await (await db.execute(
                "SELECT prepared_revision FROM hotspot_preparation_sets ORDER BY prepared_revision"
            )).fetchall()
            assert [row[0] for row in revisions] == [1]
        finally:
            await db.close()

    run(scenario())


def test_new_independent_publisher_creates_event_version_and_preparation(
    isolated_market_db,
):
    async def scenario():
        db = await database.get_db()
        try:
            first = news(907, source="finnhub/Reuters")
            first_id = await database.insert_news_item(db, first)
            group = await ingest_event_evidence(db, first, news_id=first_id)
            second = news(908, source="massive/Bloomberg", title=first["title"])
            second["summary"] = (
                "A separate regulatory filing confirms the guidance and adds "
                "a new annual revenue forecast of $42 billion."
            )
            second_id = await database.insert_news_item(db, second)
            assert await ingest_event_evidence(db, second, news_id=second_id) == group
            state = await (await db.execute(
                "SELECT source_count,version FROM news_event_groups WHERE event_group_id=?",
                (group,),
            )).fetchone()
            preparations = await (await db.execute(
                """SELECT prepared_revision,event_group_version,event_snapshot_json
                   FROM hotspot_preparation_sets WHERE event_group_id=?
                   ORDER BY prepared_revision""",
                (group,),
            )).fetchall()
            assert tuple(state) == (2, 2)
            assert [tuple(row[:2]) for row in preparations] == [(1, 1), (2, 2)]
            assert json.loads(preparations[0][2])["source_count"] == 1
            assert json.loads(preparations[1][2])["source_count"] == 2
        finally:
            await db.close()

    run(scenario())


def test_same_publisher_new_fact_does_not_satisfy_independent_source_gate(
    isolated_market_db,
):
    async def scenario():
        db = await database.get_db()
        try:
            first = news(
                981,
                source="finnhub/Reuters",
                title="NVDA analyst raises ordinary price target to $225",
            )
            first_id = await database.insert_news_item(db, first)
            group = await ingest_event_evidence(db, first, news_id=first_id)
            update = news(982, source="massive/Reuters", title=first["title"])
            update["summary"] = (
                "Reuters adds a distinct 2027 margin forecast of 41 percent."
            )
            update_id = await database.insert_news_item(db, update)
            assert await ingest_event_evidence(db, update, news_id=update_id) == group
            row = await (await db.execute(
                """SELECT source_count,version,status FROM news_event_groups
                   WHERE event_group_id=?""",
                (group,),
            )).fetchone()
            assert tuple(row) == (1, 2, "GATED")
            prepared = await (await db.execute(
                "SELECT COUNT(*) FROM hotspot_preparation_sets WHERE event_group_id=?",
                (group,),
            )).fetchone()
            assert prepared[0] == 0
        finally:
            await db.close()

    run(scenario())


def test_seekingalpha_feeds_share_one_publisher_identity(isolated_market_db):
    async def scenario():
        db = await database.get_db()
        try:
            first = news(909, source="seekingalpha/breaking")
            first_id = await database.insert_news_item(db, first)
            group = await ingest_event_evidence(db, first, news_id=first_id)
            second = news(910, source="seekingalpha/daily", title=first["title"])
            second_id = await database.insert_news_item(db, second)
            assert await ingest_event_evidence(db, second, news_id=second_id) == group
            row = await (await db.execute(
                "SELECT source_count,version FROM news_event_groups WHERE event_group_id=?",
                (group,),
            )).fetchone()
            assert tuple(row) == (1, 1)
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
    monkeypatch.setattr(settings, "hot_cycle_manual_enabled", True)
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
        "schema_version": "option-pro-macrolens-focus-v2",
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


def test_same_focus_validation_basis_skips_mention_scan(isolated_market_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news(940))
            mentions = await record_ticker_mentions(
                db,
                news_id=news_id,
                tickers=["XYZ"],
                association_method="exact_alias",
                source="alias_dictionary",
            )
            assert mentions[0]["validation_status"] == "unverified"
            await db.commit()

            observed = datetime.now(timezone.utc)

            def context(revision: int, change: float) -> FocusContext:
                return FocusContext.model_validate({
                    "schema_version": "option-pro-macrolens-focus-v2",
                    "schema_sha256": FOCUS_SCHEMA_SHA256,
                    "revision": revision,
                    "as_of": observed + timedelta(minutes=revision),
                    "data_through": observed,
                    "market_session": "regular",
                    "universe_version": "stable-validation-universe",
                    "symbols": [{
                        "ticker": "XYZ",
                        "validation_status": "canonical",
                        "universe_reasons": ["focus"],
                        "session_change_pct": change,
                        "as_of": observed + timedelta(minutes=revision),
                        "data_through": observed,
                        "data_quality": 1.0,
                        "data_status": "active",
                        "source_status": "active",
                    }],
                    "major_market_symbols": [],
                    "warnings": [],
                })

            assert await persist_focus_context(db, context(1, 1.0)) is True
            after_first = await (await db.execute(
                "SELECT COUNT(*) FROM ticker_validation_revisions"
            )).fetchone()
            assert after_first[0] == 2

            # Price changed, but ticker sets, universe and symbol data-through did not.
            assert await persist_focus_context(db, context(2, 5.0)) is True
            state = await (await db.execute(
                """SELECT last_focus_revision,rows_scanned,rows_changed,
                          validation_revisions_created,event_groups_regated
                   FROM focus_validation_state WHERE singleton_id=1"""
            )).fetchone()
            assert tuple(state) == (2, 0, 0, 0, 0)
            revisions = await (await db.execute(
                "SELECT COUNT(*) FROM ticker_validation_revisions"
            )).fetchone()
            assert revisions[0] == 2
        finally:
            await db.close()

    run(scenario())


def test_bounded_revalidation_queues_focus_revisions_and_preserves_as_of(
    isolated_market_db, monkeypatch
):
    monkeypatch.setattr(settings, "focus_revalidation_max_rows_per_run", 2)
    monkeypatch.setattr(settings, "focus_revalidation_batch_size", 2)
    monkeypatch.setattr(settings, "focus_revalidation_max_seconds_per_run", 5.0)

    async def scenario():
        db = await database.get_db()
        try:
            mention_ids = []
            for index in range(5):
                news_id = await database.insert_news_item(db, news(950 + index))
                mention = await record_ticker_mentions(
                    db,
                    news_id=news_id,
                    tickers=[f"ZZ{index}"],
                    association_method="exact_alias",
                    source="alias_dictionary",
                )
                mention_ids.append(int(mention[0]["mention_id"]))
            await db.commit()

            first_available = datetime.now(timezone.utc) + timedelta(minutes=5)
            second_available = first_available + timedelta(minutes=30)

            def context(revision: int, symbols: list[str], observed: datetime) -> FocusContext:
                return FocusContext.model_validate({
                    "schema_version": "option-pro-macrolens-focus-v2",
                    "schema_sha256": FOCUS_SCHEMA_SHA256,
                    "revision": revision,
                    "as_of": observed,
                    "data_through": observed,
                    "market_session": "regular",
                    "universe_version": "bounded-queue",
                    "symbols": [
                        {
                            "ticker": ticker,
                            "validation_status": "canonical",
                            "universe_reasons": ["test"],
                            "as_of": observed,
                            "data_through": observed,
                            "data_quality": 1.0,
                            "data_status": "active",
                            "source_status": "active",
                        }
                        for ticker in symbols
                    ],
                    "major_market_symbols": [],
                    "warnings": [],
                })

            first_symbols = [f"ZZ{index}" for index in range(5)] + ["LATE"]
            first = context(1, first_symbols, first_available)
            second = context(2, [], second_available)
            assert await persist_focus_context(
                db, first, fetched_at=first_available
            ) is True
            state = await (await db.execute(
                """SELECT last_focus_revision,pending_focus_revision,
                          pending_mention_cursor,pending_mention_max_id,
                          validation_basis_hash
                   FROM focus_validation_state WHERE singleton_id=1"""
            )).fetchone()
            assert state[0] is None
            assert tuple(state[1:3]) == (1, mention_ids[1])
            assert state[3] == mention_ids[-1]
            completed_basis_before = state[4]

            # This mention becomes visible after revision 1's frozen boundary.
            # It must not be pulled backward into that already-running pass.
            late_available = first_available + timedelta(minutes=10)
            late_news_id = await database.insert_news_item(db, news(960))
            late_basis = build_validation_basis_hash(
                canonical_symbols=set(first_symbols),
                external_symbols=set(),
                universe_version="bounded-queue",
            )
            late = await record_ticker_mention(
                db,
                news_id=late_news_id,
                ticker="LATE",
                association_method="exact_alias",
                association_confidence=1.0,
                source="alias_dictionary",
                validation_status="canonical",
                available_at=late_available,
                focus_revision=1,
                universe_version="bounded-queue",
                validation_basis_hash=late_basis,
            )
            late_mention_id = int(late["mention_id"])
            assert late_mention_id > state[3]
            await db.commit()

            # A newer snapshot is durable, but cannot replace the unfinished
            # point-in-time pass for revision 1.
            assert await persist_focus_context(
                db, second, fetched_at=second_available
            ) is True
            state = await (await db.execute(
                """SELECT last_focus_revision,pending_focus_revision,
                          pending_mention_cursor,pending_mention_max_id,
                          validation_basis_hash
                   FROM focus_validation_state WHERE singleton_id=1"""
            )).fetchone()
            assert state[0] is None
            assert tuple(state[1:3]) == (1, mention_ids[3])
            assert state[3] == mention_ids[-1]
            assert state[4] == completed_basis_before

            # Replaying the same revision advances a bounded slice but must not
            # move its first availability time.
            assert await persist_focus_context(
                db, second, fetched_at=second_available + timedelta(days=1)
            ) is False
            fetched = await (await db.execute(
                "SELECT fetched_at FROM focus_context_snapshots WHERE revision=2"
            )).fetchone()
            assert fetched[0] == second_available.isoformat()

            for _ in range(12):
                await db.close()
                result = await resume_focus_revalidation()
                db = await database.get_db()
                state = await (await db.execute(
                    """SELECT last_focus_revision,pending_run_key
                       FROM focus_validation_state WHERE singleton_id=1"""
                )).fetchone()
                if int(state[0] or 0) == 2 and state[1] is None:
                    break
                assert result["status"] in {"pending", "complete"}
            assert int(state[0]) == 2
            assert state[1] is None

            at_first = await validation_as_of(
                db,
                mention_id=mention_ids[-1],
                as_of=first_available + timedelta(minutes=1),
            )
            at_second = await validation_as_of(
                db,
                mention_id=mention_ids[-1],
                as_of=second_available + timedelta(minutes=1),
            )
            assert at_first["validation_status"] == "canonical"
            assert at_first["focus_revision"] == 1
            assert at_second["validation_status"] == "unverified"
            assert at_second["focus_revision"] == 2

            late_before_creation = await validation_as_of(
                db,
                mention_id=late_mention_id,
                as_of=first_available + timedelta(minutes=1),
            )
            late_after_creation = await validation_as_of(
                db,
                mention_id=late_mention_id,
                as_of=late_available + timedelta(minutes=1),
            )
            late_after_second = await validation_as_of(
                db,
                mention_id=late_mention_id,
                as_of=second_available + timedelta(minutes=1),
            )
            assert late_before_creation["validation_status"] == "unverified"
            assert late_before_creation["validated_at"] is None
            assert late_after_creation["validation_status"] == "canonical"
            assert late_after_creation["validated_at"] == late_available.isoformat(
                timespec="microseconds"
            )
            assert late_after_second["validation_status"] == "unverified"
            assert late_after_second["focus_revision"] == 2
            backdated = await (await db.execute(
                """SELECT COUNT(*) FROM ticker_validation_revisions
                   WHERE mention_id=? AND focus_revision=1 AND available_at<?""",
                (late_mention_id, late_available.isoformat(timespec="microseconds")),
            )).fetchone()
            assert int(backdated[0]) == 0
        finally:
            await db.close()

    run(scenario())


def test_concurrent_focus_revalidation_slice_uses_cross_connection_lease(
    isolated_market_db, monkeypatch
):
    monkeypatch.setattr(settings, "focus_revalidation_max_rows_per_run", 1)
    monkeypatch.setattr(settings, "focus_revalidation_batch_size", 1)
    monkeypatch.setattr(settings, "focus_revalidation_max_seconds_per_run", 5.0)

    async def scenario():
        db = await database.get_db()
        try:
            symbols = ["LOCKA", "LOCKB", "LOCKC"]
            for index, ticker in enumerate(symbols):
                news_id = await database.insert_news_item(db, news(965 + index))
                await record_ticker_mentions(
                    db,
                    news_id=news_id,
                    tickers=[ticker],
                    association_method="exact_alias",
                    source="alias_dictionary",
                )
            await db.commit()
            observed = datetime.now(timezone.utc) + timedelta(minutes=5)
            context = FocusContext.model_validate({
                "schema_version": "option-pro-macrolens-focus-v2",
                "schema_sha256": FOCUS_SCHEMA_SHA256,
                "revision": 1,
                "as_of": observed,
                "data_through": observed,
                "market_session": "regular",
                "universe_version": "concurrent-lease",
                "symbols": [
                    {
                        "ticker": ticker,
                        "validation_status": "canonical",
                        "universe_reasons": ["test"],
                        "as_of": observed,
                        "data_through": observed,
                        "data_quality": 1.0,
                        "data_status": "active",
                        "source_status": "active",
                    }
                    for ticker in symbols
                ],
                "major_market_symbols": [],
                "warnings": [],
            })
            await persist_focus_context(db, context, fetched_at=observed)
            pending = await (await db.execute(
                """SELECT pending_focus_revision,pending_mention_cursor
                   FROM focus_validation_state WHERE singleton_id=1"""
            )).fetchone()
            assert tuple(pending) == (1, 1)
        finally:
            await db.close()

        monkeypatch.setattr(settings, "focus_revalidation_max_rows_per_run", 100)
        entered = asyncio.Event()
        release = asyncio.Event()
        original_append = market_focus_service.append_validation_revision

        async def pause_first_append(*args, **kwargs):
            if not entered.is_set():
                entered.set()
                await release.wait()
            return await original_append(*args, **kwargs)

        monkeypatch.setattr(
            market_focus_service,
            "append_validation_revision",
            pause_first_append,
        )
        first = asyncio.create_task(resume_focus_revalidation())
        await asyncio.wait_for(entered.wait(), timeout=2)
        second_result = await asyncio.wait_for(
            resume_focus_revalidation(),
            timeout=2,
        )
        assert second_result["pending"] is True
        release.set()
        await asyncio.wait_for(first, timeout=5)

        db = await database.get_db()
        try:
            state = await (await db.execute(
                """SELECT last_focus_revision,validation_basis_hash,
                          pending_run_key,pending_focus_revision,
                          revalidation_lease_owner,revalidation_lease_expires_at,
                          revalidation_fencing_token,rows_scanned
                   FROM focus_validation_state WHERE singleton_id=1"""
            )).fetchone()
            assert state[0] == 1
            assert len(str(state[1])) == 64
            assert tuple(state[2:6]) == (None, None, None, None)
            assert int(state[6]) >= 2
            assert state[7] == 3

            prior_token = int(state[6])
            await db.execute(
                """UPDATE focus_validation_state SET
                   revalidation_lease_owner='crashed-worker',
                   revalidation_lease_expires_at='2000-01-01T00:00:00.000000+00:00'
                   WHERE singleton_id=1"""
            )
            await db.commit()
            await market_focus_service.revalidate_events_for_focus_context(
                db,
                context.model_dump(mode="json"),
            )
            recovered = await (await db.execute(
                """SELECT revalidation_lease_owner,
                          revalidation_lease_expires_at,
                          revalidation_fencing_token
                   FROM focus_validation_state WHERE singleton_id=1"""
            )).fetchone()
            assert tuple(recovered[:2]) == (None, None)
            assert recovered[2] == prior_token + 1
        finally:
            await db.close()

    run(scenario())


def test_expired_focus_lease_takeover_fences_stale_runner(
    isolated_market_db, monkeypatch
):
    monkeypatch.setattr(settings, "focus_revalidation_max_rows_per_run", 1)
    monkeypatch.setattr(settings, "focus_revalidation_batch_size", 1)
    monkeypatch.setattr(settings, "focus_revalidation_max_seconds_per_run", 5.0)

    async def scenario():
        db = await database.get_db()
        symbols = ["TAKEA", "TAKEB", "TAKEC"]
        try:
            for index, ticker in enumerate(symbols):
                news_id = await database.insert_news_item(db, news(975 + index))
                await record_ticker_mentions(
                    db,
                    news_id=news_id,
                    tickers=[ticker],
                    association_method="exact_alias",
                    source="alias_dictionary",
                )
            await db.commit()
            observed = datetime.now(timezone.utc) + timedelta(minutes=5)
            context = FocusContext.model_validate({
                "schema_version": "option-pro-macrolens-focus-v2",
                "schema_sha256": FOCUS_SCHEMA_SHA256,
                "revision": 1,
                "as_of": observed,
                "data_through": observed,
                "market_session": "regular",
                "universe_version": "lease-takeover",
                "symbols": [
                    {
                        "ticker": ticker,
                        "validation_status": "canonical",
                        "universe_reasons": ["test"],
                        "as_of": observed,
                        "data_through": observed,
                        "data_quality": 1.0,
                        "data_status": "active",
                        "source_status": "active",
                    }
                    for ticker in symbols
                ],
                "major_market_symbols": [],
                "warnings": [],
            })
            await persist_focus_context(db, context, fetched_at=observed)
        finally:
            await db.close()

        monkeypatch.setattr(settings, "focus_revalidation_max_rows_per_run", 100)
        entered = asyncio.Event()
        release = asyncio.Event()
        original_append = market_focus_service.append_validation_revision

        async def pause_stale_runner(*args, **kwargs):
            if not entered.is_set():
                entered.set()
                await release.wait()
            return await original_append(*args, **kwargs)

        monkeypatch.setattr(
            market_focus_service,
            "append_validation_revision",
            pause_stale_runner,
        )
        stale_task = asyncio.create_task(resume_focus_revalidation())
        await asyncio.wait_for(entered.wait(), timeout=2)

        takeover_db = await database.get_db()
        try:
            before_takeover = await (await takeover_db.execute(
                """SELECT revalidation_lease_owner,revalidation_fencing_token
                   FROM focus_validation_state WHERE singleton_id=1"""
            )).fetchone()
            assert before_takeover[0]
            stale_token = int(before_takeover[1])
            await takeover_db.execute(
                """UPDATE focus_validation_state SET
                   revalidation_lease_expires_at='2000-01-01T00:00:00.000000+00:00'
                   WHERE singleton_id=1"""
            )
            await takeover_db.commit()
        finally:
            await takeover_db.close()

        takeover_result = await asyncio.wait_for(
            resume_focus_revalidation(),
            timeout=5,
        )
        assert takeover_result["pending"] is False
        db = await database.get_db()
        try:
            completed = await (await db.execute(
                """SELECT last_focus_revision,validation_basis_hash,
                          pending_run_key,pending_focus_revision,pending_phase,
                          pending_mention_cursor,rows_scanned,
                          revalidation_lease_owner,revalidation_fencing_token
                   FROM focus_validation_state WHERE singleton_id=1"""
            )).fetchone()
            takeover_token = int(completed[8])
            assert completed[0] == 1
            assert len(str(completed[1])) == 64
            assert tuple(completed[2:6]) == (None, None, None, 0)
            assert completed[6] == 3
            assert completed[7] is None
            assert takeover_token == stale_token + 1
        finally:
            await db.close()

        release.set()
        stale_result = await asyncio.wait_for(stale_task, timeout=5)
        assert stale_result["pending"] is False
        db = await database.get_db()
        try:
            after_stale_resume = await (await db.execute(
                """SELECT last_focus_revision,validation_basis_hash,
                          pending_run_key,pending_focus_revision,pending_phase,
                          pending_mention_cursor,rows_scanned,
                          revalidation_lease_owner,revalidation_fencing_token
                   FROM focus_validation_state WHERE singleton_id=1"""
            )).fetchone()
            assert tuple(after_stale_resume) == tuple(completed)
            revision_count = await (await db.execute(
                "SELECT COUNT(*) FROM ticker_validation_revisions"
            )).fetchone()
            assert revision_count[0] == 6
        finally:
            await db.close()

    run(scenario())


def test_old_focus_revision_rebuilds_events_as_of_and_blocks_mixed_cycle(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)
    monkeypatch.setattr(settings, "focus_revalidation_max_seconds_per_run", 5.0)

    async def scenario():
        db = await database.get_db()
        try:
            base = datetime.now(timezone.utc)
            item = news(968, title="XYZ raises annual earnings guidance materially")
            item["source_tickers"] = []
            item["published_at"] = item["fetched_at"] = base.isoformat()
            news_id = await database.insert_news_item(db, item)
            event_group_id = await ingest_event_evidence(db, item, news_id=news_id)
            mention = await record_ticker_mentions(
                db,
                news_id=news_id,
                tickers=["XYZ"],
                association_method="exact_alias",
                source="alias_dictionary",
            )
            mention_id = int(mention[0]["mention_id"])
            await db.commit()

            def context(
                revision: int,
                observed: datetime,
                validation_status: str | None,
            ) -> FocusContext:
                symbols = []
                if validation_status is not None:
                    symbols.append({
                        "ticker": "XYZ",
                        "validation_status": validation_status,
                        "universe_reasons": ["test"],
                        "session_change_pct": 8.0,
                        "rvol_time_of_day": 2.0,
                        "breakout_state": "CONFIRMED",
                        "as_of": observed,
                        "data_through": observed,
                        "data_quality": 1.0,
                        "data_status": "active",
                        "source_status": "active",
                    })
                return FocusContext.model_validate({
                    "schema_version": "option-pro-macrolens-focus-v2",
                    "schema_sha256": FOCUS_SCHEMA_SHA256,
                    "revision": revision,
                    "as_of": observed,
                    "data_through": observed,
                    "market_session": "regular",
                    "universe_version": "point-in-time-replay",
                    "symbols": symbols,
                    "major_market_symbols": [],
                    "warnings": [],
                })

            revision_1_at = base + timedelta(minutes=1)
            revision_2_at = base + timedelta(minutes=2)
            revision_3_at = base + timedelta(minutes=3)
            await persist_focus_context(
                db,
                context(1, revision_1_at, "canonical"),
                fetched_at=revision_1_at,
            )
            initial_projection = await (await db.execute(
                """SELECT validated_tickers_json FROM news_event_groups
                   WHERE event_group_id=?""",
                (event_group_id,),
            )).fetchone()
            assert json.loads(initial_projection[0]) == ["XYZ"]

            monkeypatch.setattr(settings, "focus_revalidation_max_rows_per_run", 1)
            monkeypatch.setattr(settings, "focus_revalidation_batch_size", 1)
            await persist_focus_context(
                db,
                context(2, revision_2_at, None),
                fetched_at=revision_2_at,
            )
            future_basis = build_validation_basis_hash(
                canonical_symbols=set(),
                external_symbols={"XYZ"},
                universe_version="point-in-time-replay",
            )
            await append_validation_revision(
                db,
                mention_id=mention_id,
                validation_status="valid_external",
                available_at=revision_3_at,
                focus_revision=3,
                universe_version="point-in-time-replay",
                reason_code="future_analysis_validation",
                validation_basis_hash=future_basis,
            )
            await db.commit()
            await persist_focus_context(
                db,
                context(3, revision_3_at, "valid_external"),
                fetched_at=revision_3_at,
            )
            point_in_time = await validation_as_of(
                db,
                mention_id=mention_id,
                as_of=revision_2_at,
            )
            current = await (await db.execute(
                """SELECT current_validation_status
                   FROM news_ticker_mentions WHERE id=?""",
                (mention_id,),
            )).fetchone()
            assert point_in_time["validation_status"] == "unverified"
            assert current[0] == "valid_external"
            with pytest.raises(CycleConflict, match="focus_revalidation_pending"):
                await create_market_focus_cycle(db, trigger_type="manual")

            for _ in range(20):
                await resume_focus_revalidation()
                state = await (await db.execute(
                    """SELECT last_focus_revision,pending_run_key
                       FROM focus_validation_state WHERE singleton_id=1"""
                )).fetchone()
                if int(state[0] or 0) == 2 and state[1] is None:
                    break
            assert tuple(state) == (2, None)
            member_at_revision_2 = await (await db.execute(
                """SELECT validated_tickers_json FROM news_event_members
                   WHERE event_group_id=?""",
                (event_group_id,),
            )).fetchone()
            group_at_revision_2 = await (await db.execute(
                """SELECT validated_tickers_json FROM news_event_groups
                   WHERE event_group_id=?""",
                (event_group_id,),
            )).fetchone()
            assert json.loads(member_at_revision_2[0]) == []
            assert json.loads(group_at_revision_2[0]) == []
            assert await market_focus_service._focus_projection_revalidation_pending(db)

            for _ in range(20):
                await resume_focus_revalidation()
                state = await (await db.execute(
                    """SELECT last_focus_revision,pending_run_key
                       FROM focus_validation_state WHERE singleton_id=1"""
                )).fetchone()
                if int(state[0] or 0) == 3 and state[1] is None:
                    break
            assert tuple(state) == (3, None)
            member_at_revision_3 = await (await db.execute(
                """SELECT validated_tickers_json FROM news_event_members
                   WHERE event_group_id=?""",
                (event_group_id,),
            )).fetchone()
            group_at_revision_3 = await (await db.execute(
                """SELECT validated_tickers_json FROM news_event_groups
                   WHERE event_group_id=?""",
                (event_group_id,),
            )).fetchone()
            assert json.loads(member_at_revision_3[0]) == ["XYZ"]
            assert json.loads(group_at_revision_3[0]) == ["XYZ"]
            assert not await market_focus_service._focus_projection_revalidation_pending(db)
        finally:
            await db.close()

    run(scenario())


def test_large_event_group_member_refresh_resumes_one_row_per_slice(
    isolated_market_db, monkeypatch
):
    monkeypatch.setattr(settings, "focus_revalidation_max_rows_per_run", 1)
    monkeypatch.setattr(settings, "focus_revalidation_batch_size", 1)
    monkeypatch.setattr(settings, "focus_revalidation_max_seconds_per_run", 5.0)

    async def scenario():
        db = await database.get_db()
        try:
            base = datetime.now(timezone.utc)
            item = news(969, title="XYZ raises annual earnings guidance materially")
            item["source_tickers"] = []
            item["published_at"] = item["fetched_at"] = base.isoformat()
            news_id = await database.insert_news_item(db, item)
            event_group_id = await ingest_event_evidence(db, item, news_id=news_id)
            for index in range(1, 12):
                fingerprint = hashlib.sha256(f"large-group-{index}".encode()).hexdigest()
                await db.execute(
                    """INSERT INTO news_event_members
                       (event_group_id,news_id,source,normalized_url,title,
                        published_at,fetched_at,source_tickers_json,
                        validated_tickers_json,publisher_identity,event_type,
                        evidence_fingerprint,content_hash,created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        event_group_id,
                        news_id,
                        f"wire-{index}",
                        f"https://example.test/large-group/{index}",
                        item["title"],
                        base.isoformat(),
                        base.isoformat(),
                        "[]",
                        "[]",
                        f"publisher-{index}",
                        "earnings_guidance",
                        fingerprint,
                        fingerprint,
                        base.isoformat(),
                    ),
                )
            await db.execute(
                "UPDATE news_event_groups SET member_count=12 WHERE event_group_id=?",
                (event_group_id,),
            )
            await record_ticker_mentions(
                db,
                news_id=news_id,
                tickers=["XYZ"],
                association_method="exact_alias",
                source="alias_dictionary",
            )
            await db.commit()

            observed = base + timedelta(minutes=1)
            context = FocusContext.model_validate({
                "schema_version": "option-pro-macrolens-focus-v2",
                "schema_sha256": FOCUS_SCHEMA_SHA256,
                "revision": 1,
                "as_of": observed,
                "data_through": observed,
                "market_session": "regular",
                "universe_version": "large-group-cursor",
                "symbols": [{
                    "ticker": "XYZ",
                    "validation_status": "canonical",
                    "universe_reasons": ["test"],
                    "as_of": observed,
                    "data_through": observed,
                    "data_quality": 1.0,
                    "data_status": "active",
                    "source_status": "active",
                }],
                "major_market_symbols": [],
                "warnings": [],
            })
            await persist_focus_context(db, context, fetched_at=observed)
            prior_validated = 0
            observed_member_cursors: list[int] = []
            for _ in range(30):
                await resume_focus_revalidation()
                rows = await (await db.execute(
                    """SELECT validated_tickers_json FROM news_event_members
                       WHERE event_group_id=? ORDER BY id""",
                    (event_group_id,),
                )).fetchall()
                validated_count = sum(
                    json.loads(row[0]) == ["XYZ"] for row in rows
                )
                assert validated_count - prior_validated <= 1
                prior_validated = validated_count
                state = await (await db.execute(
                    """SELECT last_focus_revision,pending_run_key,
                              pending_active_group_id,
                              pending_group_member_cursor
                       FROM focus_validation_state WHERE singleton_id=1"""
                )).fetchone()
                if state[2]:
                    observed_member_cursors.append(int(state[3]))
                if int(state[0] or 0) == 1 and state[1] is None:
                    break
            assert prior_validated == 12
            assert observed_member_cursors == sorted(set(observed_member_cursors))
            assert len(observed_member_cursors) >= 12
            assert tuple(state[:2]) == (1, None)
            group = await (await db.execute(
                """SELECT validated_tickers_json FROM news_event_groups
                   WHERE event_group_id=?""",
                (event_group_id,),
            )).fetchone()
            assert json.loads(group[0]) == ["XYZ"]
        finally:
            await db.close()

    run(scenario())


def test_rules_only_change_uses_bounded_resume_without_advancing_basis_early(
    isolated_market_db, monkeypatch
):
    monkeypatch.setattr(settings, "focus_revalidation_max_seconds_per_run", 5.0)

    async def scenario():
        db = await database.get_db()
        try:
            for index in range(3):
                news_id = await database.insert_news_item(db, news(970 + index))
                await record_ticker_mentions(
                    db,
                    news_id=news_id,
                    tickers=[f"RULE{index}"],
                    association_method="exact_alias",
                    source="alias_dictionary",
                )
            await db.commit()
            observed = datetime.now(timezone.utc)
            context = FocusContext.model_validate({
                "schema_version": "option-pro-macrolens-focus-v2",
                "schema_sha256": FOCUS_SCHEMA_SHA256,
                "revision": 1,
                "as_of": observed,
                "data_through": observed,
                "market_session": "closed",
                "universe_version": "rules-only",
                "symbols": [],
                "major_market_symbols": [],
                "warnings": [],
            })
            await persist_focus_context(db, context, fetched_at=observed)
            before = await (await db.execute(
                """SELECT validation_basis_hash,validation_rules_version
                   FROM focus_validation_state WHERE singleton_id=1"""
            )).fetchone()
            assert before[1] == "ticker-validation-v1"
        finally:
            await db.close()

        monkeypatch.setattr(
            market_focus_service,
            "TICKER_VALIDATION_RULES_VERSION",
            "ticker-validation-v2",
        )
        monkeypatch.setattr(settings, "focus_revalidation_max_rows_per_run", 1)
        monkeypatch.setattr(settings, "focus_revalidation_batch_size", 1)
        first_slice = await resume_focus_revalidation()
        assert first_slice["pending"] is True
        db = await database.get_db()
        try:
            during = await (await db.execute(
                """SELECT validation_basis_hash,validation_rules_version,
                          pending_validation_rules_version,pending_mention_cursor
                   FROM focus_validation_state WHERE singleton_id=1"""
            )).fetchone()
            assert tuple(during[:3]) == (
                before[0],
                "ticker-validation-v1",
                "ticker-validation-v2",
            )
            assert during[3] > 0
        finally:
            await db.close()
        for _ in range(8):
            result = await resume_focus_revalidation()
            if not result["pending"]:
                break
        db = await database.get_db()
        try:
            after = await (await db.execute(
                """SELECT validation_basis_hash,validation_rules_version,
                          rows_scanned,pending_run_key
                   FROM focus_validation_state WHERE singleton_id=1"""
            )).fetchone()
            assert after[0] != before[0]
            assert after[1] == "ticker-validation-v2"
            assert after[2] == 3
            assert after[3] is None
        finally:
            await db.close()

    run(scenario())


def test_llm_ticker_returns_to_stable_provider_external_after_leaving_focus(
    isolated_market_db,
):
    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news(980))
            provider = await record_ticker_mentions(
                db,
                news_id=news_id,
                tickers=["NVDA"],
                association_method="provider_tag",
                source="finnhub/Reuters",
            )
            llm_basis = build_validation_basis_hash(
                canonical_symbols=set(),
                external_symbols={"NVDA"},
                universe_version="",
            )
            llm = await record_ticker_mention(
                db,
                news_id=news_id,
                ticker="NVDA",
                association_method="llm_inference",
                association_confidence=0.5,
                source="model_output",
                validation_status="valid_external",
                available_at=datetime.now(timezone.utc),
                focus_revision=None,
                universe_version=None,
                validation_basis_hash=llm_basis,
                legacy_association=True,
            )
            await db.commit()
            observed = datetime.now(timezone.utc) + timedelta(minutes=1)

            def context(revision: int, included: bool) -> FocusContext:
                return FocusContext.model_validate({
                    "schema_version": "option-pro-macrolens-focus-v2",
                    "schema_sha256": FOCUS_SCHEMA_SHA256,
                    "revision": revision,
                    "as_of": observed + timedelta(minutes=revision),
                    "data_through": observed + timedelta(minutes=revision),
                    "market_session": "regular",
                    "universe_version": "provider-stable",
                    "symbols": ([{
                        "ticker": "NVDA",
                        "validation_status": "canonical",
                        "universe_reasons": ["test"],
                        "as_of": observed + timedelta(minutes=revision),
                        "data_through": observed + timedelta(minutes=revision),
                        "data_quality": 1.0,
                        "data_status": "active",
                        "source_status": "active",
                    }] if included else []),
                    "major_market_symbols": [],
                    "warnings": [],
                })

            await persist_focus_context(db, context(1, True))
            entered = await validation_as_of(
                db,
                mention_id=int(llm["mention_id"]),
                as_of=observed + timedelta(minutes=3),
            )
            assert entered["validation_status"] == "canonical"
            await persist_focus_context(db, context(2, False))
            left = await validation_as_of(
                db,
                mention_id=int(llm["mention_id"]),
                as_of=observed + timedelta(minutes=4),
            )
            assert left["validation_status"] == "valid_external"
            current = await (await db.execute(
                """SELECT id,current_validation_status FROM news_ticker_mentions
                   WHERE id IN (?,?) ORDER BY id""",
                (provider[0]["mention_id"], llm["mention_id"]),
            )).fetchall()
            assert [row[1] for row in current] == ["valid_external", "valid_external"]
        finally:
            await db.close()

    run(scenario())


def test_focus_revalidation_failure_marks_persisted_snapshot_stale(
    isolated_market_db, monkeypatch
):
    payload = {
        "schema_version": "option-pro-macrolens-focus-v2",
        "schema_sha256": FOCUS_SCHEMA_SHA256,
        "revision": 1,
        "as_of": "2026-07-13T12:00:00Z",
        "data_through": "2026-07-13T11:59:00Z",
        "market_session": "regular",
        "universe_version": "failed-revalidation",
        "symbols": [],
        "major_market_symbols": ["SPY"],
        "warnings": [],
    }
    context = FocusContext.model_validate_json(json.dumps(payload))

    async def fail_revalidation(*args, **kwargs):
        raise RuntimeError("projection failed")

    monkeypatch.setattr(
        market_focus_service,
        "revalidate_events_for_focus_context",
        fail_revalidation,
    )

    async def scenario():
        db = await database.get_db()
        try:
            with pytest.raises(RuntimeError, match="focus_association_revalidation_failed"):
                await persist_focus_context(db, context)
            row = await (await db.execute(
                "SELECT revision,status FROM focus_context_snapshots"
            )).fetchone()
            assert tuple(row) == (1, "stale")
        finally:
            await db.close()

    run(scenario())


def test_nyse_schedule_handles_dst_weekends_holidays_and_early_close(monkeypatch):
    monkeypatch.setattr(settings, "hot_cycle_times_et", "08:00,12:00,16:00")
    monkeypatch.setattr(settings, "hot_cycle_optional_20_et", False)
    assert not is_nyse_trading_day(date(2026, 7, 4))
    assert not scheduled_slots_for_day(date(2026, 7, 4))
    assert is_nyse_trading_day(date(2021, 12, 31))
    assert not is_nyse_trading_day(date(2023, 1, 2))
    assert not scheduled_slots_for_day(date(2023, 1, 2))
    assert not is_nyse_trading_day(date(2026, 1, 1))
    assert not is_nyse_trading_day(date(2022, 1, 17))
    assert not is_nyse_trading_day(date(2022, 6, 20))
    assert not is_nyse_trading_day(date(2022, 7, 4))
    assert not is_nyse_trading_day(date(2022, 12, 26))
    assert is_nyse_early_close(date(2025, 7, 3))
    assert not is_nyse_early_close(date(2026, 7, 2))
    assert is_nyse_early_close(date(2026, 11, 27))
    assert is_nyse_early_close(date(2026, 12, 24))
    close_slot = scheduled_slots_for_day(date(2026, 11, 27))[-1]
    assert close_slot[0] == "scheduled_1600"
    assert close_slot[1].hour == 13
    summer = datetime(2026, 7, 13, 12, 0, tzinfo=EASTERN)
    winter = datetime(2026, 12, 14, 12, 0, tzinfo=EASTERN)
    assert due_cycle_trigger(summer) == "scheduled_1200"
    assert due_cycle_trigger(winter) == "scheduled_1200"
    assert summer.utcoffset() != winter.utcoffset()
    next_after_weekend_new_year = next_cycle_at(
        datetime(2022, 12, 30, 20, 1, tzinfo=EASTERN)
    )
    assert next_after_weekend_new_year is not None
    assert next_after_weekend_new_year.astimezone(EASTERN) == datetime(
        2023, 1, 3, 8, 0, tzinfo=EASTERN
    )


def test_finnhub_company_news_date_uses_eastern_calendar_day():
    assert finnhub_company_news_date(
        datetime(2026, 7, 14, 2, 30, tzinfo=timezone.utc)
    ) == date(2026, 7, 13)
    assert finnhub_company_news_date(
        datetime(2026, 1, 14, 2, 30, tzinfo=timezone.utc)
    ) == date(2026, 1, 13)
    assert finnhub_company_news_date(
        datetime(2026, 7, 14, 16, 0, tzinfo=timezone.utc)
    ) == date(2026, 7, 14)
    with pytest.raises(ValueError, match="requires_timezone"):
        finnhub_company_news_date(datetime(2026, 7, 14, 2, 30))


def _enable_cycles(monkeypatch):
    monkeypatch.setattr(settings, "hot_cycle_enabled", True)
    monkeypatch.setattr(settings, "hot_cycle_manual_enabled", True)
    monkeypatch.setattr(settings, "hot_cycle_daily_job_limit", 100)
    monkeypatch.setattr(settings, "hot_cycle_daily_output_token_limit", 10_000_000)
    monkeypatch.setattr(settings, "hot_cycle_manual_cooldown_seconds", 0)
    monkeypatch.setattr(settings, "openai_execution_mode", "background")


async def _seed_hotspot(db, index: int, *, source: str = "reuters"):
    item = news(index, source=source)
    news_id = await database.insert_news_item(db, item)
    await ingest_event_evidence(db, item, news_id=news_id)
    return item


def test_manual_cycle_requires_explicit_switch(isolated_market_db, monkeypatch):
    _enable_cycles(monkeypatch)
    monkeypatch.setattr(settings, "hot_cycle_manual_enabled", False)

    async def scenario():
        db = await database.get_db()
        try:
            await _seed_hotspot(db, 919)
            with pytest.raises(CycleConflict, match="manual_cycle_disabled"):
                await create_market_focus_cycle(db, trigger_type="manual")
        finally:
            await db.close()

    run(scenario())


def test_cycle_worker_cost_gate_blocks_old_unsubmitted_cycle(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)

    async def scenario():
        db = await database.get_db()
        try:
            await _seed_hotspot(db, 921)
            cycle = await create_market_focus_cycle(db, trigger_type="manual")
        finally:
            await db.close()

        monkeypatch.setattr(settings, "hot_cycle_manual_enabled", False)
        provider = CompletedCycleProvider()
        assert await run_market_focus_worker_once(
            provider=provider, worker_id="cycle-cost-gate"
        ) is False

        db = await database.get_db()
        try:
            row = await (await db.execute(
                "SELECT status,openai_response_id FROM market_focus_cycles WHERE cycle_id=?",
                (cycle["cycle_id"],),
            )).fetchone()
            assert tuple(row) == ("pending", None)
        finally:
            await db.close()

    run(scenario())


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
            monkeypatch.setattr(settings, "market_focus_failed_retention_days", 1)
            await db.execute(
                """UPDATE market_focus_cycles SET completed_at='2020-01-01T00:00:00+00:00',
                   updated_at='2020-01-01T00:00:00+00:00'"""
            )
            await db.commit()
            await cleanup_extended_retention(db)
            retained = await (await db.execute(
                "SELECT COUNT(*) FROM market_focus_cycles WHERE error_code='submission_outcome_unknown'"
            )).fetchone()
            assert retained[0] == 1
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
    monkeypatch.setattr(settings, "hotspot_conditional_threshold", 50)

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
                "schema_version": "option-pro-macrolens-focus-v2",
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


def test_cycle_completion_uses_captured_formula_target_not_current_setting(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)
    monkeypatch.setattr(settings, "catalyst_context_support_target", 100.0)

    async def scenario():
        db = await database.get_db()
        try:
            context = FocusContext.model_validate({
                "schema_version": "option-pro-macrolens-focus-v2",
                "schema_sha256": FOCUS_SCHEMA_SHA256,
                "revision": 1,
                "as_of": datetime.now(timezone.utc),
                "data_through": datetime.now(timezone.utc),
                "market_session": "regular",
                "universe_version": "formula-capture",
                "symbols": [{
                    "ticker": "NVDA",
                    "validation_status": "canonical",
                    "universe_reasons": ["test"],
                    "as_of": datetime.now(timezone.utc),
                    "data_through": datetime.now(timezone.utc),
                    "data_quality": 1.0,
                    "data_status": "active",
                    "source_status": "active",
                }],
                "major_market_symbols": [],
                "warnings": [],
            })
            await persist_focus_context(db, context)
            await _seed_hotspot(db, 988)
            cycle = await create_market_focus_cycle(db, trigger_type="manual")
            captured = json.loads(cycle["input_json"])
            assert captured["provenance"]["catalyst_context_support_target"] == 100.0
        finally:
            await db.close()

        monkeypatch.setattr(settings, "catalyst_context_support_target", 1.0)
        assert await run_market_focus_worker_once(
            provider=TickerAssessmentProvider(), worker_id="formula-capture"
        ) is True
        db = await database.get_db()
        try:
            row = await (await db.execute(
                "SELECT input_json,result_json FROM market_focus_cycles"
            )).fetchone()
            snapshot = json.loads(row[0])
            result = json.loads(row[1])
            event_weight = float(snapshot["events"][0]["event_weight"])
            expected = round(40.0 * 0.8 * min(1.0, event_weight / 100.0), 4)
            assert result["focus_ticker_assessments"][0][
                "weighted_catalyst_context"
            ] == expected
        finally:
            await db.close()

    run(scenario())


def test_unknown_cycle_formula_is_rejected_before_provider_and_retry(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)

    async def scenario():
        db = await database.get_db()
        try:
            await _seed_hotspot(db, 989)
            cycle = await create_market_focus_cycle(db, trigger_type="manual")
            payload = json.loads(cycle["input_json"])
            payload["provenance"]["catalyst_context_formula_version"] = "future-v99"
            await db.execute(
                "UPDATE market_focus_cycles SET input_json=? WHERE cycle_id=?",
                (
                    json.dumps(payload, separators=(",", ":"), sort_keys=True),
                    cycle["cycle_id"],
                ),
            )
            await db.commit()
        finally:
            await db.close()

        provider = CompletedCycleProvider()
        assert await run_market_focus_worker_once(
            provider=provider, worker_id="unknown-formula"
        ) is True
        assert provider.create_calls == 0
        db = await database.get_db()
        try:
            row = await (await db.execute(
                "SELECT status,error_code FROM market_focus_cycles WHERE cycle_id=?",
                (cycle["cycle_id"],),
            )).fetchone()
            assert tuple(row) == ("failed", "unsupported_cycle_formula_version")
            with pytest.raises(CycleConflict, match="unsupported_cycle_formula_version"):
                await retry_market_focus_cycle(db, cycle["cycle_id"])
        finally:
            await db.close()

    run(scenario())


def test_event_group_versions_only_for_material_updates_and_out_of_window_news_stays_separate(
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
            assert version[0] == 1
            material = dict(first)
            material["url"] = "https://example.test/reuters/material-update"
            material["content_hash"] = "e" * 64
            material["summary"] = "The company raised revenue guidance to $42 billion after earnings."
            material_id = await database.insert_news_item(db, material)
            assert await ingest_event_evidence(db, material, news_id=material_id) == group
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


def test_same_url_ticker_correction_versions_existing_event_group(
    isolated_market_db,
):
    async def scenario():
        db = await database.get_db()
        try:
            first = news(926, source="reuters", title="Company updates merger terms")
            first["source_tickers"] = ["AAPL"]
            first_id = await database.insert_news_item(db, first)
            group = await ingest_event_evidence(db, first, news_id=first_id)

            corrected = dict(first)
            corrected["content_hash"] = "f" * 64
            corrected["source_tickers"] = ["MSFT"]
            corrected["summary"] = "The corrected filing identifies MSFT as the affected company."
            corrected_id = await database.insert_news_item(db, corrected)
            assert await ingest_event_evidence(db, corrected, news_id=corrected_id) == group
            row = await (await db.execute(
                "SELECT version FROM news_event_groups WHERE event_group_id=?",
                (group,),
            )).fetchone()
            assert row[0] == 2
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


def test_syndicated_repost_does_not_advance_event_available_at(isolated_market_db):
    async def scenario():
        db = await database.get_db()
        try:
            first = news(942, source="finnhub/Reuters")
            first["published_at"] = "2026-07-13T10:00:00+00:00"
            first["fetched_at"] = "2026-07-13T10:01:00+00:00"
            first_id = await database.insert_news_item(db, first)
            group = await ingest_event_evidence(db, first, news_id=first_id)

            repost = news(943, source="massive/Bloomberg", title=first["title"])
            repost["summary"] = first["summary"]
            repost["published_at"] = "2026-07-13T11:00:00+00:00"
            repost["fetched_at"] = "2026-07-13T11:01:00+00:00"
            repost_id = await database.insert_news_item(db, repost)
            assert await ingest_event_evidence(db, repost, news_id=repost_id) == group

            row = await (await db.execute(
                """SELECT available_at,member_count,source_count,version
                   FROM news_event_groups WHERE event_group_id=?""",
                (group,),
            )).fetchone()
            assert tuple(row) == ("2026-07-13T10:01:00+00:00", 2, 1, 1)
            preparations = await (await db.execute(
                """SELECT prepared_revision,event_group_version
                   FROM hotspot_preparation_sets WHERE event_group_id=?
                   ORDER BY prepared_revision""",
                (group,),
            )).fetchall()
            assert [tuple(value) for value in preparations] == [(1, 1)]
            assert (await get_hotspot_status(db))["prepared_revision"] == 1
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
            news_id = await database.insert_news_item(db, item)
            await ingest_event_evidence(db, item, news_id=news_id)
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


def test_new_york_rollup_date_handles_utc_midnight_and_dst():
    assert _new_york_trading_date("2026-07-14T00:30:00+00:00") == "2026-07-13"
    assert _new_york_trading_date("2026-01-14T02:30:00+00:00") == "2026-01-13"


def test_focus_snapshot_retention_protects_ticker_lineage_revisions(
    isolated_market_db, monkeypatch
):
    monkeypatch.setattr(settings, "focus_snapshot_retention_days", 90)
    monkeypatch.setattr(settings, "focus_snapshot_full_resolution_days", 30)
    monkeypatch.setattr(settings, "retention_batch_size", 20)

    async def scenario():
        db = await database.get_db()
        try:
            now = datetime.now(timezone.utc)
            for revision, observed, status in (
                (1, now - timedelta(days=120), "stale"),
                (2, now - timedelta(days=110), "stale"),
                (3, now, "current"),
            ):
                payload = {
                    "schema_version": "option-pro-macrolens-focus-v2",
                    "revision": revision,
                    "as_of": observed.isoformat(),
                    "data_through": observed.isoformat(),
                    "universe_version": "retention-lineage",
                    "symbols": [],
                }
                encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True)
                await db.execute(
                    """INSERT INTO focus_context_snapshots
                       (revision,schema_version,as_of,data_through,market_session,
                        universe_version,payload_json,payload_hash,status,fetched_at,created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        revision,
                        "option-pro-macrolens-focus-v2",
                        observed.isoformat(),
                        observed.isoformat(),
                        "closed",
                        "retention-lineage",
                        encoded,
                        hashlib.sha256(encoded.encode()).hexdigest(),
                        status,
                        observed.isoformat(),
                        observed.isoformat(),
                    ),
                )
            news_id = await database.insert_news_item(db, news(990))
            basis = build_validation_basis_hash(
                canonical_symbols=set(),
                external_symbols={"NVDA"},
                universe_version="retention-lineage",
            )
            await record_ticker_mention(
                db,
                news_id=news_id,
                ticker="NVDA",
                association_method="provider_tag",
                association_confidence=0.95,
                source="reuters",
                validation_status="valid_external",
                available_at=now - timedelta(days=120),
                focus_revision=1,
                universe_version="retention-lineage",
                validation_basis_hash=basis,
            )
            await db.execute(
                "UPDATE focus_validation_state SET last_focus_revision=3 WHERE singleton_id=1"
            )
            await db.commit()

            stats = await cleanup_extended_retention(db)
            revisions = await (await db.execute(
                "SELECT revision FROM focus_context_snapshots ORDER BY revision"
            )).fetchall()
            assert [row[0] for row in revisions] == [1, 3]
            assert stats["focus_snapshots_deleted"] == 1
            assert stats["focus_snapshots_lineage_protected"] >= 1
            assert await (await db.execute("PRAGMA foreign_key_check")).fetchall() == []
        finally:
            await db.close()

    run(scenario())


def test_focus_snapshot_retention_preserves_every_queued_revalidation_revision(
    isolated_market_db, monkeypatch
):
    monkeypatch.setattr(settings, "focus_snapshot_retention_days", 90)
    monkeypatch.setattr(settings, "focus_snapshot_full_resolution_days", 30)
    monkeypatch.setattr(settings, "retention_batch_size", 20)

    async def scenario():
        db = await database.get_db()
        try:
            observed = datetime.now(timezone.utc) - timedelta(days=120)
            for revision in range(1, 6):
                payload = {
                    "schema_version": "option-pro-macrolens-focus-v2",
                    "revision": revision,
                    "as_of": observed.isoformat(),
                    "data_through": observed.isoformat(),
                    "universe_version": f"queued-{revision}",
                    "symbols": [],
                }
                encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True)
                await db.execute(
                    """INSERT INTO focus_context_snapshots
                       (revision,schema_version,as_of,data_through,market_session,
                        universe_version,payload_json,payload_hash,status,fetched_at,created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        revision,
                        "option-pro-macrolens-focus-v2",
                        observed.isoformat(),
                        observed.isoformat(),
                        "closed",
                        f"queued-{revision}",
                        encoded,
                        hashlib.sha256(encoded.encode()).hexdigest(),
                        "current" if revision == 5 else "stale",
                        observed.isoformat(),
                        observed.isoformat(),
                    ),
                )
            await db.execute(
                """UPDATE focus_validation_state SET last_focus_revision=2,
                   pending_focus_revision=3,pending_run_key='queued-run',
                   pending_phase='mentions' WHERE singleton_id=1"""
            )
            await db.commit()

            stats = await cleanup_extended_retention(db)
            revisions = await (await db.execute(
                "SELECT revision FROM focus_context_snapshots ORDER BY revision"
            )).fetchall()
            assert [row[0] for row in revisions] == [2, 3, 4, 5]
            assert stats["focus_snapshots_deleted"] == 1
            assert stats["focus_snapshots_lineage_protected"] >= 3
        finally:
            await db.close()

    run(scenario())


def test_focus_snapshot_retention_rolls_up_days_and_protects_cycle_revision(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)
    monkeypatch.setattr(settings, "focus_snapshot_retention_days", 90)
    monkeypatch.setattr(settings, "focus_snapshot_full_resolution_days", 30)
    monkeypatch.setattr(settings, "focus_snapshot_daily_rollup_enabled", True)
    monkeypatch.setattr(settings, "retention_batch_size", 20)

    async def scenario():
        db = await database.get_db()
        try:
            observed = datetime.now(timezone.utc)
            snapshots = (
                observed - timedelta(days=45, hours=2),
                observed - timedelta(days=45, hours=1),
                observed - timedelta(days=40),
                observed - timedelta(days=101),
                observed - timedelta(days=100),
                observed,
            )
            for revision, as_of in enumerate(snapshots, start=1):
                context = FocusContext.model_validate({
                    "schema_version": "option-pro-macrolens-focus-v2",
                    "schema_sha256": FOCUS_SCHEMA_SHA256,
                    "revision": revision,
                    "as_of": as_of,
                    "data_through": as_of,
                    "market_session": "closed",
                    "universe_version": "retention-stable",
                    "symbols": [],
                    "major_market_symbols": [],
                    "warnings": [],
                })
                assert await persist_focus_context(db, context) is True

            await _seed_hotspot(db, 941)
            cycle = await create_market_focus_cycle(db, trigger_type="manual")
            await db.execute(
                "UPDATE market_focus_cycles SET focus_revision=5 WHERE cycle_id=?",
                (cycle["cycle_id"],),
            )
            await db.commit()

            stats = await cleanup_extended_retention(db)
            assert stats["focus_snapshots_deleted"] == 2
            assert stats["focus_snapshots_retained"] == 4
            assert stats["focus_snapshot_rollup_created"] == 1
            assert stats["focus_snapshots_cycle_protected"] == 1
            assert "wal_checkpointed_pages" in stats
            assert stats["database_bytes"] >= stats["live_bytes"]
            revisions = await (await db.execute(
                "SELECT revision FROM focus_context_snapshots ORDER BY revision"
            )).fetchall()
            assert [row[0] for row in revisions] == [2, 3, 5, 6]

            second = await cleanup_extended_retention(db)
            assert second["focus_snapshots_deleted"] == 0
            assert second["focus_snapshot_rollup_created"] == 0
            assert await (await db.execute("PRAGMA foreign_key_check")).fetchall() == []
        finally:
            await db.close()

    run(scenario())


def test_retention_archives_cycles_before_cleaning_consumed_evidence(
    isolated_market_db, monkeypatch
):
    _enable_cycles(monkeypatch)
    monkeypatch.setattr(settings, "market_focus_completed_retention_days", 1)
    monkeypatch.setattr(settings, "market_focus_failed_retention_days", 1)
    monkeypatch.setattr(settings, "hotspot_preparation_retention_days", 1)
    monkeypatch.setattr(settings, "event_member_retention_days", 1)
    monkeypatch.setattr(settings, "projection_retry_retention_days", 1)
    monkeypatch.setattr(settings, "retention_batch_size", 20)

    async def create_cycle(index: int, title: str):
        db = await database.get_db()
        try:
            item = news(index, source="finnhub/Reuters", title=title)
            news_id = await database.insert_news_item(db, item)
            group = await ingest_event_evidence(db, item, news_id=news_id)
            cycle = await create_market_focus_cycle(db, trigger_type="manual")
            return group, cycle["cycle_id"]
        finally:
            await db.close()

    async def run_cycle_worker(provider_type, worker_id: str):
        return await run_market_focus_worker_once(
            provider=provider_type(), worker_id=worker_id
        )

    completed_group, completed_cycle = run(create_cycle(
        920, "NVDA raises annual earnings guidance after record demand"
    ))
    assert run(run_cycle_worker(CompletedCycleProvider, "retention-completed")) is True
    failed_group, failed_cycle = run(create_cycle(
        921, "NVDA agrees landmark acquisition of a networking supplier"
    ))
    assert run(run_cycle_worker(IncompleteCycleProvider, "retention-incomplete")) is True

    async def scenario():
        old = "2020-01-01T00:00:00+00:00"
        db = await database.get_db()
        try:
            await db.execute(
                """UPDATE market_focus_cycles SET created_at=?,updated_at=?,completed_at=?
                   WHERE cycle_id IN (?,?)""",
                (old, old, old, completed_cycle, failed_cycle),
            )
            await db.execute(
                """UPDATE hotspot_preparation_sets SET prepared_at=?,created_at=?,
                   consumed_at=CASE WHEN status='CONSUMED' THEN ? ELSE consumed_at END""",
                (old, old, old),
            )
            await db.execute("UPDATE news_event_members SET created_at=?", (old,))
            await db.execute(
                """INSERT INTO event_projection_retries
                   (payload_hash,news_id,source,payload_json,status,attempt_count,
                    created_at,updated_at,completed_at)
                   VALUES (?,NULL,'google','{}','completed',1,?,?,?),
                          (?,NULL,'google','{}','pending',0,?,?,NULL)""",
                ("a" * 64, old, old, old, "b" * 64, old, old),
            )
            await db.commit()
            stats = await cleanup_extended_retention(db)
            assert stats["market_focus_completed_archived"] == 1
            assert stats["market_focus_failed_archived"] == 1
            assert stats["hotspot_preparation_sets"] == 1
            assert stats["news_event_members"] == 1
            assert stats["event_projection_retries"] == 1
            assert "database_live_bytes_before" in stats
            assert "database_free_bytes_after" in stats

            live_cycles = await (await db.execute(
                "SELECT COUNT(*) FROM market_focus_cycles"
            )).fetchone()
            archives = await (await db.execute(
                """SELECT cycle_id,result_json,event_snapshots_json
                   FROM market_focus_cycle_archives ORDER BY cycle_id"""
            )).fetchall()
            assert live_cycles[0] == 0
            assert len(archives) == 2
            completed_archive = next(row for row in archives if row[0] == completed_cycle)
            assert completed_archive[1] is not None
            snapshots = json.loads(completed_archive[2])
            assert len(snapshots) == 1
            assert snapshots[0]["snapshot_json"]

            preparations = await (await db.execute(
                "SELECT event_group_id,status FROM hotspot_preparation_sets"
            )).fetchall()
            assert [tuple(row) for row in preparations] == [(failed_group, "PREPARED")]
            members = await (await db.execute(
                "SELECT event_group_id FROM news_event_members"
            )).fetchall()
            assert [row[0] for row in members] == [failed_group]
            retries = await (await db.execute(
                "SELECT status FROM event_projection_retries"
            )).fetchall()
            assert [row[0] for row in retries] == ["pending"]
            assert completed_group != failed_group
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
