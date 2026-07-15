from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlencode

import aiosqlite
import pytest
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.config import Settings, settings
from app.integrations.option_pro import router as integration_router
from app.integrations.option_pro import repository as option_pro_repository
from app.integrations.option_pro.auth import (
    IntegrationAPIError,
    calculate_signature,
    canonical_path,
    canonical_query,
    canonical_string,
)
from app.integrations.option_pro.contract import (
    CONTRACT_PATH,
    generated_bytes,
    resolve_contract_path,
)
from app.integrations.option_pro.repository import (
    catalyst_result_status,
    query_calendar,
    query_feed,
    query_latest,
    query_ticker,
    record_calendar_snapshot,
    upsert_source_health,
)
from app.models import catalyst_database, database
from app.models.catalysts import CatalystBatchRequest, NewsImpactAnalysis
from app.routers import calendar as calendar_router
from app.routers import settings as settings_router
from app.services import calendar_analyzer, calendar_client
from app.services.focus_context import FOCUS_SCHEMA_SHA256, FocusContext, persist_focus_context
from app.services.market_focus import ingest_event_evidence
from app.services.ticker_lineage import (
    append_validation_revision,
    build_validation_basis_hash,
    record_ticker_mention,
)
from app.services.analysis_jobs import (
    InputVersionConflict,
    claim_next_job,
    create_or_get_job,
    enqueue_auto_jobs,
    enqueue_manual_jobs_with_status,
    recover_expired_job_leases,
    retry_failed_jobs,
    request_cancel,
    run_worker_once,
)
from app.services.responses_runtime import (
    OpenAIResponsesProvider,
    ProviderRequestRejected,
    ProviderCapabilities,
    ResponseResult,
    structured_output_format,
    validate_output,
)
from app.services import analysis_jobs as analysis_jobs_service
from app.worker_healthcheck import check as worker_healthcheck


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def isolated_integration_db(tmp_path, monkeypatch):
    path = tmp_path / "integration.db"
    monkeypatch.setattr(database, "DB_PATH", str(path))
    monkeypatch.setattr(settings, "news_llm_manual_enabled", True)
    monkeypatch.setattr(settings, "news_llm_manual_daily_job_limit", 50)
    monkeypatch.setattr(settings, "news_llm_manual_daily_output_token_limit", 1_638_400)
    run(database.init_db())
    return path


def news_record(index: int, *, summary: str | None = None, fetched_at: str | None = None) -> dict:
    now = fetched_at or datetime.now(timezone.utc).isoformat()
    return {
        "source": "test/source",
        "title": f"Market headline {index} with relevant company and policy context",
        "summary": summary if summary is not None else "A sufficiently detailed market summary with facts and context. " * 4,
        "url": f"https://example.test/news/{index}",
        "image_url": None,
        "published_at": now,
        "fetched_at": now,
        "content_hash": hashlib.sha256(f"news-{index}".encode()).hexdigest(),
        "source_tickers": ["AMD"],
    }


def valid_analysis(**overrides) -> dict:
    payload = {
        "title_zh": "测试新闻",
        "headline_summary": "公开信息显示公司更新了业务展望。",
        "overall_sentiment": 30,
        "classification": "bullish",
        "confidence": 70,
        "market_relevance": 65,
        "affected_stocks": [{
            "ticker": "AMD",
            "company": "Advanced Micro Devices",
            "impact_score": 35,
            "confidence": 70,
            "horizon": "days",
            "mechanism": "direct_company",
            "reason": "业务展望直接影响近期预期。",
        }],
        "affected_sectors": ["Semiconductors"],
        "affected_commodities": [],
        "causal_summary": "业务展望变化影响市场对近期收入的预期。",
        "key_factors": ["业务展望"],
        "uncertainty_notes": ["仍需后续正式披露确认。"],
        "insufficient_context": False,
    }
    payload.update(overrides)
    return payload


def completed_market_focus_result(
    cycle_id: str,
    *,
    as_of: str = "2026-07-15T09:20:47.333424Z",
) -> dict:
    return {
        "cycle_id": cycle_id,
        "as_of": as_of,
        "market_summary": "The persisted market-focus cycle completed successfully.",
        "dominant_events": [],
        "market_uncertainties": ["Later evidence may change the display-only summary."],
        "affected_sectors": [],
        "focus_ticker_assessments": [],
        "no_new_material_catalyst": True,
        "insufficient_context": False,
        "display_only": True,
    }


async def seed_completed_market_focus_cycle(
    db: aiosqlite.Connection,
    *,
    cycle_id: str = "mfc_0123456789abcdef0123456789abcdef",
    result_as_of: str = "2026-07-15T09:20:47.333424Z",
) -> None:
    snapshot_as_of = "2026-07-15T09:20:00Z"
    result = completed_market_focus_result(cycle_id, as_of=result_as_of)
    await db.execute(
        """INSERT INTO market_focus_cycles
           (cycle_id,idempotency_key,trigger_type,status,no_new_hot_events,
            snapshot_as_of,input_schema_version,input_hash,input_json,
            event_group_count,focus_symbol_count,model,reasoning_effort,
            execution_mode,max_output_tokens,prompt_version,output_schema_version,
            prompt_cache_key,result_json,created_at,completed_at,updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            cycle_id,
            f"manual:historical:{cycle_id}",
            "manual",
            "completed",
            1,
            snapshot_as_of,
            "market-focus-input-v1",
            "a" * 64,
            "{}",
            0,
            0,
            "gpt-5.6-terra",
            "max",
            "background",
            49_152,
            "market-focus-v1",
            "market-focus-output-v1",
            "market-focus:historical-result",
            json.dumps(result, separators=(",", ":")),
            snapshot_as_of,
            result_as_of,
            result_as_of,
        ),
    )
    await db.commit()


class FakeProvider:
    def __init__(self, *, created: ResponseResult, retrieved: list[ResponseResult] | None = None):
        self.created_result = created
        self.retrieved_results = list(retrieved or [])
        self.create_calls = 0
        self.retrieve_calls = 0
        self.cancel_calls = 0
        self.sync_calls = 0

    def capabilities(self):
        return ProviderCapabilities("ok", True, True, True, True, True)

    async def create_background(self, model_input: str, **request):
        self.create_calls += 1
        assert "<untrusted_news_data>" in model_input
        self.last_create_request = request
        return self.created_result

    async def retrieve(self, response_id: str):
        self.retrieve_calls += 1
        assert response_id == "resp_private"
        return self.retrieved_results.pop(0)

    async def cancel(self, response_id: str):
        self.cancel_calls += 1
        return ResponseResult(response_id, "cancelled")

    async def create_sync(self, model_input: str, **request):
        self.sync_calls += 1
        self.last_create_request = request
        return self.created_result


class RetrieveFailureProvider(FakeProvider):
    async def retrieve(self, response_id: str):
        self.retrieve_calls += 1
        raise RuntimeError("temporary retrieve failure")


class SyncSubmissionOutcomeUnknownProvider(FakeProvider):
    async def create_sync(self, model_input: str, **request):
        self.sync_calls += 1
        raise TimeoutError("response timed out after request submission")


class CancelFailureThenObserveProvider(FakeProvider):
    async def cancel(self, response_id: str):
        self.cancel_calls += 1
        raise RuntimeError("temporary cancel failure")

    async def retrieve(self, response_id: str):
        self.retrieve_calls += 1
        return ResponseResult(
            response_id,
            "completed",
            output_text=json.dumps(valid_analysis(), ensure_ascii=False),
            usage_input_tokens=73,
            usage_cached_input_tokens=13,
            usage_output_tokens=29,
        )


class CancelReturnsCompletedProvider(FakeProvider):
    async def cancel(self, response_id: str):
        self.cancel_calls += 1
        return ResponseResult(
            response_id,
            "completed",
            output_text=json.dumps(valid_analysis(), ensure_ascii=False),
            usage_input_tokens=61,
            usage_cached_input_tokens=9,
            usage_output_tokens=24,
        )


class CancelDuringSubmitProvider(FakeProvider):
    def __init__(self, job_id: str):
        super().__init__(created=ResponseResult("resp_private", "queued"))
        self.job_id = job_id

    async def create_background(self, model_input: str, **request):
        self.create_calls += 1
        db = await database.get_db()
        try:
            pending_cancel = await request_cancel(db, self.job_id)
            assert pending_cancel is not None
            assert pending_cancel["cancel_requested_at"] is not None
        finally:
            await db.close()
        return self.created_result

    async def cancel(self, response_id: str):
        self.cancel_calls += 1
        raise RuntimeError("cancel transport failed")


def test_terra_defaults_reasoning_validation_and_contract_file():
    configured = Settings(_env_file=None)
    assert configured.default_llm_model == "gpt-5.6-terra"
    assert configured.openai_reasoning == "max"
    assert configured.openai_execution_mode == "background"
    assert configured.openai_max_output_tokens == 128000
    assert configured.news_item_max_output_tokens == 32768
    assert configured.news_llm_auto_analyze_enabled is False
    with pytest.raises(ValidationError):
        Settings(_env_file=None, openai_reasoning="extreme")
    with pytest.raises(ValidationError):
        Settings(_env_file=None, openai_max_retries=1)
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            openai_base_url="https://attacker.example/v1",
        )
    custom = Settings(
        _env_file=None,
        openai_base_url="https://trusted-proxy.example/v1",
        openai_allow_custom_base_url=True,
    )
    assert custom.openai_base_url == "https://trusted-proxy.example/v1"
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            option_pro_read_key_id="read",
            option_pro_read_secret="short",
        )
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            option_pro_read_key_id="same",
            option_pro_read_secret="r" * 32,
            option_pro_action_key_id="same",
            option_pro_action_secret="a" * 32,
        )
    with pytest.raises(ValidationError, match="OPTION_PRO_ALLOWED_CIDRS"):
        Settings(
            _env_file=None,
            option_pro_read_key_id="read",
            option_pro_read_secret="r" * 32,
        )
    secured = Settings(
        _env_file=None,
        option_pro_read_key_id="read",
        option_pro_read_secret="r" * 32,
        option_pro_allowed_cidrs="203.0.113.10/32",
    )
    assert secured.option_pro_allowed_cidrs == "203.0.113.10/32"
    assert CONTRACT_PATH.read_bytes() == generated_bytes()
    contract_models = json.loads(CONTRACT_PATH.read_bytes())["models"]
    assert {
        "HotspotStatusResponse",
        "HotspotPreparationItem",
        "HotspotListResponse",
        "MarketFocusCyclePublic",
        "MarketFocusCycleResponse",
    }.issubset(contract_models)


def test_installed_sdk_capabilities_are_visible_without_a_key(monkeypatch):
    monkeypatch.setattr(settings, "openai_api_key", "")
    monkeypatch.setattr(settings, "default_llm_api_key", "")
    provider = OpenAIResponsesProvider()
    capabilities = provider.capabilities()
    if capabilities.detail == "ModuleNotFoundError":
        pytest.skip("OpenAI SDK is installed in the locked CI/runtime image")
    assert capabilities.status == "not_configured"
    assert capabilities.responses_create is True
    assert capabilities.responses_retrieve is True
    assert capabilities.responses_cancel is True
    run(provider.close())


def test_third_party_default_key_is_never_given_to_async_openai(monkeypatch):
    captured_keys: list[str] = []

    class FakeResponses:
        async def create(self, **_kwargs):
            raise AssertionError("no network request is expected")

        async def retrieve(self, _response_id):
            raise AssertionError("no network request is expected")

        async def cancel(self, _response_id):
            raise AssertionError("no network request is expected")

    class FakeAsyncOpenAI:
        def __init__(self, *, api_key, **_kwargs):
            captured_keys.append(api_key)
            self.responses = FakeResponses()

        async def close(self):
            return None

    monkeypatch.setitem(
        sys.modules,
        "openai",
        SimpleNamespace(AsyncOpenAI=FakeAsyncOpenAI),
    )
    monkeypatch.setattr(settings, "default_llm_provider", "anthropic")
    monkeypatch.setattr(settings, "default_llm_api_key", "anthropic-secret-never-for-openai")
    monkeypatch.setattr(settings, "openai_api_key", "")
    provider = OpenAIResponsesProvider()
    assert captured_keys == ["not-configured-capability-check"]
    assert "anthropic-secret-never-for-openai" not in captured_keys
    assert provider.capabilities().status == "not_configured"
    run(provider.close())


@pytest.mark.parametrize("create_method", ("create_background", "create_sync"))
def test_openai_provider_separates_definitive_4xx_from_ambiguous_failures(
    create_method,
):
    class RejectedRequest(RuntimeError):
        status_code = 400

        def __str__(self):
            return "provider detail must not become the durable error code"

    class FakeResponses:
        def __init__(self, failure):
            self.failure = failure

        async def create(self, **_kwargs):
            raise self.failure

        async def retrieve(self, _response_id):
            raise AssertionError("no retrieve request is expected")

        async def cancel(self, _response_id):
            raise AssertionError("no cancel request is expected")

    class FakeClient:
        def __init__(self, failure):
            self.responses = FakeResponses(failure)

    rejected = OpenAIResponsesProvider(client=FakeClient(RejectedRequest()))
    with pytest.raises(ProviderRequestRejected) as caught:
        run(getattr(rejected, create_method)("bounded input"))
    assert caught.value.error_code == "provider_request_rejected"
    assert caught.value.status_code == 400
    assert str(caught.value) == "provider_request_rejected"

    class RateLimitedRequest(RuntimeError):
        status_code = 429

    rate_limited = OpenAIResponsesProvider(
        client=FakeClient(RateLimitedRequest("rate limited"))
    )
    with pytest.raises(RateLimitedRequest, match="rate limited"):
        run(getattr(rate_limited, create_method)("bounded input"))

    timed_out = OpenAIResponsesProvider(
        client=FakeClient(TimeoutError("connection timed out"))
    )
    with pytest.raises(TimeoutError, match="connection timed out"):
        run(getattr(timed_out, create_method)("bounded input"))


def test_non_openai_default_provider_creates_no_openai_work(
    isolated_integration_db, monkeypatch
):
    monkeypatch.setattr(settings, "default_llm_provider", "anthropic")
    monkeypatch.setattr(settings, "default_llm_model", "claude-sonnet-4-6")
    monkeypatch.setattr(settings, "default_llm_api_key", "anthropic-secret-never-for-openai")
    monkeypatch.setattr(settings, "openai_api_key", "")
    provider = FakeProvider(created=ResponseResult("must-not-exist", "completed"))

    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(212))
            created = await create_or_get_job(db, news_id)
            assert created.job["status"] == "failed"
            assert created.job["provider"] == "anthropic"
            assert created.job["model"] == "claude-sonnet-4-6"
            assert created.job["reasoning_effort"] == "none"
            assert created.job["error_code"] == "unsupported_analysis_provider"
        finally:
            await db.close()
        assert await run_worker_once(provider=provider, worker_id="provider-isolation") is False
        assert provider.create_calls == 0
        assert provider.sync_calls == 0

    run(scenario())


def test_contract_path_supports_source_image_and_explicit_layouts(tmp_path):
    source_module = tmp_path / "repo" / "backend" / "app" / "integrations" / "option_pro" / "contract.py"
    source_contract = tmp_path / "repo" / "contracts" / "macrolens-option-pro-v2.json"
    source_module.parent.mkdir(parents=True)
    source_contract.parent.mkdir(parents=True)
    source_module.write_text("", encoding="utf-8")
    source_contract.write_text("{}", encoding="utf-8")
    assert resolve_contract_path(source_module) == source_contract

    image_module = tmp_path / "image" / "app" / "integrations" / "option_pro" / "contract.py"
    image_contract = tmp_path / "image" / "contracts" / "macrolens-option-pro-v2.json"
    image_module.parent.mkdir(parents=True)
    image_contract.parent.mkdir(parents=True)
    image_module.write_text("", encoding="utf-8")
    image_contract.write_text("{}", encoding="utf-8")
    assert resolve_contract_path(image_module) == image_contract

    explicit = tmp_path / "mounted" / "contract.json"
    assert resolve_contract_path(source_module, str(explicit)) == explicit
    with pytest.raises(RuntimeError):
        resolve_contract_path(source_module, "relative-contract.json")


def test_worker_health_checks_real_contract_and_database(isolated_integration_db):
    async def scenario():
        db = await database.get_db()
        try:
            now = datetime.now(timezone.utc).isoformat()
            await db.execute(
                """INSERT INTO analysis_worker_state
                   (worker_id,started_at,heartbeat_at,status)
                   VALUES ('health-worker',?,?, 'idle')""",
                (now, now),
            )
            await db.commit()
        finally:
            await db.close()
        assert await worker_healthcheck() == 0

    run(scenario())


def test_http_and_container_health_share_clock_skew_safe_worker_selection(
    isolated_integration_db,
    monkeypatch,
):
    monkeypatch.setattr(settings, "option_pro_read_key_id", "read-key")
    monkeypatch.setattr(settings, "option_pro_read_secret", "read-secret")
    monkeypatch.setattr(settings, "option_pro_action_key_id", "action-key")
    monkeypatch.setattr(settings, "option_pro_action_secret", "action-secret")
    monkeypatch.setattr(settings, "option_pro_allowed_cidrs", "127.0.0.1/32")
    monkeypatch.setattr(settings, "option_pro_allow_local_http", True)
    monkeypatch.setattr(settings, "openai_api_key", "test-openai-key")

    class HealthCheckProvider:
        def capabilities(self):
            return ProviderCapabilities("ok", True, True, True, True, True)

        async def close(self):
            return None

    monkeypatch.setattr(
        integration_router,
        "OpenAIResponsesProvider",
        HealthCheckProvider,
    )
    from app.utils import scheduler as scheduler_module

    monkeypatch.setattr(
        scheduler_module,
        "get_scheduler",
        lambda: SimpleNamespace(running=True),
    )

    async def seed_future_worker():
        db = await database.get_db()
        try:
            now = datetime.now(timezone.utc)
            await db.execute(
                """INSERT INTO analysis_worker_state
                   (worker_id,started_at,heartbeat_at,status)
                   VALUES (?,?,?, 'idle')""",
                (
                    "clock-skewed-old-worker",
                    (now - timedelta(days=1)).isoformat(),
                    (now + timedelta(minutes=5)).isoformat(),
                ),
            )
            await db.execute("DELETE FROM source_health")
            await db.commit()
        finally:
            await db.close()

    async def seed_live_worker():
        db = await database.get_db()
        try:
            now = datetime.now(timezone.utc).isoformat()
            await db.execute(
                """INSERT INTO analysis_worker_state
                   (worker_id,started_at,heartbeat_at,status)
                   VALUES ('current-live-worker',?,?, 'working')""",
                (now, now),
            )
            await db.commit()
        finally:
            await db.close()

    run(seed_future_worker())
    assert run(worker_healthcheck()) == 1

    app = _integration_app()
    target = "/api/integrations/option-pro/v1/health"
    with TestClient(app) as client:
        future_only = client.get(
            target,
            headers=_signed_headers(
                "GET", target, b"", "read-key", "read-secret", "future-only-worker"
            ),
        )
        assert future_only.status_code == 200
        assert future_only.json()["analysis_queue"]["status"] == "unavailable"
        assert future_only.json()["analysis_trigger_enabled"] is False
        assert (
            "analysis_worker_heartbeat_future" in future_only.json()["warnings"]
        )

        run(seed_live_worker())
        assert run(worker_healthcheck()) == 0
        coexistence = client.get(
            target,
            headers=_signed_headers(
                "GET", target, b"", "read-key", "read-secret", "live-and-future-worker"
            ),
        )
        assert coexistence.status_code == 200
        assert coexistence.json()["warnings"] == [
            "analysis_worker_heartbeat_future"
        ], coexistence.json()["warnings"]
        assert coexistence.json()["status"] == "ok", coexistence.json()
        assert coexistence.json()["analysis_queue"]["status"] == "ok"
        assert coexistence.json()["analysis_trigger_enabled"] is True
        assert (
            "analysis_worker_heartbeat_future" in coexistence.json()["warnings"]
        )

        monkeypatch.setattr(settings, "news_llm_manual_daily_job_limit", 1)

        async def exhaust_manual_budget():
            db = await database.get_db()
            try:
                await database.insert_news_item(db, news_record(970))
                await database.insert_news_item(db, news_record(971))
            finally:
                await db.close()
            enqueue_result = await enqueue_manual_jobs_with_status(limit=2)
            assert enqueue_result.enqueued == 1
            assert enqueue_result.stop_reason == "daily_job_limit_reached"
            db = await database.get_db()
            try:
                async with db.execute(
                    """SELECT COUNT(*) FROM analysis_jobs
                       WHERE status='budget_blocked'"""
                ) as cursor:
                    assert int((await cursor.fetchone())[0]) == 0
            finally:
                await db.close()

        run(exhaust_manual_budget())
        budget_health = client.get(
            target,
            headers=_signed_headers(
                "GET", target, b"", "read-key", "read-secret", "budget-health-check"
            ),
        )
        assert budget_health.status_code == 200
        assert (
            budget_health.json()["analysis_queue"]["budget_status"]
            == "budget_blocked"
        )


def test_structured_output_validates_complete_json_and_rejects_unknown_fields():
    encoded = json.dumps(valid_analysis(), ensure_ascii=False)
    assert validate_output(encoded).classification.value == "bullish"
    with pytest.raises(ValidationError):
        validate_output(json.dumps({**valid_analysis(), "unexpected": True}))
    with pytest.raises(ValidationError):
        validate_output(f"```json\n{encoded}\n```")
    with pytest.raises(ValidationError):
        validate_output(f"prefix {encoded}")


def test_structured_output_schema_is_strict_for_every_nested_object():
    schema = structured_output_format()["schema"]
    pending = [schema]
    objects = 0
    while pending:
        node = pending.pop()
        if isinstance(node, dict):
            properties = node.get("properties")
            if isinstance(properties, dict):
                objects += 1
                assert node.get("additionalProperties") is False
                assert node.get("required") == list(properties)
            pending.extend(node.values())
        elif isinstance(node, list):
            pending.extend(node)
    assert objects >= 3


def test_stock_company_limit_matches_projection_database_constraint():
    accepted = valid_analysis()
    accepted["affected_stocks"][0]["company"] = "C" * 200
    assert validate_output(json.dumps(accepted)).affected_stocks[0].company == "C" * 200

    rejected = valid_analysis()
    rejected["affected_stocks"][0]["company"] = "C" * 201
    with pytest.raises(ValidationError):
        validate_output(json.dumps(rejected))


def test_batch_contract_caps_tickers_at_fifty():
    CatalystBatchRequest(tickers=[f"T{i}" for i in range(50)])
    with pytest.raises(ValidationError):
        CatalystBatchRequest(tickers=[f"T{i}" for i in range(51)])
    with pytest.raises(ValidationError):
        CatalystBatchRequest(tickers=["AMD", "AMD"])


def test_low_context_job_is_terminal_without_provider_call(isolated_integration_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(1, summary=""))
            result = await create_or_get_job(db, news_id)
            assert result.job["status"] == "insufficient_context"
            async with db.execute("SELECT * FROM analysis_revisions WHERE news_id=?", (news_id,)) as cursor:
                revision = await cursor.fetchone()
            assert revision["model"] == "low-context-neutral-v2"
            assert revision["usage_input_tokens"] == 0
            payload = NewsImpactAnalysis.model_validate_json(revision["payload_json"])
            assert payload.insufficient_context is True
            assert payload.confidence == 0
            assert payload.affected_stocks == []
            async with db.execute(
                "SELECT analysis_status FROM news_items WHERE id=?", (news_id,)
            ) as cursor:
                assert (await cursor.fetchone())[0] == "insufficient_context"
        finally:
            await db.close()

        provider = FakeProvider(created=ResponseResult("should_not_exist", "completed"))
        assert await run_worker_once(provider=provider, worker_id="test-low-context") is False
        assert provider.create_calls == 0

    run(scenario())


def test_background_job_recovers_by_response_id_and_records_usage_and_latency(
    isolated_integration_db,
):
    completed = ResponseResult(
        "resp_private",
        "completed",
        output_text=json.dumps(valid_analysis(), ensure_ascii=False),
        usage_input_tokens=100,
        usage_cached_input_tokens=40,
        usage_output_tokens=80,
    )
    provider = FakeProvider(
        created=ResponseResult("resp_private", "queued"),
        retrieved=[completed],
    )

    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(2))
            first = await create_or_get_job(db, news_id)
            duplicate = await create_or_get_job(db, news_id)
            assert first.job["job_id"] == duplicate.job["job_id"]
            assert duplicate.created is False
        finally:
            await db.close()

        assert await run_worker_once(provider=provider, worker_id="worker-a") is True
        db = await database.get_db()
        try:
            async with db.execute("SELECT * FROM analysis_jobs WHERE news_id=?", (news_id,)) as cursor:
                queued = dict(await cursor.fetchone())
            assert queued["status"] == "queued"
            assert queued["openai_response_id"] == "resp_private"
            submitted_at = datetime.now(timezone.utc) - timedelta(seconds=5)
            await db.execute(
                "UPDATE analysis_jobs SET next_attempt_at=?,submitted_at=? WHERE job_id=?",
                (
                    (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),
                    submitted_at.isoformat(),
                    queued["job_id"],
                ),
            )
            await db.commit()
        finally:
            await db.close()

        assert await run_worker_once(provider=provider, worker_id="worker-b") is True
        assert provider.create_calls == 1
        assert provider.retrieve_calls == 1
        db = await database.get_db()
        try:
            async with db.execute("SELECT * FROM analysis_jobs WHERE news_id=?", (news_id,)) as cursor:
                finished = dict(await cursor.fetchone())
            assert finished["status"] == "completed"
            assert finished["usage_input_tokens"] == 100
            assert finished["usage_cached_input_tokens"] == 40
            assert finished["usage_output_tokens"] == 80
            assert finished["latency_ms"] >= 4_000
            async with db.execute("SELECT COUNT(*) FROM analysis_stock_impacts WHERE news_id=?", (news_id,)) as cursor:
                assert (await cursor.fetchone())[0] == 1
        finally:
            await db.close()

    run(scenario())


def test_paid_insufficient_context_preserves_result_and_sets_terminal_status(
    isolated_integration_db,
    monkeypatch,
):
    monkeypatch.setattr(settings, "news_llm_manual_daily_job_limit", 1)
    honest_low_confidence = {
        **valid_analysis(),
        "classification": "bullish",
        "confidence": 25,
        "market_relevance": 20,
        "insufficient_context": True,
    }
    provider = FakeProvider(
        created=ResponseResult(
            "resp_private",
            "completed",
            output_text=json.dumps(honest_low_confidence, ensure_ascii=False),
            usage_input_tokens=100,
            usage_output_tokens=80,
        )
    )

    async def scenario():
        db = await database.get_db()
        try:
            system_news_id = await database.insert_news_item(
                db, news_record(201, summary="")
            )
            system_job = await create_or_get_job(db, system_news_id)
            assert system_job.job["provider"] == "system"
            assert system_job.job["status"] == "insufficient_context"
            news_id = await database.insert_news_item(db, news_record(202))
            created = await create_or_get_job(db, news_id)
        finally:
            await db.close()

        assert await run_worker_once(provider=provider, worker_id="worker-low-confidence") is True
        db = await database.get_db()
        try:
            async with db.execute(
                "SELECT status,error_code,latency_ms FROM analysis_jobs WHERE job_id=?",
                (created.job["job_id"],),
            ) as cursor:
                job = dict(await cursor.fetchone())
            assert job["status"] == "insufficient_context"
            assert job["error_code"] is None
            assert job["latency_ms"] is not None
            async with db.execute(
                "SELECT payload_json FROM analysis_revisions WHERE news_id=?",
                (news_id,),
            ) as cursor:
                revision = await cursor.fetchone()
            payload = NewsImpactAnalysis.model_validate_json(revision[0])
            assert payload.insufficient_context is True
            assert payload.classification.value == "bullish"
            assert payload.confidence == 25
            assert [stock.ticker for stock in payload.affected_stocks] == ["AMD"]
            async with db.execute(
                "SELECT ticker,validation_status FROM analysis_stock_impacts WHERE news_id=?",
                (news_id,),
            ) as cursor:
                impact = await cursor.fetchone()
            assert tuple(impact) == ("AMD", "valid_external")
            async with db.execute(
                "SELECT analysis_status FROM news_items WHERE id=?", (news_id,)
            ) as cursor:
                assert (await cursor.fetchone())[0] == "insufficient_context"

            second_news_id = await database.insert_news_item(db, news_record(203))
            second = await create_or_get_job(db, second_news_id)
            assert second.job["status"] == "budget_blocked"
            assert second.job["error_code"] == "daily_job_limit_reached"
        finally:
            await db.close()

        assert await run_worker_once(
            provider=provider, worker_id="worker-budget-blocked"
        ) is False
        assert provider.create_calls == 1

    run(scenario())


def test_paid_insufficient_context_counts_actual_output_tokens(
    isolated_integration_db,
    monkeypatch,
):
    monkeypatch.setattr(settings, "news_llm_manual_daily_job_limit", 100)
    monkeypatch.setattr(settings, "news_item_max_output_tokens", 1024)
    monkeypatch.setattr(settings, "news_llm_manual_daily_output_token_limit", 1123)
    provider = FakeProvider(
        created=ResponseResult(
            "resp_private",
            "completed",
            output_text=json.dumps(
                valid_analysis(
                    confidence=25,
                    market_relevance=20,
                    insufficient_context=True,
                ),
                ensure_ascii=False,
            ),
            usage_output_tokens=100,
        )
    )

    async def scenario():
        db = await database.get_db()
        try:
            first_news_id = await database.insert_news_item(db, news_record(204))
            first = await create_or_get_job(db, first_news_id)
        finally:
            await db.close()

        assert await run_worker_once(provider=provider, worker_id="worker-token-budget") is True

        db = await database.get_db()
        try:
            async with db.execute(
                "SELECT status,usage_output_tokens FROM analysis_jobs WHERE job_id=?",
                (first.job["job_id"],),
            ) as cursor:
                assert tuple(await cursor.fetchone()) == ("insufficient_context", 100)
            second_news_id = await database.insert_news_item(db, news_record(205))
            second = await create_or_get_job(db, second_news_id)
            assert second.job["status"] == "budget_blocked"
            assert second.job["error_code"] == "daily_output_token_limit_reached"
        finally:
            await db.close()

        assert await run_worker_once(
            provider=provider, worker_id="worker-token-budget-blocked"
        ) is False
        assert provider.create_calls == 1

    run(scenario())


def test_model_tickers_are_validated_before_trusted_projection(isolated_integration_db):
    stocks = [
        {
            "ticker": ticker,
            "company": company,
            "impact_score": 30,
            "confidence": 70,
            "horizon": "days",
            "mechanism": "direct_company",
            "reason": "The supplied news directly names the company.",
        }
        for ticker, company in (
            ("NVDA", "NVIDIA"),
            ("AMD", "Advanced Micro Devices"),
            ("AI", "C3.ai"),
            ("XYZ", "Unknown Issuer"),
        )
    ]
    provider = FakeProvider(
        created=ResponseResult(
            "resp_private",
            "completed",
            output_text=json.dumps(valid_analysis(affected_stocks=stocks), ensure_ascii=False),
        )
    )

    async def scenario():
        db = await database.get_db()
        try:
            now = datetime.now(timezone.utc).isoformat()
            context = FocusContext.model_validate_json(json.dumps({
                "schema_version": "option-pro-macrolens-focus-v2",
                "schema_sha256": FOCUS_SCHEMA_SHA256,
                "revision": 7,
                "as_of": now,
                "data_through": now,
                "market_session": "regular",
                "universe_version": "universe-7",
                "symbols": [{
                    "ticker": "NVDA",
                    "validation_status": "canonical",
                    "universe_reasons": ["dollar_volume_top20"],
                    "as_of": now,
                    "data_quality": 0.9,
                    "data_status": "active",
                }],
                "major_market_symbols": ["SPY"],
                "warnings": [],
            }))
            await persist_focus_context(db, context)
            record = news_record(220)
            news_id = await database.insert_news_item(db, record)
            event_group_id = await ingest_event_evidence(db, record, news_id=news_id)
            await create_or_get_job(db, news_id)
        finally:
            await db.close()

        assert await run_worker_once(provider=provider, worker_id="ticker-validation") is True
        db = await database.get_db()
        try:
            impacts = await (await db.execute(
                """SELECT ticker,validation_status,focus_revision,universe_version,
                          association_method FROM analysis_stock_impacts
                   WHERE news_id=? ORDER BY ticker""",
                (news_id,),
            )).fetchall()
            assert [tuple(row) for row in impacts] == [
                ("AI", "ambiguous", 7, "universe-7", "llm_inference"),
                ("AMD", "valid_external", 7, "universe-7", "llm_inference"),
                ("NVDA", "canonical", 7, "universe-7", "llm_inference"),
                ("XYZ", "unverified", 7, "universe-7", "llm_inference"),
            ]
            mentions = await (await db.execute(
                """SELECT ticker,validation_status FROM news_ticker_mentions
                   WHERE news_id=? AND association_method='llm_inference'
                   ORDER BY ticker""",
                (news_id,),
            )).fetchall()
            assert [tuple(row) for row in mentions] == [
                ("AI", "ambiguous"),
                ("AMD", "valid_external"),
                ("NVDA", "canonical"),
                ("XYZ", "unverified"),
            ]
            detail = await (await db.execute(
                "SELECT payload_json FROM analysis_revisions WHERE news_id=?", (news_id,)
            )).fetchone()
            assert {item["ticker"] for item in json.loads(detail[0])["affected_stocks"]} == {
                "AI", "AMD", "NVDA", "XYZ",
            }
            trusted, *_ = await query_ticker(
                db,
                ticker="AMD",
                as_of=datetime.now(timezone.utc),
                window_hours=72,
                limit=20,
                cursor=None,
                min_confidence=0,
                include_neutral=True,
                include_unanalyzed=True,
            )
            ambiguous, *_ = await query_ticker(
                db,
                ticker="AI",
                as_of=datetime.now(timezone.utc),
                window_hours=72,
                limit=20,
                cursor=None,
                min_confidence=0,
                include_neutral=True,
                include_unanalyzed=True,
            )
            assert len(trusted) == 1
            assert ambiguous == []
            assert {
                (item.ticker, item.validation_status)
                for item in trusted[0].analysis.stock_validations
            } == {
                ("AI", "ambiguous"),
                ("AMD", "valid_external"),
                ("NVDA", "canonical"),
                ("XYZ", "unverified"),
            }
            event = await (await db.execute(
                """SELECT version,validated_tickers_json FROM news_event_groups
                   WHERE event_group_id=?""",
                (event_group_id,),
            )).fetchone()
            assert event[0] == 2
            assert json.loads(event[1]) == ["AMD", "NVDA"]
        finally:
            await db.close()

    run(scenario())


def test_invalid_model_ticker_is_counted_but_never_persisted(isolated_integration_db):
    invalid = valid_analysis()
    invalid["affected_stocks"][0]["ticker"] = "DROP TABLE news_items"
    provider = FakeProvider(
        created=ResponseResult(
            "resp_private",
            "completed",
            output_text=json.dumps(invalid, ensure_ascii=False),
        )
    )

    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(221))
            await create_or_get_job(db, news_id)
        finally:
            await db.close()
        assert await run_worker_once(provider=provider, worker_id="invalid-ticker") is True
        db = await database.get_db()
        try:
            job = await (await db.execute(
                "SELECT status,error_code FROM analysis_jobs WHERE news_id=?", (news_id,)
            )).fetchone()
            counter = await (await db.execute(
                "SELECT count FROM projection_safety_counters WHERE counter_key='invalid_model_ticker'"
            )).fetchone()
            revisions = await (await db.execute(
                "SELECT COUNT(*) FROM analysis_revisions WHERE news_id=?", (news_id,)
            )).fetchone()
            mentions = await (await db.execute(
                "SELECT COUNT(*) FROM news_ticker_mentions WHERE news_id=?", (news_id,)
            )).fetchone()
            assert tuple(job) == ("failed", "invalid_structured_output")
            assert counter[0] == 1
            assert revisions[0] == 0
            assert mentions[0] == 0
        finally:
            await db.close()

    run(scenario())


def test_provider_completed_is_not_local_terminal_before_publish_commit(
    isolated_integration_db, monkeypatch
):
    provider = FakeProvider(
        created=ResponseResult(
            "resp_private",
            "completed",
            output_text=json.dumps(valid_analysis(), ensure_ascii=False),
        )
    )

    async def fail_before_publish(*_args, **_kwargs):
        raise RuntimeError("simulated crash before local publish")

    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(210))
            created = await create_or_get_job(db, news_id)
        finally:
            await db.close()
        monkeypatch.setattr(analysis_jobs_service, "_handle_provider_result", fail_before_publish)
        with pytest.raises(RuntimeError, match="simulated crash"):
            await run_worker_once(provider=provider, worker_id="publish-crash")
        db = await database.get_db()
        try:
            async with db.execute(
                "SELECT status,openai_response_id FROM analysis_jobs WHERE job_id=?",
                (created.job["job_id"],),
            ) as cursor:
                assert tuple(await cursor.fetchone()) == ("in_progress", "resp_private")
            await db.execute(
                "UPDATE analysis_jobs SET lease_expires_at=? WHERE job_id=?",
                ((datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(), created.job["job_id"]),
            )
            await db.commit()
            assert await recover_expired_job_leases(db) == 1
            async with db.execute(
                "SELECT status,openai_response_id FROM analysis_jobs WHERE job_id=?",
                (created.job["job_id"],),
            ) as cursor:
                assert tuple(await cursor.fetchone()) == ("queued", "resp_private")
        finally:
            await db.close()

    run(scenario())


def test_job_freezes_model_and_reasoning_at_enqueue_time(isolated_integration_db, monkeypatch):
    provider = FakeProvider(created=ResponseResult("resp_private", "queued"))

    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(200))
            created = await create_or_get_job(db, news_id)
            assert created.job["model"] == "gpt-5.6-terra"
            assert created.job["reasoning_effort"] == "max"
        finally:
            await db.close()
        monkeypatch.setattr(settings, "default_llm_model", "operator-changed-model")
        monkeypatch.setattr(settings, "openai_reasoning", "low")
        assert await run_worker_once(provider=provider, worker_id="frozen-request") is True
        assert provider.last_create_request["model"] == "gpt-5.6-terra"
        assert provider.last_create_request["reasoning_effort"] == "max"

    run(scenario())


def test_provider_model_or_reasoning_drift_fails_without_publication(isolated_integration_db):
    provider = FakeProvider(
        created=ResponseResult(
            "resp_private",
            "completed",
            output_text=json.dumps(valid_analysis(), ensure_ascii=False),
            model="unexpected-model",
            reasoning_effort="low",
        )
    )

    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(209))
            created = await create_or_get_job(db, news_id)
        finally:
            await db.close()
        assert await run_worker_once(provider=provider, worker_id="drift-check") is True
        db = await database.get_db()
        try:
            async with db.execute(
                "SELECT status,error_code FROM analysis_jobs WHERE job_id=?",
                (created.job["job_id"],),
            ) as cursor:
                assert tuple(await cursor.fetchone()) == ("failed", "provider_model_mismatch")
            async with db.execute(
                "SELECT COUNT(*) FROM analysis_revisions WHERE news_id=?", (news_id,)
            ) as cursor:
                assert int((await cursor.fetchone())[0]) == 0
        finally:
            await db.close()

    run(scenario())


def test_background_concurrency_counts_leased_and_remote_jobs(isolated_integration_db, monkeypatch):
    monkeypatch.setattr(settings, "news_llm_max_inflight", 1)
    monkeypatch.setattr(settings, "openai_max_concurrency", 1)

    async def scenario():
        db = await database.get_db()
        try:
            first_id = await database.insert_news_item(db, news_record(201))
            second_id = await database.insert_news_item(db, news_record(202))
            await create_or_get_job(db, first_id)
            await create_or_get_job(db, second_id)
            first = await claim_next_job(db, "concurrency-a")
            assert first is not None
            assert await claim_next_job(db, "concurrency-b") is None
            await db.execute(
                """UPDATE analysis_jobs SET status='queued',openai_response_id='resp_private',
                   lease_owner=NULL,lease_expires_at=NULL WHERE job_id=?""",
                (first["job_id"],),
            )
            await db.commit()
            resumed = await claim_next_job(db, "concurrency-c")
            assert resumed is not None
            assert resumed["job_id"] == first["job_id"]
            async with db.execute(
                """SELECT COUNT(*) FROM analysis_jobs
                   WHERE lease_owner='concurrency-c' AND openai_response_id IS NULL"""
            ) as cursor:
                assert int((await cursor.fetchone())[0]) == 0
        finally:
            await db.close()

    run(scenario())


def test_unknown_background_submission_reserves_news_concurrency_until_timeout(
    isolated_integration_db, monkeypatch
):
    monkeypatch.setattr(settings, "news_llm_max_inflight", 1)
    monkeypatch.setattr(settings, "openai_max_concurrency", 1)
    monkeypatch.setattr(settings, "openai_background_poll_timeout_seconds", 60)

    async def scenario():
        db = await database.get_db()
        try:
            first_id = await database.insert_news_item(db, news_record(210))
            second_id = await database.insert_news_item(db, news_record(211))
            first = await create_or_get_job(db, first_id)
            second = await create_or_get_job(db, second_id)
            now = datetime.now(timezone.utc)
            await db.execute(
                """UPDATE analysis_jobs
                   SET status='failed',error_code='submission_outcome_unknown',
                       execution_mode='background',submitted_at=?,completed_at=?,
                       lease_owner=NULL,lease_expires_at=NULL
                   WHERE job_id=?""",
                (now.isoformat(), now.isoformat(), first.job["job_id"]),
            )
            await db.commit()

            assert await claim_next_job(db, "unknown-news-blocked") is None

            expired = (now - timedelta(seconds=61)).isoformat()
            await db.execute(
                "UPDATE analysis_jobs SET submitted_at=? WHERE job_id=?",
                (expired, first.job["job_id"]),
            )
            await db.commit()
            claimed = await claim_next_job(db, "unknown-news-released")
            assert claimed is not None
            assert claimed["job_id"] == second.job["job_id"]
        finally:
            await db.close()

    run(scenario())


def test_completed_is_idempotent_and_failed_force_retry_is_append_only(
    isolated_integration_db, monkeypatch
):
    monkeypatch.setattr(settings, "news_llm_daily_job_limit", 10)

    async def scenario():
        db = await database.get_db()
        try:
            completed_id = await database.insert_news_item(db, news_record(203))
            completed = await create_or_get_job(db, completed_id)
            await db.execute(
                "UPDATE analysis_jobs SET status='completed',usage_output_tokens=17 WHERE job_id=?",
                (completed.job["job_id"],),
            )
            await db.commit()
            repeated = await create_or_get_job(db, completed_id, force=True)
            assert repeated.created is False
            assert repeated.job["job_id"] == completed.job["job_id"]

            failed_id = await database.insert_news_item(db, news_record(204))
            failed = await create_or_get_job(db, failed_id)
            await db.execute(
                """UPDATE analysis_jobs SET status='failed',error_code='invalid_structured_output',
                   usage_input_tokens=11,usage_output_tokens=7 WHERE job_id=?""",
                (failed.job["job_id"],),
            )
            await db.commit()
            without_force = await create_or_get_job(db, failed_id)
            assert without_force.created is False
            retried = await create_or_get_job(db, failed_id, force=True)
            assert retried.created is True
            assert retried.job["job_id"] != failed.job["job_id"]
            assert retried.job["retry_of_job_id"] == failed.job["job_id"]
            async with db.execute(
                """SELECT job_id,status,error_code,usage_input_tokens,usage_output_tokens
                   FROM analysis_jobs WHERE news_id=? ORDER BY execution_number""",
                (failed_id,),
            ) as cursor:
                rows = await cursor.fetchall()
            assert len(rows) == 2
            assert tuple(rows[0]) == (
                failed.job["job_id"], "failed", "invalid_structured_output", 11, 7
            )
            assert rows[1][0] == retried.job["job_id"]
            assert rows[1][1] == "pending"
        finally:
            await db.close()

    run(scenario())


def test_version_precondition_rejects_stale_content_without_creating_job(isolated_integration_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(205))
            with pytest.raises(InputVersionConflict, match="content_hash_mismatch"):
                await create_or_get_job(
                    db,
                    news_id,
                    expected_content_hash="f" * 64,
                )
            async with db.execute("SELECT COUNT(*) FROM analysis_jobs") as cursor:
                assert int((await cursor.fetchone())[0]) == 0
        finally:
            await db.close()

    run(scenario())


def test_worker_rejects_news_mutated_after_enqueue_without_provider_call(isolated_integration_db):
    provider = FakeProvider(created=ResponseResult("resp_private", "queued"))

    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(208))
            created = await create_or_get_job(db, news_id)
            await db.execute(
                "UPDATE news_items SET summary=?,updated_at=? WHERE id=?",
                ("Changed after enqueue " * 20, datetime.now(timezone.utc).isoformat(), news_id),
            )
            await db.commit()
        finally:
            await db.close()
        assert await run_worker_once(provider=provider, worker_id="version-race") is True
        assert provider.create_calls == 0
        db = await database.get_db()
        try:
            async with db.execute(
                "SELECT status,error_code FROM analysis_jobs WHERE job_id=?",
                (created.job["job_id"],),
            ) as cursor:
                assert tuple(await cursor.fetchone()) == ("failed", "news_version_changed")
        finally:
            await db.close()

    run(scenario())


def test_invalid_structured_output_publishes_no_partial_projection(isolated_integration_db):
    provider = FakeProvider(
        created=ResponseResult(
            "resp_private",
            "completed",
            output_text=json.dumps({**valid_analysis(), "unexpected": "reject"}),
            usage_input_tokens=91,
            usage_cached_input_tokens=11,
            usage_output_tokens=37,
        )
    )

    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(3))
            created = await create_or_get_job(db, news_id)
        finally:
            await db.close()
        assert await run_worker_once(provider=provider, worker_id="worker-invalid") is True
        db = await database.get_db()
        try:
            async with db.execute(
                """SELECT status,error_code,openai_response_id,usage_input_tokens,
                          usage_cached_input_tokens,usage_output_tokens,completed_at
                   FROM analysis_jobs WHERE news_id=?""",
                (news_id,),
            ) as cursor:
                row = await cursor.fetchone()
            assert row[0] == "failed"
            assert row[1] == "invalid_structured_output"
            # It remains private in storage for audit/recovery, never in public models.
            assert row[2] == "resp_private"
            assert tuple(row[3:6]) == (91, 11, 37)
            assert row[6] is not None
            async with db.execute("SELECT COUNT(*) FROM analysis_revisions WHERE news_id=?", (news_id,)) as cursor:
                assert (await cursor.fetchone())[0] == 0
            async with db.execute("SELECT COUNT(*) FROM analysis_stock_impacts WHERE news_id=?", (news_id,)) as cursor:
                assert (await cursor.fetchone())[0] == 0
            unchanged = await request_cancel(db, created.job["job_id"])
            assert unchanged is not None
            assert unchanged["status"] == "failed"
            assert unchanged["error_code"] == "invalid_structured_output"
            assert unchanged["usage_output_tokens"] == 37
        finally:
            await db.close()

    run(scenario())


def test_background_cancel_is_idempotent_and_cancelled_upstream(
    isolated_integration_db, monkeypatch
):
    monkeypatch.setattr(settings, "openai_execution_mode", "background")
    provider = FakeProvider(created=ResponseResult("resp_private", "queued"))

    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(29))
            created = await create_or_get_job(db, news_id)
        finally:
            await db.close()
        assert await run_worker_once(provider=provider, worker_id="worker-cancel-create") is True
        monkeypatch.setattr(settings, "openai_execution_mode", "worker_sync")
        db = await database.get_db()
        try:
            cancelled = await request_cancel(db, created.job["job_id"])
            repeated = await request_cancel(db, created.job["job_id"])
            assert cancelled["status"] == repeated["status"] == "cancelled"
            assert cancelled["error_code"] == "upstream_cancel_pending"
        finally:
            await db.close()
        assert await run_worker_once(provider=provider, worker_id="worker-cancel-send") is True
        assert provider.cancel_calls == 1
        db = await database.get_db()
        try:
            async with db.execute("SELECT status,error_code FROM analysis_jobs WHERE job_id=?", (created.job["job_id"],)) as cursor:
                assert tuple(await cursor.fetchone()) == ("cancelled", None)
        finally:
            await db.close()

    run(scenario())


def test_cancel_requested_during_submission_keeps_response_for_retry_observation(
    isolated_integration_db,
):
    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(206))
            created = await create_or_get_job(db, news_id)
        finally:
            await db.close()
        provider = CancelDuringSubmitProvider(created.job["job_id"])
        assert await run_worker_once(provider=provider, worker_id="cancel-during-submit") is True
        db = await database.get_db()
        try:
            async with db.execute(
                """SELECT status,error_code,cancel_attempt_count,openai_response_id,next_attempt_at
                   FROM analysis_jobs WHERE job_id=?""",
                (created.job["job_id"],),
            ) as cursor:
                row = await cursor.fetchone()
            assert row[0] == "cancelled"
            assert row[1] == "upstream_cancel_pending"
            assert row[2] == 1
            assert row[3] == "resp_private"
            assert row[4] is not None
        finally:
            await db.close()

    run(scenario())


def test_cancel_failures_retry_then_observe_upstream_terminal_state(isolated_integration_db):
    provider = CancelFailureThenObserveProvider(created=ResponseResult("resp_private", "queued"))

    async def make_due(job_id: str):
        db = await database.get_db()
        try:
            await db.execute(
                "UPDATE analysis_jobs SET next_attempt_at=? WHERE job_id=?",
                ((datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(), job_id),
            )
            await db.commit()
        finally:
            await db.close()

    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(290))
            created = await create_or_get_job(db, news_id)
        finally:
            await db.close()
        assert await run_worker_once(provider=provider, worker_id="cancel-seed") is True
        db = await database.get_db()
        try:
            await request_cancel(db, created.job["job_id"])
        finally:
            await db.close()
        for attempt in range(3):
            await make_due(created.job["job_id"])
            assert await run_worker_once(provider=provider, worker_id=f"cancel-retry-{attempt}") is True
        db = await database.get_db()
        try:
            async with db.execute(
                "SELECT status,error_code,cancel_attempt_count,openai_response_id FROM analysis_jobs WHERE job_id=?",
                (created.job["job_id"],),
            ) as cursor:
                row = await cursor.fetchone()
            assert tuple(row) == ("cancelled", "upstream_cancel_observe", 3, "resp_private")
        finally:
            await db.close()
        await make_due(created.job["job_id"])
        assert await run_worker_once(provider=provider, worker_id="cancel-observe") is True
        assert provider.cancel_calls == 3
        assert provider.retrieve_calls == 1
        db = await database.get_db()
        try:
            async with db.execute(
                """SELECT status,error_code,next_attempt_at,usage_input_tokens,
                          usage_cached_input_tokens,usage_output_tokens
                   FROM analysis_jobs WHERE job_id=?""",
                (created.job["job_id"],),
            ) as cursor:
                assert tuple(await cursor.fetchone()) == (
                    "completed", None, None, 73, 13, 29
                )
            async with db.execute(
                "SELECT COUNT(*) FROM analysis_revisions WHERE job_id=?",
                (created.job["job_id"],),
            ) as cursor:
                assert int((await cursor.fetchone())[0]) == 1
        finally:
            await db.close()

    run(scenario())


def test_cancel_response_completed_publishes_instead_of_overwriting_with_cancelled(
    isolated_integration_db,
):
    provider = CancelReturnsCompletedProvider(
        created=ResponseResult("resp_private", "queued")
    )

    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(211))
            created = await create_or_get_job(db, news_id)
        finally:
            await db.close()
        assert await run_worker_once(provider=provider, worker_id="cancel-complete-seed") is True
        db = await database.get_db()
        try:
            await request_cancel(db, created.job["job_id"])
        finally:
            await db.close()
        assert await run_worker_once(provider=provider, worker_id="cancel-complete-race") is True
        db = await database.get_db()
        try:
            async with db.execute(
                """SELECT status,usage_input_tokens,usage_cached_input_tokens,
                          usage_output_tokens,next_attempt_at
                   FROM analysis_jobs WHERE job_id=?""",
                (created.job["job_id"],),
            ) as cursor:
                assert tuple(await cursor.fetchone()) == ("completed", 61, 9, 24, None)
            async with db.execute(
                "SELECT COUNT(*) FROM analysis_revisions WHERE job_id=?",
                (created.job["job_id"],),
            ) as cursor:
                assert int((await cursor.fetchone())[0]) == 1
        finally:
            await db.close()

    run(scenario())


def test_retrieve_errors_are_separate_from_poll_count_and_keep_response_id(isolated_integration_db):
    provider = RetrieveFailureProvider(created=ResponseResult("unused", "queued"))

    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(291))
            created = await create_or_get_job(db, news_id)
            await db.execute(
                """UPDATE analysis_jobs SET status='queued',openai_response_id='resp_private',
                   attempt_count=100,next_attempt_at=? WHERE job_id=?""",
                ((datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(), created.job["job_id"]),
            )
            await db.commit()
        finally:
            await db.close()
        for attempt in range(6):
            db = await database.get_db()
            try:
                await db.execute(
                    "UPDATE analysis_jobs SET next_attempt_at=? WHERE job_id=?",
                    ((datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(), created.job["job_id"]),
                )
                await db.commit()
            finally:
                await db.close()
            assert await run_worker_once(provider=provider, worker_id=f"retrieve-retry-{attempt}") is True
        db = await database.get_db()
        try:
            async with db.execute(
                "SELECT status,openai_response_id,retrieve_error_count,attempt_count FROM analysis_jobs WHERE job_id=?",
                (created.job["job_id"],),
            ) as cursor:
                row = await cursor.fetchone()
            assert row[0] == "queued"
            assert row[1] == "resp_private"
            assert row[2] == 6
            assert row[3] == 106
        finally:
            await db.close()

    run(scenario())


def test_budget_gate_blocks_second_paid_job_before_queue(isolated_integration_db, monkeypatch):
    monkeypatch.setattr(settings, "news_llm_manual_daily_job_limit", 1)

    async def scenario():
        db = await database.get_db()
        try:
            first_id = await database.insert_news_item(db, news_record(30))
            second_id = await database.insert_news_item(db, news_record(31))
            first = await create_or_get_job(db, first_id)
            second = await create_or_get_job(db, second_id)
            assert first.job["status"] == "pending"
            assert second.job["status"] == "budget_blocked"
            assert second.job["error_code"] == "daily_job_limit_reached"
        finally:
            await db.close()

    run(scenario())


def test_output_token_budget_reserves_active_jobs_and_releases_unused_capacity(
    isolated_integration_db, monkeypatch
):
    monkeypatch.setattr(settings, "news_llm_manual_daily_job_limit", 100)
    monkeypatch.setattr(settings, "news_item_max_output_tokens", 1024)
    monkeypatch.setattr(settings, "news_llm_manual_daily_output_token_limit", 1124)

    async def scenario():
        db = await database.get_db()
        try:
            first_id = await database.insert_news_item(db, news_record(303))
            second_id = await database.insert_news_item(db, news_record(304))
            third_id = await database.insert_news_item(db, news_record(305))
            first = await create_or_get_job(db, first_id)
            blocked = await create_or_get_job(db, second_id)
            assert first.job["status"] == "pending"
            assert first.job["max_output_tokens"] == 1024
            assert blocked.job["status"] == "budget_blocked"
            assert blocked.job["error_code"] == "daily_output_token_limit_reached"

            now = datetime.now(timezone.utc).isoformat()
            await db.execute(
                """UPDATE analysis_jobs
                   SET status='completed',usage_output_tokens=100,completed_at=?,updated_at=?
                   WHERE job_id=?""",
                (now, now, first.job["job_id"]),
            )
            await db.commit()
            released = await create_or_get_job(db, third_id)
            assert released.job["status"] == "pending"
        finally:
            await db.close()

    run(scenario())


def test_default_auto_queue_does_not_create_jobs_or_change_news_status(isolated_integration_db):
    async def scenario():
        db = await database.get_db()
        try:
            irrelevant = news_record(34)
            irrelevant["title"] = "Community garden announces its weekend volunteer schedule"
            irrelevant["summary"] = "Residents will meet at the neighborhood garden for planting and cleanup activities. " * 3
            irrelevant_id = await database.insert_news_item(db, irrelevant)
            tagged = news_record(35)
            tagged["source_tickers"] = ["AMD"]
            tagged_id = await database.insert_news_item(db, tagged)
        finally:
            await db.close()
        assert await enqueue_auto_jobs(limit=10) == 0
        db = await database.get_db()
        try:
            async with db.execute("SELECT analysis_status,analysis_error FROM news_items WHERE id=?", (irrelevant_id,)) as cursor:
                irrelevant_row = await cursor.fetchone()
            assert tuple(irrelevant_row) == ("pending", "")
            async with db.execute("SELECT priority,status FROM analysis_jobs WHERE news_id=?", (tagged_id,)) as cursor:
                assert await cursor.fetchone() is None
        finally:
            await db.close()

    run(scenario())


def test_worker_sync_uses_no_background_response_id(isolated_integration_db, monkeypatch):
    monkeypatch.setattr(settings, "openai_execution_mode", "worker_sync")
    monkeypatch.setattr(settings, "news_item_max_output_tokens", 1024)
    provider = FakeProvider(
        created=ResponseResult(
            "transient_sync_id",
            "completed",
            output_text=json.dumps(valid_analysis(), ensure_ascii=False),
        )
    )

    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(32))
            created = await create_or_get_job(db, news_id)
            assert created.job["execution_mode"] == "worker_sync"
            assert created.job["max_output_tokens"] == 1024
        finally:
            await db.close()
        monkeypatch.setattr(settings, "openai_execution_mode", "background")
        monkeypatch.setattr(settings, "news_item_max_output_tokens", 32768)
        assert await run_worker_once(provider=provider, worker_id="worker-sync") is True
        assert provider.sync_calls == 1
        assert provider.create_calls == 0
        assert provider.last_create_request["max_output_tokens"] == 1024
        db = await database.get_db()
        try:
            async with db.execute("SELECT status,openai_response_id FROM analysis_jobs WHERE news_id=?", (news_id,)) as cursor:
                row = await cursor.fetchone()
            assert row[0] == "completed"
            assert row[1] is None
        finally:
            await db.close()

    run(scenario())


def test_background_response_is_retrieved_after_switch_to_worker_sync(
    isolated_integration_db, monkeypatch
):
    monkeypatch.setattr(settings, "openai_execution_mode", "background")
    provider = FakeProvider(
        created=ResponseResult("resp_private", "queued"),
        retrieved=[
            ResponseResult(
                "resp_private",
                "completed",
                output_text=json.dumps(valid_analysis(), ensure_ascii=False),
            )
        ],
    )

    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(306))
            created = await create_or_get_job(db, news_id)
            assert created.job["execution_mode"] == "background"
        finally:
            await db.close()

        assert await run_worker_once(provider=provider, worker_id="background-submit") is True
        monkeypatch.setattr(settings, "openai_execution_mode", "worker_sync")
        db = await database.get_db()
        try:
            await db.execute(
                "UPDATE analysis_jobs SET next_attempt_at=? WHERE job_id=?",
                (
                    (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),
                    created.job["job_id"],
                ),
            )
            await db.commit()
        finally:
            await db.close()

        assert await run_worker_once(provider=provider, worker_id="background-retrieve") is True
        assert provider.create_calls == 1
        assert provider.sync_calls == 0
        assert provider.retrieve_calls == 1
        db = await database.get_db()
        try:
            async with db.execute(
                "SELECT status FROM analysis_jobs WHERE job_id=?",
                (created.job["job_id"],),
            ) as cursor:
                assert (await cursor.fetchone())[0] == "completed"
        finally:
            await db.close()

    run(scenario())


def test_unknown_background_submission_outcome_is_not_requeued(isolated_integration_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(33))
            created = await create_or_get_job(db, news_id)
            past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
            await db.execute(
                """UPDATE analysis_jobs SET status='in_progress',error_code='submission_in_progress',
                   lease_owner='dead-worker',lease_expires_at=? WHERE job_id=?""",
                (past, created.job["job_id"]),
            )
            await db.commit()
            assert await recover_expired_job_leases(db) == 1
            async with db.execute("SELECT status,error_code,next_attempt_at FROM analysis_jobs WHERE job_id=?", (created.job["job_id"],)) as cursor:
                row = await cursor.fetchone()
            assert tuple(row) == ("failed", "submission_outcome_unknown", None)
        finally:
            await db.close()

    run(scenario())


def test_unknown_worker_sync_submission_outcome_is_not_retried(
    isolated_integration_db, monkeypatch
):
    monkeypatch.setattr(settings, "openai_execution_mode", "worker_sync")
    provider = SyncSubmissionOutcomeUnknownProvider(
        created=ResponseResult(None, "failed")
    )

    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(330))
            created = await create_or_get_job(db, news_id)
        finally:
            await db.close()

        assert await run_worker_once(provider=provider, worker_id="sync-unknown") is True
        db = await database.get_db()
        try:
            row = await (await db.execute(
                "SELECT status,error_code,next_attempt_at FROM analysis_jobs WHERE job_id=?",
                (created.job["job_id"],),
            )).fetchone()
            assert tuple(row) == ("failed", "submission_outcome_unknown", None)
            assert await retry_failed_jobs(db, news_id=news_id) == []
        finally:
            await db.close()
        assert provider.sync_calls == 1

    run(scenario())


def test_legacy_schema_upgrade_is_idempotent_and_does_not_publish_logic_chain(tmp_path, monkeypatch):
    path = tmp_path / "legacy.db"
    monkeypatch.setattr(database, "DB_PATH", str(path))
    backfill_calls = {"legacy": 0, "impacts": 0, "events": 0}

    original_legacy = catalyst_database._backfill_legacy_analyses
    original_impacts = catalyst_database._backfill_stock_impact_validation
    original_events = catalyst_database._backfill_event_evidence_fingerprints

    async def counted_legacy(db):
        backfill_calls["legacy"] += 1
        await original_legacy(db)

    async def counted_impacts(db):
        backfill_calls["impacts"] += 1
        await original_impacts(db)

    async def counted_events(db):
        backfill_calls["events"] += 1
        await original_events(db)

    monkeypatch.setattr(catalyst_database, "_backfill_legacy_analyses", counted_legacy)
    monkeypatch.setattr(catalyst_database, "_backfill_stock_impact_validation", counted_impacts)
    monkeypatch.setattr(catalyst_database, "_backfill_event_evidence_fingerprints", counted_events)

    async def scenario():
        async with aiosqlite.connect(path) as db:
            await db.execute(database.CREATE_NEWS_ITEMS)
            await db.execute(database.CREATE_ANALYSES)
            await db.execute(database.CREATE_SETTINGS)
            await db.execute(
                """CREATE TABLE source_health (
                    source TEXT PRIMARY KEY,status TEXT NOT NULL,last_attempt_at TEXT,
                    last_success_at TEXT,data_through TEXT,consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT,error_code TEXT,updated_at TEXT NOT NULL
                )"""
            )
            legacy_jobs_sql = catalyst_database.CREATE_ANALYSIS_JOBS.replace(
                ",'incomplete_output'", ""
            ).replace(
                "    source_input_hash TEXT NOT NULL,\n", ""
            ).replace(
                "    content_hash TEXT NOT NULL,\n", ""
            ).replace(
                "    UNIQUE(news_id, input_hash, model, prompt_version, schema_version)\n",
                "    UNIQUE(news_id, input_hash, model, prompt_version, schema_version)\n",
            )
            await db.execute(legacy_jobs_sql)
            await db.execute(
                """INSERT INTO news_items
                   (id,source,title,summary,url,published_at,fetched_at,content_hash,analysis_status)
                   VALUES (1,'legacy','Legacy headline','Legacy summary with sufficient context',
                           'https://example.test/legacy','2026-07-10T10:00:00+00:00',
                           '2026-07-10T10:01:00+00:00',?,'completed')""",
                ("f" * 64,),
            )
            await db.execute(
                """INSERT INTO analyses
                   (news_id,title_zh,headline_summary,overall_sentiment,classification,confidence,
                    affected_stocks,affected_sectors,affected_commodities,logic_chain,key_factors,
                    llm_provider,llm_model,analyzed_at)
                   VALUES (1,'旧新闻','旧摘要',10,'bullish',60,?,'[]','[]',?,'[]',
                           'openai','gpt-4o-mini','2026-07-10T10:02:00+00:00')""",
                (
                    json.dumps([{"ticker": "AMD", "company": "AMD", "impact_score": 20, "reason": "旧原因"}]),
                    "private step one -> private step two",
                ),
            )
            await db.execute("INSERT INTO settings(key,value) VALUES ('default_llm_model','\"gpt-4o-mini\"')")
            await db.execute(
                """INSERT INTO analysis_jobs
                   (job_id,news_id,input_hash,status,model,reasoning_effort,prompt_version,
                    schema_version,created_at,updated_at)
                   VALUES ('legacy-job',1,?,'failed','legacy-model','none','legacy-v1',
                           'legacy-v1','2026-07-10T10:02:00+00:00',
                           '2026-07-10T10:02:00+00:00')""",
                ("a" * 64,),
            )
            await db.commit()

        await database.init_db()
        await database.init_db()
        db = await database.get_db()
        try:
            async with db.execute("SELECT payload_json,is_legacy FROM analysis_revisions WHERE news_id=1") as cursor:
                revision = await cursor.fetchone()
            payload = json.loads(revision[0])
            assert revision[1] == 1
            assert payload["causal_summary"] == "旧版分析未保存可安全公开的因果摘要。"
            assert "private step" not in revision[0]
            async with db.execute(
                """SELECT confidence,horizon,mechanism,validation_status,
                          association_method,validated_at
                   FROM analysis_stock_impacts"""
            ) as cursor:
                impact = await cursor.fetchone()
            assert tuple(impact[:5]) == (
                0, "uncertain", "other", "unverified", "llm_inference",
            )
            assert impact[5] is not None
            async with db.execute("SELECT COUNT(*) FROM analysis_revisions") as cursor:
                assert (await cursor.fetchone())[0] == 1
            async with db.execute(
                "SELECT key FROM settings WHERE key IN ('default_llm_provider','default_llm_model')"
            ) as cursor:
                assert await cursor.fetchall() == []
            async with db.execute("PRAGMA foreign_key_check") as cursor:
                assert await cursor.fetchall() == []
            async with db.execute("PRAGMA table_info(source_health)") as cursor:
                source_columns = {row[1] for row in await cursor.fetchall()}
            assert {
                "raw_count",
                "inserted_count",
                "duplicates_count",
                "source_fetch_status",
                "news_persistence_status",
                "event_projection_status",
            } <= source_columns
            async with db.execute("PRAGMA user_version") as cursor:
                assert (await cursor.fetchone())[0] == 5
            async with db.execute(
                "SELECT source_input_hash,content_hash FROM analysis_jobs WHERE job_id='legacy-job'"
            ) as cursor:
                legacy_job = await cursor.fetchone()
            assert tuple(legacy_job) == ("a" * 64, "f" * 64)
        finally:
            await db.close()

        assert backfill_calls == {"legacy": 1, "impacts": 1, "events": 1}

    run(scenario())


def test_point_in_time_hides_analysis_until_available_at(isolated_integration_db):
    published = "2026-07-12T10:00:00+00:00"
    fetched = "2026-07-12T10:04:00+00:00"
    analyzed = "2026-07-12T10:06:00+00:00"

    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(4, fetched_at=fetched))
            await db.execute("UPDATE news_items SET published_at=? WHERE id=?", (published, news_id))
            payload = json.dumps(valid_analysis(), ensure_ascii=False, separators=(",", ":"))
            await db.execute(
                """INSERT INTO analysis_revisions
                   (news_id,job_id,revision,input_hash,payload_json,provider,model,reasoning_effort,
                    prompt_version,schema_version,fetched_at,analyzed_at,available_at,created_at)
                   VALUES (?,NULL,1,? ,?,'test','gpt-5.6-terra','max','news-impact-v2',
                           'news-impact-schema-v2',?,?,?,?)""",
                (news_id, "a" * 64, payload, fetched, analyzed, analyzed, analyzed),
            )
            await db.commit()
            before, _, _, _ = await query_feed(
                db, as_of=datetime.fromisoformat("2026-07-12T10:05:00+00:00"),
                window_hours=24, limit=20, cursor=None, source=None, classification=None,
                min_confidence=0, min_abs_impact=0, analysis_status=None,
            )
            after, _, _, _ = await query_feed(
                db, as_of=datetime.fromisoformat("2026-07-12T10:07:00+00:00"),
                window_hours=24, limit=20, cursor=None, source=None, classification=None,
                min_confidence=0, min_abs_impact=0, analysis_status=None,
            )
            assert len(before) == len(after) == 1
            assert before[0].analysis is None
            assert before[0].classification if hasattr(before[0], "classification") else True
            assert after[0].analysis is not None
            assert after[0].analysis.available_at == datetime.fromisoformat(analyzed)
        finally:
            await db.close()

    run(scenario())


def test_failed_schema_migration_rolls_back_and_restores_foreign_keys(tmp_path, monkeypatch):
    path = tmp_path / "migration-failure.db"

    async def scenario():
        db = await aiosqlite.connect(path)
        try:
            await db.execute("PRAGMA foreign_keys=ON")
            await db.execute(
                """CREATE TABLE news_items (
                       id INTEGER PRIMARY KEY,
                       fetched_at TEXT NOT NULL,
                       analysis_status TEXT NOT NULL DEFAULT 'pending'
                   )"""
            )
            await db.execute("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT)")
            await db.commit()

            original_add_column = catalyst_database._add_column
            calls = 0

            async def fail_during_migration(connection, table, column, definition):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise RuntimeError("injected_migration_failure")
                await original_add_column(connection, table, column, definition)

            monkeypatch.setattr(catalyst_database, "_add_column", fail_during_migration)
            with pytest.raises(RuntimeError, match="injected_migration_failure"):
                await catalyst_database.init_catalyst_schema(db)

            assert not db.in_transaction
            async with db.execute("PRAGMA foreign_keys") as cursor:
                assert (await cursor.fetchone())[0] == 1
            async with db.execute("PRAGMA user_version") as cursor:
                assert (await cursor.fetchone())[0] == 0
            async with db.execute("PRAGMA table_info(news_items)") as cursor:
                columns = {row[1] for row in await cursor.fetchall()}
            assert "updated_at" not in columns
        finally:
            await db.close()

    run(scenario())


def test_calendar_actual_is_append_only_and_point_in_time(isolated_integration_db):
    base_event = {
        "date": "2026-07-15T12:30:00+00:00",
        "country_code": "USD",
        "title": "Consumer Price Index",
        "impact": "high",
        "forecast": "2.5%",
        "previous": "2.4%",
        "actual": "",
    }

    async def scenario():
        db = await database.get_db()
        try:
            await record_calendar_snapshot(
                db, [base_event], source_fetched_at="2026-07-15T10:00:00+00:00", stale=False,
            )
            await record_calendar_snapshot(
                db, [{**base_event, "actual": "2.6%"}],
                source_fetched_at="2026-07-15T11:00:00+00:00", stale=False,
            )
            earlier, _ = await query_calendar(
                db, date_from=date(2026, 7, 15), date_to=date(2026, 7, 15),
                as_of=datetime.fromisoformat("2026-07-15T10:30:00+00:00"),
                currencies=["USD"], min_impact="high",
            )
            later, _ = await query_calendar(
                db, date_from=date(2026, 7, 15), date_to=date(2026, 7, 15),
                as_of=datetime.fromisoformat("2026-07-15T11:30:00+00:00"),
                currencies=["USD"], min_impact="high",
            )
            assert earlier[0].actual is None
            assert later[0].actual == "2.6%"
            async with db.execute("SELECT COUNT(*) FROM calendar_event_revisions") as cursor:
                assert (await cursor.fetchone())[0] == 2
        finally:
            await db.close()

    run(scenario())


def test_calendar_same_values_record_fresh_stale_fresh_point_in_time(isolated_integration_db):
    event = {
        "date": "2026-07-15T12:30:00+00:00",
        "country_code": "USD",
        "title": "Retail Sales",
        "impact": "high",
        "forecast": "0.4%",
        "previous": "0.3%",
        "actual": "",
    }

    async def scenario():
        db = await database.get_db()
        try:
            await record_calendar_snapshot(
                db,
                [event],
                source_fetched_at="2026-07-15T09:00:00+00:00",
                observed_at="2026-07-15T09:00:00+00:00",
                stale=False,
            )
            await record_calendar_snapshot(
                db,
                [event],
                source_fetched_at="2026-07-15T09:00:00+00:00",
                observed_at="2026-07-15T10:00:00+00:00",
                stale=True,
            )
            await record_calendar_snapshot(
                db,
                [event],
                source_fetched_at="2026-07-15T11:00:00+00:00",
                observed_at="2026-07-15T11:00:00+00:00",
                stale=False,
            )
            states = []
            for value in ("09:30:00", "10:30:00", "11:30:00"):
                items, _ = await query_calendar(
                    db,
                    date_from=date(2026, 7, 15),
                    date_to=date(2026, 7, 15),
                    as_of=datetime.fromisoformat(f"2026-07-15T{value}+00:00"),
                    currencies=["USD"],
                    min_impact="high",
                )
                states.append(items[0].is_stale)
            assert states == [False, True, False]
            async with db.execute("SELECT COUNT(*) FROM calendar_event_revisions") as cursor:
                assert int((await cursor.fetchone())[0]) == 3
        finally:
            await db.close()

    run(scenario())


def test_calendar_delayed_response_is_not_visible_before_response_arrival(
    isolated_integration_db, monkeypatch, tmp_path
):
    raw_event = {
        "date": "2026-07-15T12:30:00+00:00",
        "title": "Consumer Price Index",
        "country": "USD",
        "impact": "High",
        "forecast": "2.5%",
        "previous": "2.4%",
        "actual": "2.6%",
    }

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return [raw_event]

    class DelayedClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, *_args, **_kwargs):
            return FakeResponse()

    times = iter(
        (
            "2026-07-15T10:00:00Z",
            "2026-07-15T10:00:15Z",
            "2026-07-15T10:00:16Z",
        )
    )
    monkeypatch.setattr(calendar_client, "_utc_now", lambda: next(times))
    monkeypatch.setattr(calendar_client.httpx, "AsyncClient", lambda **_kwargs: DelayedClient())
    monkeypatch.setattr(calendar_client, "CACHE_FILE", tmp_path / "calendar.json")
    monkeypatch.setattr(calendar_client, "_calendar_cache", [])
    monkeypatch.setattr(calendar_client, "_cache_time", 0.0)
    monkeypatch.setattr(
        calendar_client,
        "_calendar_status",
        {
            "source": "faireconomy",
            "stale": False,
            "as_of": None,
            "last_attempt": None,
            "last_success": None,
            "last_error": None,
        },
    )

    async def scenario():
        events = await calendar_client.fetch_economic_calendar(force=True)
        assert events[0]["actual"] == "2.6%"
        db = await database.get_db()
        try:
            before, _ = await query_calendar(
                db,
                date_from=date(2026, 7, 15),
                date_to=date(2026, 7, 15),
                as_of=datetime.fromisoformat("2026-07-15T10:00:05+00:00"),
                currencies=["USD"],
                min_impact="high",
            )
            after, _ = await query_calendar(
                db,
                date_from=date(2026, 7, 15),
                date_to=date(2026, 7, 15),
                as_of=datetime.fromisoformat("2026-07-15T10:00:17+00:00"),
                currencies=["USD"],
                min_impact="high",
            )
            assert before == []
            assert after[0].actual == "2.6%"
            assert after[0].source_fetched_at == datetime.fromisoformat(
                "2026-07-15T10:00:15+00:00"
            )
            assert after[0].available_at == datetime.fromisoformat(
                "2026-07-15T10:00:16+00:00"
            )
        finally:
            await db.close()

    run(scenario())


def test_calendar_stale_fallback_preserves_persistent_source_health(
    isolated_integration_db, tmp_path, monkeypatch
):
    fetched_at = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()
    raw_event = {
        "date": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
        "title": "Consumer Price Index",
        "country": "USD",
        "impact": "High",
        "forecast": "0.2%",
        "previous": "0.1%",
        "actual": "",
    }

    class FailingClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, url, **_kwargs):
            raise RuntimeError(f"offline: {url.split('?', 1)[0]}")

    async def scenario():
        db = await database.get_db()
        try:
            await upsert_source_health(
                db,
                source="faireconomy",
                status="degraded",
                last_attempt_at=fetched_at,
                last_success_at=fetched_at,
                data_through=fetched_at,
                consecutive_failures=2,
                next_attempt_at=None,
                raw_count=0,
                inserted_count=None,
                duplicates_count=None,
                error_code="calendar_source_failed",
            )
        finally:
            await db.close()
        events = await calendar_client.fetch_economic_calendar(force=True)
        assert events and events[0]["is_stale"] is True
        db = await database.get_db()
        try:
            async with db.execute(
                """SELECT last_success_at,consecutive_failures,raw_count,next_attempt_at
                   FROM source_health WHERE source='faireconomy'"""
            ) as cursor:
                row = await cursor.fetchone()
            assert row[0] == fetched_at
            assert row[1] == 3
            assert row[2] == 0
            assert row[3] is not None
        finally:
            await db.close()

    cache_file = tmp_path / "calendar-cache.json"
    monkeypatch.setattr(calendar_client, "CACHE_FILE", cache_file)
    monkeypatch.setattr(calendar_client, "_calendar_cache", [])
    monkeypatch.setattr(calendar_client, "_cache_time", 0.0)
    monkeypatch.setattr(calendar_client, "_cache_ttl", calendar_client.CACHE_TTL)
    monkeypatch.setattr(
        calendar_client,
        "_calendar_status",
        {
            "source": "faireconomy",
            "stale": False,
            "as_of": None,
            "last_attempt": None,
            "last_success": None,
            "last_error": None,
        },
    )
    monkeypatch.setattr(calendar_client.httpx, "AsyncClient", lambda **_kwargs: FailingClient())
    calendar_client._atomic_write_cache([raw_event], fetched_at)
    run(scenario())


def test_news_and_ticker_status_expose_degraded_source_as_stale(isolated_integration_db):
    async def scenario():
        db = await database.get_db()
        try:
            record = news_record(207)
            record["source_tickers"] = ["AMD"]
            await database.insert_news_item(db, record)
            await upsert_source_health(
                db,
                source="test",
                status="degraded",
                last_attempt_at=record["fetched_at"],
                last_success_at=record["fetched_at"],
                data_through=record["fetched_at"],
                consecutive_failures=1,
                next_attempt_at=None,
                raw_count=1,
                inserted_count=1,
                duplicates_count=0,
                error_code="source_fetch_failed",
            )
            items, _, _, _ = await query_feed(
                db,
                as_of=datetime.now(timezone.utc) + timedelta(seconds=1),
                window_hours=24,
                limit=20,
                cursor=None,
                source=None,
                classification=None,
                min_confidence=0,
                min_abs_impact=0,
                analysis_status=None,
            )
            assert len(items) == 1
            assert items[0].is_stale is True
            assert await catalyst_result_status(db, items) == "stale"

            old_success = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
            await upsert_source_health(
                db,
                source="test",
                status="ok",
                last_attempt_at=old_success,
                last_success_at=old_success,
                data_through=old_success,
                consecutive_failures=0,
                next_attempt_at=None,
                raw_count=1,
                inserted_count=0,
                duplicates_count=1,
                error_code=None,
            )
            aged, _, _, _ = await query_feed(
                db,
                as_of=datetime.now(timezone.utc) + timedelta(seconds=1),
                window_hours=24,
                limit=20,
                cursor=None,
                source=None,
                classification=None,
                min_confidence=0,
                min_abs_impact=0,
                analysis_status=None,
            )
            assert aged[0].is_stale is True
        finally:
            await db.close()

    run(scenario())


def test_feed_drops_provider_tickers_that_violate_the_public_contract(isolated_integration_db):
    async def scenario():
        db = await database.get_db()
        try:
            record = news_record(208)
            record["source_tickers"] = [
                "AMD",
                "ema.pr.a:ca",
                "$MSFT",
                "MSFT",
                "AMD",
                "NOT A TICKER",
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            ]
            news_id = await database.insert_news_item(db, record)
            assert news_id is not None
            await ingest_event_evidence(db, record, news_id=news_id)
            await db.commit()
            items, _, _, _ = await query_feed(
                db,
                as_of=datetime.now(timezone.utc) + timedelta(seconds=1),
                window_hours=24,
                limit=20,
                cursor=None,
                source=None,
                classification=None,
                min_confidence=0,
                min_abs_impact=0,
                analysis_status=None,
            )
            assert len(items) == 1
            assert items[0].source_tickers == ["AMD", "MSFT"]

            _, latest_items, _, _, _, _ = await query_latest(
                db,
                updated_after=datetime.now(timezone.utc) - timedelta(days=1),
                limit=20,
                cursor=None,
            )
            assert len(latest_items) == 1
            assert latest_items[0].source_tickers == ["AMD", "MSFT"]
        finally:
            await db.close()

    run(scenario())


def test_feed_source_tickers_use_only_trusted_non_llm_projection(
    isolated_integration_db,
):
    async def scenario():
        db = await database.get_db()
        try:
            record = news_record(218)
            record["source_tickers"] = ["XYZ"]
            news_id = await database.insert_news_item(db, record)
            assert news_id is not None
            basis = build_validation_basis_hash(
                canonical_symbols=set(),
                external_symbols=set(),
                universe_version="provider-projection-test",
            )
            mention = await record_ticker_mention(
                db,
                news_id=news_id,
                ticker="XYZ",
                association_method="exact_alias",
                association_confidence=0.8,
                source="alias_dictionary",
                validation_status="unverified",
                available_at=datetime.now(timezone.utc),
                focus_revision=None,
                universe_version="provider-projection-test",
                validation_basis_hash=basis,
                reason_code="fixture_unverified",
            )
            await db.commit()
            first_as_of = datetime.now(timezone.utc) + timedelta(seconds=1)
            untrusted, _, _, _ = await query_feed(
                db,
                as_of=first_as_of,
                window_hours=24,
                limit=20,
                cursor=None,
                source=None,
                classification=None,
                min_confidence=0,
                min_abs_impact=0,
                analysis_status=None,
            )
            assert untrusted[0].source_tickers == []

            canonical_at = first_as_of + timedelta(seconds=1)
            canonical_basis = build_validation_basis_hash(
                canonical_symbols={"XYZ"},
                external_symbols=set(),
                universe_version="provider-projection-test",
            )
            await append_validation_revision(
                db,
                mention_id=int(mention["mention_id"]),
                validation_status="canonical",
                available_at=canonical_at,
                focus_revision=1,
                universe_version="provider-projection-test",
                reason_code="fixture_canonical",
                validation_basis_hash=canonical_basis,
            )
            await db.commit()
            trusted, _, _, _ = await query_feed(
                db,
                as_of=canonical_at + timedelta(seconds=1),
                window_hours=24,
                limit=20,
                cursor=None,
                source=None,
                classification=None,
                min_confidence=0,
                min_abs_impact=0,
                analysis_status=None,
            )
            assert trusted[0].source_tickers == ["XYZ"]
        finally:
            await db.close()

    run(scenario())


def test_empty_latest_advances_watermark_without_losing_later_change(isolated_integration_db):
    async def scenario():
        db = await database.get_db()
        try:
            started = datetime.now(timezone.utc)
            snapshot, items, watermark, cursor, has_more, _ = await query_latest(
                db, updated_after=started - timedelta(days=1), limit=20, cursor=None,
            )
            assert snapshot.startswith("chg_")
            assert items == []
            assert cursor is None
            assert has_more is False
            assert watermark is not None
            assert started <= watermark <= datetime.now(timezone.utc)
        finally:
            await db.close()
        await asyncio.sleep(0.01)
        db = await database.get_db()
        try:
            later_id = await database.insert_news_item(db, news_record(292))
            _, later_items, next_watermark, _, later_more, _ = await query_latest(
                db, updated_after=watermark, limit=20, cursor=None,
            )
            assert [item.news_id for item in later_items] == [later_id]
            assert later_more is False
            assert next_watermark >= watermark
        finally:
            await db.close()

    run(scenario())


def test_late_historical_validation_is_visible_after_latest_watermark(
    isolated_integration_db,
):
    async def scenario():
        db = await database.get_db()
        try:
            initial_effective_at = datetime.now(timezone.utc) - timedelta(hours=2)
            news_id = await database.insert_news_item(
                db,
                news_record(293, fetched_at=initial_effective_at.isoformat()),
            )
            initial_basis = build_validation_basis_hash(
                canonical_symbols=set(),
                external_symbols=set(),
                universe_version="late-history",
            )
            mention = await record_ticker_mention(
                db,
                news_id=news_id,
                ticker="XYZ",
                association_method="exact_alias",
                association_confidence=1.0,
                source="alias_dictionary",
                validation_status="unverified",
                available_at=initial_effective_at,
                focus_revision=1,
                universe_version="late-history",
                validation_basis_hash=initial_basis,
            )
            await db.commit()

            _, initial_items, watermark, _, _, _ = await query_latest(
                db,
                updated_after=initial_effective_at - timedelta(hours=1),
                limit=20,
                cursor=None,
            )
            assert [item.news_id for item in initial_items] == [news_id]
            assert watermark is not None
            before, _, _, _ = await query_ticker(
                db,
                ticker="XYZ",
                as_of=watermark,
                window_hours=24,
                limit=20,
                cursor=None,
                min_confidence=0,
                include_neutral=True,
                include_unanalyzed=True,
            )
            assert before == []

            await asyncio.sleep(0.01)
            observed_at = datetime.now(timezone.utc)
            late_effective_at = watermark - timedelta(minutes=30)
            late_basis = build_validation_basis_hash(
                canonical_symbols={"XYZ"},
                external_symbols=set(),
                universe_version="late-history-corrected",
            )
            _, created = await append_validation_revision(
                db,
                mention_id=int(mention["mention_id"]),
                validation_status="canonical",
                available_at=late_effective_at,
                observed_at=observed_at,
                focus_revision=2,
                universe_version="late-history-corrected",
                reason_code="delayed_focus_repair",
                validation_basis_hash=late_basis,
            )
            assert created is True
            await db.commit()

            current, _, _, _ = await query_ticker(
                db,
                ticker="XYZ",
                as_of=observed_at + timedelta(seconds=1),
                window_hours=24,
                limit=20,
                cursor=None,
                min_confidence=0,
                include_neutral=True,
                include_unanalyzed=True,
            )
            assert [item.news_id for item in current] == [news_id]
            _, changed_items, next_watermark, _, has_more, _ = await query_latest(
                db,
                updated_after=watermark,
                limit=20,
                cursor=None,
            )
            assert [item.news_id for item in changed_items] == [news_id]
            assert has_more is False
            assert next_watermark >= watermark

            timestamps = await (await db.execute(
                """SELECT v.available_at,v.created_at,c.updated_at
                   FROM ticker_validation_revisions v
                   JOIN integration_changes c
                     ON c.entity_type='analysis'
                    AND c.entity_id=CAST(? AS TEXT)
                    AND c.payload_hash=v.validation_basis_hash
                   WHERE v.mention_id=? AND v.validation_basis_hash=?""",
                (news_id, mention["mention_id"], late_basis),
            )).fetchone()
            assert timestamps is not None
            assert datetime.fromisoformat(timestamps[0]) < watermark
            assert datetime.fromisoformat(timestamps[1]) > watermark
            assert timestamps[2] == timestamps[1]
        finally:
            await db.close()

    run(scenario())


def test_calendar_get_is_read_only_and_post_creates_persistent_job(
    isolated_integration_db, monkeypatch
):
    monkeypatch.setattr(settings, "calendar_llm_manual_enabled", True)
    monkeypatch.setattr(settings, "calendar_llm_daily_job_limit", 10)
    monkeypatch.setattr(
        settings, "calendar_llm_daily_output_token_limit", 200_000
    )
    calls = {"fetch": 0}

    async def fake_fetch():
        calls["fetch"] += 1
        return []

    async def fake_identity():
        return "openai", "gpt-5.6-terra"

    monkeypatch.setattr(calendar_router, "fetch_economic_calendar", fake_fetch)
    monkeypatch.setattr(calendar_router, "get_calendar_model_identity", fake_identity)
    result = run(calendar_router.get_economic_calendar())
    assert result["events"] == []
    assert calls["fetch"] == 1
    created = run(calendar_router.analyze_economic_calendar(None))
    assert created["job_id"].startswith("calj_")
    assert created["status"] == "insufficient_context"
    assert created["created"] is True
    assert calls["fetch"] == 2
    polled = run(calendar_router.get_calendar_analysis_job(created["job_id"], None))
    assert polled["job_id"] == created["job_id"]
    assert polled["status"] == "insufficient_context"
    assert calls["fetch"] == 2


def test_terra_runtime_identity_ignores_and_rejects_database_overrides(
    isolated_integration_db, monkeypatch
):
    monkeypatch.setattr(settings, "default_llm_provider", "openai")
    monkeypatch.setattr(settings, "default_llm_model", "gpt-5.6-terra")

    async def scenario():
        db = await database.get_db()
        try:
            await database.set_setting(db, "default_llm_provider", "anthropic")
            await database.set_setting(db, "default_llm_model", "claude-sonnet-4-6")
            news_id = await database.insert_news_item(db, news_record(79))
            created = await create_or_get_job(db, news_id)
        finally:
            await db.close()

        provider, model = await calendar_analyzer.get_calendar_model_identity()
        public_settings = await settings_router.get_settings()
        assert (provider, model) == ("openai", "gpt-5.6-terra")
        assert (created.job["provider"], created.job["model"]) == (
            "openai",
            "gpt-5.6-terra",
        )
        assert public_settings["default_llm_provider"] == "openai"
        assert public_settings["default_llm_model"] == "gpt-5.6-terra"
        assert public_settings["runtime_llm_settings_source"] == "environment"

        with pytest.raises(HTTPException) as exc_info:
            await settings_router.update_settings(
                settings_router.SettingsUpdateRequest(
                    default_llm_provider="anthropic",
                    default_llm_model="claude-sonnet-4-6",
                ),
                None,
            )
        assert exc_info.value.status_code == 409
        assert exc_info.value.detail["code"] == "runtime_llm_settings_managed_by_environment"

    run(scenario())


def _signed_headers(method: str, target: str, body: bytes, key_id: str, secret: str, nonce: str, timestamp: int | None = None):
    path, _, query = target.partition("?")
    timestamp_text = str(timestamp if timestamp is not None else int(time.time()))
    digest = hashlib.sha256(body).hexdigest()
    canonical = canonical_string(method, path, canonical_query(query), timestamp_text, nonce, digest)
    return {
        "X-Optix-Key-Id": key_id,
        "X-Optix-Timestamp": timestamp_text,
        "X-Optix-Nonce": nonce,
        "X-Optix-Content-SHA256": digest,
        "X-Optix-Signature": calculate_signature(secret, canonical),
        "Content-Type": "application/json",
    }


def _integration_app():
    app = FastAPI()
    app.include_router(integration_router.router)

    @app.middleware("http")
    async def fixed_test_client_address(request, call_next):
        # Starlette 0.38 does not expose TestClient(client=...). Keep the
        # service-source boundary deterministic without depending on that API.
        request.scope["client"] = ("127.0.0.1", 50000)
        return await call_next(request)

    @app.exception_handler(IntegrationAPIError)
    async def handle(request, exc):
        return integration_router.error_response(request, exc)

    return app


def test_trusted_tls_proxy_is_honored_but_untrusted_forwarded_proto_is_rejected(
    isolated_integration_db, monkeypatch
):
    monkeypatch.setattr(settings, "option_pro_read_key_id", "read-key")
    monkeypatch.setattr(settings, "option_pro_read_secret", "read-secret")
    monkeypatch.setattr(settings, "option_pro_allowed_cidrs", "203.0.113.10/32")
    monkeypatch.setattr(settings, "option_pro_trusted_proxy_cidrs", "127.0.0.1/32")
    monkeypatch.setattr(settings, "option_pro_allow_local_http", False)
    target = "/api/integrations/option-pro/v1/health"
    app = _integration_app()

    with TestClient(app) as client:
        trusted_headers = _signed_headers(
            "GET", target, b"", "read-key", "read-secret", "nonce-trusted-tls-01"
        )
        trusted_headers.update(
            {"X-Forwarded-For": "203.0.113.10", "X-Forwarded-Proto": "https"}
        )
        assert client.get(target, headers=trusted_headers).status_code == 200

        monkeypatch.setattr(settings, "option_pro_allowed_cidrs", "")
        no_allowlist_headers = _signed_headers(
            "GET", target, b"", "read-key", "read-secret", "nonce-no-allowlist-2"
        )
        no_allowlist_headers.update(
            {"X-Forwarded-For": "203.0.113.10", "X-Forwarded-Proto": "https"}
        )
        no_allowlist = client.get(target, headers=no_allowlist_headers)
        assert no_allowlist.status_code == 503
        assert no_allowlist.json()["code"] == "invalid_server_configuration"

        monkeypatch.setattr(settings, "option_pro_allowed_cidrs", "203.0.113.10/32")
        monkeypatch.setattr(settings, "option_pro_trusted_proxy_cidrs", "")
        spoofed_headers = _signed_headers(
            "GET", target, b"", "read-key", "read-secret", "nonce-spoofed-tls-02"
        )
        spoofed_headers.update(
            {"X-Forwarded-For": "203.0.113.10", "X-Forwarded-Proto": "https"}
        )
        denied = client.get(target, headers=spoofed_headers)
        assert denied.status_code == 403
        assert denied.json()["code"] == "https_required"


def test_container_topology_keeps_cleartext_ports_private_and_bypasses_browser_proxy():
    root = Path(__file__).resolve().parents[2]
    compose = (root / "docker-compose.yml").read_text(encoding="utf-8")
    nginx = (root / "frontend/nginx.conf").read_text(encoding="utf-8")
    assert '"127.0.0.1:3000:8080"' in compose
    assert '"127.0.0.1:8000:8000"' in compose
    assert "location ^~ /api/integrations/option-pro/" in nginx
    assert "return 404;" in nginx


def test_hmac_scope_rotation_replay_and_expired_timestamp(isolated_integration_db, monkeypatch):
    monkeypatch.setattr(settings, "option_pro_read_key_id", "read-key")
    monkeypatch.setattr(settings, "option_pro_read_secret", "read-current-secret")
    monkeypatch.setattr(settings, "option_pro_previous_read_secret", "read-previous-secret")
    monkeypatch.setattr(settings, "option_pro_action_key_id", "action-key")
    monkeypatch.setattr(settings, "option_pro_action_secret", "action-current-secret")
    monkeypatch.setattr(settings, "option_pro_allowed_cidrs", "127.0.0.1/32")
    monkeypatch.setattr(settings, "option_pro_allow_local_http", True)
    monkeypatch.setattr(settings, "openai_api_key", "")
    monkeypatch.setattr(settings, "default_llm_api_key", "")

    async def seed_source_health():
        db = await database.get_db()
        try:
            await upsert_source_health(
                db,
                source="test_source",
                status="ok",
                last_attempt_at="2026-07-12T10:00:00+00:00",
                last_success_at="2026-07-12T10:00:01+00:00",
                data_through="2026-07-12T09:59:00+00:00",
                consecutive_failures=0,
                next_attempt_at="2026-07-12T10:05:00+00:00",
                raw_count=12,
                inserted_count=7,
                duplicates_count=5,
                error_code=None,
            )
        finally:
            await db.close()

    run(seed_source_health())
    app = _integration_app()
    target = "/api/integrations/option-pro/v1/health?b=two&a=hello%20world&a="

    with TestClient(app) as client:
        headers = _signed_headers("GET", target, b"", "read-key", "read-current-secret", "nonce-current-0001")
        response = client.get(target, headers=headers)
        assert response.status_code == 200
        assert response.json()["schema_version"] == "macrolens-option-pro-v2"
        assert response.json()["analysis_queue"]["status"] == "unavailable"
        assert response.json()["analysis_trigger_enabled"] is False
        assert "analysis_worker_heartbeat_missing" in response.json()["warnings"]
        source = response.json()["sources"]["test_source"]
        assert source["last_attempt_at"] == "2026-07-12T10:00:00Z"
        assert source["next_attempt_at"] == "2026-07-12T10:05:00Z"
        assert (source["raw_count"], source["inserted_count"], source["duplicates_count"]) == (12, 7, 5)
        assert "openai_response_id" not in response.text

        replay = client.get(target, headers=headers)
        assert replay.status_code == 409
        assert replay.json()["code"] == "nonce_replayed"

        rotated = _signed_headers("GET", target, b"", "read-key", "read-previous-secret", "nonce-previous-0002")
        assert client.get(target, headers=rotated).status_code == 200

        expired = _signed_headers(
            "GET", target, b"", "read-key", "read-current-secret", "nonce-expired-00003",
            timestamp=int(time.time()) - settings.option_pro_signature_clock_skew_seconds - 10,
        )
        assert client.get(target, headers=expired).status_code == 401

        job_target = "/api/integrations/option-pro/v1/analysis-jobs"
        body = json.dumps(
            {"news_id": 1, "expected_content_hash": "0" * 64, "force": False},
            separators=(",", ":"),
        ).encode()
        read_only = _signed_headers("POST", job_target, body, "read-key", "read-current-secret", "nonce-read-action-004")
        denied = client.post(job_target, content=body, headers=read_only)
        assert denied.status_code == 403
        assert denied.json()["code"] == "insufficient_scope"

        action = _signed_headers("POST", job_target, body, "action-key", "action-current-secret", "nonce-action-key-005")
        action_response = client.post(job_target, content=body, headers=action)
        assert action_response.status_code == 404
        assert action_response.json()["code"] == "news_not_found"

        bad_signature = dict(_signed_headers("GET", target, b"", "read-key", "read-current-secret", "nonce-bad-sign-0006"))
        bad_signature["X-Optix-Signature"] = "0" * 64
        assert client.get(target, headers=bad_signature).status_code == 401

        bad_hash = _signed_headers("POST", job_target, body, "action-key", "action-current-secret", "nonce-bad-hash-0007")
        bad_hash_response = client.post(job_target, content=b"{}", headers=bad_hash)
        assert bad_hash_response.status_code == 401
        assert bad_hash_response.json()["code"] == "body_hash_mismatch"


def test_canonical_query_preserves_repeated_empty_values_and_rfc3986_spaces():
    assert canonical_query("z=1&a=hello+world&a=&a=%2F") == "a=&a=%2F&a=hello%20world&z=1"


def test_signed_manual_analysis_gate_rejects_before_writing_job(
    isolated_integration_db, monkeypatch
):
    monkeypatch.setattr(settings, "option_pro_read_key_id", "read-key")
    monkeypatch.setattr(settings, "option_pro_read_secret", "read-secret")
    monkeypatch.setattr(settings, "option_pro_action_key_id", "action-key")
    monkeypatch.setattr(settings, "option_pro_action_secret", "action-secret")
    monkeypatch.setattr(settings, "option_pro_allowed_cidrs", "127.0.0.1/32")
    monkeypatch.setattr(settings, "option_pro_allow_local_http", True)

    async def seed():
        db = await database.get_db()
        try:
            return await database.insert_news_item(db, news_record(79))
        finally:
            await db.close()

    news_id = run(seed())
    target = "/api/integrations/option-pro/v1/analysis-jobs"
    body = json.dumps(
        {
            "news_id": news_id,
            "expected_content_hash": hashlib.sha256(b"news-79").hexdigest(),
            "force": False,
        },
        separators=(",", ":"),
    ).encode()
    app = _integration_app()

    monkeypatch.setattr(settings, "news_llm_manual_enabled", False)
    with TestClient(app) as client:
        disabled_headers = _signed_headers(
            "POST", target, body, "action-key", "action-secret", "nonce-manual-disabled-1"
        )
        disabled = client.post(target, content=body, headers=disabled_headers)
        assert disabled.status_code == 409
        assert disabled.json()["code"] == "disabled"

        monkeypatch.setattr(settings, "news_llm_manual_enabled", True)
        monkeypatch.setattr(settings, "news_llm_manual_daily_job_limit", 10)
        monkeypatch.setattr(settings, "news_llm_manual_daily_output_token_limit", None)
        budget_headers = _signed_headers(
            "POST", target, body, "action-key", "action-secret", "nonce-manual-budget-0002"
        )
        unbudgeted = client.post(target, content=body, headers=budget_headers)
        assert unbudgeted.status_code == 409
        assert unbudgeted.json()["code"] == "budget_configuration_required"

    async def count_jobs():
        db = await database.get_db()
        try:
            async with db.execute("SELECT COUNT(*) FROM analysis_jobs") as cursor:
                return int((await cursor.fetchone())[0])
        finally:
            await db.close()

    assert run(count_jobs()) == 0


def test_signed_latest_cursor_reuses_implicit_updated_after_across_pages(
    isolated_integration_db, monkeypatch
):
    monkeypatch.setattr(settings, "option_pro_read_key_id", "read-key")
    monkeypatch.setattr(settings, "option_pro_read_secret", "read-secret")
    monkeypatch.setattr(settings, "option_pro_action_key_id", "action-key")
    monkeypatch.setattr(settings, "option_pro_action_secret", "action-secret")
    monkeypatch.setattr(settings, "option_pro_allowed_cidrs", "127.0.0.1/32")
    monkeypatch.setattr(settings, "option_pro_allow_local_http", True)

    async def seed():
        db = await database.get_db()
        try:
            for index in range(3):
                await database.insert_news_item(db, news_record(810 + index))
        finally:
            await db.close()

    run(seed())
    app = _integration_app()
    prefix = "/api/integrations/option-pro/v1/latest"
    first_target = f"{prefix}?limit=1"

    with TestClient(app) as client:
        first = client.get(
            first_target,
            headers=_signed_headers(
                "GET", first_target, b"", "read-key", "read-secret", "nonce-latest-default-01"
            ),
        )
        assert first.status_code == 200
        first_page = first.json()
        assert first_page["has_more"] is True
        assert first_page["next_cursor"]
        assert len(first_page["items"]) == 1

        second_target = (
            f"{prefix}?"
            f"{urlencode({'limit': 1, 'cursor': first_page['next_cursor']})}"
        )
        second = client.get(
            second_target,
            headers=_signed_headers(
                "GET", second_target, b"", "read-key", "read-secret", "nonce-latest-default-02"
            ),
        )
        assert second.status_code == 200, second.text
        second_page = second.json()
        assert second_page["snapshot_token"] == first_page["snapshot_token"]
        assert second_page["items"][0]["news_id"] != first_page["items"][0]["news_id"]


def test_signed_latest_cursor_requires_original_explicit_updated_after(
    isolated_integration_db, monkeypatch
):
    monkeypatch.setattr(settings, "option_pro_read_key_id", "read-key")
    monkeypatch.setattr(settings, "option_pro_read_secret", "read-secret")
    monkeypatch.setattr(settings, "option_pro_action_key_id", "action-key")
    monkeypatch.setattr(settings, "option_pro_action_secret", "action-secret")
    monkeypatch.setattr(settings, "option_pro_allowed_cidrs", "127.0.0.1/32")
    monkeypatch.setattr(settings, "option_pro_allow_local_http", True)

    async def seed():
        db = await database.get_db()
        try:
            for index in range(3):
                await database.insert_news_item(db, news_record(820 + index))
        finally:
            await db.close()

    run(seed())
    app = _integration_app()
    prefix = "/api/integrations/option-pro/v1/latest"
    original_updated_after = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(
        microsecond=0
    ).isoformat()
    first_target = (
        f"{prefix}?"
        f"{urlencode({'updated_after': original_updated_after, 'limit': 1})}"
    )

    with TestClient(app) as client:
        first = client.get(
            first_target,
            headers=_signed_headers(
                "GET", first_target, b"", "read-key", "read-secret", "nonce-latest-explicit-01"
            ),
        )
        assert first.status_code == 200
        first_page = first.json()
        assert first_page["has_more"] is True
        assert first_page["next_cursor"]

        same_filter_target = (
            f"{prefix}?"
            f"{urlencode({'updated_after': original_updated_after, 'limit': 1, 'cursor': first_page['next_cursor']})}"
        )
        same_filter = client.get(
            same_filter_target,
            headers=_signed_headers(
                "GET", same_filter_target, b"", "read-key", "read-secret", "nonce-latest-explicit-02"
            ),
        )
        assert same_filter.status_code == 200, same_filter.text

        changed_filter_target = (
            f"{prefix}?"
            f"{urlencode({'updated_after': first_page['next_updated_after'], 'limit': 1, 'cursor': first_page['next_cursor']})}"
        )
        changed_filter = client.get(
            changed_filter_target,
            headers=_signed_headers(
                "GET", changed_filter_target, b"", "read-key", "read-secret", "nonce-latest-explicit-03"
            ),
        )
        assert changed_filter.status_code == 400
        assert changed_filter.json()["code"] == "invalid_cursor"


def test_latest_cursor_keeps_first_page_retention_boundary_when_time_advances(
    isolated_integration_db, monkeypatch
):
    monkeypatch.setattr(settings, "option_pro_read_secret", "test-read-secret")
    observed = (
        datetime.now(timezone.utc).replace(microsecond=0) + timedelta(minutes=1)
    )
    later = observed + timedelta(seconds=6)
    call_times = iter((observed, later))
    monkeypatch.setattr(option_pro_repository, "utc_now", lambda: next(call_times))

    async def exercise():
        db = await database.get_db()
        try:
            for index in range(3):
                await database.insert_news_item(db, news_record(830 + index))
            boundary = observed - timedelta(days=7)
            first = await query_latest(
                db,
                updated_after=boundary,
                limit=1,
                cursor=None,
            )
            assert first[4] is True
            assert first[3]
            second = await query_latest(
                db,
                updated_after=boundary,
                limit=1,
                cursor=first[3],
            )
            assert second[0] == first[0]
            assert second[1]
        finally:
            await db.close()

    run(exercise())


def test_canonical_path_discards_legacy_testclient_query_suffix():
    assert canonical_path(b"/v1/health?b=two&a=hello%20world&a=") == "/v1/health"
    assert canonical_path(b"/v1/news%3Farchive?limit=20") == "/v1/news%3Farchive"


def test_public_cycle_replays_historical_result_json_without_relaxing_schema():
    cycle_id = "mfc_0123456789abcdef0123456789abcdef"
    persisted = completed_market_focus_result(cycle_id)
    public = integration_router._public_cycle(
        {
            "cycle_id": cycle_id,
            "result_json": json.dumps(persisted, separators=(",", ":")),
            "no_new_hot_events": 1,
        }
    )

    assert isinstance(public["result"]["as_of"], datetime)
    assert public["result"]["as_of"].tzinfo is not None
    assert public["result"]["as_of"].utcoffset() == timedelta(0)

    offset = {**persisted, "as_of": "2026-07-15T18:20:47.333424+09:00"}
    normalized = integration_router._public_cycle(
        {
            "cycle_id": cycle_id,
            "result_json": json.dumps(offset, separators=(",", ":")),
            "no_new_hot_events": 1,
        }
    )
    assert normalized["result"]["as_of"] == datetime(
        2026, 7, 15, 9, 20, 47, 333424, tzinfo=timezone.utc
    )

    invalid = {**persisted, "unexpected_internal_field": "must remain rejected"}
    with pytest.raises(IntegrationAPIError) as captured:
        integration_router._public_cycle(
            {
                "cycle_id": cycle_id,
                "result_json": json.dumps(invalid, separators=(",", ":")),
                "no_new_hot_events": 1,
            }
        )
    assert captured.value.status_code == 500
    assert captured.value.code == "persisted_market_focus_result_invalid"
    assert "unexpected_internal_field" not in captured.value.message

    naive = {**persisted, "as_of": "2026-07-15T09:20:47.333424"}
    with pytest.raises(IntegrationAPIError) as naive_error:
        integration_router._public_cycle(
            {
                "cycle_id": cycle_id,
                "result_json": json.dumps(naive, separators=(",", ":")),
                "no_new_hot_events": 1,
            }
        )
    assert naive_error.value.code == "persisted_market_focus_result_invalid"


def test_completed_market_focus_cycle_latest_and_point_reads_persisted_result(
    isolated_integration_db, monkeypatch
):
    monkeypatch.setattr(settings, "option_pro_read_key_id", "read-key")
    monkeypatch.setattr(settings, "option_pro_read_secret", "read-secret")
    monkeypatch.setattr(settings, "option_pro_allowed_cidrs", "127.0.0.1/32")
    monkeypatch.setattr(settings, "option_pro_allow_local_http", True)
    cycle_id = "mfc_0123456789abcdef0123456789abcdef"
    result_as_of = "2026-07-15T09:20:47.333424Z"

    async def seed():
        db = await database.get_db()
        try:
            await seed_completed_market_focus_cycle(
                db,
                cycle_id=cycle_id,
                result_as_of=result_as_of,
            )
        finally:
            await db.close()

    run(seed())
    app = _integration_app()
    prefix = "/api/integrations/option-pro/v1"
    targets = (
        f"{prefix}/market-focus-cycles/latest",
        f"{prefix}/market-focus-cycles/{cycle_id}",
    )
    expected_as_of = datetime.fromisoformat(result_as_of.replace("Z", "+00:00"))

    with TestClient(app) as client:
        for index, target in enumerate(targets, start=1):
            headers = _signed_headers(
                "GET",
                target,
                b"",
                "read-key",
                "read-secret",
                f"nonce-completed-cycle-read-{index}",
            )
            response = client.get(target, headers=headers)

            assert response.status_code == 200, (target, response.text)
            cycle = response.json()["cycle"]
            assert cycle["cycle_id"] == cycle_id
            assert cycle["status"] == "completed"
            assert datetime.fromisoformat(
                cycle["result"]["as_of"].replace("Z", "+00:00")
            ) == expected_as_of
            assert cycle["result"]["display_only"] is True
            assert "openai_response_id" not in response.text


def test_invalid_persisted_market_focus_result_returns_safe_integration_error(
    isolated_integration_db, monkeypatch
):
    monkeypatch.setattr(settings, "option_pro_read_key_id", "read-key")
    monkeypatch.setattr(settings, "option_pro_read_secret", "read-secret")
    monkeypatch.setattr(settings, "option_pro_allowed_cidrs", "127.0.0.1/32")
    monkeypatch.setattr(settings, "option_pro_allow_local_http", True)
    cycle_id = "mfc_0123456789abcdef0123456789abcdef"

    async def seed():
        db = await database.get_db()
        try:
            await seed_completed_market_focus_cycle(db, cycle_id=cycle_id)
            await db.execute(
                "UPDATE market_focus_cycles SET result_json=? WHERE cycle_id=?",
                ('{"private":"must-not-leak"}', cycle_id),
            )
            await db.commit()
        finally:
            await db.close()

    run(seed())
    app = _integration_app()
    prefix = "/api/integrations/option-pro/v1"
    targets = (
        f"{prefix}/market-focus-cycles/latest",
        f"{prefix}/market-focus-cycles/{cycle_id}",
    )
    with TestClient(app, raise_server_exceptions=False) as client:
        for index, target in enumerate(targets, start=1):
            headers = _signed_headers(
                "GET",
                target,
                b"",
                "read-key",
                "read-secret",
                f"nonce-invalid-cycle-read-{index}",
            )
            response = client.get(target, headers=headers)

            assert response.status_code == 500
            assert response.json()["code"] == "persisted_market_focus_result_invalid"
            assert "must-not-leak" not in response.text


def test_signed_integration_endpoints_match_committed_contract(isolated_integration_db, monkeypatch):
    monkeypatch.setattr(settings, "option_pro_read_key_id", "read-key")
    monkeypatch.setattr(settings, "option_pro_read_secret", "read-secret")
    monkeypatch.setattr(settings, "option_pro_action_key_id", "action-key")
    monkeypatch.setattr(settings, "option_pro_action_secret", "action-secret")
    monkeypatch.setattr(settings, "option_pro_allowed_cidrs", "127.0.0.1/32")
    monkeypatch.setattr(settings, "option_pro_allow_local_http", True)

    async def seed():
        db = await database.get_db()
        try:
            record = news_record(80, summary="")
            record["source_tickers"] = ["AMD"]
            return await database.insert_news_item(db, record)
        finally:
            await db.close()

    news_id = run(seed())
    app = _integration_app()
    prefix = "/api/integrations/option-pro/v1"
    with TestClient(app) as client:
        create_target = f"{prefix}/analysis-jobs"
        create_body = json.dumps(
            {
                "news_id": news_id,
                "expected_content_hash": hashlib.sha256(b"news-80").hexdigest(),
                "force": False,
            },
            separators=(",", ":"),
        ).encode()
        create_headers = _signed_headers(
            "POST", create_target, create_body, "action-key", "action-secret", "nonce-contract-create-1"
        )
        created = client.post(create_target, content=create_body, headers=create_headers)
        assert created.status_code == 202
        job = created.json()
        assert job["status"] == "insufficient_context"
        assert job["result"]["model"] == "low-context-neutral-v2"
        assert job["content_hash"] == hashlib.sha256(b"news-80").hexdigest()
        assert len(job["input_hash"]) == 64
        assert job["change_sequence"] is not None
        assert "openai_response_id" not in created.text

        stale_body = json.dumps(
            {"news_id": news_id, "expected_content_hash": "f" * 64, "force": False},
            separators=(",", ":"),
        ).encode()
        stale_headers = _signed_headers(
            "POST", create_target, stale_body, "action-key", "action-secret", "nonce-contract-stale-1"
        )
        stale = client.post(create_target, content=stale_body, headers=stale_headers)
        assert stale.status_code == 409
        assert stale.json()["code"] == "news_version_conflict"

        targets = [
            f"{prefix}/feed?window_hours=72&limit=20",
            f"{prefix}/latest?updated_after=2026-07-12T00%3A00%3A00Z&limit=20",
            f"{prefix}/news/{news_id}",
            f"{prefix}/catalysts/AMD?include_neutral=true",
            f"{prefix}/calendar?date_from=2026-07-12&date_to=2026-07-19",
            f"{prefix}/analysis-jobs/{job['job_id']}",
            f"{prefix}/hotspots/status",
            f"{prefix}/hotspots?limit=20",
            f"{prefix}/market-focus-cycles/latest",
        ]
        for index, target in enumerate(targets, start=1):
            headers = _signed_headers(
                "GET", target, b"", "read-key", "read-secret", f"nonce-contract-read-{index:02d}"
            )
            response = client.get(target, headers=headers)
            assert response.status_code == 200, (target, response.text)
            assert response.json()["schema_version"] == "macrolens-option-pro-v2"
            assert response.json()["schema_sha256"] == hashlib.sha256(CONTRACT_PATH.read_bytes()).hexdigest()
            assert "openai_response_id" not in response.text

        batch_target = f"{prefix}/catalysts/batch"
        batch_body = json.dumps({"tickers": ["AMD"], "include_neutral": True}, separators=(",", ":")).encode()
        batch_headers = _signed_headers(
            "POST", batch_target, batch_body, "read-key", "read-secret", "nonce-contract-batch-1"
        )
        batch = client.post(batch_target, content=batch_body, headers=batch_headers)
        assert batch.status_code == 200
        assert batch.json()["results"]["AMD"]["status"] == "active"

        async def mention_available_at():
            db = await database.get_db()
            try:
                async with db.execute(
                    """SELECT v.available_at FROM ticker_validation_revisions v
                       JOIN news_ticker_mentions m ON m.id=v.mention_id
                       WHERE m.news_id=? AND m.ticker='AMD'
                       ORDER BY v.available_at,v.id LIMIT 1""",
                    (news_id,),
                ) as cursor:
                    return str((await cursor.fetchone())[0])
            finally:
                await db.close()

        visible_at = datetime.fromisoformat(run(mention_available_at()))
        for nonce, cutoff, expected_count in (
            ("nonce-contract-batch-asof-before", visible_at - timedelta(microseconds=1), 0),
            ("nonce-contract-batch-asof-exact", visible_at, 1),
        ):
            body = json.dumps(
                {
                    "tickers": ["AMD"],
                    "as_of": cutoff.isoformat(),
                    "include_neutral": True,
                },
                separators=(",", ":"),
            ).encode()
            response = client.post(
                batch_target,
                content=body,
                headers=_signed_headers(
                    "POST", batch_target, body, "read-key", "read-secret", nonce
                ),
            )
            assert response.status_code == 200, response.text
            assert len(response.json()["results"]["AMD"]["items"]) == expected_count
