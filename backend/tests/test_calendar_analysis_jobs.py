from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.config import settings
from app.models import database
from app.routers import calendar as calendar_router
from app.services.analysis_jobs import claim_next_job, create_or_get_job
from app.services.calendar_analysis_jobs import (
    claim_next_calendar_job,
    create_or_get_calendar_job,
    get_calendar_job,
    load_completed_calendar_analysis,
    recover_expired_calendar_job_leases,
    run_calendar_worker_once,
)
from app.services.calendar_analyzer import _event_id
from app.services.responses_runtime import ProviderCapabilities, ResponseResult


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def isolated_calendar_db(tmp_path, monkeypatch):
    path = tmp_path / "calendar-jobs.db"
    monkeypatch.setattr(database, "DB_PATH", str(path))
    monkeypatch.setattr(settings, "calendar_llm_manual_enabled", True)
    monkeypatch.setattr(settings, "calendar_llm_daily_job_limit", 10)
    monkeypatch.setattr(
        settings, "calendar_llm_daily_output_token_limit", 200_000
    )
    monkeypatch.setattr(settings, "news_llm_manual_enabled", True)
    monkeypatch.setattr(settings, "news_llm_manual_daily_job_limit", 50)
    monkeypatch.setattr(settings, "news_llm_manual_daily_output_token_limit", 1_638_400)
    run(database.init_db())
    return path


def calendar_event(index: int = 1) -> dict:
    return {
        "date": f"2026-07-{13 + index:02d}T08:30:00-04:00",
        "title": f"Consumer Price Index {index}",
        "country_code": "USD",
        "country": "美国",
        "impact": "high",
        "forecast": "0.2%",
        "previous": "0.1%",
        "actual": "",
    }


def calendar_result(event: dict) -> dict:
    return {
        "events": [
            {
                "event_id": _event_id(event),
                "title": event["title"],
                "title_zh": "美国消费者价格指数",
                "stock_impact": "neutral",
                "commodity_impact": "neutral",
                "explanation": "数据尚未公布，方向仍有不确定性。",
            }
        ]
    }


class FakeCalendarProvider:
    def __init__(self, result: ResponseResult):
        self.result = result
        self.create_calls = 0
        self.background_calls = 0
        self.sync_calls = 0
        self.retrieve_calls = 0
        self.options = None

    def capabilities(self):
        return ProviderCapabilities("ok", True, True, True, True, True)

    async def create_background(self, model_input: str, **options):
        self.create_calls += 1
        self.background_calls += 1
        self.options = options
        assert "<untrusted_calendar_data>" in model_input
        return self.result

    async def retrieve(self, response_id: str):
        self.retrieve_calls += 1
        return self.result

    async def cancel(self, response_id: str):
        return ResponseResult(response_id, "cancelled")

    async def create_sync(self, model_input: str, **options):
        self.create_calls += 1
        self.sync_calls += 1
        self.options = options
        return self.result


class SyncCalendarSubmissionOutcomeUnknownProvider(FakeCalendarProvider):
    async def create_sync(self, model_input: str, **options):
        self.create_calls += 1
        self.sync_calls += 1
        raise TimeoutError("response timed out after request submission")


def test_calendar_worker_sync_unknown_outcome_is_not_retried(
    isolated_calendar_db, monkeypatch
):
    monkeypatch.setattr(settings, "openai_execution_mode", "worker_sync")
    event = calendar_event(8)
    provider = SyncCalendarSubmissionOutcomeUnknownProvider(
        ResponseResult(None, "failed")
    )

    async def scenario():
        db = await database.get_db()
        try:
            created = await create_or_get_calendar_job(
                db, [event], provider="openai", model=settings.default_llm_model
            )
        finally:
            await db.close()

        assert await run_calendar_worker_once(
            provider=provider, worker_id="calendar-sync-unknown"
        ) is True
        db = await database.get_db()
        try:
            row = await get_calendar_job(db, created.job["job_id"])
            assert row["status"] == "failed"
            assert row["error_code"] == "submission_outcome_unknown"
            replay = await create_or_get_calendar_job(
                db,
                [event],
                provider="openai",
                model=settings.default_llm_model,
                force=True,
            )
            assert replay.created is False
            assert replay.job["job_id"] == created.job["job_id"]
        finally:
            await db.close()
        assert provider.sync_calls == 1

    run(scenario())


def test_calendar_post_creates_job_and_get_only_polls_storage(
    isolated_calendar_db, monkeypatch
):
    event = calendar_event()
    fetch_calls = 0

    async def fake_fetch():
        nonlocal fetch_calls
        fetch_calls += 1
        return [dict(event)]

    async def fake_identity():
        return "openai", "gpt-5.6-terra"

    monkeypatch.setattr(calendar_router, "fetch_economic_calendar", fake_fetch)
    monkeypatch.setattr(calendar_router, "get_calendar_model_identity", fake_identity)
    monkeypatch.setattr(settings, "admin_token", "calendar-admin")

    app = FastAPI()
    app.include_router(calendar_router.router)
    headers = {"X-Admin-Token": "calendar-admin"}
    with TestClient(app) as client:
        created = client.post("/api/calendar/analyze", headers=headers)
        assert created.status_code == 202
        body = created.json()
        assert body["status"] == "pending"
        assert body["created"] is True
        assert body["job_id"].startswith("calj_")

        polled = client.get(
            f"/api/calendar/analyze/{body['job_id']}",
            headers=headers,
        )
        assert polled.status_code == 200
        assert polled.json()["status"] == "pending"
        assert polled.json()["result"] is None

        calendar = client.get("/api/calendar")
        assert calendar.status_code == 200
        assert calendar.json()["events"][0].get("title_zh") is None
        assert calendar.json()["analyzed"] == 0
        assert calendar.json()["analysis_capability"] == "enabled"
    assert fetch_calls == 2


def test_calendar_post_is_fail_closed_before_fetch_or_job_creation(
    isolated_calendar_db, monkeypatch
):
    fetch_calls = 0

    async def fake_fetch():
        nonlocal fetch_calls
        fetch_calls += 1
        return [calendar_event()]

    monkeypatch.setattr(calendar_router, "fetch_economic_calendar", fake_fetch)
    monkeypatch.setattr(settings, "admin_token", "calendar-admin")
    monkeypatch.setattr(settings, "calendar_llm_manual_enabled", False)

    app = FastAPI()
    app.include_router(calendar_router.router)
    with TestClient(app) as client:
        response = client.post(
            "/api/calendar/analyze",
            headers={"X-Admin-Token": "calendar-admin"},
        )
        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "disabled"

        monkeypatch.setattr(settings, "calendar_llm_manual_enabled", True)
        monkeypatch.setattr(
            settings, "calendar_llm_daily_output_token_limit", None
        )
        unbudgeted = client.post(
            "/api/calendar/analyze",
            headers={"X-Admin-Token": "calendar-admin"},
        )
        assert unbudgeted.status_code == 409
        assert (
            unbudgeted.json()["detail"]["code"]
            == "budget_configuration_required"
        )

    assert fetch_calls == 0

    async def count_jobs():
        db = await database.get_db()
        try:
            async with db.execute("SELECT COUNT(*) FROM calendar_analysis_jobs") as cursor:
                return int((await cursor.fetchone())[0])
        finally:
            await db.close()

    assert run(count_jobs()) == 0


def test_calendar_worker_uses_mocked_responses_and_persists_result(
    isolated_calendar_db, monkeypatch
):
    monkeypatch.setattr(settings, "openai_execution_mode", "background")
    monkeypatch.setattr(settings, "calendar_max_output_tokens", 1024)
    event = calendar_event()
    completed = ResponseResult(
        response_id="resp_calendar",
        status="completed",
        output_text=json.dumps(calendar_result(event), ensure_ascii=False),
        usage_input_tokens=120,
        usage_cached_input_tokens=20,
        usage_output_tokens=75,
    )
    provider = FakeCalendarProvider(completed)

    async def scenario():
        db = await database.get_db()
        try:
            created = await create_or_get_calendar_job(
                db,
                [event],
                provider="openai",
                model="gpt-5.6-terra",
            )
            assert created.created is True
            assert created.job["status"] == "pending"
            assert created.job["execution_mode"] == "background"
            assert created.job["max_output_tokens"] == 1024
            job_id = created.job["job_id"]
        finally:
            await db.close()

        monkeypatch.setattr(settings, "openai_execution_mode", "worker_sync")
        monkeypatch.setattr(settings, "calendar_max_output_tokens", 16384)

        assert await run_calendar_worker_once(
            provider=provider,
            worker_id="calendar-worker-test",
        ) is True

        db = await database.get_db()
        try:
            job = await get_calendar_job(db, job_id)
            assert job is not None
            assert job["status"] == "completed"
            assert job["openai_response_id"] == "resp_calendar"
            assert job["usage_input_tokens"] == 120
            assert job["usage_cached_input_tokens"] == 20
            assert job["usage_output_tokens"] == 75
            assert job["result_json"]
            assert job["lease_owner"] is None
            assert job["lease_expires_at"] is None
            assert job["next_attempt_at"] is None
            analyzed = await load_completed_calendar_analysis(
                db,
                [event],
                provider="openai",
                model="gpt-5.6-terra",
            )
            assert analyzed is not None
            assert analyzed[0]["title_zh"] == "美国消费者价格指数"
            duplicate = await create_or_get_calendar_job(
                db,
                [event],
                provider="openai",
                model="gpt-5.6-terra",
            )
            assert duplicate.created is False
            assert duplicate.job["job_id"] == job_id
        finally:
            await db.close()

        assert await run_calendar_worker_once(
            provider=provider,
            worker_id="calendar-worker-test",
        ) is False

    run(scenario())

    async def fake_fetch():
        return [dict(event)]

    async def fake_identity():
        return "openai", "gpt-5.6-terra"

    monkeypatch.setattr(calendar_router, "fetch_economic_calendar", fake_fetch)
    monkeypatch.setattr(calendar_router, "get_calendar_model_identity", fake_identity)
    calendar = run(calendar_router.get_economic_calendar())
    assert calendar["analyzed"] == 1
    assert calendar["events"][0]["title_zh"] == "美国消费者价格指数"
    assert provider.create_calls == 1
    assert provider.background_calls == 1
    assert provider.sync_calls == 0
    assert provider.retrieve_calls == 0
    assert provider.options["model"] == "gpt-5.6-terra"
    assert provider.options["reasoning_effort"] == settings.openai_reasoning
    assert provider.options["max_output_tokens"] == 1024
    assert provider.options["output_format"]["type"] == "json_schema"
    assert "untrusted data" in provider.options["instructions"]


def test_disabled_calendar_worker_only_observes_existing_response(
    isolated_calendar_db, monkeypatch
):
    first_event = calendar_event(31)
    observed_event = calendar_event(32)
    provider = FakeCalendarProvider(
        ResponseResult(
            response_id="resp_calendar_existing",
            status="completed",
            output_text=json.dumps(calendar_result(observed_event), ensure_ascii=False),
        )
    )

    async def scenario():
        db = await database.get_db()
        try:
            unsubmitted = await create_or_get_calendar_job(
                db,
                [first_event],
                provider="openai",
                model="gpt-5.6-terra",
            )
            observed = await create_or_get_calendar_job(
                db,
                [observed_event],
                provider="openai",
                model="gpt-5.6-terra",
            )
            await db.execute(
                """UPDATE calendar_analysis_jobs
                   SET status='queued',openai_response_id='resp_calendar_existing'
                   WHERE job_id=?""",
                (observed.job["job_id"],),
            )
            await db.commit()
        finally:
            await db.close()

        monkeypatch.setattr(settings, "calendar_llm_manual_enabled", False)
        assert await run_calendar_worker_once(
            provider=provider,
            worker_id="calendar-disabled-observer",
        ) is True
        assert provider.retrieve_calls == 1
        assert provider.create_calls == 0

        db = await database.get_db()
        try:
            observed_row = await get_calendar_job(db, observed.job["job_id"])
            unsubmitted_row = await get_calendar_job(db, unsubmitted.job["job_id"])
            assert observed_row["status"] == "completed"
            assert unsubmitted_row["status"] == "pending"
        finally:
            await db.close()

        assert await run_calendar_worker_once(
            provider=provider,
            worker_id="calendar-disabled-no-submit",
        ) is False
        assert provider.create_calls == 0

    run(scenario())


def test_background_calendar_response_is_linked_before_later_poll(
    isolated_calendar_db, monkeypatch
):
    monkeypatch.setattr(settings, "openai_execution_mode", "background")
    event = calendar_event()
    provider = FakeCalendarProvider(
        ResponseResult(
            "resp_calendar_queued",
            "queued",
            model="gpt-5.6-terra",
            reasoning_effort=settings.openai_reasoning,
        )
    )

    async def scenario():
        db = await database.get_db()
        try:
            created = await create_or_get_calendar_job(
                db,
                [event],
                provider="openai",
                model="gpt-5.6-terra",
            )
            job_id = created.job["job_id"]
        finally:
            await db.close()

        assert await run_calendar_worker_once(
            provider=provider,
            worker_id="calendar-queued-test",
        ) is True
        db = await database.get_db()
        try:
            job = await get_calendar_job(db, job_id)
            assert job is not None
            assert job["status"] == "queued"
            assert job["openai_response_id"] == "resp_calendar_queued"
            assert job["next_attempt_at"] is not None
            assert job["lease_owner"] is None
        finally:
            await db.close()

        monkeypatch.setattr(settings, "openai_execution_mode", "worker_sync")
        provider.result = ResponseResult(
            "resp_calendar_queued",
            "completed",
            output_text=json.dumps(calendar_result(event), ensure_ascii=False),
            model="gpt-5.6-terra",
            reasoning_effort=settings.openai_reasoning,
        )
        db = await database.get_db()
        try:
            await db.execute(
                "UPDATE calendar_analysis_jobs SET next_attempt_at=? WHERE job_id=?",
                (
                    (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),
                    job_id,
                ),
            )
            await db.commit()
        finally:
            await db.close()
        assert await run_calendar_worker_once(
            provider=provider,
            worker_id="calendar-queued-test",
        ) is True

    run(scenario())
    assert provider.create_calls == 1
    assert provider.background_calls == 1
    assert provider.sync_calls == 0
    assert provider.retrieve_calls == 1


def test_calendar_worker_rejects_provider_model_drift(isolated_calendar_db):
    event = calendar_event()
    provider = FakeCalendarProvider(
        ResponseResult(
            "resp_calendar_drift",
            "completed",
            output_text=json.dumps(calendar_result(event), ensure_ascii=False),
            model="unexpected-model",
            reasoning_effort=settings.openai_reasoning,
            usage_input_tokens=55,
            usage_cached_input_tokens=5,
            usage_output_tokens=21,
        )
    )

    async def scenario():
        db = await database.get_db()
        try:
            created = await create_or_get_calendar_job(
                db,
                [event],
                provider="openai",
                model="gpt-5.6-terra",
            )
            job_id = created.job["job_id"]
        finally:
            await db.close()

        assert await run_calendar_worker_once(
            provider=provider,
            worker_id="calendar-drift-test",
        ) is True
        db = await database.get_db()
        try:
            job = await get_calendar_job(db, job_id)
            assert job is not None
            assert job["status"] == "failed"
            assert job["error_code"] == "calendar_provider_model_mismatch"
            assert job["result_json"] is None
            assert job["usage_input_tokens"] == 55
            assert job["usage_cached_input_tokens"] == 5
            assert job["usage_output_tokens"] == 21
            assert job["lease_owner"] is None
            assert job["lease_expires_at"] is None
        finally:
            await db.close()

    run(scenario())


def test_calendar_budget_is_separate_and_no_event_job_never_reaches_provider(
    isolated_calendar_db, monkeypatch
):
    monkeypatch.setattr(settings, "calendar_llm_daily_job_limit", 1)
    provider = FakeCalendarProvider(ResponseResult("unused", "completed"))

    async def scenario():
        db = await database.get_db()
        try:
            first = await create_or_get_calendar_job(
                db,
                [calendar_event(1)],
                provider="openai",
                model="gpt-5.6-terra",
            )
            blocked = await create_or_get_calendar_job(
                db,
                [calendar_event(2)],
                provider="openai",
                model="gpt-5.6-terra",
            )
            empty = await create_or_get_calendar_job(
                db,
                [],
                provider="openai",
                model="gpt-5.6-terra",
            )
            assert first.job["status"] == "pending"
            assert blocked.job["status"] == "budget_blocked"
            assert blocked.job["error_code"] == "calendar_daily_job_limit_reached"
            assert empty.job["status"] == "insufficient_context"
            assert json.loads(empty.job["result_json"]) == {"events": []}
        finally:
            await db.close()

    run(scenario())
    assert provider.create_calls == 0


def test_calendar_output_budget_reserves_active_jobs_and_releases_unused_capacity(
    isolated_calendar_db, monkeypatch
):
    monkeypatch.setattr(settings, "calendar_llm_daily_job_limit", 10)
    monkeypatch.setattr(settings, "calendar_max_output_tokens", 1024)
    monkeypatch.setattr(settings, "calendar_llm_daily_output_token_limit", 1124)

    async def scenario():
        db = await database.get_db()
        try:
            first = await create_or_get_calendar_job(
                db, [calendar_event(41)], provider="openai", model="gpt-5.6-terra"
            )
            blocked = await create_or_get_calendar_job(
                db, [calendar_event(42)], provider="openai", model="gpt-5.6-terra"
            )
            assert first.job["status"] == "pending"
            assert first.job["max_output_tokens"] == 1024
            assert blocked.job["status"] == "budget_blocked"
            assert blocked.job["error_code"] == "calendar_daily_output_token_limit_reached"

            now = datetime.now(timezone.utc).isoformat()
            await db.execute(
                """UPDATE calendar_analysis_jobs
                   SET status='completed',usage_output_tokens=100,completed_at=?,updated_at=?
                   WHERE job_id=?""",
                (now, now, first.job["job_id"]),
            )
            await db.commit()
            released = await create_or_get_calendar_job(
                db, [calendar_event(43)], provider="openai", model="gpt-5.6-terra"
            )
            assert released.job["status"] == "pending"
        finally:
            await db.close()

    run(scenario())


def test_calendar_failed_retry_is_append_only_and_unknown_submission_is_not_retried(
    isolated_calendar_db,
):
    event = calendar_event(3)

    async def scenario():
        db = await database.get_db()
        try:
            first = await create_or_get_calendar_job(
                db, [event], provider="openai", model="gpt-5.6-terra"
            )
            await db.execute(
                """UPDATE calendar_analysis_jobs
                   SET status='failed',error_code='invalid_calendar_structured_output',
                       usage_input_tokens=40,usage_output_tokens=12,completed_at=?
                   WHERE job_id=?""",
                (datetime.now(timezone.utc).isoformat(), first.job["job_id"]),
            )
            await db.commit()
            reused = await create_or_get_calendar_job(
                db, [event], provider="openai", model="gpt-5.6-terra"
            )
            assert reused.created is False
            assert reused.job["job_id"] == first.job["job_id"]
            retried = await create_or_get_calendar_job(
                db,
                [event],
                provider="openai",
                model="gpt-5.6-terra",
                force=True,
            )
            assert retried.created is True
            assert retried.job["job_id"] != first.job["job_id"]
            assert retried.job["retry_of_job_id"] == first.job["job_id"]
            async with db.execute(
                """SELECT status,error_code,usage_input_tokens,usage_output_tokens
                   FROM calendar_analysis_jobs WHERE job_id=?""",
                (first.job["job_id"],),
            ) as cursor:
                assert tuple(await cursor.fetchone()) == (
                    "failed", "invalid_calendar_structured_output", 40, 12
                )

            await db.execute(
                """UPDATE calendar_analysis_jobs
                   SET status='failed',error_code='submission_outcome_unknown'
                   WHERE job_id=?""",
                (retried.job["job_id"],),
            )
            await db.commit()
            unknown = await create_or_get_calendar_job(
                db,
                [event],
                provider="openai",
                model="gpt-5.6-terra",
                force=True,
            )
            assert unknown.created is False
            assert unknown.job["job_id"] == retried.job["job_id"]
        finally:
            await db.close()

    run(scenario())


def test_calendar_worker_sync_interruption_is_explicitly_retryable(
    isolated_calendar_db, monkeypatch
):
    monkeypatch.setattr(settings, "openai_execution_mode", "worker_sync")
    event = calendar_event(4)

    async def scenario():
        db = await database.get_db()
        try:
            created = await create_or_get_calendar_job(
                db, [event], provider="openai", model="gpt-5.6-terra"
            )
            expired = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
            await db.execute(
                """UPDATE calendar_analysis_jobs
                   SET status='in_progress',error_code='worker_sync_in_progress',
                       lease_owner='dead-sync-worker',lease_expires_at=?
                   WHERE job_id=?""",
                (expired, created.job["job_id"]),
            )
            await db.commit()
            assert await recover_expired_calendar_job_leases(db) == 1
            interrupted = await get_calendar_job(db, created.job["job_id"])
            assert interrupted is not None
            assert interrupted["status"] == "failed"
            assert interrupted["error_code"] == "worker_interrupted"
            retried = await create_or_get_calendar_job(
                db,
                [event],
                provider="openai",
                model="gpt-5.6-terra",
                force=True,
            )
            assert retried.created is True
            assert retried.job["job_id"] != created.job["job_id"]
            assert retried.job["retry_of_job_id"] == created.job["job_id"]
        finally:
            await db.close()

    run(scenario())


def test_calendar_background_unknown_submission_remains_non_retryable(
    isolated_calendar_db, monkeypatch
):
    monkeypatch.setattr(settings, "openai_execution_mode", "background")
    event = calendar_event(5)

    async def scenario():
        db = await database.get_db()
        try:
            created = await create_or_get_calendar_job(
                db, [event], provider="openai", model="gpt-5.6-terra"
            )
            expired = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
            await db.execute(
                """UPDATE calendar_analysis_jobs
                   SET status='in_progress',error_code='submission_in_progress',
                       lease_owner='dead-background-worker',lease_expires_at=?
                   WHERE job_id=?""",
                (expired, created.job["job_id"]),
            )
            await db.commit()
            assert await recover_expired_calendar_job_leases(db) == 1
            unknown = await get_calendar_job(db, created.job["job_id"])
            assert unknown is not None
            assert unknown["error_code"] == "submission_outcome_unknown"
            repeated = await create_or_get_calendar_job(
                db,
                [event],
                provider="openai",
                model="gpt-5.6-terra",
                force=True,
            )
            assert repeated.created is False
            assert repeated.job["job_id"] == created.job["job_id"]
        finally:
            await db.close()

    run(scenario())


def test_calendar_claim_respects_news_remote_concurrency(
    isolated_calendar_db, monkeypatch
):
    monkeypatch.setattr(settings, "openai_execution_mode", "background")
    monkeypatch.setattr(settings, "openai_max_concurrency", 1)
    monkeypatch.setattr(settings, "calendar_llm_max_inflight", 1)
    provider = FakeCalendarProvider(ResponseResult("resp_calendar", "queued"))

    async def scenario():
        now = datetime.now(timezone.utc).isoformat()
        news = {
            "source": "test/source",
            "title": "Federal Reserve policy update with broad market implications",
            "summary": "Detailed market context and policy facts. " * 5,
            "url": "https://example.test/calendar-concurrency",
            "image_url": None,
            "published_at": now,
            "fetched_at": now,
            "content_hash": "calendar-concurrency-news",
        }
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news)
            news_job = await create_or_get_job(db, news_id)
            await db.execute(
                """UPDATE analysis_jobs
                   SET status='queued',openai_response_id='resp_news',next_attempt_at=?
                   WHERE job_id=?""",
                (now, news_job.job["job_id"]),
            )
            await db.commit()
            calendar_job = await create_or_get_calendar_job(
                db,
                [calendar_event()],
                provider="openai",
                model="gpt-5.6-terra",
            )
            assert calendar_job.job["status"] == "pending"
        finally:
            await db.close()

        monkeypatch.setattr(settings, "openai_execution_mode", "worker_sync")
        assert await run_calendar_worker_once(
            provider=provider,
            worker_id="calendar-shared-capacity",
        ) is False

    run(scenario())
    assert provider.create_calls == 0


def test_unknown_background_submissions_reserve_calendar_and_cross_queue_capacity(
    isolated_calendar_db, monkeypatch
):
    monkeypatch.setattr(settings, "openai_execution_mode", "background")
    monkeypatch.setattr(settings, "openai_max_concurrency", 1)
    monkeypatch.setattr(settings, "news_llm_max_inflight", 1)
    monkeypatch.setattr(settings, "calendar_llm_max_inflight", 1)
    monkeypatch.setattr(settings, "openai_background_poll_timeout_seconds", 60)

    async def scenario():
        now = datetime.now(timezone.utc)
        db = await database.get_db()
        try:
            first_calendar = await create_or_get_calendar_job(
                db,
                [calendar_event(21)],
                provider="openai",
                model="gpt-5.6-terra",
            )
            second_calendar = await create_or_get_calendar_job(
                db,
                [calendar_event(22)],
                provider="openai",
                model="gpt-5.6-terra",
            )
            await db.execute(
                """UPDATE calendar_analysis_jobs
                   SET status='failed',error_code='submission_outcome_unknown',
                       execution_mode='background',submitted_at=?,completed_at=?,
                       lease_owner=NULL,lease_expires_at=NULL
                   WHERE job_id=?""",
                (now.isoformat(), now.isoformat(), first_calendar.job["job_id"]),
            )

            news_id = await database.insert_news_item(
                db,
                {
                    "source": "test/source",
                    "title": "Federal Reserve update with broad market implications",
                    "summary": "Detailed market context and confirmed policy facts. " * 5,
                    "url": "https://example.test/unknown-calendar-concurrency",
                    "image_url": None,
                    "published_at": now.isoformat(),
                    "fetched_at": now.isoformat(),
                    "content_hash": "unknown-calendar-concurrency-news",
                },
            )
            news_job = await create_or_get_job(db, news_id)
            await db.commit()

            assert await claim_next_calendar_job(db, "unknown-calendar-blocked") is None
            assert await claim_next_job(db, "unknown-calendar-blocks-news") is None

            expired = (now - timedelta(seconds=61)).isoformat()
            await db.execute(
                "UPDATE calendar_analysis_jobs SET submitted_at=? WHERE job_id=?",
                (expired, first_calendar.job["job_id"]),
            )
            await db.commit()
            claimed_calendar = await claim_next_calendar_job(
                db,
                "unknown-calendar-released",
            )
            assert claimed_calendar is not None
            assert claimed_calendar["job_id"] == second_calendar.job["job_id"]

            await db.execute(
                """UPDATE calendar_analysis_jobs
                   SET status='failed',error_code='submission_outcome_unknown',
                       execution_mode='background',submitted_at=?,completed_at=?,
                       lease_owner=NULL,lease_expires_at=NULL
                   WHERE job_id=?""",
                (
                    now.isoformat(),
                    now.isoformat(),
                    second_calendar.job["job_id"],
                ),
            )
            await db.commit()
            assert await claim_next_job(db, "unknown-calendar-reblocks-news") is None

            await db.execute(
                "UPDATE calendar_analysis_jobs SET submitted_at=? WHERE job_id=?",
                (expired, second_calendar.job["job_id"]),
            )
            await db.commit()
            claimed_news = await claim_next_job(db, "unknown-calendar-news-released")
            assert claimed_news is not None
            assert claimed_news["job_id"] == news_job.job["job_id"]
        finally:
            await db.close()

    run(scenario())


def test_unknown_news_submission_reserves_calendar_global_capacity(
    isolated_calendar_db, monkeypatch
):
    monkeypatch.setattr(settings, "openai_execution_mode", "background")
    monkeypatch.setattr(settings, "openai_max_concurrency", 1)
    monkeypatch.setattr(settings, "calendar_llm_max_inflight", 1)
    monkeypatch.setattr(settings, "openai_background_poll_timeout_seconds", 60)

    async def scenario():
        now = datetime.now(timezone.utc)
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(
                db,
                {
                    "source": "test/source",
                    "title": "Federal Reserve policy update with broad market implications",
                    "summary": "Detailed market context and confirmed policy facts. " * 5,
                    "url": "https://example.test/unknown-news-concurrency",
                    "image_url": None,
                    "published_at": now.isoformat(),
                    "fetched_at": now.isoformat(),
                    "content_hash": "unknown-news-concurrency-calendar",
                },
            )
            news_job = await create_or_get_job(db, news_id)
            await db.execute(
                """UPDATE analysis_jobs
                   SET status='failed',error_code='submission_outcome_unknown',
                       execution_mode='background',submitted_at=?,completed_at=?,
                       lease_owner=NULL,lease_expires_at=NULL
                   WHERE job_id=?""",
                (now.isoformat(), now.isoformat(), news_job.job["job_id"]),
            )
            await db.commit()
            calendar_job = await create_or_get_calendar_job(
                db,
                [calendar_event(23)],
                provider="openai",
                model="gpt-5.6-terra",
            )
            await db.commit()

            assert await claim_next_calendar_job(db, "unknown-news-blocks-calendar") is None

            await db.execute(
                "UPDATE analysis_jobs SET submitted_at=? WHERE job_id=?",
                (
                    (now - timedelta(seconds=61)).isoformat(),
                    news_job.job["job_id"],
                ),
            )
            await db.commit()
            claimed = await claim_next_calendar_job(db, "unknown-news-calendar-released")
            assert claimed is not None
            assert claimed["job_id"] == calendar_job.job["job_id"]
        finally:
            await db.close()

    run(scenario())
