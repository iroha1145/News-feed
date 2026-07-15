import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.config import settings
from app.deps import auth as auth_deps
from app.models import database
from app.models.schemas import LLMAnalysisPayload
from app.routers import analysis as analysis_router
from app.routers import auth as auth_router
from app.routers import quotes
from app.routers import x_sentiment as x_sentiment_router
from app.services import grok_x_monitor, llm_analyzer
from app.utils.dedup import compute_content_hash, compute_legacy_content_hash


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    path = tmp_path / "macrolens-test.db"
    monkeypatch.setattr(database, "DB_PATH", str(path))
    monkeypatch.setattr(settings, "news_llm_manual_enabled", True)
    monkeypatch.setattr(settings, "news_llm_manual_daily_job_limit", 50)
    monkeypatch.setattr(settings, "news_llm_manual_daily_output_token_limit", 1_638_400)
    run(database.init_db())
    return path


def news_record(index: int, summary: str = "A sufficiently detailed market summary " * 4) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "source": "test/source",
        "title": f"Test market headline {index} with enough detail for analysis",
        "summary": summary,
        "url": f"https://example.test/{index}",
        "image_url": None,
        "published_at": now,
        "fetched_at": now,
        "content_hash": f"hash-{index}",
    }


def analysis_record(news_id: int, analyzed_at: str, classification: str = "neutral") -> dict:
    return {
        "news_id": news_id,
        "title_zh": "测试标题",
        "headline_summary": "测试摘要",
        "overall_sentiment": 0,
        "classification": classification,
        "confidence": 80,
        "affected_stocks": "[]",
        "affected_sectors": json.dumps(["Technology"]),
        "affected_commodities": "[]",
        "logic_chain": "信息 → 中性影响",
        "key_factors": "[]",
        "llm_provider": "test",
        "llm_model": "test-model",
        "analyzed_at": analyzed_at,
    }


def test_session_login_logout_and_header_compatibility(monkeypatch):
    monkeypatch.setattr(settings, "admin_token", "correct-horse-battery-staple")
    monkeypatch.setattr(settings, "session_ttl_seconds", 600)
    monkeypatch.setattr(settings, "session_cookie_secure", False)
    auth_deps._sessions.clear()
    auth_router._login_attempts.clear()

    app = FastAPI()
    app.include_router(auth_router.router)

    @app.get("/protected")
    async def protected(_: None = Depends(auth_deps.require_admin)):
        return {"ok": True}

    with TestClient(app) as client:
        assert client.get("/protected").status_code == 401
        response = client.post(
            "/api/auth/login",
            json={"token": "correct-horse-battery-staple"},
        )
        assert response.status_code == 200
        assert response.json()["expires_in"] == 600
        cookie = response.headers["set-cookie"]
        assert "macrolens_admin_session=" in cookie
        assert "HttpOnly" in cookie
        assert client.get("/api/auth/session").json() == {"authenticated": True}
        assert client.get("/protected").status_code == 200
        assert client.post("/api/auth/logout").status_code == 200
        assert client.get("/api/auth/session").json() == {"authenticated": False}

    with TestClient(app) as client:
        response = client.get(
            "/protected",
            headers={"X-Admin-Token": "correct-horse-battery-staple"},
        )
        assert response.status_code == 200

    monkeypatch.setattr(settings, "session_cookie_secure", True)
    with TestClient(app) as client:
        response = client.post(
            "/api/auth/login",
            json={"token": "correct-horse-battery-staple"},
        )
        assert "Secure" in response.headers["set-cookie"]


def test_login_rate_limit(monkeypatch):
    monkeypatch.setattr(settings, "admin_token", "expected")
    auth_router._login_attempts.clear()
    app = FastAPI()
    app.include_router(auth_router.router)

    with TestClient(app) as client:
        for _ in range(auth_router.LOGIN_MAX_ATTEMPTS):
            assert client.post("/api/auth/login", json={"token": "wrong"}).status_code == 401
        response = client.post("/api/auth/login", json={"token": "wrong"})
        assert response.status_code == 429
        assert int(response.headers["retry-after"]) > 0


def test_llm_payload_rejects_invalid_types_ranges_and_shapes():
    valid = {
        "title_zh": "标题",
        "headline_summary": "摘要",
        "overall_sentiment": 0,
        "classification": "neutral",
        "confidence": 50,
        "affected_stocks": [],
        "affected_sectors": [],
        "affected_commodities": [],
        "logic_chain": "信息 → 中性",
        "key_factors": [],
    }
    assert LLMAnalysisPayload.model_validate(valid).classification.value == "neutral"
    for invalid in (
        {**valid, "confidence": "50"},
        {**valid, "overall_sentiment": 101},
        {**valid, "classification": "positive"},
        {**valid, "affected_sectors": [1]},
        {**valid, "unexpected": True},
    ):
        with pytest.raises(ValidationError):
            LLMAnalysisPayload.model_validate(invalid)


def test_model_market_scenario_payload_is_strict_and_bounded():
    valid = {
        "trending_tickers": [{
            "ticker": "SPY",
            "mention_sentiment": "mixed",
            "buzz_level": "low",
            "narrative": "Recent saved headlines show mixed macro signals.",
        }],
        "overall_retail_sentiment": 0,
        "key_narratives": ["Mixed macro signals"],
        "meme_stock_alerts": [],
        "fear_greed_estimate": 50,
    }
    assert grok_x_monitor._parse_scenario(json.dumps(valid)).fear_greed_estimate == 50
    with pytest.raises(ValidationError):
        grok_x_monitor._parse_scenario(json.dumps({**valid, "fear_greed_estimate": 150}))
    with pytest.raises(ValidationError):
        grok_x_monitor._parse_scenario(json.dumps({**valid, "overall_retail_sentiment": "0"}))


def test_disabled_model_market_scenario_never_opens_database_or_queues_refresh(
    isolated_db, monkeypatch
):
    monkeypatch.setattr(settings, "x_sentiment_enabled", False)
    monkeypatch.setattr(settings, "admin_token", "scenario-admin")
    database_opened = False
    refresh_called = False

    async def forbidden_get_db():
        nonlocal database_opened
        database_opened = True
        raise AssertionError("disabled scenario must not open the database")

    async def forbidden_refresh():
        nonlocal refresh_called
        refresh_called = True
        raise AssertionError("disabled scenario must not queue provider work")

    monkeypatch.setattr(grok_x_monitor, "get_db", forbidden_get_db)
    assert run(grok_x_monitor.run_x_sentiment_analysis()) is None
    assert database_opened is False

    monkeypatch.setattr(
        x_sentiment_router, "run_x_sentiment_analysis", forbidden_refresh
    )
    app = FastAPI()
    app.include_router(x_sentiment_router.router)
    with TestClient(app) as client:
        response = client.post(
            "/api/x-sentiment/refresh",
            headers={"X-Admin-Token": "scenario-admin"},
        )
        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "disabled"
    assert refresh_called is False

    async def count_rows():
        db = await database.get_db()
        try:
            async with db.execute("SELECT COUNT(*) FROM x_sentiments") as cursor:
                return int((await cursor.fetchone())[0])
        finally:
            await db.close()

    assert run(count_rows()) == 0


def test_manual_analysis_routes_fail_closed_and_report_real_enqueue_count(
    isolated_db, monkeypatch
):
    monkeypatch.setattr(settings, "admin_token", "analysis-admin")

    async def seed_news(index: int):
        db = await database.get_db()
        try:
            return await database.insert_news_item(db, news_record(index))
        finally:
            await db.close()

    first_news_id = run(seed_news(901))
    app = FastAPI()
    app.include_router(analysis_router.router)
    headers = {"X-Admin-Token": "analysis-admin"}

    monkeypatch.setattr(settings, "news_llm_manual_enabled", False)
    with TestClient(app) as client:
        disabled = client.post("/api/analysis/trigger", headers=headers)
        assert disabled.status_code == 409
        assert disabled.json()["detail"]["code"] == "disabled"
        disabled_retry = client.post(
            f"/api/analysis/retry-failed?news_id={first_news_id}",
            headers=headers,
        )
        assert disabled_retry.status_code == 409
        assert disabled_retry.json()["detail"]["code"] == "disabled"

    monkeypatch.setattr(settings, "news_llm_manual_enabled", True)
    monkeypatch.setattr(settings, "news_llm_manual_daily_job_limit", 10)
    monkeypatch.setattr(settings, "news_llm_manual_daily_output_token_limit", None)
    with TestClient(app) as client:
        unbudgeted = client.post("/api/analysis/trigger", headers=headers)
        assert unbudgeted.status_code == 409
        assert (
            unbudgeted.json()["detail"]["code"]
            == "budget_configuration_required"
        )
        unbudgeted_retry = client.post(
            f"/api/analysis/retry-failed?news_id={first_news_id}",
            headers=headers,
        )
        assert unbudgeted_retry.status_code == 409

    async def assert_no_jobs():
        db = await database.get_db()
        try:
            async with db.execute("SELECT COUNT(*) FROM analysis_jobs") as cursor:
                assert int((await cursor.fetchone())[0]) == 0
            news = await database.get_news_item_by_id(
                db, first_news_id, include_internal=True
            )
            assert news["analysis_status"] == "pending"
        finally:
            await db.close()

    run(assert_no_jobs())

    monkeypatch.setattr(
        settings, "news_llm_manual_daily_output_token_limit", 1_000_000
    )
    with TestClient(app) as client:
        enabled = client.post("/api/analysis/trigger?batch_size=5", headers=headers)
        assert enabled.status_code == 200
        assert enabled.json() == {
            "status": "queued",
            "batch_size": 5,
            "enqueued": 1,
            "capability": "enabled",
        }
        no_more = client.post("/api/analysis/trigger?batch_size=5", headers=headers)
        assert no_more.status_code == 200
        assert no_more.json()["status"] == "no_eligible_news"
        assert no_more.json()["enqueued"] == 0

        run(seed_news(902))
        monkeypatch.setattr(settings, "news_llm_manual_daily_job_limit", 1)
        budget_full = client.post(
            "/api/analysis/trigger?batch_size=5",
            headers=headers,
        )
        assert budget_full.status_code == 200
        assert budget_full.json()["status"] == "budget_blocked"
        assert budget_full.json()["enqueued"] == 0
        assert budget_full.json()["stop_reason"] == "daily_job_limit_reached"

    async def assert_manual_job():
        db = await database.get_db()
        try:
            async with db.execute(
                "SELECT request_origin,status FROM analysis_jobs WHERE news_id=?",
                (first_news_id,),
            ) as cursor:
                assert tuple(await cursor.fetchone()) == ("manual", "pending")
        finally:
            await db.close()

    run(assert_manual_job())


def test_database_pragmas_indexes_batch_insert_and_absolute_path(isolated_db, monkeypatch):
    assert database.SQLITE_BUSY_TIMEOUT_MS == 30_000
    connect_timeouts = []
    real_connect = database.aiosqlite.connect

    def recording_connect(*args, **kwargs):
        connect_timeouts.append(kwargs.get("timeout"))
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(database.aiosqlite, "connect", recording_connect)

    async def scenario():
        db = await database.get_db()
        try:
            assert await database.insert_news_items_batch(
                db,
                [news_record(1), news_record(2), news_record(2)],
            ) == 2
            async with db.execute("PRAGMA journal_mode") as cursor:
                assert (await cursor.fetchone())[0].lower() == "wal"
            async with db.execute("PRAGMA foreign_keys") as cursor:
                assert (await cursor.fetchone())[0] == 1
            async with db.execute("PRAGMA busy_timeout") as cursor:
                assert (await cursor.fetchone())[0] == database.SQLITE_BUSY_TIMEOUT_MS
            async with db.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_news_analysis_status_published'"
            ) as cursor:
                assert await cursor.fetchone()
        finally:
            await db.close()

    run(scenario())
    run(database.init_db())
    assert connect_timeouts == [30.0, 30.0]

    absolute = isolated_db.parent / "absolute.db"
    monkeypatch.setattr(settings, "database_url", f"sqlite+aiosqlite:///{absolute}")
    assert database._resolve_db_path() == str(absolute)
    monkeypatch.setattr(settings, "database_url", "sqlite+aiosqlite:///data/relative.db")
    assert database._resolve_db_path() == "data/relative.db"


def test_batch_insert_respects_legacy_hashes_and_cross_source_fuzzy_matches(isolated_db):
    async def scenario():
        db = await database.get_db()
        try:
            old = news_record(5)
            old["title"] = "Legacy headline"
            old["published_at"] = "2026-07-09T01:00:00Z"
            old["content_hash"] = compute_legacy_content_hash(old["title"], old["url"])
            assert await database.insert_news_item(db, old) is not None

            repeated = news_record(6)
            repeated["title"] = old["title"]
            repeated["published_at"] = "2026-07-10T01:00:00Z"
            repeated["content_hash"] = compute_content_hash(
                repeated["title"], repeated["url"], repeated["published_at"]
            )
            repeated["legacy_content_hash"] = compute_legacy_content_hash(
                repeated["title"], repeated["url"]
            )
            assert await database.insert_news_items_batch(db, [repeated]) == 0

            first = news_record(7)
            first["title"] = "Fed holds rates steady and signals patience"
            first["source"] = "provider/one"
            first["content_hash"] = compute_content_hash(
                first["title"], first["url"], first["published_at"]
            )
            assert await database.insert_news_item(db, first) is not None

            rewrite = news_record(8)
            rewrite["title"] = "Fed holds rates steady, signals patience"
            rewrite["source"] = "provider/two"
            rewrite["content_hash"] = compute_content_hash(
                rewrite["title"], rewrite["url"], rewrite["published_at"]
            )
            rewrite["legacy_content_hash"] = compute_legacy_content_hash(
                rewrite["title"], rewrite["url"]
            )
            assert await database.insert_news_items_batch(db, [rewrite]) == 0
        finally:
            await db.close()

    run(scenario())


def test_expired_analysis_lease_is_recovered(isolated_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(10))
            legacy_news_id = await database.insert_news_item(db, news_record(11))
            assert news_id is not None
            assert await database.claim_news_for_analysis(db, news_id)
            await db.execute(
                "UPDATE news_items SET analysis_lease_expires_at = ? WHERE id = ?",
                ("2000-01-01T00:00:00+00:00", news_id),
            )
            await db.execute(
                """UPDATE news_items
                   SET analysis_status = 'processing', analysis_attempts = 1,
                       analysis_claimed_at = NULL, analysis_lease_expires_at = NULL
                   WHERE id = ?""",
                (legacy_news_id,),
            )
            await db.commit()
            assert await database.recover_stale_analysis_leases(db) == 2
            item = await database.get_news_item_by_id(db, news_id)
            legacy_item = await database.get_news_item_by_id(db, legacy_news_id)
            assert item["analysis_status"] == "pending"
            assert item["analysis_lease_expires_at"] is None
            assert legacy_item["analysis_status"] == "pending"
        finally:
            await db.close()

    run(scenario())


def test_low_context_news_skips_llm_but_keeps_neutral_analysis(isolated_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(20, summary=""))
            item = await database.get_news_item_by_id(db, news_id)
            result = await llm_analyzer.analyze_news_item(item, db)
            assert result is not None
            assert result["classification"] == "neutral"
            assert result["confidence"] == 0
            assert result["llm_provider"] == "system"
            stored = await database.get_analysis_for_news(db, news_id)
            refreshed = await database.get_news_item_by_id(db, news_id)
            assert stored is not None
            assert refreshed["analysis_status"] == "insufficient_context"
        finally:
            await db.close()

    run(scenario())


def test_legacy_analyzer_only_enqueues_durable_job(isolated_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(30))
            item = await database.get_news_item_by_id(db, news_id)
            assert await llm_analyzer.analyze_news_item(item, db) is None
            refreshed = await database.get_news_item_by_id(db, news_id)
            assert refreshed["analysis_status"] == "pending"
            assert refreshed["analysis_lease_expires_at"] is None
            async with db.execute("SELECT status FROM analysis_jobs WHERE news_id=?", (news_id,)) as cursor:
                assert (await cursor.fetchone())[0] == "pending"
        finally:
            await db.close()

    run(scenario())


def test_analysis_retention_keeps_news_and_marks_pruned_rows_skipped(isolated_db):
    async def scenario():
        db = await database.get_db()
        try:
            await database.set_setting(db, "analysis_retention_limit", 2)
            news_ids = []
            base = datetime.now(timezone.utc) - timedelta(days=3)
            for index in range(3):
                news_id = await database.insert_news_item(db, news_record(40 + index))
                news_ids.append(news_id)
                await database.save_analysis_result(
                    db,
                    analysis_record(news_id, (base + timedelta(days=index)).isoformat()),
                )

            # save_analysis_result enforces the configured limit continuously.
            deleted = await database.cleanup_retained_data(db)
            assert deleted["analyses"] == 0
            async with db.execute("SELECT COUNT(*) FROM news_items") as cursor:
                assert (await cursor.fetchone())[0] == 3
            async with db.execute("SELECT COUNT(*) FROM analyses") as cursor:
                assert (await cursor.fetchone())[0] == 2
            oldest = await database.get_news_item_by_id(db, news_ids[0])
            assert oldest["analysis_status"] == "skipped"
        finally:
            await db.close()

    run(scenario())


def test_stats_default_to_seven_days_and_neutral_is_not_bearish(isolated_db):
    async def scenario():
        db = await database.get_db()
        try:
            recent_id = await database.insert_news_item(db, news_record(50))
            old_id = await database.insert_news_item(db, news_record(51))
            recent_record = analysis_record(recent_id, datetime.now(timezone.utc).isoformat())
            recent_record["affected_stocks"] = json.dumps([{
                "ticker": "SPX",
                "company": "S&P 500",
                "impact_score": 0,
                "reason": "Direct index mention",
            }])
            await database.save_analysis_result(db, recent_record)
            await database.save_analysis_result(
                db,
                analysis_record(old_id, (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()),
            )
            stats = await database.get_analysis_stats(db)
            assert stats["window_days"] == 7
            assert stats["total_analyzed"] == 1

            sentiment = await database.get_asset_sentiment(db, "^GSPC", days=7)
            assert sentiment["neutral"] == 1
            assert "中性" in sentiment["description"]
            assert "偏空" not in sentiment["description"]
        finally:
            await db.close()

    run(scenario())


def test_news_classification_filter_applies_before_pagination(isolated_db):
    async def scenario():
        db = await database.get_db()
        try:
            bullish_id = await database.insert_news_item(db, news_record(60))
            bearish_id = await database.insert_news_item(db, news_record(61))
            now = datetime.now(timezone.utc).isoformat()
            await database.save_analysis_result(
                db,
                {**analysis_record(bullish_id, now, "bullish"), "overall_sentiment": 50},
            )
            await database.save_analysis_result(
                db,
                {**analysis_record(bearish_id, now, "bearish"), "overall_sentiment": -50},
            )

            total, items = await database.get_news_items(
                db,
                page=1,
                page_size=1,
                classification="bullish",
            )
            assert total == 1
            assert len(items) == 1
            assert items[0]["analysis"]["classification"] == "bullish"
        finally:
            await db.close()

    run(scenario())


def test_regular_ticker_sentiment_uses_the_requested_symbol(isolated_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(70))
            record = analysis_record(news_id, datetime.now(timezone.utc).isoformat(), "bullish")
            record["overall_sentiment"] = 60
            record["affected_stocks"] = json.dumps([{
                "ticker": "AMD",
                "company": "Advanced Micro Devices",
                "impact_score": 60,
                "reason": "Test",
            }])
            await database.save_analysis_result(db, record)

            sentiment = await database.get_asset_sentiment(db, "AMD", days=7)
            assert sentiment["total"] == 1
            assert sentiment["bullish"] == 1
            assert sentiment["score"] is not None
        finally:
            await db.close()

    run(scenario())


def test_legacy_analysis_json_is_normalized_at_the_read_boundary(isolated_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id = await database.insert_news_item(db, news_record(71))
            record = analysis_record(news_id, datetime.now(timezone.utc).isoformat(), "neutral")
            record["affected_stocks"] = json.dumps([{
                "ticker": "$AMD",
                "impact_score": "not-a-number",
            }])
            record["affected_sectors"] = json.dumps({"unexpected": "shape"})
            record["affected_commodities"] = json.dumps(["Gold"])
            await database.save_analysis_result(db, record)

            stored = await database.get_analysis_for_news(db, news_id)
            assert stored["affected_stocks"] == [{
                "ticker": "AMD",
                "company": "AMD",
                "impact_score": 0,
                "reason": "",
            }]
            assert stored["affected_sectors"] == []
            assert stored["affected_commodities"] == [{
                "name": "Gold",
                "impact_score": 0,
                "reason": "",
            }]
            assert (await database.get_analysis_stats(db))["total_analyzed"] == 1
        finally:
            await db.close()

    run(scenario())


def test_quote_symbol_validation_bounded_cache_and_no_static_constituents(monkeypatch):
    cache = quotes.BoundedTTLCache(max_size=2, ttl_seconds=60)
    cache.set("one", 1)
    cache.set("two", 2)
    cache.set("three", 3)
    assert len(cache) == 2
    assert cache.get("one") is None
    assert quotes._validated_symbol("brk-b") == "BRK-B"
    with pytest.raises(HTTPException):
        quotes._validated_symbol("../etc/passwd")

    called = []

    async def fake_to_thread(function, *args):
        called.append((function, args))
        return {"symbol": args[0], "source": quotes.YAHOO_SOURCE, "as_of": "test"}

    monkeypatch.setattr(quotes.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(quotes, "_profile_cache", quotes.BoundedTTLCache(2, 60))
    monkeypatch.setattr(quotes, "_yfinance_slots", None)
    assert run(quotes.get_profile("TEST"))["symbol"] == "TEST"
    assert called and called[0][0] is quotes._fetch_profile_sync

    constituents = run(quotes.get_constituents("^GSPC"))
    assert constituents == {
        "symbol": "^GSPC",
        "constituents": [],
        "source": None,
        "as_of": None,
    }
    assert quotes._empty_quote("^GSPC", quotes.INDICES["^GSPC"], "test")["marketOpen"] is None


def test_quote_provider_failure_logs_strip_query_credentials(monkeypatch, caplog):
    async def fail_provider(*_args, **_kwargs):
        raise RuntimeError(
            "provider failed at https://query.example.test/chart?token=top-secret&symbol=SPY"
        )

    monkeypatch.setattr(quotes, "_run_yfinance", fail_provider)
    monkeypatch.setattr(quotes, "_market_cache", quotes.BoundedTTLCache(2, 60))
    with caplog.at_level("WARNING", logger=quotes.__name__):
        response = run(quotes.get_market_quotes())
    assert response["quotes"]
    assert "https://query.example.test/chart" in caplog.text
    assert "?token=" not in caplog.text
    assert "top-secret" not in caplog.text


def test_live_and_health_check_database_and_scheduler(isolated_db, monkeypatch):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        from app import main
    finally:
        asyncio.set_event_loop(None)
        loop.close()
    from app.utils import scheduler

    class RunningScheduler:
        running = True

    monkeypatch.setattr(scheduler, "get_scheduler", lambda: RunningScheduler())
    assert run(main.live())["status"] == "alive"
    healthy = run(main.health())
    assert healthy["database"] == "ok"
    assert healthy["scheduler"] == "running"

    monkeypatch.setattr(scheduler, "get_scheduler", lambda: None)
    with pytest.raises(HTTPException) as exc_info:
        run(main.health())
    assert exc_info.value.status_code == 503
