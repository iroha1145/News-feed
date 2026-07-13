import json
import os
import tempfile
import unittest
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

from app.services import calendar_analyzer, calendar_client, googlenews_client, newsapi_client
from app.services import news_aggregator
from app.services import market_focus
from app.utils import scheduler
from app.models import database
from app.integrations.option_pro.repository import upsert_source_health
from app.services.seekingalpha_client import _parse_sa_item
from app.utils.dedup import (
    compute_content_hash,
    compute_legacy_content_hash,
    deduplicate_batch,
    normalize_url,
)
from app.utils.http import redact_text, safe_exception_message


class SourceParsingTests(unittest.TestCase):
    def test_seekingalpha_date_is_converted_to_utc(self):
        item = ET.fromstring(
            """
            <item>
              <title>Market update</title>
              <link>https://seekingalpha.com/news/1?utm_source=rss</link>
              <pubDate>Fri, 10 Jul 2026 04:35:14 -0400</pubDate>
            </item>
            """
        )
        parsed = _parse_sa_item(item, "breaking")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["published_at"], "2026-07-10T08:35:14Z")
        self.assertEqual(parsed["source"], "seekingalpha/breaking")

    def test_google_low_context_item_is_kept(self):
        item = ET.fromstring(
            """
            <item>
              <title>Short headline - Publisher</title>
              <link>https://news.google.com/rss/articles/abc</link>
              <pubDate>Fri, 10 Jul 2026 08:00:00 GMT</pubDate>
              <source>Publisher</source>
            </item>
            """
        )
        parsed = googlenews_client._parse_rss_item(item)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["title"], "Short headline")
        self.assertIsNone(parsed["summary"])

    def test_newsapi_low_context_item_is_kept(self):
        parsed = newsapi_client._parse_item(
            {"title": "Brief", "url": "https://example.com/brief", "source": {"name": "Wire"}},
            "top-headlines",
        )
        self.assertIsNotNone(parsed)
        self.assertIsNone(parsed["summary"])


class DeduplicationTests(unittest.TestCase):
    def test_tracking_parameters_are_removed_but_content_parameters_remain(self):
        normalized = normalize_url(
            "https://Example.com/article?id=42&utm_source=newsletter&fbclid=secret#section"
        )
        self.assertEqual(normalized, "https://example.com/article?id=42")

    def test_hash_uses_publication_day_bucket(self):
        first = compute_content_hash("Same title", "https://one.example/a", "2026-07-10T01:00:00Z")
        same_day = compute_content_hash("Same title", "https://two.example/b", "2026-07-10T23:00:00Z")
        next_day = compute_content_hash("Same title", "https://one.example/a", "2026-07-11T01:00:00Z")
        self.assertEqual(first, same_day)
        self.assertNotEqual(first, next_day)

    def test_hash_uses_url_when_publication_date_is_missing(self):
        first = compute_content_hash("Market update", "https://one.example/a")
        second = compute_content_hash("Market update", "https://two.example/b")
        self.assertNotEqual(first, second)

    def test_legacy_hash_matches_the_previous_title_normalization(self):
        self.assertEqual(
            compute_legacy_content_hash("  Market   Update!  "),
            compute_legacy_content_hash("market update"),
        )

    def test_batch_near_duplicate_detection(self):
        items, duplicate_count = deduplicate_batch(
            [
                {
                    "title": "Fed holds rates steady and signals patience",
                    "url": "https://one.example/story?utm_source=x",
                    "published_at": "2026-07-10T01:00:00Z",
                },
                {
                    "title": "Fed holds rates steady, signals patience",
                    "url": "https://two.example/story",
                    "published_at": "2026-07-10T02:00:00Z",
                },
            ],
            threshold=0.85,
        )
        self.assertEqual(len(items), 1)
        self.assertEqual(duplicate_count, 1)


class RedactionTests(unittest.TestCase):
    def test_arbitrary_error_text_is_redacted(self):
        secret = "secret-value-123"
        message = redact_text(
            f"GET https://api.example.com/news?apiKey={secret}&page=1 "
            f"Authorization=Bearer {secret} token={secret}",
            (secret,),
        )
        self.assertNotIn(secret, message)
        self.assertNotIn("?", message)
        self.assertIn("[REDACTED]", message)

    def test_json_shaped_credentials_are_redacted(self):
        message = redact_text('{"api_key": "secret-json", "Authorization": "Bearer token-json"}')
        self.assertNotIn("secret-json", message)
        self.assertNotIn("token-json", message)

    def test_http_status_error_does_not_include_query(self):
        request = httpx.Request("GET", "https://api.example.com/news?token=top-secret")
        response = httpx.Response(401, request=request)
        error = httpx.HTTPStatusError("bad", request=request, response=response)
        message = safe_exception_message(error, secrets=("top-secret",))
        self.assertEqual(message, "HTTP 401 at https://api.example.com/news")


class HeaderAuthenticationTests(unittest.IsolatedAsyncioTestCase):
    async def test_newsapi_key_is_sent_in_header(self):
        calls = []

        class FakeResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return {"articles": []}

        class FakeClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def get(self, url, **kwargs):
                calls.append((url, kwargs))
                return FakeResponse()

        with patch.object(newsapi_client.httpx, "AsyncClient", return_value=FakeClient()):
            await newsapi_client.fetch_newsapi_news("header-secret")

        self.assertEqual(len(calls), 2)
        for _url, kwargs in calls:
            self.assertEqual(kwargs["headers"]["X-Api-Key"], "header-secret")
            self.assertNotIn("apiKey", kwargs["params"])


class CalendarCacheTests(unittest.TestCase):
    def test_last_known_good_write_is_atomic_and_readable(self):
        fetched_at = datetime.now(timezone.utc).isoformat()
        with tempfile.TemporaryDirectory() as directory:
            cache_file = Path(directory) / "calendar.json"
            with patch.object(calendar_client, "CACHE_FILE", cache_file):
                calendar_client._atomic_write_cache([{"title": "CPI"}], fetched_at)
                events, loaded_at = calendar_client._load_last_known_good()
        self.assertEqual(events, [{"title": "CPI"}])
        self.assertEqual(loaded_at, fetched_at)

    def test_last_known_good_rejects_an_old_week(self):
        old_time = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
        with tempfile.TemporaryDirectory() as directory:
            cache_file = Path(directory) / "calendar.json"
            cache_file.write_text(
                json.dumps({"fetched_at": old_time, "events": [{"title": "Old CPI"}]}),
                encoding="utf-8",
            )
            with patch.object(calendar_client, "CACHE_FILE", cache_file):
                with self.assertRaisesRegex(ValueError, "too old"):
                    calendar_client._load_last_known_good()

    def test_analysis_cache_key_changes_when_actual_changes(self):
        base = {
            "title": "CPI",
            "date": "2026-07-10T08:30:00-04:00",
            "country_code": "USD",
            "forecast": "0.2%",
            "previous": "0.1%",
            "actual": "",
        }
        released = {**base, "actual": "0.3%"}
        self.assertNotEqual(
            calendar_analyzer._cache_key([base]),
            calendar_analyzer._cache_key([released]),
        )

    def test_analysis_cache_key_changes_when_model_changes(self):
        event = {"title": "CPI", "date": "2026-07-10T12:30:00Z"}
        self.assertNotEqual(
            calendar_analyzer._cache_key([event], "openai", "model-a"),
            calendar_analyzer._cache_key([event], "openai", "model-b"),
        )

    def test_same_title_events_merge_by_date_and_country_identity(self):
        first = {"title": "Bank Holiday", "date": "2026-07-10", "country_code": "USD"}
        second = {"title": "Bank Holiday", "date": "2026-07-11", "country_code": "JPY"}
        analyzed = [{
            "event_id": calendar_analyzer._event_id(first),
            "title": "Bank Holiday",
            "title_zh": "美国假日",
            "stock_impact": "neutral",
            "commodity_impact": "neutral",
            "explanation": "美国市场事件。",
        }, {
            "event_id": calendar_analyzer._event_id(second),
            "title": "Bank Holiday",
            "title_zh": "日本假日",
            "stock_impact": "neutral",
            "commodity_impact": "neutral",
            "explanation": "日本市场事件。",
        }]
        merged = calendar_analyzer.merge_analysis([first, second], analyzed)
        self.assertEqual([item["title_zh"] for item in merged], ["美国假日", "日本假日"])


class CalendarFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_refresh_returns_stale_marked_cache(self):
        class FailingClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def get(self, url, **_kwargs):
                request = httpx.Request("GET", url)
                raise httpx.ConnectError("offline", request=request)

        raw_event = {
            "date": "2026-07-10T08:30:00-04:00",
            "title": "CPI",
            "country": "USD",
            "impact": "High",
            "forecast": "0.2%",
            "previous": "0.1%",
            "actual": "",
        }
        initial_status = {
            "source": "faireconomy",
            "stale": False,
            "as_of": None,
            "last_attempt": None,
            "last_success": None,
            "last_error": None,
        }
        with tempfile.TemporaryDirectory() as directory:
            cache_file = Path(directory) / "calendar.json"
            with (
                patch.object(calendar_client, "CACHE_FILE", cache_file),
                patch.object(calendar_client, "_calendar_cache", []),
                patch.object(calendar_client, "_cache_time", 0.0),
                patch.object(calendar_client, "_calendar_status", initial_status),
                patch.object(calendar_client.httpx, "AsyncClient", return_value=FailingClient()),
            ):
                fetched_at = datetime.now(timezone.utc).isoformat()
                calendar_client._atomic_write_cache([raw_event], fetched_at)
                events = await calendar_client.fetch_economic_calendar(force=True)
                status = calendar_client.get_calendar_status()

        self.assertEqual(len(events), 1)
        self.assertTrue(events[0]["is_stale"])
        self.assertTrue(status["stale"])
        self.assertEqual(status["as_of"], fetched_at)
        self.assertIsNotNone(status["last_error"])


class SourceScheduleTests(unittest.TestCase):
    def test_default_source_states_and_intervals(self):
        expected = {
            "finnhub": (True, 300),
            "massive": (True, 3600),
            "google": (True, 900),
            "seekingalpha_breaking": (True, 300),
            "seekingalpha_daily": (True, 21600),
            "newsapi": (False, 1800),
            "gnews": (False, 1800),
        }
        actual = {
            name: (definition.default_enabled, definition.default_interval)
            for name, definition in news_aggregator.SOURCE_DEFINITIONS.items()
        }
        self.assertEqual(actual, expected)

    def test_environment_can_enable_an_opt_in_source(self):
        with patch.dict(os.environ, {"NEWSAPI_NEWS_ENABLED": "true"}):
            self.assertTrue(news_aggregator.is_source_enabled("newsapi"))

    def test_status_shape_contains_health_fields(self):
        required = {
            "last_attempt", "last_success", "last_error", "duration_ms",
            "raw", "inserted", "duplicates",
            "source_fetch_status", "news_persistence_status", "event_projection_status",
        }
        self.assertTrue(all(required <= set(item) for item in news_aggregator.get_source_statuses()))


class SourceHealthPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_restart_restores_last_success_before_refresh(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "source-health.db")
            with patch.object(database, "DB_PATH", path):
                await database.init_db()
                db = await database.get_db()
                try:
                    await upsert_source_health(
                        db,
                        source="google",
                        status="degraded",
                        last_attempt_at="2026-07-12T10:05:00+00:00",
                        last_success_at="2026-07-12T10:00:00+00:00",
                        data_through="2026-07-12T10:00:00+00:00",
                        consecutive_failures=2,
                        next_attempt_at="2026-07-12T10:15:00+00:00",
                        raw_count=12,
                        inserted_count=7,
                        duplicates_count=5,
                        error_code="source_fetch_failed",
                    )
                finally:
                    await db.close()
                with patch.object(news_aggregator, "_source_status", {}):
                    await news_aggregator.initialize_source_health()
                    google = next(
                        item for item in news_aggregator.get_source_statuses()
                        if item["source"] == "google"
                    )
                    self.assertEqual(google["last_success"], "2026-07-12T10:00:00+00:00")
                    self.assertEqual(google["consecutive_failures"], 2)

    async def test_local_projection_failure_is_queued_without_source_backoff(self):
        async def fetch_google():
            now = datetime.now(timezone.utc).isoformat()
            return [{
                "source": "google/Reuters",
                "title": "NVDA raises earnings guidance after strong demand",
                "summary": "The company raised revenue guidance after its earnings report.",
                "url": "https://example.test/projection-retry",
                "image_url": None,
                "published_at": now,
                "source_tickers": ["NVDA"],
                "ticker_association_method": "provider_tag",
            }]

        definition = news_aggregator.SourceDefinition(
            "google", fetch_google, True, 900, "google_news"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "projection-retry.db")
            with (
                patch.object(database, "DB_PATH", path),
                patch.dict(news_aggregator.SOURCE_DEFINITIONS, {"google": definition}),
                patch.object(news_aggregator, "_source_status", {}),
                patch.object(news_aggregator, "_backoff_until", {}),
            ):
                await database.init_db()
                with patch.object(
                    market_focus,
                    "ingest_event_evidence",
                    new=AsyncMock(side_effect=RuntimeError("local projection failed")),
                ):
                    result = await news_aggregator.aggregate_source("google", force=True)
                self.assertEqual(result["status"], "degraded")
                self.assertEqual(result["projection_queued"], 1)
                status = next(
                    item for item in news_aggregator.get_source_statuses()
                    if item["source"] == "google"
                )
                self.assertEqual(status["source_fetch_status"], "ok")
                self.assertEqual(status["news_persistence_status"], "ok")
                self.assertEqual(status["event_projection_status"], "degraded")
                self.assertEqual(status["consecutive_failures"], 0)
                self.assertIsNone(status["next_attempt_at"])

                db = await database.get_db()
                try:
                    health = await (await db.execute(
                        """SELECT source_fetch_status,news_persistence_status,
                                  event_projection_status,consecutive_failures,next_attempt_at
                           FROM source_health WHERE source='google'"""
                    )).fetchone()
                    retry = await (await db.execute(
                        "SELECT status,attempt_count FROM event_projection_retries"
                    )).fetchone()
                    self.assertEqual(tuple(health), ("ok", "ok", "degraded", 0, None))
                    self.assertEqual(tuple(retry), ("pending", 0))
                finally:
                    await db.close()

                retried = await news_aggregator.process_projection_retry_queue()
                repeated = await news_aggregator.process_projection_retry_queue()
                self.assertEqual(retried, {"attempted": 1, "completed": 1, "failed": 0})
                self.assertEqual(repeated, {"attempted": 0, "completed": 0, "failed": 0})
                db = await database.get_db()
                try:
                    member_count = await (await db.execute(
                        "SELECT COUNT(*) FROM news_event_members"
                    )).fetchone()
                    self.assertEqual(member_count[0], 1)
                finally:
                    await db.close()

    async def test_scheduler_runs_the_projection_retry_queue(self):
        retry = AsyncMock(return_value={"attempted": 0, "completed": 0, "failed": 0})
        with patch.object(news_aggregator, "process_projection_retry_queue", new=retry):
            await scheduler._job_retry_event_projections()
        retry.assert_awaited_once_with()

    async def test_projection_retry_recovers_stale_in_progress_row(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "projection-stale.db")
            with patch.object(database, "DB_PATH", path):
                await database.init_db()
                now = datetime.now(timezone.utc)
                record = {
                    "source": "google/Reuters",
                    "title": "NVDA raises earnings guidance after strong demand",
                    "summary": "The company raised revenue guidance after its earnings report.",
                    "url": "https://example.test/projection-stale",
                    "published_at": now.isoformat(),
                    "fetched_at": now.isoformat(),
                    "content_hash": "a" * 64,
                    "source_tickers": ["NVDA"],
                    "ticker_association_method": "provider_tag",
                }
                db = await database.get_db()
                try:
                    retry_id = await news_aggregator.enqueue_projection_retry(
                        db, record=record, news_id=None, source="google"
                    )
                    await db.execute(
                        """UPDATE event_projection_retries SET status='in_progress',updated_at=?
                           WHERE retry_id=?""",
                        ((now - timedelta(minutes=20)).isoformat(), retry_id),
                    )
                    await db.commit()
                finally:
                    await db.close()

                result = await news_aggregator.process_projection_retry_queue()
                self.assertEqual(result, {"attempted": 1, "completed": 1, "failed": 0})

    async def test_projection_retry_dead_letters_poison_payload_at_attempt_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "projection-dead-letter.db")
            with patch.object(database, "DB_PATH", path):
                await database.init_db()
                now = datetime.now(timezone.utc)
                record = {
                    "source": "google/Reuters",
                    "title": "Poison event projection payload",
                    "summary": "A deterministic invalid local projection payload.",
                    "url": "https://example.test/projection-poison",
                    "published_at": now.isoformat(),
                    "fetched_at": now.isoformat(),
                    "content_hash": "b" * 64,
                    "source_tickers": ["NVDA"],
                    "ticker_association_method": "provider_tag",
                }
                db = await database.get_db()
                try:
                    retry_id = await news_aggregator.enqueue_projection_retry(
                        db, record=record, news_id=None, source="google"
                    )
                    await db.commit()
                finally:
                    await db.close()

                with (
                    patch.object(news_aggregator.app_settings, "projection_retry_max_attempts", 2),
                    patch.object(
                        market_focus,
                        "ingest_event_evidence",
                        new=AsyncMock(side_effect=ValueError("poison")),
                    ),
                ):
                    first = await news_aggregator.process_projection_retry_queue()
                    self.assertEqual(first["failed"], 1)
                    db = await database.get_db()
                    try:
                        await db.execute(
                            "UPDATE event_projection_retries SET next_attempt_at=? WHERE retry_id=?",
                            ((datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(), retry_id),
                        )
                        await db.commit()
                    finally:
                        await db.close()
                    second = await news_aggregator.process_projection_retry_queue()
                    third = await news_aggregator.process_projection_retry_queue()
                self.assertEqual(second["failed"], 1)
                self.assertEqual(third, {"attempted": 0, "completed": 0, "failed": 0})
                db = await database.get_db()
                try:
                    row = await (await db.execute(
                        """SELECT status,attempt_count,next_attempt_at,last_error_code
                           FROM event_projection_retries WHERE retry_id=?""",
                        (retry_id,),
                    )).fetchone()
                    self.assertEqual(
                        tuple(row),
                        ("failed", 2, None, "event_projection_dead_letter"),
                    )
                finally:
                    await db.close()


if __name__ == "__main__":
    unittest.main()
