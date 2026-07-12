import json
import os
import tempfile
import unittest
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import httpx

from app.services import calendar_analyzer, calendar_client, googlenews_client, newsapi_client
from app.services import news_aggregator
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
            "massive": (True, 300),
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


if __name__ == "__main__":
    unittest.main()
