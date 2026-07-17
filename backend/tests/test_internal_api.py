from __future__ import annotations

import httpx
import pytest
from datetime import datetime, timedelta, timezone

from app.main import app
from app.models import database


AUTH = {"Authorization": "Bearer test-internal-token"}


def _item(number: int) -> dict:
    return {
        "source": "test",
        "title": f"News item {number}",
        "summary": f"Summary {number}",
        "url": f"https://example.com/news/{number}",
        "image_url": None,
        "published_at": f"2026-07-15T00:0{number}:00Z",
        "fetched_at": f"2026-07-15T00:1{number}:00Z",
        "source_tickers": [f"T{number}"],
    }


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def _client():
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    )


@pytest.mark.asyncio
async def test_only_expected_http_routes_are_exposed(clean_db):
    assert {route.path for route in app.routes} == {
        "/health",
        "/internal/v1/health",
        "/internal/v1/news/changes",
        "/internal/v1/news/{news_id}",
        "/internal/v1/calendar",
    }
    for route in app.routes:
        assert route.methods == {"GET"}


@pytest.mark.asyncio
async def test_bearer_auth_rejects_missing_wrong_and_duplicate_headers(clean_db):
    async with await _client() as client:
        assert (await client.get("/internal/v1/health")).status_code == 401
        assert (
            await client.get(
                "/internal/v1/health", headers={"Authorization": "Bearer wrong"}
            )
        ).status_code == 401
        duplicate = await client.get(
            "/internal/v1/health",
            headers=[
                ("Authorization", "Bearer test-internal-token"),
                ("Authorization", "Bearer test-internal-token"),
            ],
        )
        assert duplicate.status_code == 401
        assert (await client.get("/internal/v1/health", headers=AUTH)).status_code == 200


@pytest.mark.asyncio
async def test_bearer_auth_uses_constant_time_comparison(clean_db, monkeypatch):
    from app.deps import bearer

    calls = []
    original = bearer.secrets.compare_digest

    def observed(left, right):
        calls.append((left, right))
        return original(left, right)

    monkeypatch.setattr(bearer.secrets, "compare_digest", observed)
    async with await _client() as client:
        response = await client.get("/internal/v1/health", headers=AUTH)
    assert response.status_code == 200
    assert calls == [("test-internal-token", "test-internal-token")]


@pytest.mark.asyncio
async def test_internal_api_fails_closed_when_internal_token_is_unset(clean_db, monkeypatch):
    from app.config import INTERNAL_TOKEN_ENV, settings

    monkeypatch.delitem(settings.environment, INTERNAL_TOKEN_ENV, raising=False)
    async with await _client() as client:
        response = await client.get(
            "/internal/v1/health", headers={"Authorization": "Bearer test-internal-token"}
        )
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "internal_token_not_configured"


@pytest.mark.asyncio
async def test_internal_token_never_appears_in_http_responses(clean_db):
    async with await _client() as client:
        responses = [
            await client.get("/health"),
            await client.get("/internal/v1/health", headers=AUTH),
            await client.get("/internal/v1/news/changes", headers=AUTH),
            await client.get("/internal/v1/news/1", headers=AUTH),
            await client.get("/internal/v1/calendar", headers=AUTH),
        ]
    for response in responses:
        assert "test-internal-token" not in response.text


@pytest.mark.asyncio
async def test_news_changes_normalizes_legacy_naive_utc_timestamps(clean_db):
    db = await database.get_db()
    try:
        news_ids = [
            await database.insert_news_item(db, _item(1)),
            await database.insert_news_item(db, _item(2)),
        ]
        assert all(news_id is not None for news_id in news_ids)
        await db.executemany(
            """UPDATE news_changes
               SET published_at=?,fetched_at=?,updated_at=?
               WHERE news_id=?""",
            [
                (
                    f"2026-07-15T00:0{index}:00",
                    f"2026-07-15T00:1{index}:00",
                    f"2026-07-15T00:1{index}:00",
                    news_id,
                )
                for index, news_id in enumerate(news_ids, start=1)
            ],
        )
        await db.commit()
        async with db.execute(
            """SELECT payload_hash,content_hash FROM news_changes
               ORDER BY change_sequence"""
        ) as cursor:
            hashes_before = [tuple(row) for row in await cursor.fetchall()]
    finally:
        await db.close()

    async with await _client() as client:
        first_response = await client.get(
            "/internal/v1/news/changes", headers=AUTH, params={"after_sequence": 0, "limit": 1}
        )
        first_page = first_response.json()
        second_response = await client.get(
            "/internal/v1/news/changes",
            headers=AUTH,
            params={"cursor": first_page["next_cursor"], "limit": 1},
        )

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    second_page = second_response.json()
    assert first_page["has_more"] is True
    assert first_page["next_cursor"]
    assert first_page["next_updated_after"] is None
    assert first_page["next_after_sequence"] is None
    assert second_page["has_more"] is False
    assert second_page["next_cursor"] is None
    assert second_page["next_updated_after"] == second_page["watermark"]["as_of"]
    assert second_page["next_after_sequence"] == second_page["watermark"]["sequence"]
    assert first_page["watermark"] == second_page["watermark"]
    changes = first_page["items"] + second_page["items"]
    assert len(changes) == 2
    for index, change in enumerate(changes, start=1):
        assert change["changed_at"].endswith("Z")
        assert change["available_at"] == change["changed_at"]
        assert change["source_updated_at"] == f"2026-07-15T00:1{index}:00Z"
        assert change["news"]["published_at"] == f"2026-07-15T00:0{index}:00Z"
        assert change["news"]["fetched_at"] == f"2026-07-15T00:1{index}:00Z"
        assert change["news"]["updated_at"] == f"2026-07-15T00:1{index}:00Z"

    db = await database.get_db()
    try:
        async with db.execute(
            """SELECT published_at,fetched_at,updated_at,payload_hash,content_hash
               FROM news_changes ORDER BY change_sequence""",
        ) as cursor:
            stored_rows = [tuple(row) for row in await cursor.fetchall()]
    finally:
        await db.close()
    assert [row[:3] for row in stored_rows] == [
        (
            f"2026-07-15T00:0{index}:00",
            f"2026-07-15T00:1{index}:00",
            f"2026-07-15T00:1{index}:00",
        )
        for index in (1, 2)
    ]
    assert [row[3:] for row in stored_rows] == hashes_before


@pytest.mark.asyncio
async def test_news_pages_keep_frozen_watermark_and_next_round_gets_new_item(clean_db):
    db = await database.get_db()
    try:
        await database.insert_news_items_batch(db, [_item(1), _item(2), _item(3)])
    finally:
        await db.close()

    async with await _client() as client:
        first = (
            await client.get(
                "/internal/v1/news/changes",
                headers=AUTH,
                params={"limit": 1, "after_sequence": 0},
            )
        ).json()
        frozen = first["watermark"]["sequence"]

        db = await database.get_db()
        try:
            await database.insert_news_item(db, _item(4))
        finally:
            await db.close()

        second_response = await client.get(
            "/internal/v1/news/changes",
            headers=AUTH,
            params={"cursor": first["next_cursor"], "limit": 10},
        )
        assert second_response.status_code == 200
        second = second_response.json()
        current_titles = [first["items"][0]["news"]["title"]] + [
            item["news"]["title"] for item in second["items"] if item["news"]
        ]
        assert "News item 4" not in current_titles
        assert second["watermark"]["sequence"] == frozen
        assert second["has_more"] is False

        next_round = (
            await client.get(
                "/internal/v1/news/changes",
                headers=AUTH,
                params={
                    "after_sequence": second["next_after_sequence"],
                    "updated_after": second["next_updated_after"],
                },
            )
        ).json()
        assert [item["news"]["title"] for item in next_round["items"]] == ["News item 4"]
        empty_round = (
            await client.get(
                "/internal/v1/news/changes",
                headers=AUTH,
                params={
                    "after_sequence": next_round["next_after_sequence"],
                    "updated_after": next_round["next_updated_after"],
                },
            )
        ).json()
        assert empty_round["items"] == []
        assert empty_round["watermark"]["sequence"] == next_round["watermark"]["sequence"]


@pytest.mark.asyncio
async def test_news_cursor_rejects_damage_and_changed_filters(clean_db):
    db = await database.get_db()
    try:
        await database.insert_news_items_batch(db, [_item(1), _item(2)])
    finally:
        await db.close()
    async with await _client() as client:
        damaged = await client.get(
            "/internal/v1/news/changes", headers=AUTH, params={"cursor": "not-base64!"}
        )
        first = (
            await client.get(
                "/internal/v1/news/changes",
                headers=AUTH,
                params={"limit": 1, "after_sequence": 0},
            )
        ).json()
        changed = await client.get(
            "/internal/v1/news/changes",
            headers=AUTH,
            params={
                "cursor": first["next_cursor"],
                "updated_after": "2026-07-15T00:00:00Z",
            },
        )
        changed_sequence = await client.get(
            "/internal/v1/news/changes",
            headers=AUTH,
            params={"cursor": first["next_cursor"], "after_sequence": 1},
        )
        repeated_sequence = await client.get(
            "/internal/v1/news/changes",
            headers=AUTH,
            params={"cursor": first["next_cursor"], "after_sequence": 0},
        )
    assert damaged.status_code == 400
    assert changed.status_code == 400
    assert changed_sequence.status_code == 400
    assert repeated_sequence.status_code == 400


@pytest.mark.asyncio
async def test_news_sequence_checkpoint_closes_uncommitted_write_gap(clean_db):
    writer = await database.get_db()
    try:
        prepared = database._prepare_news(_item(1))
        news_id = await database._insert_prepared_news(writer, prepared)
        async with await _client() as client:
            before_commit = (
                await client.get(
                    "/internal/v1/news/changes",
                    headers=AUTH,
                    params={"after_sequence": 0},
                )
            ).json()
            assert before_commit["items"] == []
            assert before_commit["next_after_sequence"] == 0

            await writer.commit()
            after_commit = (
                await client.get(
                    "/internal/v1/news/changes",
                    headers=AUTH,
                    params={
                        "after_sequence": before_commit["next_after_sequence"],
                        "updated_after": before_commit["next_updated_after"],
                    },
                )
            ).json()
    finally:
        await writer.rollback()
        await writer.close()

    assert [change["news_id"] for change in after_commit["items"]] == [news_id]
    assert after_commit["next_after_sequence"] == after_commit["watermark"]["sequence"]


@pytest.mark.asyncio
async def test_news_change_replay_keeps_sources_frozen(clean_db, monkeypatch):
    from app.routers import internal

    clock = {"now": datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)}
    monkeypatch.setattr(database, "utc_now", lambda: clock["now"])
    monkeypatch.setattr(internal, "utc_now", lambda: clock["now"])
    first_item = _item(1)
    first_item.update(
        source="finnhub",
        title="Chip demand accelerates",
        url="https://finnhub.example/chip-demand",
        source_tickers=["AMD"],
    )
    db = await database.get_db()
    try:
        await database.insert_news_item(db, first_item)
    finally:
        await db.close()

    async with await _client() as client:
        original = (
            await client.get(
                "/internal/v1/news/changes",
                headers=AUTH,
                params={"after_sequence": 0},
            )
        ).json()

        clock["now"] += timedelta(seconds=1)
        second_item = dict(first_item)
        second_item.update(
            source="massive",
            url="https://massive.example/chip-demand-report",
        )
        db = await database.get_db()
        try:
            await database.insert_news_item(db, second_item)
        finally:
            await db.close()

        replay = (
            await client.get(
                "/internal/v1/news/changes",
                headers=AUTH,
                params={"after_sequence": 0},
            )
        ).json()
        historical_detail = (
            await client.get(
                "/internal/v1/news/1",
                headers=AUTH,
                params={"as_of": original["items"][0]["available_at"]},
            )
        ).json()
        current_detail = (
            await client.get("/internal/v1/news/1", headers=AUTH)
        ).json()

    assert original["items"][0] == replay["items"][0]
    assert replay["items"][0]["news"]["sources"] == ["finnhub"]
    assert replay["items"][1]["news"]["sources"] == ["finnhub", "massive"]
    assert historical_detail["item"]["sources"] == ["finnhub"]
    assert current_detail["item"]["sources"] == ["finnhub", "massive"]


@pytest.mark.asyncio
async def test_news_detail_honors_as_of(clean_db):
    before = database.utc_now() - timedelta(seconds=1)
    db = await database.get_db()
    try:
        news_id = await database.insert_news_item(db, _item(1))
    finally:
        await db.close()
    async with await _client() as client:
        historical = await client.get(
            f"/internal/v1/news/{news_id}",
            headers=AUTH,
            params={"as_of": database.utc_text(before)},
        )
        current = await client.get(f"/internal/v1/news/{news_id}", headers=AUTH)
    assert historical.status_code == 404
    assert current.status_code == 200
    assert current.json()["item"]["source_tickers"] == ["T1"]


@pytest.mark.asyncio
async def test_cross_source_duplicate_surfaces_corroborating_sources(clean_db):
    first = _item(1)
    first.update(
        source="finnhub",
        title="Chip demand accelerates",
        url="https://finnhub.example/chip-demand",
        source_tickers=["AMD"],
    )
    second = dict(first)
    second.update(
        source="massive",
        url="https://massive.example/chip-demand-report",
    )
    db = await database.get_db()
    try:
        result = await database.insert_news_items_batch(db, [first, second])
        async with db.execute("SELECT id FROM news_items") as cursor:
            news_id = int((await cursor.fetchone())[0])
    finally:
        await db.close()

    async with await _client() as client:
        changes_response = await client.get(
            "/internal/v1/news/changes", headers=AUTH
        )
        detail_response = await client.get(
            f"/internal/v1/news/{news_id}", headers=AUTH
        )

    assert result == {"inserted": 1, "duplicates": 1}
    assert changes_response.status_code == 200
    assert len(changes_response.json()["items"]) == 2
    latest = changes_response.json()["items"][-1]["news"]
    assert latest["sources"] == ["finnhub", "massive"]
    assert latest["source_count"] == 2
    assert detail_response.status_code == 200
    assert detail_response.json()["item"]["sources"] == ["finnhub", "massive"]
    assert detail_response.json()["item"]["source_count"] == 2


@pytest.mark.asyncio
async def test_news_default_page_stays_below_five_megabytes(clean_db):
    items = [
        {
            "source": f"source-{number}",
            "title": f"{number} " + ("x" * 3_998),
            "summary": "s" * 50_000,
            "url": f"https://example.com/{number}/" + ("u" * 7_900),
            "image_url": None,
            "published_at": "2026-07-15T00:00:00Z",
            "fetched_at": "2026-07-15T00:01:00Z",
            "source_tickers": [f"T{number}"],
        }
        for number in range(51)
    ]
    db = await database.get_db()
    try:
        result = await database.insert_news_items_batch(db, items)
    finally:
        await db.close()

    async with await _client() as client:
        default_page = await client.get("/internal/v1/news/changes", headers=AUTH)

    assert result == {"inserted": 51, "duplicates": 0}
    assert default_page.status_code == 200
    assert len(default_page.json()["items"]) == 50
    assert default_page.json()["has_more"] is True
    assert len(default_page.content) < 5 * 1024 * 1024


@pytest.mark.asyncio
async def test_news_explicit_batch_pages_at_five_hundred(clean_db):
    items = [
        {
            "source": f"source-{number}",
            "title": f"Headline {number}",
            "summary": "Summary",
            "url": f"https://example.com/{number}",
            "image_url": None,
            "published_at": "2026-07-15T00:00:00Z",
            "fetched_at": "2026-07-15T00:01:00Z",
            "source_tickers": [],
        }
        for number in range(501)
    ]
    db = await database.get_db()
    try:
        result = await database.insert_news_items_batch(db, items)
    finally:
        await db.close()

    async with await _client() as client:
        first_page = await client.get(
            "/internal/v1/news/changes", headers=AUTH, params={"limit": 500}
        )
        first_payload = first_page.json()
        second_page = await client.get(
            "/internal/v1/news/changes",
            headers=AUTH,
            params={"cursor": first_payload["next_cursor"], "limit": 500},
        )
        too_large = await client.get(
            "/internal/v1/news/changes", headers=AUTH, params={"limit": 501}
        )

    assert result == {"inserted": 501, "duplicates": 0}
    assert first_page.status_code == 200
    assert len(first_payload["items"]) == 500
    assert first_payload["has_more"] is True
    assert first_payload["next_cursor"]
    assert len(first_page.content) < 5 * 1024 * 1024
    assert second_page.status_code == 200
    assert len(second_page.json()["items"]) == 1
    assert second_page.json()["has_more"] is False
    assert second_page.json()["next_cursor"] is None
    assert too_large.status_code == 422


@pytest.mark.asyncio
async def test_calendar_reads_database_only_and_freezes_snapshot(clean_db, monkeypatch):
    from app.services import calendar_client

    async def forbidden_fetch(*args, **kwargs):
        raise AssertionError("calendar read endpoint attempted network access")

    monkeypatch.setattr(calendar_client, "fetch_economic_calendar", forbidden_fetch)
    events = [
        {
            "date": f"2026-07-16T0{hour}:00:00-04:00",
            "title": f"Event {hour}",
            "country_code": "USD",
            "country": "美国",
            "impact": "high",
            "impact_zh": "高",
            "forecast": "1",
            "previous": "0",
            "actual": "",
        }
        for hour in (1, 2, 3)
    ]
    db = await database.get_db()
    try:
        _, old_sequence = await database.record_calendar_snapshot(
            db,
            events,
            source_fetched_at="2026-07-15T01:00:00Z",
            stale=False,
            observed_at=database.utc_text(),
        )
    finally:
        await db.close()

    async with await _client() as client:
        first = (
            await client.get(
                "/internal/v1/calendar",
                headers=AUTH,
                params={"limit": 1, "after_sequence": 0},
            )
        ).json()
        db = await database.get_db()
        try:
            await database.record_calendar_snapshot(
                db,
                events[:1],
                source_fetched_at="2026-07-15T02:00:00Z",
                stale=False,
                observed_at=database.utc_text(),
            )
        finally:
            await db.close()
        second = (
            await client.get(
                "/internal/v1/calendar",
                headers=AUTH,
                params={"cursor": first["next_cursor"], "limit": 10},
            )
        ).json()
        newest = (await client.get("/internal/v1/calendar", headers=AUTH)).json()
        unchanged = (
            await client.get(
                "/internal/v1/calendar",
                headers=AUTH,
                params={
                    "after_sequence": newest["next_after_sequence"],
                    "updated_after": newest["next_updated_after"],
                },
            )
        ).json()
    assert first["watermark"]["sequence"] == old_sequence
    assert second["watermark"]["sequence"] == old_sequence
    assert [first["items"][0]["title"]] + [item["title"] for item in second["items"]] == [
        "Event 1",
        "Event 2",
        "Event 3",
    ]
    assert [item["title"] for item in newest["items"]] == ["Event 1"]
    assert unchanged["items"] == []
    assert unchanged["watermark"]["sequence"] == newest["watermark"]["sequence"]


@pytest.mark.asyncio
async def test_calendar_sequence_checkpoint_closes_uncommitted_snapshot_gap(clean_db):
    writer = await database.get_db()
    observed = database.utc_text()
    token = "cal_" + ("a" * 40)
    try:
        cursor = await writer.execute(
            """INSERT INTO etl_calendar_snapshots(
                   snapshot_token,source,source_fetched_at,data_through,is_stale,
                   available_at,available_at_us,payload_hash
               ) VALUES(?,?,?,?,?,?,?,?)""",
            (
                token,
                "faireconomy",
                observed,
                observed,
                0,
                observed,
                database.epoch_microseconds(observed),
                "snapshot-hash",
            ),
        )
        sequence = int(cursor.lastrowid)
        await writer.execute(
            """INSERT INTO etl_calendar_events(
                   snapshot_sequence,ordinal,event_id,country_code,country,title,impact,
                   impact_zh,scheduled_at,scheduled_at_utc,forecast,previous,actual,
                   is_stale,source_fetched_at,available_at,content_hash
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                sequence,
                1,
                "race-event",
                "USD",
                "美国",
                "Commit race event",
                "high",
                "高",
                "2026-07-16T08:30:00-04:00",
                "2026-07-16T12:30:00Z",
                None,
                None,
                None,
                0,
                observed,
                observed,
                "event-hash",
            ),
        )

        async with await _client() as client:
            before_commit = (
                await client.get(
                    "/internal/v1/calendar",
                    headers=AUTH,
                    params={"after_sequence": 0},
                )
            ).json()
            assert before_commit["items"] == []
            assert before_commit["next_after_sequence"] == 0

            await writer.commit()
            after_commit = (
                await client.get(
                    "/internal/v1/calendar",
                    headers=AUTH,
                    params={
                        "after_sequence": before_commit["next_after_sequence"],
                        "updated_after": before_commit["next_updated_after"],
                    },
                )
            ).json()
    finally:
        await writer.rollback()
        await writer.close()

    assert [item["event_id"] for item in after_commit["items"]] == ["race-event"]
    assert after_commit["watermark"]["sequence"] == sequence
    assert after_commit["next_after_sequence"] == sequence
