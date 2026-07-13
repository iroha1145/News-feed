from __future__ import annotations

import asyncio
import hashlib
import json
import time
import tracemalloc
from datetime import datetime, timedelta, timezone

import aiosqlite
import pytest

from app.integrations.option_pro.repository import query_feed, query_ticker
from app.models import catalyst_database, database
from app.models.catalyst_database import CATALYST_SCHEMA_MIGRATION, init_catalyst_schema
from app.services.ticker_lineage import (
    append_validation_revision,
    build_validation_basis_hash,
    record_ticker_mention,
    validation_as_of,
)


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def lineage_db(tmp_path, monkeypatch):
    path = tmp_path / "ticker-lineage.db"
    monkeypatch.setattr(database, "DB_PATH", str(path))
    run(database.init_db())
    return path


def _analysis_payload(ticker: str = "XYZ") -> str:
    return json.dumps(
        {
            "title_zh": "测试新闻",
            "headline_summary": "公开信息显示公司更新了业务展望。",
            "overall_sentiment": 30,
            "classification": "bullish",
            "confidence": 70,
            "market_relevance": 65,
            "affected_stocks": [
                {
                    "ticker": ticker,
                    "company": "Example Corp",
                    "impact_score": 35,
                    "confidence": 70,
                    "horizon": "days",
                    "mechanism": "direct_company",
                    "reason": "业务展望直接影响近期预期。",
                }
            ],
            "affected_sectors": [],
            "affected_commodities": [],
            "causal_summary": "业务展望变化影响市场对近期收入的预期。",
            "key_factors": ["业务展望"],
            "uncertainty_notes": ["仍需后续正式披露确认。"],
            "insufficient_context": False,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


async def _insert_analysis(db, *, ticker: str = "XYZ") -> tuple[int, int]:
    fetched_at = "2026-07-01T09:00:00+00:00"
    news_id = await database.insert_news_item(
        db,
        {
            "source": "test/source",
            "title": "Example company updates its outlook",
            "summary": "A sufficiently detailed public summary.",
            "url": "https://example.test/ticker-lineage",
            "image_url": None,
            "published_at": fetched_at,
            "fetched_at": fetched_at,
            "content_hash": hashlib.sha256(b"ticker-lineage-news").hexdigest(),
            "source_tickers": [],
        },
    )
    cursor = await db.execute(
        """INSERT INTO analysis_revisions
           (news_id,job_id,revision,input_hash,payload_json,provider,model,reasoning_effort,
            prompt_version,schema_version,fetched_at,analyzed_at,available_at,created_at)
           VALUES (?,NULL,1,?,?,'test','fixture-model','none','fixture-v1','fixture-v1',?,?,?,?)""",
        (
            news_id,
            "a" * 64,
            _analysis_payload(ticker),
            fetched_at,
            "2026-07-01T09:01:00+00:00",
            "2026-07-01T09:01:00+00:00",
            "2026-07-01T09:01:00+00:00",
        ),
    )
    return news_id, int(cursor.lastrowid)


async def _restore_real_v3_lineage_shape(db: aiosqlite.Connection) -> None:
    """Rebuild the two changed tables with the exact pre-v4 column shape."""

    await db.commit()
    await db.execute("PRAGMA foreign_keys=OFF")
    await db.execute(
        """CREATE TABLE news_ticker_mentions_v3 (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             news_id INTEGER NOT NULL REFERENCES news_items(id) ON DELETE CASCADE,
             ticker TEXT NOT NULL CHECK(length(ticker) BETWEEN 1 AND 20),
             association_method TEXT NOT NULL CHECK(association_method IN (
               'provider_tag','company_endpoint','exact_alias','event_propagation','llm_inference'
             )),
             association_confidence REAL NOT NULL CHECK(association_confidence BETWEEN 0 AND 1),
             validation_status TEXT NOT NULL CHECK(validation_status IN (
               'canonical','valid_external','ambiguous','invalid','unverified'
             )),
             validated_at TEXT,
             focus_revision INTEGER,
             universe_version TEXT,
             source TEXT NOT NULL,
             created_at TEXT NOT NULL
           )"""
    )
    await db.execute(
        """INSERT INTO news_ticker_mentions_v3
           (id,news_id,ticker,association_method,association_confidence,validation_status,
            validated_at,focus_revision,universe_version,source,created_at)
           SELECT id,news_id,ticker,association_method,association_confidence,validation_status,
                  validated_at,focus_revision,universe_version,source,created_at
           FROM news_ticker_mentions"""
    )
    await db.execute(
        """CREATE TABLE analysis_stock_impacts_v3 (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             analysis_id INTEGER NOT NULL REFERENCES analysis_revisions(id) ON DELETE CASCADE,
             news_id INTEGER NOT NULL REFERENCES news_items(id) ON DELETE CASCADE,
             ticker TEXT NOT NULL CHECK(length(ticker) BETWEEN 1 AND 20),
             company TEXT NOT NULL CHECK(length(company) BETWEEN 1 AND 200),
             impact_score INTEGER NOT NULL CHECK(impact_score BETWEEN -100 AND 100),
             confidence INTEGER NOT NULL CHECK(confidence BETWEEN 0 AND 100),
             horizon TEXT NOT NULL CHECK(horizon IN ('intraday','days','weeks','uncertain')),
             mechanism TEXT NOT NULL CHECK(mechanism IN (
               'direct_company','supplier_customer','sector_readthrough','macro_rate',
               'commodity_input','regulatory','competitive','other'
             )),
             reason TEXT NOT NULL CHECK(length(reason) BETWEEN 1 AND 2000),
             source TEXT NOT NULL,
             content_hash TEXT NOT NULL,
             published_at TEXT,
             fetched_at TEXT NOT NULL,
             analyzed_at TEXT NOT NULL,
             available_at TEXT NOT NULL,
             model TEXT NOT NULL,
             reasoning_effort TEXT NOT NULL,
             prompt_version TEXT NOT NULL,
             schema_version TEXT NOT NULL,
             validation_status TEXT NOT NULL DEFAULT 'unverified' CHECK(validation_status IN (
               'canonical','valid_external','ambiguous','invalid','unverified'
             )),
             validated_at TEXT,
             focus_revision INTEGER,
             universe_version TEXT,
             association_method TEXT NOT NULL DEFAULT 'llm_inference'
               CHECK(association_method='llm_inference'),
             UNIQUE(analysis_id,ticker)
           )"""
    )
    await db.execute(
        """INSERT INTO analysis_stock_impacts_v3
           (id,analysis_id,news_id,ticker,company,impact_score,confidence,horizon,mechanism,
            reason,source,content_hash,published_at,fetched_at,analyzed_at,available_at,
            model,reasoning_effort,prompt_version,schema_version,validation_status,validated_at,
            focus_revision,universe_version,association_method)
           SELECT id,analysis_id,news_id,ticker,company,impact_score,confidence,horizon,mechanism,
                  reason,source,content_hash,published_at,fetched_at,analyzed_at,available_at,
                  model,reasoning_effort,prompt_version,schema_version,validation_status,validated_at,
                  focus_revision,universe_version,association_method
           FROM analysis_stock_impacts"""
    )
    await db.execute("DROP TABLE ticker_validation_revisions")
    await db.execute("DROP TABLE focus_validation_state")
    await db.execute("DROP TABLE analysis_stock_impacts")
    await db.execute("DROP TABLE news_ticker_mentions")
    await db.execute("ALTER TABLE news_ticker_mentions_v3 RENAME TO news_ticker_mentions")
    await db.execute("ALTER TABLE analysis_stock_impacts_v3 RENAME TO analysis_stock_impacts")
    await db.execute("PRAGMA user_version=3")
    await db.commit()
    await db.execute("PRAGMA foreign_keys=ON")


def test_schema_v4_contains_lineage_and_focus_validation_state(lineage_db):
    async def scenario():
        db = await database.get_db()
        try:
            async with db.execute("PRAGMA user_version") as cursor:
                assert int((await cursor.fetchone())[0]) == CATALYST_SCHEMA_MIGRATION == 4
            async with db.execute("PRAGMA table_info(news_ticker_mentions)") as cursor:
                mention_columns = {str(row[1]) for row in await cursor.fetchall()}
            assert {
                "analysis_revision_id",
                "last_checked_at",
                "current_validation_status",
                "current_validation_revision_id",
            } <= mention_columns
            async with db.execute("SELECT * FROM focus_validation_state") as cursor:
                state = dict(await cursor.fetchone())
            assert state["singleton_id"] == 1
            assert state["rows_scanned"] == state["rows_changed"] == 0
            async with db.execute("PRAGMA foreign_key_check") as cursor:
                assert await cursor.fetchall() == []
        finally:
            await db.close()

    run(scenario())


def test_existing_v4_recreates_validation_change_trigger_with_observed_time(
    lineage_db,
):
    async def scenario():
        db = await database.get_db()
        try:
            await db.execute(
                "DROP TRIGGER IF EXISTS trg_ticker_validation_integration_insert"
            )
            await db.execute(
                """CREATE TRIGGER trg_ticker_validation_integration_insert
                   AFTER INSERT ON ticker_validation_revisions
                   BEGIN
                     INSERT INTO integration_changes(
                       entity_type,entity_id,operation,payload_hash,updated_at
                     )
                     SELECT 'analysis',CAST(m.news_id AS TEXT),'upsert',
                            NEW.validation_basis_hash,NEW.available_at
                     FROM news_ticker_mentions m WHERE m.id=NEW.mention_id;
                   END"""
            )
            await db.commit()
            await init_catalyst_schema(db)
            trigger = await (await db.execute(
                """SELECT sql FROM sqlite_master
                   WHERE type='trigger'
                     AND name='trg_ticker_validation_integration_insert'"""
            )).fetchone()
            assert trigger is not None
            assert "NEW.created_at" in str(trigger[0])
            assert "NEW.available_at" not in str(trigger[0])
        finally:
            await db.close()

    run(scenario())


def test_validation_as_of_probe_uses_mention_time_index(lineage_db):
    async def scenario():
        db = await database.get_db()
        try:
            async with db.execute(
                """EXPLAIN QUERY PLAN
                   SELECT latest.id FROM ticker_validation_revisions latest
                   WHERE latest.mention_id=? AND latest.available_at<=?
                   ORDER BY latest.available_at DESC,latest.id DESC LIMIT 1""",
                (1, "2026-07-01T10:00:00.000000+00:00"),
            ) as cursor:
                plan = " ".join(str(row[3]) for row in await cursor.fetchall())
            assert "idx_ticker_validation_as_of" in plan
        finally:
            await db.close()

    run(scenario())


def test_mention_identity_is_idempotent_and_analysis_revisions_are_separate(lineage_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id, analysis_id = await _insert_analysis(db)
            basis = build_validation_basis_hash(
                canonical_symbols=set(),
                external_symbols=set(),
                universe_version="universe-1",
            )
            first_id = None
            for index in range(100):
                mention = await record_ticker_mention(
                    db,
                    news_id=news_id,
                    ticker="XYZ",
                    association_method="llm_inference",
                    association_confidence=0.5,
                    source="model_output",
                    validation_status="unverified",
                    available_at=datetime(2026, 7, 1, 9, 2, tzinfo=timezone.utc)
                    + timedelta(seconds=index),
                    focus_revision=1,
                    universe_version="universe-1",
                    validation_basis_hash=basis,
                    analysis_revision_id=analysis_id,
                )
                first_id = first_id or mention["mention_id"]
                assert mention["mention_id"] == first_id
            async with db.execute(
                "SELECT COUNT(*) FROM news_ticker_mentions WHERE news_id=?",
                (news_id,),
            ) as cursor:
                assert int((await cursor.fetchone())[0]) == 1
            async with db.execute(
                "SELECT COUNT(*) FROM ticker_validation_revisions WHERE mention_id=?",
                (first_id,),
            ) as cursor:
                assert int((await cursor.fetchone())[0]) == 1

            second = await db.execute(
                """INSERT INTO analysis_revisions
                   (news_id,job_id,revision,input_hash,payload_json,provider,model,reasoning_effort,
                    prompt_version,schema_version,fetched_at,analyzed_at,available_at,created_at)
                   SELECT news_id,NULL,2,?,payload_json,provider,model,reasoning_effort,
                          prompt_version,schema_version,fetched_at,?,?,?
                   FROM analysis_revisions WHERE id=?""",
                (
                    "b" * 64,
                    "2026-07-01T10:00:00+00:00",
                    "2026-07-01T10:00:00+00:00",
                    "2026-07-01T10:00:00+00:00",
                    analysis_id,
                ),
            )
            second_mention = await record_ticker_mention(
                db,
                news_id=news_id,
                ticker="XYZ",
                association_method="llm_inference",
                association_confidence=0.5,
                source="model_output",
                validation_status="unverified",
                available_at="2026-07-01T10:00:00+00:00",
                focus_revision=1,
                universe_version="universe-1",
                validation_basis_hash=basis,
                analysis_revision_id=int(second.lastrowid),
            )
            assert second_mention["mention_id"] != first_id
            await db.commit()
        finally:
            await db.close()

    run(scenario())


def test_late_repaired_mention_does_not_appear_in_earlier_stock_validations(lineage_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id, analysis_id = await _insert_analysis(db)
            basis = build_validation_basis_hash(
                canonical_symbols=set(), external_symbols=set(), universe_version="u1"
            )
            await record_ticker_mention(
                db,
                news_id=news_id,
                ticker="XYZ",
                association_method="llm_inference",
                association_confidence=0.5,
                source="late_migration_repair",
                validation_status="unverified",
                available_at="2026-07-01T10:00:00+00:00",
                focus_revision=1,
                universe_version="u1",
                validation_basis_hash=basis,
                analysis_revision_id=analysis_id,
            )
            await db.commit()

            async def validations_at(cutoff: datetime):
                items, *_ = await query_feed(
                    db,
                    as_of=cutoff,
                    window_hours=24,
                    limit=20,
                    cursor=None,
                    source=None,
                    classification=None,
                    min_confidence=0,
                    min_abs_impact=0,
                    analysis_status=None,
                )
                return items[0].analysis.stock_validations

            assert await validations_at(
                datetime(2026, 7, 1, 9, 30, tzinfo=timezone.utc)
            ) == []
            current = await validations_at(
                datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc)
            )
            assert [(row.ticker, row.validation_status) for row in current] == [
                ("XYZ", "unverified")
            ]
        finally:
            await db.close()

    run(scenario())


def test_late_source_mention_cannot_be_projected_into_an_older_ticker_query(lineage_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id, _analysis_id = await _insert_analysis(db)
            t0 = datetime(2026, 7, 1, 9, 30, tzinfo=timezone.utc)
            t1 = datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc)
            initial_basis = build_validation_basis_hash(
                canonical_symbols=set(), external_symbols=set(), universe_version="future"
            )
            mention = await record_ticker_mention(
                db,
                news_id=news_id,
                ticker="XYZ",
                association_method="provider_tag",
                association_confidence=0.95,
                source="delayed/provider",
                validation_status="unverified",
                available_at=t1,
                focus_revision=2,
                universe_version="future",
                validation_basis_hash=initial_basis,
            )
            older_basis = build_validation_basis_hash(
                canonical_symbols={"XYZ"}, external_symbols=set(), universe_version="older"
            )
            await append_validation_revision(
                db,
                mention_id=int(mention["mention_id"]),
                validation_status="canonical",
                available_at=t0,
                focus_revision=1,
                universe_version="older",
                reason_code="delayed_old_focus_revalidation",
                validation_basis_hash=older_basis,
            )
            await db.commit()

            items, *_ = await query_ticker(
                db,
                ticker="XYZ",
                as_of=t0,
                window_hours=24,
                limit=20,
                cursor=None,
                min_confidence=0,
                include_neutral=True,
                include_unanalyzed=True,
            )
            assert items == []
        finally:
            await db.close()

    run(scenario())


def test_as_of_validation_controls_ticker_visibility_and_public_analysis(lineage_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id, analysis_id = await _insert_analysis(db)
            t0 = datetime(2026, 7, 1, 9, 2, tzinfo=timezone.utc)
            basis0 = build_validation_basis_hash(
                canonical_symbols=set(), external_symbols=set(), universe_version="u0"
            )
            mention = await record_ticker_mention(
                db,
                news_id=news_id,
                ticker="XYZ",
                association_method="llm_inference",
                association_confidence=0.5,
                source="model_output",
                validation_status="unverified",
                available_at=t0,
                focus_revision=1,
                universe_version="u0",
                validation_basis_hash=basis0,
                analysis_revision_id=analysis_id,
            )
            await db.execute(
                """INSERT INTO analysis_stock_impacts
                   (analysis_id,analysis_revision_id,mention_id,news_id,ticker,company,
                    impact_score,confidence,horizon,mechanism,reason,source,content_hash,
                    published_at,fetched_at,analyzed_at,available_at,model,reasoning_effort,
                    prompt_version,schema_version,validation_status,validated_at,focus_revision,
                    universe_version,association_method)
                   VALUES (?,?,?,?,?,'Example Corp',35,70,'days','direct_company',
                           'Public reason','test/source',?,NULL,?,?,?,'fixture-model','none',
                           'fixture-v1','fixture-v1','unverified',?,1,'u0','llm_inference')""",
                (
                    analysis_id,
                    analysis_id,
                    mention["mention_id"],
                    news_id,
                    "XYZ",
                    "c" * 64,
                    "2026-07-01T09:00:00+00:00",
                    "2026-07-01T09:01:00+00:00",
                    "2026-07-01T09:01:00+00:00",
                    t0.isoformat(),
                ),
            )
            t1 = datetime(2026, 7, 1, 10, 0, 0, 900_000, tzinfo=timezone.utc)
            basis1 = build_validation_basis_hash(
                canonical_symbols={"XYZ"}, external_symbols=set(), universe_version="u1"
            )
            await append_validation_revision(
                db,
                mention_id=int(mention["mention_id"]),
                validation_status="canonical",
                available_at=t1,
                focus_revision=2,
                universe_version="u1",
                reason_code="focus_entered",
                validation_basis_hash=basis1,
            )
            t2 = datetime(2026, 7, 1, 11, 0, tzinfo=timezone.utc)
            basis2 = build_validation_basis_hash(
                canonical_symbols=set(), external_symbols=set(), universe_version="u2"
            )
            await append_validation_revision(
                db,
                mention_id=int(mention["mention_id"]),
                validation_status="unverified",
                available_at=t2,
                focus_revision=3,
                universe_version="u2",
                reason_code="focus_left",
                validation_basis_hash=basis2,
            )
            await db.commit()

            async def ticker_items(cutoff: datetime):
                items, *_ = await query_ticker(
                    db,
                    ticker="XYZ",
                    as_of=cutoff,
                    window_hours=24,
                    limit=20,
                    cursor=None,
                    min_confidence=0,
                    include_neutral=True,
                    include_unanalyzed=True,
                )
                return items

            assert await ticker_items(t0 + timedelta(minutes=1)) == []
            exact_feed, *_ = await query_feed(
                db,
                as_of=t0,
                window_hours=24,
                limit=20,
                cursor=None,
                source=None,
                classification=None,
                min_confidence=0,
                min_abs_impact=0,
                analysis_status=None,
            )
            assert exact_feed[0].analysis.stock_validations[0].validated_at == t0
            assert await ticker_items(
                datetime(2026, 7, 1, 10, 0, 0, 100_000, tzinfo=timezone.utc)
            ) == []
            visible = await ticker_items(t1 + timedelta(minutes=1))
            assert len(visible) == 1
            assert visible[0].analysis.stock_validations[0].validation_status == "canonical"
            assert await ticker_items(t2 + timedelta(minutes=1)) == []
            historical = await ticker_items(t1 + timedelta(minutes=1))
            assert len(historical) == 1

            old_feed, *_ = await query_feed(
                db,
                as_of=t0 + timedelta(minutes=1),
                window_hours=24,
                limit=20,
                cursor=None,
                source=None,
                classification=None,
                min_confidence=0,
                min_abs_impact=0,
                analysis_status=None,
            )
            assert old_feed[0].analysis.stock_validations[0].validation_status == "unverified"
            assert (
                await validation_as_of(
                    db, mention_id=int(mention["mention_id"]), as_of=t1 + timedelta(minutes=1)
                )
            )["validation_status"] == "canonical"
        finally:
            await db.close()

    run(scenario())


def test_validation_basis_can_recur_and_fractional_seconds_do_not_look_ahead(lineage_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id, analysis_id = await _insert_analysis(db)
            basis_a = build_validation_basis_hash(
                canonical_symbols={"XYZ"}, external_symbols=set(), universe_version="a"
            )
            basis_b = build_validation_basis_hash(
                canonical_symbols=set(), external_symbols=set(), universe_version="b"
            )
            mention = await record_ticker_mention(
                db,
                news_id=news_id,
                ticker="XYZ",
                association_method="llm_inference",
                association_confidence=0.5,
                source="model_output",
                validation_status="canonical",
                available_at="2026-07-01T10:00:00.100000+00:00",
                focus_revision=1,
                universe_version="a",
                validation_basis_hash=basis_a,
                analysis_revision_id=analysis_id,
            )
            await append_validation_revision(
                db,
                mention_id=mention["mention_id"],
                validation_status="unverified",
                available_at="2026-07-01T10:00:00.500000+00:00",
                focus_revision=2,
                universe_version="b",
                reason_code="focus_left",
                validation_basis_hash=basis_b,
            )
            state, created = await append_validation_revision(
                db,
                mention_id=mention["mention_id"],
                validation_status="canonical",
                available_at="2026-07-01T10:00:00.900000+00:00",
                focus_revision=3,
                universe_version="a",
                reason_code="focus_reentered",
                validation_basis_hash=basis_a,
            )
            assert created is True
            assert state["validation_status"] == "canonical"
            before_reentry = await validation_as_of(
                db,
                mention_id=mention["mention_id"],
                as_of="2026-07-01T10:00:00.800000+00:00",
            )
            assert before_reentry["validation_status"] == "unverified"
            after_reentry = await validation_as_of(
                db,
                mention_id=mention["mention_id"],
                as_of="2026-07-01T10:00:00.950000+00:00",
            )
            assert after_reentry["validation_status"] == "canonical"
            with pytest.raises(ValueError, match="validation_basis_status_conflict"):
                await append_validation_revision(
                    db,
                    mention_id=mention["mention_id"],
                    validation_status="ambiguous",
                    available_at="2026-07-01T10:00:01.000000+00:00",
                    focus_revision=4,
                    universe_version="a",
                    reason_code="impossible_same_basis_conflict",
                    validation_basis_hash=basis_a,
                )
            async with db.execute(
                "SELECT COUNT(*) FROM ticker_validation_revisions WHERE mention_id=?",
                (mention["mention_id"],),
            ) as cursor:
                assert int((await cursor.fetchone())[0]) == 3
        finally:
            await db.close()

    run(scenario())


def test_historical_revision_is_not_swallowed_by_future_matching_status(lineage_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id, _ = await _insert_analysis(db)
            initial_at = datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc)
            historical_at = datetime(2026, 7, 1, 9, 0, tzinfo=timezone.utc)
            future_at = datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc)
            initial_basis = build_validation_basis_hash(
                canonical_symbols=set(),
                external_symbols=set(),
                universe_version="initial",
            )
            mention = await record_ticker_mention(
                db,
                news_id=news_id,
                ticker="XYZ",
                association_method="exact_alias",
                association_confidence=1.0,
                source="alias_dictionary",
                validation_status="unverified",
                available_at=initial_at,
                focus_revision=None,
                universe_version="initial",
                validation_basis_hash=initial_basis,
            )
            historical_basis = build_validation_basis_hash(
                canonical_symbols={"XYZ"},
                external_symbols=set(),
                universe_version="historical-focus",
            )
            future_basis = build_validation_basis_hash(
                canonical_symbols={"XYZ"},
                external_symbols=set(),
                universe_version="future-poll",
            )

            # A later poll lands first while the older Focus pass is queued.
            await append_validation_revision(
                db,
                mention_id=mention["mention_id"],
                validation_status="canonical",
                available_at=future_at,
                focus_revision=2,
                universe_version="future-poll",
                reason_code="future_poll",
                validation_basis_hash=future_basis,
            )
            _, historical_created = await append_validation_revision(
                db,
                mention_id=mention["mention_id"],
                validation_status="canonical",
                available_at=historical_at,
                focus_revision=1,
                universe_version="historical-focus",
                reason_code="queued_focus_pass",
                validation_basis_hash=historical_basis,
            )
            await db.commit()

            assert historical_created is True
            at_historical = await validation_as_of(
                db,
                mention_id=mention["mention_id"],
                as_of=historical_at + timedelta(minutes=1),
            )
            assert at_historical["validation_status"] == "canonical"
            assert at_historical["focus_revision"] == 1
            current = await (await db.execute(
                """SELECT current_validation_status,focus_revision
                   FROM news_ticker_mentions WHERE id=?""",
                (mention["mention_id"],),
            )).fetchone()
            assert tuple(current) == ("canonical", 2)
            counts = await (await db.execute(
                """SELECT
                     (SELECT COUNT(*) FROM news_ticker_mentions WHERE id=?),
                     (SELECT COUNT(*) FROM ticker_validation_revisions WHERE mention_id=?)""",
                (mention["mention_id"], mention["mention_id"]),
            )).fetchone()
            assert tuple(counts) == (1, 3)
        finally:
            await db.close()

    run(scenario())


def test_v3_to_v4_migration_deduplicates_and_backfills_conservatively(lineage_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id, analysis_id = await _insert_analysis(db, ticker="XYZ")
            second = await db.execute(
                """INSERT INTO analysis_revisions
                   (news_id,job_id,revision,input_hash,payload_json,provider,model,reasoning_effort,
                    prompt_version,schema_version,fetched_at,analyzed_at,available_at,created_at)
                   SELECT news_id,NULL,2,?,payload_json,provider,model,reasoning_effort,
                          prompt_version,schema_version,fetched_at,?,?,?
                   FROM analysis_revisions WHERE id=?""",
                (
                    "d" * 64,
                    "2026-07-01T10:00:00+00:00",
                    "2026-07-01T10:00:00+00:00",
                    "2026-07-01T10:00:00+00:00",
                    analysis_id,
                ),
            )
            second_analysis_id = int(second.lastrowid)
            await db.execute("DROP INDEX idx_ticker_mentions_natural_key")
            first = await db.execute(
                """INSERT INTO news_ticker_mentions
                   (news_id,ticker,association_method,association_confidence,validation_status,
                    validated_at,focus_revision,universe_version,source,created_at,
                    analysis_revision_id,last_checked_at,current_validation_status,
                    current_validation_revision_id,legacy_association)
                   VALUES (?,'AMD','provider_tag',0.7,'valid_external',?,1,'u1','provider',?,
                           NULL,?,'valid_external',NULL,0)""",
                (
                    news_id,
                    "2026-07-01T09:10:00+00:00",
                    "2026-07-01T09:09:00+00:00",
                    "2026-07-01T09:10:00+00:00",
                ),
            )
            first_provider_id = int(first.lastrowid)
            await db.execute(
                """INSERT INTO news_ticker_mentions
                   (news_id,ticker,association_method,association_confidence,validation_status,
                    validated_at,focus_revision,universe_version,source,created_at,
                    analysis_revision_id,last_checked_at,current_validation_status,
                    current_validation_revision_id,legacy_association)
                   VALUES (?,'AMD','provider_tag',0.95,'canonical',?,2,'u2','provider',?,
                           NULL,?,'canonical',NULL,0)""",
                (
                    news_id,
                    "2026-07-01T10:10:00+00:00",
                    "2026-07-01T10:09:00+00:00",
                    "2026-07-01T10:10:00+00:00",
                ),
            )
            await db.execute(
                """INSERT INTO news_ticker_mentions
                   (news_id,ticker,association_method,association_confidence,validation_status,
                    validated_at,focus_revision,universe_version,source,created_at,
                    analysis_revision_id,last_checked_at,current_validation_status,
                    current_validation_revision_id,legacy_association)
                   VALUES (?,'XYZ','llm_inference',0.5,'unverified',NULL,NULL,NULL,
                           'model_output',?,NULL,NULL,'unverified',NULL,0)""",
                (news_id, "2026-07-01T10:01:00+00:00"),
            )
            for revision_id, available_at in (
                (analysis_id, "2026-07-01T09:02:00+00:00"),
                (second_analysis_id, "2026-07-01T10:02:00+00:00"),
            ):
                await db.execute(
                    """INSERT INTO analysis_stock_impacts
                       (analysis_id,analysis_revision_id,mention_id,news_id,ticker,company,
                        impact_score,confidence,horizon,mechanism,reason,source,content_hash,
                        published_at,fetched_at,analyzed_at,available_at,model,reasoning_effort,
                        prompt_version,schema_version,validation_status,validated_at,focus_revision,
                        universe_version,association_method)
                       VALUES (?,NULL,NULL,?,'XYZ','Example Corp',25,60,'days','direct_company',
                               'Legacy reason','test/source',?,NULL,?,?,?,'legacy','none',
                               'legacy-v1','legacy-v1','unverified',NULL,NULL,NULL,'llm_inference')""",
                    (
                        revision_id,
                        news_id,
                        hashlib.sha256(f"impact-{revision_id}".encode()).hexdigest(),
                        "2026-07-01T09:00:00+00:00",
                        available_at,
                        available_at,
                    ),
                )
            await _restore_real_v3_lineage_shape(db)
            async with db.execute("PRAGMA table_info(news_ticker_mentions)") as cursor:
                pre_migration_columns = {str(row[1]) for row in await cursor.fetchall()}
            assert "current_validation_revision_id" not in pre_migration_columns
            async with db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ticker_validation_revisions'"
            ) as cursor:
                assert await cursor.fetchone() is None
            await init_catalyst_schema(db)

            async with db.execute("PRAGMA user_version") as cursor:
                assert int((await cursor.fetchone())[0]) == 4
            async with db.execute(
                """SELECT id,association_confidence,validation_status,validated_at,last_checked_at,
                          focus_revision,universe_version
                   FROM news_ticker_mentions
                   WHERE news_id=? AND ticker='AMD' AND source='provider'""",
                (news_id,),
            ) as cursor:
                provider = await cursor.fetchall()
            assert len(provider) == 1
            assert int(provider[0][0]) == first_provider_id
            assert float(provider[0][1]) == 0.95
            assert str(provider[0][2]) == "canonical"
            assert str(provider[0][3]) == "2026-07-01T10:10:00+00:00"
            assert str(provider[0][4]) == "2026-07-01T10:10:00+00:00"
            assert int(provider[0][5]) == 2
            assert str(provider[0][6]) == "u2"
            async with db.execute(
                """SELECT available_at,legacy_backfill,focus_revision,universe_version
                   FROM ticker_validation_revisions
                   WHERE mention_id=?""",
                (first_provider_id,),
            ) as cursor:
                validation = await cursor.fetchone()
            assert tuple(validation) == (
                "2026-07-01T10:10:00.000000+00:00",
                1,
                2,
                "u2",
            )
            async with db.execute(
                """SELECT COUNT(*) FROM news_ticker_mentions
                   WHERE news_id=? AND ticker='XYZ' AND association_method='llm_inference'
                     AND analysis_revision_id IS NOT NULL""",
                (news_id,),
            ) as cursor:
                assert int((await cursor.fetchone())[0]) == 2
            async with db.execute(
                """SELECT legacy_association FROM news_ticker_mentions
                   WHERE news_id=? AND ticker='XYZ' AND analysis_revision_id IS NULL""",
                (news_id,),
            ) as cursor:
                assert int((await cursor.fetchone())[0]) == 1
            async with db.execute(
                """SELECT v.available_at,m.created_at
                   FROM ticker_validation_revisions v
                   JOIN news_ticker_mentions m ON m.id=v.mention_id
                   WHERE m.news_id=? AND m.ticker='XYZ'
                     AND m.analysis_revision_id IS NULL""",
                (news_id,),
            ) as cursor:
                legacy_available_at, legacy_created_at = await cursor.fetchone()
            assert str(legacy_available_at) > str(legacy_created_at)
            async with db.execute(
                """SELECT COUNT(*) FROM analysis_stock_impacts
                   WHERE analysis_revision_id=analysis_id AND mention_id IS NOT NULL"""
            ) as cursor:
                assert int((await cursor.fetchone())[0]) == 2
            async with db.execute("PRAGMA foreign_key_check") as cursor:
                assert await cursor.fetchall() == []

            async with db.execute("SELECT COUNT(*) FROM ticker_validation_revisions") as cursor:
                validation_count = int((await cursor.fetchone())[0])
            await init_catalyst_schema(db)
            async with db.execute("SELECT COUNT(*) FROM ticker_validation_revisions") as cursor:
                assert int((await cursor.fetchone())[0]) == validation_count
            with pytest.raises(aiosqlite.IntegrityError):
                await db.execute(
                    """INSERT INTO news_ticker_mentions
                       (news_id,ticker,association_method,association_confidence,validation_status,
                        source,created_at,current_validation_status)
                       VALUES (?,'AMD','provider_tag',0.5,'unverified','provider',?, 'unverified')""",
                    (news_id, "2026-07-01T12:00:00+00:00"),
                )
            await db.rollback()
        finally:
            await db.close()

    run(scenario())


def test_v3_to_v4_migration_failure_rolls_back_schema_and_data(
    lineage_db, monkeypatch
):
    async def scenario():
        db = await database.get_db()
        try:
            news_id, _ = await _insert_analysis(db)
            await db.execute(
                """INSERT INTO news_ticker_mentions
                   (news_id,ticker,association_method,association_confidence,validation_status,
                    source,created_at,current_validation_status)
                   VALUES (?,'XYZ','provider_tag',0.8,'valid_external','provider',?,
                           'valid_external')""",
                (news_id, "2026-07-01T09:05:00+00:00"),
            )
            await _restore_real_v3_lineage_shape(db)

            async def fail_after_destructive_step(migration_db, **_kwargs):
                await migration_db.execute("DELETE FROM news_ticker_mentions")
                raise RuntimeError("injected_v4_migration_failure")

            monkeypatch.setattr(
                catalyst_database, "_migrate_ticker_lineage_v4", fail_after_destructive_step
            )
            with pytest.raises(RuntimeError, match="injected_v4_migration_failure"):
                await init_catalyst_schema(db)

            async with db.execute("PRAGMA user_version") as cursor:
                assert int((await cursor.fetchone())[0]) == 3
            async with db.execute("SELECT COUNT(*) FROM news_ticker_mentions") as cursor:
                assert int((await cursor.fetchone())[0]) == 1
            async with db.execute("PRAGMA table_info(news_ticker_mentions)") as cursor:
                columns = {str(row[1]) for row in await cursor.fetchall()}
            assert "current_validation_revision_id" not in columns
            async with db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ticker_validation_revisions'"
            ) as cursor:
                assert await cursor.fetchone() is None
            async with db.execute("PRAGMA foreign_keys") as cursor:
                assert int((await cursor.fetchone())[0]) == 1
        finally:
            await db.close()

    run(scenario())


def test_v3_to_v4_migration_deduplicates_100k_mentions_within_budget(lineage_db):
    async def scenario():
        db = await database.get_db()
        try:
            news_id, _ = await _insert_analysis(db)
            await _restore_real_v3_lineage_shape(db)
            insert_sql = """INSERT INTO news_ticker_mentions
                (news_id,ticker,association_method,association_confidence,validation_status,
                 validated_at,focus_revision,universe_version,source,created_at)
                VALUES (?,'AMD','provider_tag',?,?,?,?,?,'provider',?)"""
            ordinary = (
                news_id,
                0.5,
                "unverified",
                "2026-07-01T09:00:00+00:00",
                1,
                "u1",
                "2026-07-01T08:59:00+00:00",
            )
            for _ in range(20):
                await db.executemany(insert_sql, [ordinary] * 5_000)
            await db.execute(
                """UPDATE news_ticker_mentions SET association_confidence=0.99,
                     validation_status='canonical',validated_at='2026-07-01T10:00:00+00:00',
                     focus_revision=2,universe_version='u2'
                   WHERE id=(SELECT MAX(id) FROM news_ticker_mentions)"""
            )
            await db.commit()

            tracemalloc.start()
            started = time.monotonic()
            await init_catalyst_schema(db)
            elapsed = time.monotonic() - started
            _, peak_bytes = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            async with db.execute("SELECT COUNT(*) FROM news_ticker_mentions") as cursor:
                assert int((await cursor.fetchone())[0]) == 1
            async with db.execute(
                """SELECT association_confidence,current_validation_status,focus_revision,
                          universe_version FROM news_ticker_mentions"""
            ) as cursor:
                survivor = await cursor.fetchone()
            assert tuple(survivor) == (0.99, "canonical", 2, "u2")
            async with db.execute("SELECT COUNT(*) FROM ticker_validation_revisions") as cursor:
                assert int((await cursor.fetchone())[0]) == 1
            async with db.execute("PRAGMA integrity_check") as cursor:
                assert str((await cursor.fetchone())[0]).lower() == "ok"
            async with db.execute("PRAGMA foreign_key_check") as cursor:
                assert await cursor.fetchall() == []
            assert elapsed < 60, (
                f"100k mention migration exceeded budget: {elapsed:.2f}s, "
                f"peak={peak_bytes / 1024 / 1024:.1f}MiB"
            )
            assert peak_bytes < 256 * 1024 * 1024
        finally:
            await db.close()

    run(scenario())
