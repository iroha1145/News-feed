import json
import logging
import re
from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, StrictInt, ValidationError

from app.config import settings as app_settings
from app.models.database import get_db, get_setting, insert_x_sentiment
from app.services.llm_providers.grok_provider import GrokProvider
from app.utils.http import safe_exception_message

logger = logging.getLogger(__name__)

_last_error: Optional[str] = None


class ScenarioTicker(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    ticker: str = Field(min_length=1, max_length=20)
    mention_sentiment: Literal["bullish", "bearish", "mixed"]
    buzz_level: Literal["high", "medium", "low"]
    narrative: str = Field(min_length=1, max_length=1000)


class ScenarioAlert(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    ticker: str = Field(min_length=1, max_length=20)
    risk_level: Literal["high", "medium", "low"]
    description: str = Field(min_length=1, max_length=1000)


class MarketScenarioPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    trending_tickers: list[ScenarioTicker] = Field(default_factory=list, max_length=30)
    overall_retail_sentiment: StrictInt = Field(ge=-100, le=100)
    key_narratives: list[str] = Field(default_factory=list, max_length=30)
    meme_stock_alerts: list[ScenarioAlert] = Field(default_factory=list, max_length=30)
    fear_greed_estimate: StrictInt = Field(ge=0, le=100)


def get_last_error() -> Optional[str]:
    return _last_error


SCENARIO_SYSTEM_PROMPT = """You are a financial market analyst. Build a hypothetical retail-market scenario using only the supplied recent news context.
Do not claim that you observed X, Twitter, social posts, live mention counts, or current social-media trends.
Return one valid JSON object matching the requested schema and no additional fields."""

SCENARIO_PROMPT = """Using only the recent saved headlines below, estimate a clearly labeled market scenario.
The tickers and narratives are model inferences from news, not observed social-media activity. If evidence is weak, use mixed sentiment, low buzz, and fewer tickers.

Return this JSON schema:
{
  "trending_tickers": [
    {"ticker": "<ticker>", "mention_sentiment": "<bullish|bearish|mixed>", "buzz_level": "<high|medium|low>", "narrative": "<news-grounded reason>"}
  ],
  "overall_retail_sentiment": <-100 to 100>,
  "key_narratives": ["<news-grounded narrative>"],
  "meme_stock_alerts": [
    {"ticker": "<ticker>", "risk_level": "<high|medium|low>", "description": "<news-grounded risk>"}
  ],
  "fear_greed_estimate": <0 to 100>
}

Recent saved news:
{context}
"""


async def _recent_news_context(db, limit: int = 20) -> str:
    async with db.execute(
        """SELECT source, title, summary, published_at
           FROM news_items
           WHERE title IS NOT NULL AND trim(title) != ''
           ORDER BY datetime(COALESCE(published_at, fetched_at)) DESC
           LIMIT ?""",
        (limit,),
    ) as cursor:
        rows = await cursor.fetchall()

    lines = []
    for row in rows:
        summary = re.sub(r"\s+", " ", str(row[2] or "")).strip()[:500]
        lines.append(
            f"- [{row[3] or 'time unknown'}] {row[0]}: {row[1]}"
            + (f" — {summary}" if summary else "")
        )
    return "\n".join(lines)


def _parse_scenario(raw: str) -> MarketScenarioPayload:
    cleaned = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
    json_start = cleaned.find("{")
    json_end = cleaned.rfind("}")
    if json_start >= 0 and json_end > json_start:
        cleaned = cleaned[json_start:json_end + 1]
    return MarketScenarioPayload.model_validate(json.loads(cleaned))


async def run_x_sentiment_analysis() -> Optional[dict]:
    """Generate a news-grounded model scenario; no live X data is queried."""
    global _last_error
    if not app_settings.x_sentiment_enabled:
        _last_error = "Model market scenario is disabled"
        return None
    db = await get_db()
    try:
        grok_key = await get_setting(db, "grok_api_key") or app_settings.grok_api_key
        if not grok_key:
            _last_error = "Grok API key not configured"
            logger.warning(_last_error)
            return None

        context = await _recent_news_context(db)
        if not context:
            _last_error = "No recent news context available"
            logger.warning(_last_error)
            return None

        grok_model = await get_setting(db, "grok_model") or app_settings.grok_model
        grok_base_url = await get_setting(db, "grok_base_url") or app_settings.grok_base_url
        provider = GrokProvider(api_key=grok_key, model=grok_model, base_url=grok_base_url)

        try:
            raw = await provider.analyze(
                SCENARIO_PROMPT.replace("{context}", context),
                SCENARIO_SYSTEM_PROMPT,
            )
            payload = _parse_scenario(raw)
        except (json.JSONDecodeError, ValidationError) as exc:
            _last_error = "Model scenario output did not match the required schema"
            logger.error("Model scenario validation failed: %s", safe_exception_message(exc))
            return None
        except Exception as exc:
            _last_error = "Model scenario provider request failed"
            logger.error(
                "Model scenario request failed: %s",
                safe_exception_message(exc, secrets=(grok_key,)),
            )
            return None

        validated = payload.model_dump(mode="json")
        analyzed_at = datetime.now(timezone.utc).isoformat()
        sentiment_record = {
            "query": "News-grounded model market scenario; not live social-media data",
            "trending_tickers": json.dumps(validated["trending_tickers"], ensure_ascii=False),
            "retail_sentiment_score": validated["overall_retail_sentiment"],
            "key_narratives": json.dumps(validated["key_narratives"], ensure_ascii=False),
            "meme_stocks": json.dumps(validated["meme_stock_alerts"], ensure_ascii=False),
            "raw_analysis": json.dumps(validated, ensure_ascii=False),
            "fear_greed_estimate": validated["fear_greed_estimate"],
            "analyzed_at": analyzed_at,
        }

        sentiment_id = await insert_x_sentiment(db, sentiment_record)
        _last_error = None
        logger.info("News-grounded market scenario stored with id=%s", sentiment_id)
        return {
            "id": sentiment_id,
            "trending_tickers": validated["trending_tickers"],
            "retail_sentiment_score": validated["overall_retail_sentiment"],
            "key_narratives": validated["key_narratives"],
            "meme_stocks": validated["meme_stock_alerts"],
            "fear_greed_estimate": validated["fear_greed_estimate"],
            "analyzed_at": analyzed_at,
        }
    finally:
        await db.close()
