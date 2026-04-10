import json
import logging
import re
from datetime import datetime
from typing import Any, Optional

import aiosqlite

from app.config import settings as app_settings
from app.models.database import (
    get_db, insert_analysis, get_unanalyzed_news, get_setting,
    claim_news_for_analysis, mark_analysis_completed, mark_analysis_failed,
)
from app.services.llm_providers import (
    BaseLLMProvider,
    OpenAIProvider,
    AnthropicProvider,
    GrokProvider,
    OllamaProvider,
)

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a senior macro-economic analyst specializing in US equities and precious metals markets.
Analyze the following news article and provide structured sentiment analysis with Chinese translation.

You MUST respond in valid JSON with this exact schema:
{
  "title_zh": "<Chinese translation of the news title>",
  "headline_summary": "<Brief summary in Chinese>",
  "overall_sentiment": <integer -100 to 100, where -100 is extremely bearish, 100 is extremely bullish>,
  "classification": "<bullish|bearish|neutral>",
  "confidence": <integer 0-100>,
  "affected_stocks": [
    {"ticker": "<stock ticker>", "company": "<company name>", "impact_score": <-100 to 100>, "reason": "<explanation in Chinese>"}
  ],
  "affected_sectors": ["<sector name>", ...],
  "affected_commodities": [
    {"name": "<Gold/Silver/Platinum/Palladium>", "impact_score": <-100 to 100>, "reason": "<explanation in Chinese>"}
  ],
  "logic_chain": "<Step by step reasoning in Chinese: A → B → C → impact>",
  "key_factors": ["<factor1>", "<factor2>", ...]
}

Rules:
- ALL text output (title_zh, headline_summary, reason, logic_chain) MUST be in Chinese
- Focus on US stock market and precious metals (Gold, Silver, Platinum, Palladium)
- Be specific with stock tickers (e.g., AAPL, NVDA, GLD, SLV)
- Consider both direct and indirect impacts
- If the news is not market-relevant, set classification to "neutral" and confidence to low
- Logic chain should show clear causal reasoning in Chinese"""


def _get_provider(provider_name: str, model: str, api_key: str, overrides: dict = {}) -> BaseLLMProvider:
    if provider_name == "anthropic":
        return AnthropicProvider(api_key=api_key, model=model)
    elif provider_name == "grok":
        grok_base_url = overrides.get("grok_base_url") or app_settings.grok_base_url
        return GrokProvider(api_key=api_key, model=model, base_url=grok_base_url)
    elif provider_name == "ollama":
        return OllamaProvider(base_url=overrides.get("ollama_base_url") or app_settings.ollama_base_url, model=model)
    else:
        openai_base_url = overrides.get("openai_base_url") or app_settings.openai_base_url
        return OpenAIProvider(api_key=api_key, model=model, base_url=openai_base_url)


async def _get_runtime_settings(db: aiosqlite.Connection) -> dict:
    """Merge env settings with DB overrides."""
    overrides = {}
    for key in ["default_llm_provider", "default_llm_model", "default_llm_api_key",
                "openai_api_key", "anthropic_api_key", "grok_api_key", "ollama_base_url",
                "openai_base_url", "grok_base_url"]:
        val = await get_setting(db, key)
        if val is not None:
            overrides[key] = val
    return overrides


def _resolve_api_key(provider: str, overrides: dict) -> str:
    key_map = {
        "openai": overrides.get("openai_api_key") or app_settings.openai_api_key or overrides.get("default_llm_api_key") or app_settings.default_llm_api_key,
        "anthropic": overrides.get("anthropic_api_key") or app_settings.anthropic_api_key or overrides.get("default_llm_api_key") or app_settings.default_llm_api_key,
        "grok": overrides.get("grok_api_key") or app_settings.grok_api_key or overrides.get("default_llm_api_key") or app_settings.default_llm_api_key,
        "ollama": "",
    }
    return key_map.get(provider, "")


def _extract_json_object(raw_response: str) -> dict:
    cleaned = re.sub(r'<think>.*?</think>', '', raw_response, flags=re.DOTALL).strip()
    json_start = cleaned.find('{')
    json_end = cleaned.rfind('}')
    if json_start >= 0 and json_end > json_start:
        cleaned = cleaned[json_start:json_end + 1]
    return json.loads(cleaned)


async def analyze_news_item(news_item: dict, db: aiosqlite.Connection) -> Optional[dict]:
    """Analyze a single news item and store the result."""
    news_id = news_item["id"]
    claimed = await claim_news_for_analysis(db, news_id)
    if not claimed:
        logger.debug(f"News {news_id} already claimed, skipping")
        return None

    overrides = await _get_runtime_settings(db)

    provider_name = overrides.get("default_llm_provider") or app_settings.default_llm_provider
    model = overrides.get("default_llm_model") or app_settings.default_llm_model
    api_key = _resolve_api_key(provider_name, overrides)

    provider = _get_provider(provider_name, model, api_key, overrides)

    title = news_item.get("title", "")
    summary = news_item.get("summary", "") or ""
    user_prompt = f"Title: {title}\n\nSummary: {summary}"

    raw_response = ""
    try:
        raw_response = await provider.analyze(user_prompt, SYSTEM_PROMPT)
        parsed = _extract_json_object(raw_response)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse LLM JSON response for news_id={news_id}: {e}\nRaw: {raw_response[:500]}")
        await mark_analysis_failed(db, news_id, str(e))
        return None
    except Exception as e:
        logger.error(f"LLM analysis failed for news_id={news_id}: {e}")
        await mark_analysis_failed(db, news_id, str(e))
        return None

    analysis = {
        "news_id": news_id,
        "title_zh": parsed.get("title_zh", ""),
        "headline_summary": parsed.get("headline_summary", ""),
        "overall_sentiment": int(parsed.get("overall_sentiment", 0)),
        "classification": parsed.get("classification", "neutral"),
        "confidence": int(parsed.get("confidence", 0)),
        "affected_stocks": json.dumps(parsed.get("affected_stocks", [])),
        "affected_sectors": json.dumps(parsed.get("affected_sectors", [])),
        "affected_commodities": json.dumps(parsed.get("affected_commodities", [])),
        "logic_chain": parsed.get("logic_chain", ""),
        "key_factors": json.dumps(parsed.get("key_factors", [])),
        "llm_provider": provider_name,
        "llm_model": model,
        "analyzed_at": datetime.utcnow().isoformat(),
    }

    analysis_id = await insert_analysis(db, analysis)
    await mark_analysis_completed(db, news_id)
    logger.info(f"Analyzed news_id={news_id} -> analysis_id={analysis_id} [{analysis['classification']}]")
    return analysis


async def run_analysis_batch(batch_size: Optional[int] = None) -> int:
    """Analyze a batch of unanalyzed news items. Returns number analyzed."""
    db = await get_db()
    try:
        if batch_size is None:
            db_val = await get_setting(db, "analysis_batch_size")
            batch_size = int(db_val) if db_val else app_settings.analysis_batch_size
        size = batch_size
        items = await get_unanalyzed_news(db, limit=size)
        if not items:
            logger.debug("No unanalyzed news items found")
            return 0

        count = 0
        for item in items:
            result = await analyze_news_item(item, db)
            if result:
                count += 1

        logger.info(f"Analysis batch complete: {count}/{len(items)} items analyzed")
        return count
    finally:
        await db.close()
