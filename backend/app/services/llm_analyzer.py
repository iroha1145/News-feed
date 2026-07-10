import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional

import aiosqlite
from pydantic import ValidationError

from app.config import settings as app_settings
from app.models.database import (
    get_db, get_unanalyzed_news, get_setting, save_analysis_result,
    claim_news_for_analysis, mark_analysis_failed,
)
from app.models.schemas import LLMAnalysisPayload, SentimentClassification
from app.services.llm_providers import (
    BaseLLMProvider,
    OpenAIProvider,
    AnthropicProvider,
    GrokProvider,
    OllamaProvider,
)
from app.utils.http import safe_exception_message

logger = logging.getLogger(__name__)

MIN_SUMMARY_CHARS = 40
MIN_TOTAL_CONTEXT_CHARS = 100

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


def _get_provider(
    provider_name: str,
    model: str,
    api_key: str,
    overrides: Optional[dict] = None,
) -> BaseLLMProvider:
    overrides = overrides or {}
    if provider_name == "anthropic":
        return AnthropicProvider(api_key=api_key, model=model)
    elif provider_name == "grok":
        grok_base_url = overrides.get("grok_base_url") or app_settings.grok_base_url
        return GrokProvider(api_key=api_key, model=model, base_url=grok_base_url)
    elif provider_name == "ollama":
        return OllamaProvider(base_url=overrides.get("ollama_base_url") or app_settings.ollama_base_url, model=model)
    elif provider_name == "openai":
        openai_base_url = overrides.get("openai_base_url") or app_settings.openai_base_url
        return OpenAIProvider(api_key=api_key, model=model, base_url=openai_base_url)
    raise ValueError(f"Unsupported LLM provider: {provider_name}")


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


def _has_sufficient_context(news_item: dict) -> bool:
    title = str(news_item.get("title") or "").strip()
    summary = str(news_item.get("summary") or "").strip()
    return len(summary) >= MIN_SUMMARY_CHARS and (len(title) + len(summary)) >= MIN_TOTAL_CONTEXT_CHARS


def _low_context_payload() -> LLMAnalysisPayload:
    return LLMAnalysisPayload(
        title_zh="",
        headline_summary="原文信息不足，系统保留该新闻，但不生成方向性判断。",
        overall_sentiment=0,
        classification=SentimentClassification.neutral,
        confidence=0,
        affected_stocks=[],
        affected_sectors=[],
        affected_commodities=[],
        logic_chain="文章仅含标题或摘要过短，缺少形成可靠因果判断所需的上下文。",
        key_factors=["上下文不足"],
    )


def _analysis_record(
    news_id: int,
    payload: LLMAnalysisPayload,
    provider_name: str,
    model: str,
) -> dict:
    validated = payload.model_dump(mode="json")
    return {
        "news_id": news_id,
        "title_zh": validated["title_zh"],
        "headline_summary": validated["headline_summary"],
        "overall_sentiment": validated["overall_sentiment"],
        "classification": validated["classification"],
        "confidence": validated["confidence"],
        "affected_stocks": json.dumps(validated["affected_stocks"], ensure_ascii=False),
        "affected_sectors": json.dumps(validated["affected_sectors"], ensure_ascii=False),
        "affected_commodities": json.dumps(validated["affected_commodities"], ensure_ascii=False),
        "logic_chain": validated["logic_chain"],
        "key_factors": json.dumps(validated["key_factors"], ensure_ascii=False),
        "llm_provider": provider_name,
        "llm_model": model,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
    }


async def analyze_news_item(news_item: dict, db: aiosqlite.Connection) -> Optional[dict]:
    """Analyze a single news item and store the result."""
    news_id = news_item["id"]
    claimed = await claim_news_for_analysis(db, news_id)
    if not claimed:
        logger.debug(f"News {news_id} already claimed, skipping")
        return None

    raw_response = ""
    api_key = ""
    try:
        if not _has_sufficient_context(news_item):
            payload = _low_context_payload()
            provider_name = "system"
            model = "low-context-neutral-v1"
            logger.info("News %s has insufficient context; storing a neutral result without an LLM call", news_id)
        else:
            overrides = await _get_runtime_settings(db)
            provider_name = overrides.get("default_llm_provider") or app_settings.default_llm_provider
            model = overrides.get("default_llm_model") or app_settings.default_llm_model
            api_key = _resolve_api_key(provider_name, overrides)
            provider = _get_provider(provider_name, model, api_key, overrides)

            title = str(news_item.get("title") or "")
            summary = str(news_item.get("summary") or "")
            user_prompt = f"Title: {title}\n\nSummary: {summary}"
            raw_response = await provider.analyze(user_prompt, SYSTEM_PROMPT)
            parsed = _extract_json_object(raw_response)
            payload = LLMAnalysisPayload.model_validate(parsed)

        analysis = _analysis_record(news_id, payload, provider_name, model)
        analysis_id = await save_analysis_result(db, analysis)
        logger.info(
            "Analyzed news_id=%s -> analysis_id=%s [%s]",
            news_id,
            analysis_id,
            analysis["classification"],
        )
        return analysis
    except (json.JSONDecodeError, ValidationError) as exc:
        logger.error(
            "Invalid structured analysis for news_id=%s: %s",
            news_id,
            type(exc).__name__,
        )
        try:
            await mark_analysis_failed(db, news_id, "Invalid structured model output")
        except Exception:
            logger.error("Failed to release analysis lease for news_id=%s", news_id)
        return None
    except Exception as exc:
        error = safe_exception_message(exc, secrets=(api_key,))
        logger.error("Analysis failed for news_id=%s: %s", news_id, error)
        try:
            await mark_analysis_failed(db, news_id, error)
        except Exception:
            logger.error("Failed to release analysis lease for news_id=%s", news_id)
        return None


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
