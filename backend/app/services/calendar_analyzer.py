import hashlib
import json
import logging
import os
import time
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from app.config import settings as app_settings
from app.models.database import get_db, get_setting
from app.services.llm_providers import (
    BaseLLMProvider,
    OpenAIProvider,
    AnthropicProvider,
    GrokProvider,
    OllamaProvider,
)
from app.utils.http import safe_exception_message

logger = logging.getLogger(__name__)


class CalendarAnalysisItem(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    event_id: str = Field(min_length=8, max_length=64)
    title: str = Field(min_length=1, max_length=500)
    title_zh: str = Field(min_length=1, max_length=500)
    stock_impact: Literal["bullish", "bearish", "neutral"]
    commodity_impact: Literal["bullish", "bearish", "neutral"]
    explanation: str = Field(min_length=1, max_length=2000)


class CalendarAnalysisPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    events: list[CalendarAnalysisItem] = Field(default_factory=list, max_length=200)


def _analysis_cache_ttl() -> int:
    raw_value = os.getenv(
        "CALENDAR_ANALYSIS_CACHE_TTL",
        str(getattr(app_settings, "calendar_analysis_cache_ttl", 3600)),
    )
    try:
        return max(60, int(raw_value))
    except (TypeError, ValueError):
        logger.warning("Invalid calendar analysis cache TTL; using 3600 seconds")
        return 3600


# In-memory cache: maps an event snapshot to (monotonic timestamp, analysis).
ANALYSIS_CACHE_TTL = _analysis_cache_ttl()
_analysis_cache: dict[str, tuple[float, list[dict]]] = {}

CALENDAR_SYSTEM_PROMPT = """You are a senior macro-economic analyst specializing in US equities and precious metals markets.
Analyze the following list of economic calendar events and assess their likely impact on stocks and commodities.

You MUST respond in valid JSON with this exact schema:
{
  "events": [
	    {
	      "event_id": "<input event ID, must match exactly>",
	      "title": "<original event title, must match exactly>",
      "title_zh": "<Chinese translation of the event title>",
      "stock_impact": "<bullish|bearish|neutral>",
      "commodity_impact": "<bullish|bearish|neutral>",
      "explanation": "<Chinese explanation of why, 1-2 sentences>"
    }
  ]
}

Rules:
- Return one entry per input event, preserving event_id and original title exactly
- ALL explanation and title_zh text MUST be in Chinese
- stock_impact refers to broad US equity market impact
- commodity_impact refers mainly to Gold, Silver, and other precious metals
- Consider both direct and indirect market effects
- For RELEASED events, compare actual vs forecast/previous to determine impact direction"""


def _get_provider(provider_name: str, model: str, api_key: str, overrides: Optional[dict] = None) -> BaseLLMProvider:
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


def _resolve_api_key(provider: str, overrides: dict) -> str:
    key_map = {
        "openai": overrides.get("openai_api_key") or app_settings.openai_api_key or overrides.get("default_llm_api_key") or app_settings.default_llm_api_key,
        "anthropic": overrides.get("anthropic_api_key") or app_settings.anthropic_api_key or overrides.get("default_llm_api_key") or app_settings.default_llm_api_key,
        "grok": overrides.get("grok_api_key") or app_settings.grok_api_key or overrides.get("default_llm_api_key") or app_settings.default_llm_api_key,
        "ollama": "",
    }
    return key_map.get(provider, "")


def _cache_key(
    events: list[dict],
    provider_name: Optional[str] = None,
    model: Optional[str] = None,
) -> str:
    snapshot = [
        {
            "title": event.get("title", ""),
            "date": event.get("date", ""),
            "country": event.get("country_code") or event.get("country", ""),
            "actual": event.get("actual", ""),
            "forecast": event.get("forecast", ""),
            "previous": event.get("previous", ""),
        }
        for event in events
    ]
    snapshot.sort(key=lambda event: json.dumps(event, ensure_ascii=False, sort_keys=True))
    cache_input = {
        "events": snapshot,
        "provider": provider_name or app_settings.default_llm_provider,
        "model": model or app_settings.default_llm_model,
    }
    return json.dumps(cache_input, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _event_id(event: dict) -> str:
    identity = "|".join(
        str(event.get(key) or "")
        for key in ("title", "date", "country_code")
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]


def get_cached_analysis(
    events: list[dict],
    provider_name: Optional[str] = None,
    model: Optional[str] = None,
) -> Optional[list[dict]]:
    cache_key = _cache_key(events, provider_name, model)
    cached = _analysis_cache.get(cache_key)
    if cached is None:
        return None
    created_at, analysis = cached
    if time.monotonic() - created_at >= ANALYSIS_CACHE_TTL:
        _analysis_cache.pop(cache_key, None)
        return None
    return [dict(item) for item in analysis]


async def _load_runtime_overrides() -> dict:
    db = await get_db()
    try:
        overrides = {}
        for key in ["default_llm_provider", "default_llm_model", "default_llm_api_key",
                    "openai_api_key", "anthropic_api_key", "grok_api_key", "ollama_base_url",
                    "openai_base_url", "grok_base_url"]:
            value = await get_setting(db, key)
            if value is not None:
                overrides[key] = value
        return overrides
    finally:
        await db.close()


async def get_calendar_model_identity() -> tuple[str, str]:
    overrides = await _load_runtime_overrides()
    return (
        overrides.get("default_llm_provider") or app_settings.default_llm_provider,
        overrides.get("default_llm_model") or app_settings.default_llm_model,
    )


def merge_analysis(events: list[dict], analyzed: list[dict]) -> list[dict]:
    """Merge model results by stable event identity, not a non-unique title."""
    lookup = {str(item.get("event_id") or ""): item for item in analyzed}
    result = []
    for e in events:
        merged = dict(e)
        match = lookup.get(_event_id(e))
        if match:
            merged["title_zh"] = match.get("title_zh", "")
            merged["stock_impact"] = match.get("stock_impact", "neutral")
            merged["commodity_impact"] = match.get("commodity_impact", "neutral")
            merged["explanation"] = match.get("explanation", "")
        result.append(merged)
    return result


async def analyze_calendar_events(events: list[dict]) -> list[dict]:
    """Run LLM analysis on calendar events. Returns analyzed event dicts and caches result."""
    if not events:
        return []
    overrides = await _load_runtime_overrides()
    provider_name = overrides.get("default_llm_provider") or app_settings.default_llm_provider
    model = overrides.get("default_llm_model") or app_settings.default_llm_model
    cache_key = _cache_key(events, provider_name, model)
    cached = get_cached_analysis(events, provider_name, model)
    if cached is not None:
        logger.info("Returning cached calendar analysis")
        return cached
    api_key = _resolve_api_key(provider_name, overrides)

    lines = []
    for e in events:
        line = (
            f"- ID={_event_id(e)} | {e.get('title', '')} "
            f"({e.get('country', '')} {e.get('impact', '')} impact, {e.get('date', '')})"
        )
        if e.get('actual'):
            line += f" [RELEASED: actual={e['actual']}, forecast={e.get('forecast','N/A')}, previous={e.get('previous','N/A')}]"
        elif e.get('forecast'):
            line += f" [UPCOMING: forecast={e['forecast']}, previous={e.get('previous','N/A')}]"
        lines.append(line)
    event_list = "\n".join(lines)
    user_prompt = f"Analyze these economic calendar events. For events marked [RELEASED], compare actual vs forecast/previous to determine market impact:\n\n{event_list}"

    raw_response = ""
    try:
        provider = _get_provider(provider_name, model, api_key, overrides)
        raw_response = await provider.analyze(user_prompt, CALENDAR_SYSTEM_PROMPT)
        # Strip <think> tags from reasoning models
        import re
        cleaned = re.sub(r'<think>.*?</think>', '', raw_response, flags=re.DOTALL).strip()
        json_start = cleaned.find('{')
        json_end = cleaned.rfind('}')
        if json_start >= 0 and json_end > json_start:
            cleaned = cleaned[json_start:json_end + 1]
        parsed = CalendarAnalysisPayload.model_validate(json.loads(cleaned))
        expected_events = {
            _event_id(event): str(event.get("title") or "")
            for event in events
        }
        analyzed = [
            item.model_dump()
            for item in parsed.events
            if expected_events.get(item.event_id) == item.title
        ]
    except (json.JSONDecodeError, ValidationError) as exc:
        logger.error("Failed to validate calendar analysis response: %s", safe_exception_message(exc))
        return []
    except Exception as exc:
        logger.error("Calendar analysis failed: %s", safe_exception_message(exc, secrets=(api_key,)))
        return []

    _analysis_cache[cache_key] = (time.monotonic(), [dict(item) for item in analyzed])
    logger.info(f"Calendar analysis complete: {len(analyzed)} events analyzed")
    return analyzed
