import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field, field_validator

from app.config import settings as app_settings
from app.deps.auth import require_admin
from app.models.database import get_db, get_all_settings, set_setting, get_setting
from app.services.llm_providers import OpenAIProvider, AnthropicProvider, GrokProvider, OllamaProvider

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/settings", tags=["settings"])

SENSITIVE_KEYS = {
    "default_llm_api_key",
    "openai_api_key",
    "anthropic_api_key",
    "grok_api_key",
    "finnhub_api_key",
    "newsapi_api_key",
    "gnews_api_key",
    "massive_api_key",
}

PROVIDER_MODELS = {
    "openai": ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-3.5-turbo"],
    "anthropic": ["claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5-20251001"],
    "grok": ["grok-beta", "grok-2"],
    "ollama": ["llama3", "mistral", "codellama", "phi3"],
}

ALLOWED_OLLAMA_HOSTS = {"localhost", "127.0.0.1", "host.docker.internal"}


def _redact(key: str, value: Any) -> Any:
    if key in SENSITIVE_KEYS and value:
        return "********"
    return value


def _validate_ollama_base_url(value: str) -> str:
    from urllib.parse import urlparse

    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("ollama_base_url must use http or https")
    if parsed.hostname not in ALLOWED_OLLAMA_HOSTS:
        raise ValueError("ollama_base_url must target localhost, 127.0.0.1, or host.docker.internal")
    return value


def _merge_settings(db_overrides: dict) -> dict:
    """Merge env settings with DB overrides, redacting sensitive values."""
    env_vals = {
        "default_llm_provider": app_settings.default_llm_provider,
        "default_llm_model": app_settings.default_llm_model,
        "default_llm_api_key": app_settings.default_llm_api_key,
        "openai_api_key": app_settings.openai_api_key,
        "anthropic_api_key": app_settings.anthropic_api_key,
        "grok_api_key": app_settings.grok_api_key,
        "ollama_base_url": app_settings.ollama_base_url,
        "news_poll_interval": app_settings.news_poll_interval,
        "analysis_batch_size": app_settings.analysis_batch_size,
        "x_sentiment_interval": app_settings.x_sentiment_interval,
        "finnhub_api_key": app_settings.finnhub_api_key,
        "newsapi_api_key": app_settings.newsapi_api_key,
        "gnews_api_key": app_settings.gnews_api_key,
    }
    merged = {**env_vals, **db_overrides}
    return {k: _redact(k, v) for k, v in merged.items()}


class SettingsUpdateRequest(BaseModel):
    default_llm_provider: Optional[str] = None
    default_llm_model: Optional[str] = None
    default_llm_api_key: Optional[str] = None
    openai_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None
    grok_api_key: Optional[str] = None
    ollama_base_url: Optional[str] = None
    news_poll_interval: Optional[int] = Field(default=None, ge=30)
    analysis_batch_size: Optional[int] = Field(default=None, ge=1, le=20)
    x_sentiment_interval: Optional[int] = Field(default=None, ge=30)
    finnhub_api_key: Optional[str] = None
    newsapi_api_key: Optional[str] = None
    gnews_api_key: Optional[str] = None

    @field_validator("ollama_base_url")
    @classmethod
    def validate_ollama_base_url(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        return _validate_ollama_base_url(value)


class TestLLMRequest(BaseModel):
    provider: str
    model: str
    api_key: Optional[str] = None


@router.get("")
async def get_settings():
    db = await get_db()
    try:
        db_overrides = await get_all_settings(db)
        return _merge_settings(db_overrides)
    finally:
        await db.close()


@router.put("")
async def update_settings(body: SettingsUpdateRequest, _: None = Depends(require_admin)):
    db = await get_db()
    try:
        updated = {}
        scheduler_keys = {"news_poll_interval", "x_sentiment_interval"}
        needs_scheduler_reload = False
        for key, value in body.model_dump(exclude_none=True).items():
            if key in SENSITIVE_KEYS:
                logger.warning("Ignoring attempt to persist sensitive setting '%s' via API", key)
                updated[key] = _redact(key, value)
                continue
            await set_setting(db, key, value)
            updated[key] = _redact(key, value)
            if key in scheduler_keys:
                needs_scheduler_reload = True

        msg = "Settings saved to database"

        # Reload scheduler if interval settings changed
        if needs_scheduler_reload:
            try:
                from app.utils.scheduler import stop_scheduler, start_scheduler
                stop_scheduler()
                await start_scheduler()
                msg += ". Scheduler reloaded with new intervals."
            except Exception as e:
                logger.error(f"Failed to reload scheduler: {e}")
                msg += ". Warning: scheduler reload failed, restart backend to apply interval changes."

        return {"updated": updated, "message": msg}
    finally:
        await db.close()


@router.get("/providers")
async def list_providers():
    db = await get_db()
    try:
        overrides = await get_all_settings(db)

        def resolve_key(provider: str) -> str:
            key_map = {
                "openai": overrides.get("openai_api_key") or app_settings.openai_api_key,
                "anthropic": overrides.get("anthropic_api_key") or app_settings.anthropic_api_key,
                "grok": overrides.get("grok_api_key") or app_settings.grok_api_key,
                "ollama": "",
            }
            return key_map.get(provider, "")

        providers = []
        for name, models in PROVIDER_MODELS.items():
            api_key = resolve_key(name)
            configured = bool(api_key) if name != "ollama" else True
            providers.append({
                "name": name,
                "configured": configured,
                "models": models,
            })

        return {"providers": providers}
    finally:
        await db.close()


@router.post("/test-llm")
async def test_llm_connection(body: TestLLMRequest, _: None = Depends(require_admin)):
    db = await get_db()
    try:
        overrides = await get_all_settings(db)

        api_key = body.api_key
        if not api_key:
            key_map = {
                "openai": overrides.get("openai_api_key") or app_settings.openai_api_key,
                "anthropic": overrides.get("anthropic_api_key") or app_settings.anthropic_api_key,
                "grok": overrides.get("grok_api_key") or app_settings.grok_api_key,
                "ollama": "",
            }
            api_key = key_map.get(body.provider, "")

        if body.provider == "openai":
            provider = OpenAIProvider(api_key=api_key, model=body.model)
        elif body.provider == "anthropic":
            provider = AnthropicProvider(api_key=api_key, model=body.model)
        elif body.provider == "grok":
            provider = GrokProvider(api_key=api_key, model=body.model)
        elif body.provider == "ollama":
            ollama_url = overrides.get("ollama_base_url") or app_settings.ollama_base_url
            provider = OllamaProvider(base_url=ollama_url, model=body.model)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown provider: {body.provider}")

        available = await provider.is_available()
        return {
            "provider": body.provider,
            "model": body.model,
            "available": available,
            "status": "ok" if available else "unavailable",
        }
    finally:
        await db.close()
