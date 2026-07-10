import logging
import os
from pathlib import Path
from typing import Literal, Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


def _find_env_file() -> str:
    """Find .env file: check CWD first, then project root (parent of backend/)."""
    candidates = [
        Path.cwd() / ".env",                         # CWD (Docker or project root)
        Path(__file__).resolve().parent.parent.parent / ".env",  # backend/app/config.py -> ../../.env (project root)
    ]
    for p in candidates:
        if p.is_file():
            return str(p)
    return ".env"  # fallback


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_find_env_file(),
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
    )

    # News APIs
    finnhub_api_key: str = ""
    newsapi_api_key: str = ""
    gnews_api_key: str = ""
    massive_api_key: str = ""

    # Default LLM
    default_llm_provider: Literal["openai", "anthropic", "grok", "ollama"] = "openai"
    default_llm_model: str = Field(default="gpt-4o-mini", min_length=1, max_length=200)
    default_llm_api_key: str = ""

    # Additional LLM keys
    openai_api_key: str = ""
    anthropic_api_key: str = ""
    grok_api_key: str = ""
    grok_model: str = Field(default="grok-4", min_length=1, max_length=200)
    ollama_base_url: str = "http://localhost:11434"
    openai_base_url: str = "https://api.openai.com/v1"
    grok_base_url: str = "https://api.x.ai/v1"

    # App
    analysis_batch_size: int = Field(default=10, ge=1, le=100)
    x_sentiment_interval: int = Field(default=21600, ge=300)
    database_url: str = "sqlite+aiosqlite:///data/macrolens.db"
    cors_origins: str = ""  # comma-separated origins
    admin_token: str = ""
    session_cookie_secure: bool = False
    session_ttl_seconds: int = Field(default=28800, ge=300, le=604800)

    # News-source scheduling. Paid quota-limited aggregators stay opt-in.
    finnhub_news_enabled: bool = True
    finnhub_news_interval: int = Field(default=300, ge=30)
    massive_news_enabled: bool = True
    massive_news_interval: int = Field(default=300, ge=30)
    google_news_enabled: bool = True
    google_news_interval: int = Field(default=900, ge=30)
    seekingalpha_breaking_enabled: bool = True
    seekingalpha_breaking_interval: int = Field(default=300, ge=30)
    seekingalpha_daily_enabled: bool = True
    seekingalpha_daily_interval: int = Field(default=21600, ge=30)
    newsapi_news_enabled: bool = False
    newsapi_news_interval: int = Field(default=1800, ge=30)
    gnews_news_enabled: bool = False
    gnews_news_interval: int = Field(default=1800, ge=30)

    calendar_analysis_cache_ttl: int = Field(default=3600, ge=60, le=86400)
    analysis_retention_limit: int = Field(default=350, ge=1, le=100000)
    news_retention_days: Optional[int] = Field(default=None, ge=1, le=3650)
    x_sentiment_retention_days: Optional[int] = Field(default=None, ge=1, le=3650)

    def validate_config(self) -> list[str]:
        """Check for common config mistakes. Returns list of warnings."""
        warnings = []

        # Detect swapped grok_api_key and grok_base_url
        if self.grok_api_key and self.grok_api_key.startswith(("http://", "https://")):
            warnings.append(
                f"GROK_API_KEY looks like a URL ('{self.grok_api_key[:30]}...'). "
                f"Did you swap GROK_API_KEY and GROK_BASE_URL?"
            )
        if self.grok_base_url and not self.grok_base_url.startswith(("http://", "https://")):
            warnings.append(
                f"GROK_BASE_URL doesn't look like a URL ('{self.grok_base_url[:30]}'). "
                f"Did you swap GROK_API_KEY and GROK_BASE_URL?"
            )

        # Same check for OpenAI
        if self.openai_api_key and self.openai_api_key.startswith(("http://", "https://")):
            warnings.append(
                f"OPENAI_API_KEY looks like a URL. Did you swap OPENAI_API_KEY and OPENAI_BASE_URL?"
            )
        if self.openai_base_url and not self.openai_base_url.startswith(("http://", "https://")):
            warnings.append(
                f"OPENAI_BASE_URL doesn't look like a URL ('{self.openai_base_url[:30]}')."
            )

        return warnings


settings = Settings()

# Run validation on startup
_warnings = settings.validate_config()
for w in _warnings:
    logger.warning(f"\u26a0\ufe0f  Config issue: {w}")
