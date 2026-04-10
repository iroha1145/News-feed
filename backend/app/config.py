import logging
import os
from pathlib import Path
from pydantic_settings import BaseSettings

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
    # News APIs
    finnhub_api_key: str = ""
    newsapi_api_key: str = ""
    gnews_api_key: str = ""
    massive_api_key: str = ""

    # Default LLM
    default_llm_provider: str = "openai"  # openai/anthropic/grok/ollama
    default_llm_model: str = "gpt-4o"
    default_llm_api_key: str = ""

    # Additional LLM keys
    openai_api_key: str = ""
    anthropic_api_key: str = ""
    grok_api_key: str = ""
    ollama_base_url: str = "http://localhost:11434"
    openai_base_url: str = "https://api.openai.com/v1"
    grok_base_url: str = "https://api.x.ai/v1"

    # App
    news_poll_interval: int = 60  # seconds
    analysis_batch_size: int = 10
    x_sentiment_interval: int = 1800  # seconds (30 minutes)
    database_url: str = "sqlite+aiosqlite:///data/macrolens.db"
    cors_origins: str = ""  # comma-separated origins
    admin_token: str = ""

    class Config:
        env_file = _find_env_file()
        env_file_encoding = "utf-8"

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
