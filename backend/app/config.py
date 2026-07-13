import logging
import os
import ipaddress
from pathlib import Path
from typing import Literal, Optional
from urllib.parse import urlparse

from pydantic import Field, field_validator, model_validator
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
    default_llm_model: str = Field(default="gpt-5.6-terra", min_length=1, max_length=200)
    default_llm_api_key: str = ""

    # Additional LLM keys
    openai_api_key: str = ""
    anthropic_api_key: str = ""
    grok_api_key: str = ""
    grok_model: str = Field(default="grok-4", min_length=1, max_length=200)
    ollama_base_url: str = "http://localhost:11434"
    openai_base_url: str = "https://api.openai.com/v1"
    openai_allow_custom_base_url: bool = False
    openai_allow_local_http: bool = False
    grok_base_url: str = "https://api.x.ai/v1"

    # OpenAI Responses runtime. Long-running requests are executed only by the
    # dedicated analysis worker; web requests never wait for model completion.
    openai_reasoning: Literal["none", "low", "medium", "high", "xhigh", "max"] = "max"
    openai_execution_mode: Literal["background", "worker_sync"] = "background"
    openai_sync_timeout_seconds: int = Field(default=900, ge=1, le=1800)
    openai_background_poll_timeout_seconds: int = Field(default=1800, ge=30, le=3600)
    openai_background_initial_poll_seconds: int = Field(default=2, ge=1, le=60)
    openai_background_max_poll_seconds: int = Field(default=15, ge=1, le=120)
    # Provider-wide safety ceiling. Individual task types use their own lower
    # limits so a news item and a market-focus cycle never share one budget.
    openai_max_output_tokens: int = Field(default=128000, ge=256, le=128000)
    openai_max_concurrency: int = Field(default=2, ge=1, le=16)
    # Background submission retries are owned by the durable job state machine.
    # SDK-level retries could create an unlinked duplicate paid response.
    openai_max_retries: int = Field(default=0, ge=0, le=0)
    news_impact_prompt_version: str = Field(default="news-impact-v2", min_length=1, max_length=100)
    news_impact_schema_version: str = Field(default="news-impact-schema-v2", min_length=1, max_length=100)

    # Persistent analysis queue and cost gates.
    news_llm_auto_analyze_enabled: bool = False
    news_llm_manual_enabled: bool = False
    news_item_max_output_tokens: int = Field(default=32768, ge=256, le=128000)
    news_llm_max_inflight: int = Field(default=2, ge=1, le=16)
    news_llm_max_queued: int = Field(default=200, ge=1, le=10_000)
    news_llm_daily_job_limit: Optional[int] = Field(default=None, ge=1, le=1_000_000)
    news_llm_daily_output_token_limit: Optional[int] = Field(default=None, ge=1, le=1_000_000_000)
    news_llm_manual_daily_job_limit: Optional[int] = Field(default=None, ge=1, le=1_000_000)
    news_llm_manual_daily_output_token_limit: Optional[int] = Field(default=None, ge=1, le=1_000_000_000)
    news_llm_min_context_chars: int = Field(default=100, ge=1, le=10_000)
    news_llm_min_market_relevance: int = Field(default=35, ge=0, le=100)
    analysis_worker_poll_seconds: int = Field(default=5, ge=1, le=300)
    analysis_worker_lease_seconds: int = Field(default=120, ge=30, le=1800)
    analysis_worker_quick_check_interval_seconds: int = Field(
        default=1800,
        ge=60,
        le=86400,
    )
    analysis_job_retry_cooldown_seconds: int = Field(default=300, ge=1, le=86400)

    # Pull-only focus context from option-pro. Empty credentials keep the
    # capability disabled while preserving the last local snapshot.
    option_pro_focus_base_url: str = ""
    option_pro_focus_key_id: str = Field(default="", max_length=128)
    option_pro_focus_secret: str = Field(default="", max_length=4096)
    option_pro_focus_verify_tls: bool = True
    option_pro_focus_ca_bundle: str = ""
    option_pro_focus_interval_seconds: int = Field(default=1800, ge=60, le=86400)
    option_pro_focus_connect_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    option_pro_focus_read_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    # Bounds the complete pull, including retries and backoff.
    option_pro_focus_timeout_seconds: int = Field(default=20, ge=1, le=120)
    option_pro_focus_max_response_bytes: int = Field(
        default=1_048_576,
        ge=1_024,
        le=8_388_608,
    )
    option_pro_focus_max_attempts: int = Field(default=3, ge=1, le=5)
    option_pro_focus_retry_backoff_seconds: float = Field(default=0.25, ge=0, le=5)
    option_pro_focus_circuit_failure_threshold: int = Field(default=3, ge=1, le=20)
    option_pro_focus_circuit_reset_seconds: int = Field(default=60, ge=1, le=3600)
    # Every focus persistence call performs at most one bounded revalidation
    # slice.  A durable cursor resumes the same round on the next pull.
    focus_revalidation_max_rows_per_run: int = Field(default=1000, ge=1, le=10000)
    focus_revalidation_max_seconds_per_run: float = Field(default=2.0, gt=0, le=30)
    focus_revalidation_batch_size: int = Field(default=200, ge=1, le=1000)
    focus_revalidation_resume_interval_seconds: int = Field(default=60, ge=5, le=3600)

    # Deterministic hotspot preparation and bounded market-focus cycles.
    hotspot_direct_threshold: float = Field(default=75.0, ge=0, le=100)
    hotspot_conditional_threshold: float = Field(default=60.0, ge=0, le=100)
    hotspot_gate_version: str = Field(default="hotspot-gate-v2", min_length=1, max_length=100)
    hotspot_market_data_quality_min: float = Field(default=0.6, ge=0, le=1)
    hot_cycle_enabled: bool = False
    hot_cycle_schedule_enabled: bool = False
    hot_cycle_times_et: str = "08:00,12:00,16:00"
    hot_cycle_optional_20_et: bool = False
    hot_cycle_manual_enabled: bool = False
    hot_cycle_manual_cooldown_seconds: int = Field(default=900, ge=0, le=86400)
    hot_cycle_max_events: int = Field(default=8, ge=1, le=20)
    hot_cycle_max_focus_symbols: int = Field(default=20, ge=1, le=40)
    hot_cycle_model: str = Field(default="gpt-5.6-terra", min_length=1, max_length=200)
    hot_cycle_reasoning: Literal["none", "low", "medium", "high", "xhigh", "max"] = "max"
    hot_cycle_max_output_tokens: int = Field(default=49152, ge=256, le=128000)
    hot_cycle_daily_job_limit: Optional[int] = Field(default=None, ge=1, le=1_000_000)
    hot_cycle_daily_output_token_limit: Optional[int] = Field(default=None, ge=1, le=1_000_000_000)
    hot_cycle_prompt_version: str = Field(default="market-focus-v1", min_length=1, max_length=100)
    hot_cycle_schema_version: str = Field(default="market-focus-schema-v1", min_length=1, max_length=100)
    # Bootstrap calibration for display-only Catalyst context.  This does not
    # participate in the formal stock score.
    catalyst_context_support_target: float = Field(default=80.0, gt=0, le=1000)

    # Option Pro Integration API. Empty keys keep the remote surface disabled
    # while the ordinary MacroLens application remains fully operational.
    option_pro_read_key_id: str = Field(default="", max_length=128)
    option_pro_read_secret: str = Field(default="", max_length=4096)
    option_pro_action_key_id: str = Field(default="", max_length=128)
    option_pro_action_secret: str = Field(default="", max_length=4096)
    option_pro_previous_read_secret: str = Field(default="", max_length=4096)
    option_pro_previous_action_secret: str = Field(default="", max_length=4096)
    option_pro_allowed_cidrs: str = ""
    option_pro_signature_clock_skew_seconds: int = Field(default=300, ge=30, le=3600)
    option_pro_nonce_ttl_seconds: int = Field(default=600, ge=60, le=7200)
    option_pro_source_stale_after_seconds: int = Field(default=86400, ge=60, le=604800)
    option_pro_trusted_proxy_cidrs: str = ""
    option_pro_allow_local_http: bool = False

    # App
    analysis_batch_size: int = Field(default=10, ge=1, le=100)
    x_sentiment_enabled: bool = False
    x_sentiment_interval: int = Field(default=21600, ge=300)
    database_url: str = "sqlite+aiosqlite:///data/macrolens.db"
    cors_origins: str = ""  # comma-separated origins
    admin_token: str = ""
    session_cookie_secure: bool = False
    session_ttl_seconds: int = Field(default=28800, ge=300, le=604800)

    # News-source scheduling. Paid quota-limited aggregators stay opt-in.
    finnhub_news_enabled: bool = True
    finnhub_news_interval: int = Field(default=300, ge=30)
    finnhub_focus_interval: int = Field(default=1800, ge=1800, le=3600)
    massive_news_enabled: bool = True
    massive_news_interval: int = Field(default=3600, ge=30)
    massive_focus_interval: int = Field(default=2700, ge=1800, le=3600)
    massive_focus_request_limit: int = Field(default=10, ge=1, le=40)
    finnhub_focus_request_limit: int = Field(default=20, ge=1, le=40)
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

    # Economic-calendar analysis has its own queue and spend limits. It still
    # shares the provider-wide OpenAI concurrency ceiling with news analysis.
    calendar_analysis_prompt_version: str = Field(
        default="calendar-impact-v1", min_length=1, max_length=100
    )
    calendar_analysis_schema_version: str = Field(
        default="calendar-impact-schema-v1", min_length=1, max_length=100
    )
    calendar_llm_manual_enabled: bool = False
    calendar_llm_max_inflight: int = Field(default=1, ge=1, le=16)
    calendar_llm_max_queued: int = Field(default=10, ge=1, le=10_000)
    calendar_max_output_tokens: int = Field(default=16384, ge=256, le=128000)
    calendar_llm_daily_job_limit: Optional[int] = Field(
        default=None, ge=1, le=1_000_000
    )
    calendar_llm_daily_output_token_limit: Optional[int] = Field(
        default=None, ge=1, le=1_000_000_000
    )
    calendar_analysis_cache_ttl: int = Field(default=3600, ge=60, le=86400)
    calendar_fetch_interval_seconds: int = Field(default=600, ge=60, le=86400)
    analysis_retention_limit: int = Field(default=350, ge=1, le=100000)
    news_retention_days: Optional[int] = Field(default=None, ge=1, le=3650)
    x_sentiment_retention_days: Optional[int] = Field(default=None, ge=1, le=3650)
    news_item_retention_days: Optional[int] = Field(default=None, ge=1, le=3650)
    analysis_job_retention_days: Optional[int] = Field(default=None, ge=1, le=3650)
    analysis_revision_retention_days: Optional[int] = Field(default=None, ge=1, le=3650)
    stock_impact_retention_days: Optional[int] = Field(default=None, ge=1, le=3650)
    event_group_retention_days: Optional[int] = Field(default=None, ge=1, le=3650)
    analysis_cycle_retention_days: Optional[int] = Field(default=None, ge=1, le=3650)
    calendar_revision_retention_days: Optional[int] = Field(default=None, ge=1, le=3650)
    integration_change_retention_days: Optional[int] = Field(default=None, ge=1, le=3650)
    hotspot_preparation_retention_days: int = Field(default=90, ge=1, le=3650)
    market_focus_completed_retention_days: int = Field(default=365, ge=1, le=3650)
    market_focus_failed_retention_days: int = Field(default=30, ge=1, le=3650)
    focus_snapshot_retention_days: int = Field(default=90, ge=1, le=3650)
    focus_snapshot_full_resolution_days: int = Field(default=30, ge=1, le=3650)
    focus_snapshot_daily_rollup_enabled: bool = True
    event_member_retention_days: int = Field(default=90, ge=1, le=3650)
    projection_retry_retention_days: int = Field(default=30, ge=1, le=3650)
    projection_retry_max_attempts: int = Field(default=6, ge=1, le=100)
    retention_batch_size: int = Field(default=500, ge=1, le=10000)

    @field_validator(
        "default_llm_model",
        "news_impact_prompt_version",
        "news_impact_schema_version",
        "calendar_analysis_prompt_version",
        "calendar_analysis_schema_version",
        "hotspot_gate_version",
        "hot_cycle_model",
        "hot_cycle_prompt_version",
        "hot_cycle_schema_version",
    )
    @classmethod
    def validate_bounded_identifier(cls, value: str) -> str:
        value = value.strip()
        if not value or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-" for ch in value):
            raise ValueError("must be a bounded model or version identifier")
        return value

    @field_validator("option_pro_read_key_id", "option_pro_action_key_id", "option_pro_focus_key_id")
    @classmethod
    def validate_key_id(cls, value: str) -> str:
        value = value.strip()
        if value and any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-" for ch in value):
            raise ValueError("key id contains unsupported characters")
        return value

    @field_validator("option_pro_allowed_cidrs", "option_pro_trusted_proxy_cidrs")
    @classmethod
    def validate_integration_cidrs(cls, value: str) -> str:
        normalized: list[str] = []
        for item in value.split(","):
            item = item.strip()
            if not item:
                continue
            try:
                normalized.append(str(ipaddress.ip_network(item, strict=False)))
            except ValueError as exc:
                raise ValueError("integration CIDR list contains an invalid network") from exc
        return ",".join(normalized)

    @field_validator(
        "option_pro_read_secret",
        "option_pro_action_secret",
        "option_pro_previous_read_secret",
        "option_pro_previous_action_secret",
        "option_pro_focus_secret",
    )
    @classmethod
    def validate_integration_secret(cls, value: str) -> str:
        value = value.strip()
        if value and len(value.encode("utf-8")) < 32:
            raise ValueError("integration HMAC secrets must contain at least 32 bytes")
        return value

    @model_validator(mode="after")
    def validate_sensitive_endpoints_and_credentials(self):
        if self.focus_snapshot_full_resolution_days > self.focus_snapshot_retention_days:
            raise ValueError(
                "FOCUS_SNAPSHOT_FULL_RESOLUTION_DAYS must not exceed "
                "FOCUS_SNAPSHOT_RETENTION_DAYS"
            )
        if any(
            value > self.openai_max_output_tokens
            for value in (
                self.news_item_max_output_tokens,
                self.hot_cycle_max_output_tokens,
                self.calendar_max_output_tokens,
            )
        ):
            raise ValueError("task output-token limits must not exceed OPENAI_MAX_OUTPUT_TOKENS")
        parsed = urlparse(self.openai_base_url.strip())
        if (
            not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("OPENAI_BASE_URL must be an absolute URL without credentials, query, or fragment")
        hostname = parsed.hostname.rstrip(".").lower()
        official = hostname == "api.openai.com"
        if not official and not self.openai_allow_custom_base_url:
            raise ValueError("custom OPENAI_BASE_URL requires OPENAI_ALLOW_CUSTOM_BASE_URL=true")
        if parsed.scheme != "https":
            local = hostname == "localhost"
            if not local:
                try:
                    local = ipaddress.ip_address(hostname).is_loopback
                except ValueError:
                    local = False
            if not (parsed.scheme == "http" and local and self.openai_allow_local_http):
                raise ValueError("OPENAI_BASE_URL must use HTTPS")

        pairs = (
            ("read", self.option_pro_read_key_id, self.option_pro_read_secret),
            ("action", self.option_pro_action_key_id, self.option_pro_action_secret),
        )
        for label, key_id, secret in pairs:
            if bool(key_id) != bool(secret):
                raise ValueError(f"Option Pro {label} key id and secret must be configured together")
        if (
            self.option_pro_read_key_id
            and self.option_pro_read_key_id == self.option_pro_action_key_id
        ):
            raise ValueError("Option Pro read and action key ids must be different")
        if self.option_pro_previous_read_secret and not self.option_pro_read_secret:
            raise ValueError("previous read secret requires a current read secret")
        if self.option_pro_previous_action_secret and not self.option_pro_action_secret:
            raise ValueError("previous action secret requires a current action secret")
        if self.option_pro_previous_read_secret == self.option_pro_read_secret and self.option_pro_read_secret:
            raise ValueError("previous and current read secrets must be different")
        if self.option_pro_previous_action_secret == self.option_pro_action_secret and self.option_pro_action_secret:
            raise ValueError("previous and current action secrets must be different")
        if (self.option_pro_read_key_id or self.option_pro_action_key_id) and not self.option_pro_allowed_cidrs:
            raise ValueError("Option Pro keys require a non-empty OPTION_PRO_ALLOWED_CIDRS allow-list")
        if self.option_pro_nonce_ttl_seconds < self.option_pro_signature_clock_skew_seconds:
            raise ValueError("nonce TTL must be at least the signature clock-skew window")
        if bool(self.option_pro_focus_key_id) != bool(self.option_pro_focus_secret):
            raise ValueError("Option Pro focus key id and secret must be configured together")
        focus_url = self.option_pro_focus_base_url.strip()
        if focus_url:
            focus = urlparse(focus_url)
            if (
                focus.scheme != "https"
                or not focus.hostname
                or focus.username
                or focus.password
                or focus.query
                or focus.fragment
                or focus.params
                or focus.path not in {"", "/"}
            ):
                raise ValueError("OPTION_PRO_FOCUS_BASE_URL must be an HTTPS origin without credentials, query, or fragment")
            if not self.option_pro_focus_verify_tls:
                raise ValueError("OPTION_PRO_FOCUS_VERIFY_TLS must remain enabled")
            if self.option_pro_focus_timeout_seconds < max(
                self.option_pro_focus_connect_timeout_seconds,
                self.option_pro_focus_read_timeout_seconds,
            ):
                raise ValueError(
                    "OPTION_PRO_FOCUS_TIMEOUT_SECONDS must cover connect and read timeouts"
                )
        return self

    @property
    def automatic_news_analysis_capability(self) -> str:
        if not self.news_llm_auto_analyze_enabled:
            return "disabled"
        if self.news_llm_daily_job_limit is None or self.news_llm_daily_output_token_limit is None:
            return "budget_configuration_required"
        return "enabled"

    @property
    def manual_news_analysis_capability(self) -> str:
        if not self.news_llm_manual_enabled:
            return "disabled"
        if (
            self.news_llm_manual_daily_job_limit is None
            or self.news_llm_manual_daily_output_token_limit is None
        ):
            return "budget_configuration_required"
        return "enabled"

    @property
    def manual_calendar_analysis_capability(self) -> str:
        if not self.calendar_llm_manual_enabled:
            return "disabled"
        if (
            self.calendar_llm_daily_job_limit is None
            or self.calendar_llm_daily_output_token_limit is None
        ):
            return "budget_configuration_required"
        return "enabled"

    @property
    def automatic_hot_cycle_capability(self) -> str:
        if not self.hot_cycle_enabled:
            return "disabled"
        if self.hot_cycle_daily_job_limit is None or self.hot_cycle_daily_output_token_limit is None:
            return "budget_configuration_required"
        return "enabled"

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
