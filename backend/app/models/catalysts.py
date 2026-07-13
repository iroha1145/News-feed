from __future__ import annotations

import re
from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Any, Literal, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from app.models.market_focus import MarketFocusCyclePublicAnalysis


SCHEMA_VERSION = "macrolens-option-pro-v1"

BoundedText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=2000)]
ShortText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]
CompanyText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
Ticker = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        to_upper=True,
        min_length=1,
        max_length=20,
        pattern=r"^[A-Z0-9][A-Z0-9.^/_-]{0,19}$",
    ),
]


class StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        allow_inf_nan=False,
        str_strip_whitespace=True,
        validate_assignment=True,
    )


class UTCModel(StrictModel):
    @field_validator("*", mode="before")
    @classmethod
    def reject_naive_datetimes(cls, value: Any, info):
        if value is None:
            return value
        annotation = cls.model_fields.get(info.field_name)
        if annotation is None:
            return value
        text = str(annotation.annotation)
        if "datetime" not in text:
            return value
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return value
        else:
            return value
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("datetime must include a timezone")
        return parsed.astimezone(timezone.utc)

    @field_serializer("*", when_used="json", check_fields=False)
    def serialize_utc(self, value: Any):
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return value


class Sentiment(str, Enum):
    bullish = "bullish"
    bearish = "bearish"
    neutral = "neutral"


class ImpactHorizon(str, Enum):
    intraday = "intraday"
    days = "days"
    weeks = "weeks"
    uncertain = "uncertain"


class ImpactMechanism(str, Enum):
    direct_company = "direct_company"
    supplier_customer = "supplier_customer"
    sector_readthrough = "sector_readthrough"
    macro_rate = "macro_rate"
    commodity_input = "commodity_input"
    regulatory = "regulatory"
    competitive = "competitive"
    other = "other"


class AnalysisJobStatus(str, Enum):
    pending = "pending"
    queued = "queued"
    in_progress = "in_progress"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"
    insufficient_context = "insufficient_context"
    budget_blocked = "budget_blocked"
    incomplete_output = "incomplete_output"


class AnalysisStatus(str, Enum):
    not_requested = "not_requested"
    pending = "pending"
    queued = "queued"
    in_progress = "in_progress"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"
    insufficient_context = "insufficient_context"
    budget_blocked = "budget_blocked"
    incomplete_output = "incomplete_output"


class PublicDataStatus(str, Enum):
    active = "active"
    empty = "empty"
    stale = "stale"
    unavailable = "unavailable"


class AffectedStockImpact(StrictModel):
    ticker: Ticker
    company: CompanyText
    impact_score: StrictInt = Field(ge=-100, le=100)
    confidence: StrictInt = Field(ge=0, le=100)
    horizon: ImpactHorizon
    mechanism: ImpactMechanism
    reason: BoundedText


class AffectedCommodityImpact(StrictModel):
    name: ShortText
    impact_score: StrictInt = Field(ge=-100, le=100)
    reason: BoundedText


class NewsImpactAnalysis(StrictModel):
    title_zh: ShortText
    headline_summary: BoundedText
    overall_sentiment: StrictInt = Field(ge=-100, le=100)
    classification: Sentiment
    confidence: StrictInt = Field(ge=0, le=100)
    market_relevance: StrictInt = Field(ge=0, le=100)
    affected_stocks: list[AffectedStockImpact] = Field(default_factory=list, max_length=50)
    affected_sectors: list[ShortText] = Field(default_factory=list, max_length=50)
    affected_commodities: list[AffectedCommodityImpact] = Field(default_factory=list, max_length=30)
    causal_summary: BoundedText
    key_factors: list[ShortText] = Field(default_factory=list, max_length=30)
    uncertainty_notes: list[ShortText] = Field(default_factory=list, max_length=30)
    insufficient_context: StrictBool

    @field_validator("affected_stocks")
    @classmethod
    def unique_stock_tickers(cls, values: list[AffectedStockImpact]) -> list[AffectedStockImpact]:
        seen: set[str] = set()
        for value in values:
            if value.ticker in seen:
                raise ValueError("affected_stocks contains a duplicate ticker")
            seen.add(value.ticker)
        return values


class PublicAnalysis(NewsImpactAnalysis, UTCModel):
    analysis_id: StrictInt = Field(ge=1)
    revision: StrictInt = Field(ge=1)
    model: Annotated[str, StringConstraints(min_length=1, max_length=200)]
    reasoning: Literal["none", "low", "medium", "high", "xhigh", "max"]
    prompt_version: Annotated[str, StringConstraints(min_length=1, max_length=100)]
    schema_version: Annotated[str, StringConstraints(min_length=1, max_length=100)]
    analyzed_at: datetime
    available_at: datetime


class ContractResponse(UTCModel):
    schema_version: Literal[SCHEMA_VERSION] = SCHEMA_VERSION
    schema_sha256: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    request_id: Annotated[str, StringConstraints(min_length=8, max_length=100)]


class CatalystItem(UTCModel):
    news_id: StrictInt = Field(ge=1)
    content_hash: Annotated[str, StringConstraints(min_length=8, max_length=128)]
    source: ShortText
    title: BoundedText
    summary: Optional[Annotated[str, StringConstraints(max_length=20_000)]] = None
    url: Annotated[str, StringConstraints(min_length=1, max_length=4096)]
    published_at: Optional[datetime] = None
    fetched_at: datetime
    updated_at: datetime
    change_sequence: StrictInt = Field(ge=1)
    source_tickers: list[Ticker] = Field(default_factory=list, max_length=100)
    analysis_status: AnalysisStatus
    analysis: Optional[PublicAnalysis] = None
    analyzed_at: Optional[datetime] = None
    available_at: Optional[datetime] = None
    is_stale: StrictBool = False


class FeedResponse(ContractResponse):
    as_of: datetime
    data_through: Optional[datetime] = None
    items: list[CatalystItem]
    next_cursor: Optional[Annotated[str, StringConstraints(min_length=1, max_length=4096)]] = None
    has_more: StrictBool


class LatestResponse(ContractResponse):
    snapshot_token: Annotated[str, StringConstraints(min_length=8, max_length=200)]
    data_through: Optional[datetime] = None
    next_updated_after: Optional[datetime] = None
    next_cursor: Optional[Annotated[str, StringConstraints(min_length=1, max_length=4096)]] = None
    has_more: StrictBool
    items: list[CatalystItem]


class NewsResponse(ContractResponse):
    item: CatalystItem


class CatalystTickerResponse(ContractResponse):
    ticker: Ticker
    status: PublicDataStatus
    as_of: datetime
    data_through: Optional[datetime] = None
    items: list[CatalystItem]
    next_cursor: Optional[Annotated[str, StringConstraints(min_length=1, max_length=4096)]] = None
    has_more: StrictBool


class CatalystBatchRequest(UTCModel):
    tickers: list[Ticker] = Field(min_length=1, max_length=50)
    as_of: Optional[datetime] = None
    window_hours: StrictInt = Field(default=72, ge=1, le=24 * 30)
    limit: StrictInt = Field(default=20, ge=1, le=100)
    min_confidence: StrictInt = Field(default=0, ge=0, le=100)
    include_neutral: StrictBool = False
    include_unanalyzed: StrictBool = True

    @field_validator("tickers")
    @classmethod
    def unique_tickers(cls, values: list[str]) -> list[str]:
        if len(set(values)) != len(values):
            raise ValueError("tickers must be unique")
        return values


class BatchTickerResult(UTCModel):
    status: PublicDataStatus
    data_through: Optional[datetime] = None
    items: list[CatalystItem]
    next_cursor: Optional[Annotated[str, StringConstraints(min_length=1, max_length=4096)]] = None


class CatalystBatchResponse(ContractResponse):
    as_of: datetime
    results: dict[Ticker, BatchTickerResult]


class CalendarEvent(UTCModel):
    event_id: Annotated[str, StringConstraints(min_length=8, max_length=128)]
    currency: Annotated[str, StringConstraints(pattern=r"^[A-Z]{3}$")]
    title: BoundedText
    impact: Literal["low", "medium", "high", "holiday"]
    scheduled_at: datetime
    forecast: Optional[Annotated[str, StringConstraints(max_length=500)]] = None
    previous: Optional[Annotated[str, StringConstraints(max_length=500)]] = None
    actual: Optional[Annotated[str, StringConstraints(max_length=500)]] = None
    is_stale: StrictBool
    source_fetched_at: datetime
    available_at: datetime


class CalendarResponse(ContractResponse):
    as_of: datetime
    data_through: Optional[datetime] = None
    items: list[CalendarEvent]


class QueueHealth(UTCModel):
    status: Literal["ok", "degraded", "unavailable", "not_configured"]
    pending: StrictInt = Field(ge=0)
    queued: StrictInt = Field(ge=0)
    in_progress: StrictInt = Field(ge=0)
    oldest_job_at: Optional[datetime] = None
    budget_status: Literal["ok", "budget_configuration_required", "budget_blocked"]


class ComponentHealth(UTCModel):
    status: Literal["ok", "degraded", "unavailable", "not_configured", "disabled"]
    last_attempt_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    data_through: Optional[datetime] = None
    consecutive_failures: StrictInt = Field(default=0, ge=0)
    next_attempt_at: Optional[datetime] = None
    raw_count: Optional[StrictInt] = Field(default=None, ge=0)
    inserted_count: Optional[StrictInt] = Field(default=None, ge=0)
    duplicates_count: Optional[StrictInt] = Field(default=None, ge=0)
    detail: Optional[Annotated[str, StringConstraints(max_length=500)]] = None


class IntegrationHealthResponse(ContractResponse):
    status: Literal["ok", "degraded", "unavailable", "not_configured"]
    as_of: datetime
    data_through: Optional[datetime] = None
    database: ComponentHealth
    scheduler: ComponentHealth
    analysis_queue: QueueHealth
    model: Annotated[str, StringConstraints(min_length=1, max_length=200)]
    reasoning: Literal["none", "low", "medium", "high", "xhigh", "max"]
    execution_mode: Literal["background", "worker_sync"]
    analysis_trigger_enabled: StrictBool
    sources: dict[Annotated[str, StringConstraints(min_length=1, max_length=100)], ComponentHealth]
    warnings: list[Annotated[str, StringConstraints(min_length=1, max_length=500)]] = Field(max_length=50)


class AnalysisJobCreateRequest(StrictModel):
    news_id: StrictInt = Field(ge=1)
    expected_content_hash: Annotated[str, StringConstraints(min_length=8, max_length=128)]
    expected_change_sequence: Optional[StrictInt] = Field(default=None, ge=1)
    force: StrictBool = False


class AnalysisJobResponse(ContractResponse):
    job_id: Annotated[str, StringConstraints(min_length=8, max_length=100)]
    news_id: StrictInt = Field(ge=1)
    content_hash: Annotated[str, StringConstraints(min_length=8, max_length=128)]
    input_hash: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    change_sequence: Optional[StrictInt] = Field(default=None, ge=1)
    status: AnalysisJobStatus
    model: Annotated[str, StringConstraints(min_length=1, max_length=200)]
    reasoning: Literal["none", "low", "medium", "high", "xhigh", "max"]
    submitted_at: Optional[datetime] = None
    updated_at: datetime
    completed_at: Optional[datetime] = None
    error_code: Optional[Annotated[str, StringConstraints(min_length=1, max_length=100)]] = None
    retry_after: Optional[StrictInt] = Field(default=None, ge=0)
    result: Optional[PublicAnalysis] = None


class ErrorBody(ContractResponse):
    code: Annotated[str, StringConstraints(min_length=1, max_length=100)]
    message: Annotated[str, StringConstraints(min_length=1, max_length=500)]
    retryable: StrictBool
    retry_after_seconds: Optional[StrictInt] = Field(default=None, ge=0)
    resync_from: Optional[datetime] = None
    server_time: Optional[datetime] = None
    latest_window_days: Optional[StrictInt] = Field(default=None, ge=1, le=30)


class MarketFocusCycleCreateRequest(StrictModel):
    trigger: Literal["manual", "scheduled_0800", "scheduled_1200", "scheduled_1600", "scheduled_2000"] = "manual"
    expected_prepared_revision: Optional[StrictInt] = Field(default=None, ge=0)
    retry_cycle_id: Optional[Annotated[str, StringConstraints(pattern=r"^mfc_[a-f0-9]{32}$")]] = None

    @model_validator(mode="after")
    def validate_retry_shape(self):
        if self.retry_cycle_id is not None and self.expected_prepared_revision is not None:
            raise ValueError("retry_cycle_id and expected_prepared_revision are mutually exclusive")
        return self


class HotspotStatusResponse(ContractResponse):
    prepared_revision: StrictInt = Field(ge=0)
    last_consumed_revision: StrictInt = Field(ge=0)
    prepared_hot_count: StrictInt = Field(ge=0)
    prepared_since: Optional[datetime] = None
    last_cycle_at: Optional[datetime] = None
    next_scheduled_at: Optional[datetime] = None
    active_cycle_id: Optional[
        Annotated[str, StringConstraints(pattern=r"^mfc_[a-f0-9]{32}$")]
    ] = None
    cooldown_until: Optional[datetime] = None
    manual_enabled: StrictBool
    capability: Literal["enabled", "disabled", "budget_configuration_required"]
    model: Annotated[str, StringConstraints(min_length=1, max_length=200)]
    reasoning: Literal["none", "low", "medium", "high", "xhigh", "max"]
    data_through: Optional[datetime] = None


class HotspotPreparationItem(UTCModel):
    prepared_revision: StrictInt = Field(ge=1)
    event_group_id: Annotated[str, StringConstraints(min_length=1, max_length=100)]
    event_group_version: StrictInt = Field(ge=1)
    gate_version: Annotated[str, StringConstraints(min_length=1, max_length=100)]
    hot_score: float = Field(ge=0, le=100)
    component_scores: dict[str, Optional[float]]
    active_weights: dict[str, float]
    reasons: list[ShortText] = Field(default_factory=list, max_length=30)
    event_snapshot_json: Annotated[str, StringConstraints(min_length=2, max_length=100_000)]
    status: Literal["PREPARED", "LEASED", "CONSUMED"]
    prepared_at: datetime
    leased_cycle_id: Optional[
        Annotated[str, StringConstraints(pattern=r"^mfc_[a-f0-9]{32}$")]
    ] = None
    consumed_cycle_id: Optional[
        Annotated[str, StringConstraints(pattern=r"^mfc_[a-f0-9]{32}$")]
    ] = None
    consumed_at: Optional[datetime] = None
    created_at: datetime
    representative_title: BoundedText
    event_type: Annotated[str, StringConstraints(min_length=1, max_length=100)]
    available_at: datetime
    first_published_at: Optional[datetime] = None
    last_published_at: Optional[datetime] = None
    source_count: StrictInt = Field(ge=1)
    source_names: list[ShortText] = Field(default_factory=list, max_length=100)
    validated_tickers: list[Ticker] = Field(default_factory=list, max_length=100)


class HotspotListResponse(ContractResponse):
    as_of: datetime
    items: list[HotspotPreparationItem] = Field(default_factory=list, max_length=100)


class MarketFocusCyclePublic(UTCModel):
    cycle_id: Annotated[str, StringConstraints(pattern=r"^mfc_[a-f0-9]{32}$")]
    scheduled_slot: Optional[Annotated[str, StringConstraints(max_length=100)]] = None
    idempotency_key: Annotated[str, StringConstraints(min_length=1, max_length=200)]
    retry_of_cycle_id: Optional[
        Annotated[str, StringConstraints(pattern=r"^mfc_[a-f0-9]{32}$")]
    ] = None
    execution_number: StrictInt = Field(ge=1)
    trigger_type: Literal[
        "manual", "scheduled_0800", "scheduled_1200", "scheduled_1600", "scheduled_2000"
    ]
    status: Literal[
        "pending", "queued", "in_progress", "completed", "failed", "cancelled",
        "budget_blocked", "incomplete_output", "insufficient_context",
    ]
    no_new_hot_events: StrictBool
    prepared_revision: StrictInt = Field(ge=0)
    last_consumed_revision_at_start: StrictInt = Field(ge=0)
    consumes_through_revision: Optional[StrictInt] = Field(default=None, ge=1)
    focus_revision: Optional[StrictInt] = Field(default=None, ge=1)
    snapshot_as_of: datetime
    input_schema_version: Annotated[str, StringConstraints(min_length=1, max_length=100)]
    input_hash: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    event_group_count: StrictInt = Field(ge=0)
    focus_symbol_count: StrictInt = Field(ge=0)
    provider: Annotated[str, StringConstraints(min_length=1, max_length=100)]
    model: Annotated[str, StringConstraints(min_length=1, max_length=200)]
    reasoning_effort: Literal["none", "low", "medium", "high", "xhigh", "max"]
    execution_mode: Literal["background", "worker_sync"]
    max_output_tokens: StrictInt = Field(ge=256, le=128_000)
    prompt_version: Annotated[str, StringConstraints(min_length=1, max_length=100)]
    output_schema_version: Annotated[str, StringConstraints(min_length=1, max_length=100)]
    result: Optional[MarketFocusCyclePublicAnalysis] = None
    error_code: Optional[Annotated[str, StringConstraints(max_length=100)]] = None
    attempt_count: StrictInt = Field(ge=0)
    retrieve_error_count: StrictInt = Field(ge=0)
    cancel_attempt_count: StrictInt = Field(ge=0)
    next_attempt_at: Optional[datetime] = None
    cancel_requested_at: Optional[datetime] = None
    latency_ms: Optional[StrictInt] = Field(default=None, ge=0)
    usage_input_tokens: StrictInt = Field(ge=0)
    usage_cached_input_tokens: StrictInt = Field(ge=0)
    usage_cache_write_tokens: StrictInt = Field(ge=0)
    usage_reasoning_tokens: StrictInt = Field(ge=0)
    usage_output_tokens: StrictInt = Field(ge=0)
    usage_total_tokens: StrictInt = Field(ge=0)
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    updated_at: datetime


class MarketFocusCycleResponse(ContractResponse):
    cycle: Optional[MarketFocusCyclePublic] = None
