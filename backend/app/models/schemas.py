from datetime import datetime
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator


class SentimentClassification(str, Enum):
    bullish = "bullish"
    bearish = "bearish"
    neutral = "neutral"


class NewsItemBase(BaseModel):
    source: str
    title: str
    summary: Optional[str] = None
    url: str
    image_url: Optional[str] = None
    published_at: Optional[datetime] = None


class NewsItemCreate(NewsItemBase):
    content_hash: str


class NewsItem(NewsItemBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    fetched_at: datetime
    content_hash: str


class AffectedStock(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    ticker: str = Field(min_length=1, max_length=20)
    company: str = Field(min_length=1, max_length=200)
    impact_score: StrictInt = Field(ge=-100, le=100)
    reason: str = Field(min_length=1, max_length=2000)

    @field_validator("ticker")
    @classmethod
    def normalize_ticker(cls, value: str) -> str:
        return value.upper()


class AffectedCommodity(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: str = Field(min_length=1, max_length=100)
    impact_score: StrictInt = Field(ge=-100, le=100)
    reason: str = Field(min_length=1, max_length=2000)


class AnalysisBase(BaseModel):
    overall_sentiment: StrictInt = Field(ge=-100, le=100)
    classification: SentimentClassification
    confidence: StrictInt = Field(ge=0, le=100)
    affected_stocks: list[AffectedStock] = Field(default_factory=list)
    affected_sectors: list[str] = Field(default_factory=list)
    affected_commodities: list[AffectedCommodity] = Field(default_factory=list)
    logic_chain: str
    key_factors: list[str] = Field(default_factory=list)
    llm_provider: str
    llm_model: str


class LLMAnalysisPayload(BaseModel):
    """Strict contract for untrusted structured output returned by an LLM."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    title_zh: str = Field(max_length=500)
    headline_summary: str = Field(max_length=2000)
    overall_sentiment: StrictInt = Field(ge=-100, le=100)
    classification: SentimentClassification
    confidence: StrictInt = Field(ge=0, le=100)
    affected_stocks: list[AffectedStock] = Field(default_factory=list, max_length=50)
    affected_sectors: list[str] = Field(default_factory=list, max_length=50)
    affected_commodities: list[AffectedCommodity] = Field(default_factory=list, max_length=30)
    logic_chain: str = Field(min_length=1, max_length=8000)
    key_factors: list[str] = Field(default_factory=list, max_length=50)

    @field_validator("affected_sectors", "key_factors")
    @classmethod
    def validate_text_lists(cls, values: list[str]) -> list[str]:
        cleaned = []
        for value in values:
            stripped = value.strip()
            if not stripped:
                raise ValueError("list entries must not be empty")
            if len(stripped) > 500:
                raise ValueError("list entries must be at most 500 characters")
            cleaned.append(stripped)
        return cleaned


class AnalysisCreate(AnalysisBase):
    news_id: int


class Analysis(AnalysisBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    news_id: int
    analyzed_at: datetime


class NewsItemWithAnalysis(NewsItem):
    analysis: Optional[Analysis] = None


class TrendingTicker(BaseModel):
    ticker: str
    mention_sentiment: str  # bullish/bearish/mixed
    buzz_level: str  # high/medium/low
    narrative: str


class MemeStockAlert(BaseModel):
    ticker: str
    risk_level: str  # high/medium/low
    description: str


class XSentimentBase(BaseModel):
    query: str
    trending_tickers: list[TrendingTicker] = Field(default_factory=list)
    retail_sentiment_score: int
    key_narratives: list[str] = Field(default_factory=list)
    meme_stocks: list[MemeStockAlert] = Field(default_factory=list)
    raw_analysis: str


class XSentimentCreate(XSentimentBase):
    pass


class XSentiment(XSentimentBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    analyzed_at: datetime


class AnalysisStats(BaseModel):
    window_days: int = 7
    total_analyzed: int
    avg_sentiment: float
    bullish_count: int
    bearish_count: int
    neutral_count: int
    sector_breakdown: dict[str, int]
    sector_sentiment: dict[str, dict[str, Any]] = Field(default_factory=dict)
    top_affected_stocks: list[dict[str, Any]]
