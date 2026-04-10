from datetime import datetime
from typing import Any, Optional
from pydantic import BaseModel, Field


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
    id: int
    fetched_at: datetime
    content_hash: str

    class Config:
        from_attributes = True


class AffectedStock(BaseModel):
    ticker: str
    company: str
    impact_score: int
    reason: str


class AffectedCommodity(BaseModel):
    name: str
    impact_score: int
    reason: str


class AnalysisBase(BaseModel):
    overall_sentiment: int
    classification: str  # bullish/bearish/neutral
    confidence: int
    affected_stocks: list[AffectedStock] = Field(default_factory=list)
    affected_sectors: list[str] = Field(default_factory=list)
    affected_commodities: list[AffectedCommodity] = Field(default_factory=list)
    logic_chain: str
    key_factors: list[str] = Field(default_factory=list)
    llm_provider: str
    llm_model: str


class AnalysisCreate(AnalysisBase):
    news_id: int


class Analysis(AnalysisBase):
    id: int
    news_id: int
    analyzed_at: datetime

    class Config:
        from_attributes = True


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
    id: int
    analyzed_at: datetime

    class Config:
        from_attributes = True


class SettingItem(BaseModel):
    key: str
    value: Any


class SettingsUpdate(BaseModel):
    default_llm_provider: Optional[str] = None
    default_llm_model: Optional[str] = None
    default_llm_api_key: Optional[str] = None
    openai_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None
    grok_api_key: Optional[str] = None
    ollama_base_url: Optional[str] = None
    news_poll_interval: Optional[int] = None
    analysis_batch_size: Optional[int] = None
    finnhub_api_key: Optional[str] = None
    newsapi_api_key: Optional[str] = None
    gnews_api_key: Optional[str] = None


class LLMProviderStatus(BaseModel):
    name: str
    configured: bool
    models: list[str]


class TestLLMRequest(BaseModel):
    provider: str
    model: str
    api_key: Optional[str] = None


class PaginatedResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[Any]


class AnalysisStats(BaseModel):
    total_analyzed: int
    avg_sentiment: float
    bullish_count: int
    bearish_count: int
    neutral_count: int
    sector_breakdown: dict[str, int]
    top_affected_stocks: list[dict[str, Any]]
