export interface NewsItemImageFallback {
  urlToImage?: string | null;
}

export interface NewsItem extends NewsItemImageFallback {
  id: number;
  source: string;
  title: string;
  summary: string | null;
  url: string;
  image_url: string | null;
  published_at: string | null;
  fetched_at: string;
  analysis_status?: 'pending' | 'processing' | 'completed' | 'failed' | 'skipped';
  analysis?: Analysis | null;
  is_pinned?: boolean;
}

export interface CalendarEvent {
  date: string;
  title: string;
  country_code: string;
  country: string;
  impact: 'high' | 'medium' | 'low' | 'holiday' | string;
  impact_zh: string;
  forecast: string;
  previous: string;
  actual: string;
  title_zh?: string;
  stock_impact?: 'bullish' | 'bearish' | 'neutral';
  commodity_impact?: 'bullish' | 'bearish' | 'neutral';
  explanation?: string;
  is_stale?: boolean;
  source_fetched_at?: string | null;
}

export interface Analysis {
  id: number;
  news_id: number;
  title_zh: string;
  headline_summary: string;
  overall_sentiment: number; // -100 to 100
  classification: 'bullish' | 'bearish' | 'neutral';
  confidence: number; // 0-100
  affected_stocks: AffectedStock[];
  affected_sectors: string[];
  affected_commodities: AffectedCommodity[];
  logic_chain: string;
  key_factors: string[];
  llm_provider: string;
  llm_model: string;
  analyzed_at: string;
  news?: NewsItem;
  news_title?: string;
  news_source?: string;
  news_url?: string;
}

export interface AffectedStock {
  ticker: string;
  company: string;
  impact_score: number;
  reason: string;
}

export interface AffectedCommodity {
  name: string;
  impact_score: number;
  reason: string;
}

export interface ModelMarketScenario {
  id: number;
  trending_tickers: TrendingTicker[];
  retail_sentiment_score: number;
  key_narratives: string[];
  meme_stocks: MemeStockAlert[];
  fear_greed_estimate?: number;
  analyzed_at: string;
}

/** @deprecated Use ModelMarketScenario; the server route keeps its legacy name. */
export type XSentiment = ModelMarketScenario;

export interface TrendingTicker {
  ticker: string;
  mention_sentiment: 'bullish' | 'bearish' | 'mixed';
  buzz_level: 'high' | 'medium' | 'low';
  narrative: string;
}

export interface MemeStockAlert {
  ticker: string;
  risk_level: 'high' | 'medium' | 'low';
  description: string;
}

// Settings — matches the merged dict from GET /api/settings
export interface AppSettings {
  default_llm_provider: string;
  default_llm_model: string;
  default_llm_api_key: string;
  openai_api_key: string;
  anthropic_api_key: string;
  grok_api_key: string;
  ollama_base_url: string;
  analysis_batch_size: number;
  x_sentiment_interval: number;
  finnhub_api_key: string;
  newsapi_api_key: string;
  gnews_api_key: string;
  [key: string]: unknown; // DB overrides may add extra keys
}

// Providers — from GET /api/settings/providers
export interface ProviderInfo {
  name: string;
  configured: boolean;
  models: string[];
}

// Trigger / refresh responses
export interface TriggerAnalysisResponse {
  status: string;
  batch_size: number;
}

export interface RefreshXSentimentResponse {
  status: string;
  message: string;
}

// Stats — from GET /api/analysis/stats
export interface TopAffectedStockStat {
  ticker: string;
  avg_impact: number;
  mention_count: number;
}

export interface SectorSentimentData {
  count: number;
  avg_sentiment: number;
  bullish: number;
  bearish: number;
  neutral: number;
}

export interface AnalysisStats {
  window_days?: number;
  total_analyzed: number;
  avg_sentiment: number;
  bullish_count: number;
  bearish_count: number;
  neutral_count: number;
  pending_count?: number;
  sector_breakdown?: Record<string, number>;
  sector_sentiment?: Record<string, SectorSentimentData>;
  top_affected_stocks?: TopAffectedStockStat[];
}

export interface NewsListResponse {
  items: NewsItem[];
  total: number;
}

export interface AnalysisListResponse {
  items: Analysis[];
  total: number;
}
