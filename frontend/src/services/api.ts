import type {
  NewsItem,
  NewsListResponse,
  Analysis,
  AnalysisListResponse,
  AnalysisStats,
  XSentiment,
  AppSettings,
  CalendarEvent,
} from '../types';

const BASE_URL = import.meta.env.VITE_API_BASE_URL || '';

if (import.meta.env.PROD && !BASE_URL) {
  console.warn('VITE_API_BASE_URL is not set in production; frontend will use relative API paths.')
}

class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
    this.name = 'ApiError';
  }
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const url = `${BASE_URL}${path}`;
  const res = await fetch(url, {
    headers: { 'Content-Type': 'application/json', ...options?.headers },
    ...options,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new ApiError(res.status, text);
  }
  return res.json() as Promise<T>;
}

// News
export const getNews = (params?: { page?: number; page_size?: number }) => {
  const qs = new URLSearchParams();
  if (params?.page != null) qs.set('page', String(params.page));
  if (params?.page_size != null) qs.set('page_size', String(params.page_size));
  const query = qs.toString() ? `?${qs}` : '';
  return request<NewsListResponse>(`/api/news${query}`);
};

export const getNewsById = (id: number) =>
  request<NewsItem>(`/api/news/${id}`);

export const fetchNews = () =>
  request<{ status: string; new_items: number }>('/api/news/fetch', { method: 'POST' });

// Analysis
export const getAnalyses = (params?: { page?: number; page_size?: number }) => {
  const qs = new URLSearchParams();
  if (params?.page != null) qs.set('page', String(params.page));
  if (params?.page_size != null) qs.set('page_size', String(params.page_size));
  const query = qs.toString() ? `?${qs}` : '';
  return request<AnalysisListResponse>(`/api/analysis${query}`);
};

export const getLatestAnalyses = (n = 20) =>
  request<Analysis[]>(`/api/analysis/latest?n=${n}`);

export const getAnalysisByNewsId = (newsId: number) =>
  request<{ analysis: Analysis; news: NewsItem | null }>(`/api/analysis/by-news/${newsId}`);

export const triggerAnalysis = () =>
  request<import('../types').TriggerAnalysisResponse>('/api/analysis/trigger', { method: 'POST' });

export const getAnalysisStats = () =>
  request<AnalysisStats>('/api/analysis/stats');

// X Sentiment
export const getXSentiment = async (): Promise<XSentiment | null> => {
  const res = await request<{ data: XSentiment | null }>('/api/x-sentiment');
  return res.data;
};

export const refreshXSentiment = () =>
  request<import('../types').RefreshXSentimentResponse>('/api/x-sentiment/refresh', { method: 'POST' });

export const getXSentimentHistory = async (): Promise<XSentiment[]> => {
  const res = await request<{ items: XSentiment[]; total: number }>('/api/x-sentiment/history');
  return res.items ?? [];
};

// Settings
export const getSettings = () =>
  request<AppSettings>('/api/settings');

export interface SettingsUpdateResponse {
  updated: Record<string, unknown>;
  message: string;
}

export const updateSettings = (settings: Partial<AppSettings>) =>
  request<SettingsUpdateResponse>('/api/settings', {
    method: 'PUT',
    body: JSON.stringify(settings),
  });

export const getProviders = () =>
  request<{ providers: import('../types').ProviderInfo[] }>('/api/settings/providers');

export const testLlm = (provider: string, model: string, apiKey?: string) =>
  request<{ provider: string; model: string; available: boolean; status: string }>('/api/settings/test-llm', {
    method: 'POST',
    body: JSON.stringify({ provider, model, api_key: apiKey }),
  });

// Market Quotes
export interface MarketQuote {
  symbol: string
  name: string
  label: string
  price: number | null
  change: number | null
  changePercent: number | null
  previousClose: number | null
  yearLow: number | null
  yearHigh: number | null
  marketOpen: boolean
  type: 'index' | 'commodity'
}

export const getMarketQuotes = () =>
  request<{ quotes: MarketQuote[] }>('/api/quotes')

// Candles (OHLCV + EMA/SMA)
export interface Candle {
  time: string
  open: number
  high: number
  low: number
  close: number
  volume: number
}

export interface MAPoint {
  time: string
  value: number
}

export interface CandleData {
  symbol: string
  timeframe: string
  candles: Candle[]
  ema20: MAPoint[]
  sma50: MAPoint[]
}

export const getCandles = (symbol: string, timeframe = '1D') =>
  request<CandleData>(`/api/quotes/${encodeURIComponent(symbol)}/candles?timeframe=${timeframe}`)

// Profile (fundamentals)
export interface AssetProfile {
  symbol: string
  name: string
  shortName: string
  description: string
  market_cap: number | null
  pe_ratio: number | null
  dividend_yield: number | null
  avg_volume: number | null
  last_volume: number | null
  open: number | null
  day_high: number | null
  day_low: number | null
  year_low: number | null
  year_high: number | null
  fifty_day_avg: number | null
  two_hundred_day_avg: number | null
  beta: number | null
}

export const getAssetProfile = (symbol: string) =>
  request<AssetProfile>(`/api/quotes/${encodeURIComponent(symbol)}/profile`)

// Asset Sentiment (aggregated)
export interface AssetSentiment {
  symbol: string
  days: number
  score: number | null
  total: number
  bullish: number
  bearish: number
  neutral: number
  signal: string | null
  description: string | null
  tags: string[]
}

export const getAssetSentiment = (symbol: string, days = 7) =>
  request<AssetSentiment>(`/api/quotes/${encodeURIComponent(symbol)}/sentiment?days=${days}`)

// Top Constituents
export interface Constituent {
  ticker: string
  name: string
  weight: number
  changePercent: number | null
}

export const getConstituents = (symbol: string) =>
  request<{ symbol: string; constituents: Constituent[] }>(`/api/quotes/${encodeURIComponent(symbol)}/constituents`)

// Calendar
export const getCalendar = () =>
  request<{ events: CalendarEvent[]; count: number }>('/api/calendar');

export const analyzeCalendar = () =>
  request<{ events: CalendarEvent[]; count: number; analyzed: number }>('/api/calendar/analyze', { method: 'POST' });
