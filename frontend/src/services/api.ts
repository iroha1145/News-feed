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
export const getNews = (params?: { page?: number; page_size?: number }, signal?: AbortSignal) => {
  const qs = new URLSearchParams();
  if (params?.page != null) qs.set('page', String(params.page));
  if (params?.page_size != null) qs.set('page_size', String(params.page_size));
  const query = qs.toString() ? `?${qs}` : '';
  return request<NewsListResponse>(`/api/news${query}`, { signal });
};

export const getNewsById = (id: number, signal?: AbortSignal) =>
  request<NewsItem>(`/api/news/${id}`, { signal });

export const fetchNews = (signal?: AbortSignal) =>
  request<{ status: string; new_items: number }>('/api/news/fetch', { method: 'POST', signal });

// Analysis
export const getAnalyses = (params?: { page?: number; page_size?: number }, signal?: AbortSignal) => {
  const qs = new URLSearchParams();
  if (params?.page != null) qs.set('page', String(params.page));
  if (params?.page_size != null) qs.set('page_size', String(params.page_size));
  const query = qs.toString() ? `?${qs}` : '';
  return request<AnalysisListResponse>(`/api/analysis${query}`, { signal });
};

export const getLatestAnalyses = (n = 20, signal?: AbortSignal) =>
  request<Analysis[]>(`/api/analysis/latest?n=${n}`, { signal });

export const getAnalysisByNewsId = (newsId: number, signal?: AbortSignal) =>
  request<{ analysis: Analysis; news: NewsItem | null }>(`/api/analysis/by-news/${newsId}`, { signal });

export const triggerAnalysis = (signal?: AbortSignal) =>
  request<import('../types').TriggerAnalysisResponse>('/api/analysis/trigger', { method: 'POST', signal });

export const getAnalysisStats = (signal?: AbortSignal) =>
  request<AnalysisStats>('/api/analysis/stats', { signal });

// X Sentiment
export const getXSentiment = async (signal?: AbortSignal): Promise<XSentiment | null> => {
  const res = await request<{ data: XSentiment | null }>('/api/x-sentiment', { signal });
  return res.data;
};

export const refreshXSentiment = (signal?: AbortSignal) =>
  request<import('../types').RefreshXSentimentResponse>('/api/x-sentiment/refresh', { method: 'POST', signal });

export const getXSentimentHistory = async (signal?: AbortSignal): Promise<XSentiment[]> => {
  const res = await request<{ items: XSentiment[]; total: number }>('/api/x-sentiment/history', { signal });
  return res.items ?? [];
};

// Settings
export const getSettings = (signal?: AbortSignal) =>
  request<AppSettings>('/api/settings', { signal });

export interface SettingsUpdateResponse {
  updated: Record<string, unknown>;
  message: string;
}

export const updateSettings = (settings: Partial<AppSettings>, signal?: AbortSignal) =>
  request<SettingsUpdateResponse>('/api/settings', {
    method: 'PUT',
    body: JSON.stringify(settings),
    signal,
  });

export const getProviders = (signal?: AbortSignal) =>
  request<{ providers: import('../types').ProviderInfo[] }>('/api/settings/providers', { signal });

export const testLlm = (provider: string, model: string, apiKey?: string, signal?: AbortSignal) =>
  request<{ provider: string; model: string; available: boolean; status: string }>('/api/settings/test-llm', {
    method: 'POST',
    body: JSON.stringify({ provider, model, api_key: apiKey }),
    signal,
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

export const getMarketQuotes = (signal?: AbortSignal) =>
  request<{ quotes: MarketQuote[] }>('/api/quotes', { signal })

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

export const getCandles = (symbol: string, timeframe = '1D', signal?: AbortSignal) =>
  request<CandleData>(`/api/quotes/${encodeURIComponent(symbol)}/candles?timeframe=${timeframe}`, { signal })

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

export const getAssetProfile = (symbol: string, signal?: AbortSignal) =>
  request<AssetProfile>(`/api/quotes/${encodeURIComponent(symbol)}/profile`, { signal })

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

export const getAssetSentiment = (symbol: string, days = 7, signal?: AbortSignal) =>
  request<AssetSentiment>(`/api/quotes/${encodeURIComponent(symbol)}/sentiment?days=${days}`, { signal })

// Top Constituents
export interface Constituent {
  ticker: string
  name: string
  weight: number
  changePercent: number | null
}

export const getConstituents = (symbol: string, signal?: AbortSignal) =>
  request<{ symbol: string; constituents: Constituent[] }>(`/api/quotes/${encodeURIComponent(symbol)}/constituents`, { signal })

// Calendar
export const getCalendar = (signal?: AbortSignal) =>
  request<{ events: CalendarEvent[]; count: number }>('/api/calendar', { signal });

export const analyzeCalendar = (signal?: AbortSignal) =>
  request<{ events: CalendarEvent[]; count: number; analyzed: number }>('/api/calendar/analyze', { method: 'POST', signal });
