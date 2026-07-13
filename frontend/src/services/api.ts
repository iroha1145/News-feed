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

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
    this.name = 'ApiError';
  }
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const url = `${BASE_URL}${path}`;
  let res: Response;
  try {
    res = await fetch(url, {
      ...options,
      credentials: 'include',
      headers: {
        ...(options?.body ? { 'Content-Type': 'application/json' } : {}),
        ...options?.headers,
      },
    });
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') throw error;
    throw new ApiError(0, '无法连接服务器，请检查网络或稍后重试。');
  }
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    let message = text || res.statusText || `请求失败（${res.status}）`;
    try {
      const parsed = JSON.parse(text) as { detail?: unknown; message?: unknown };
      if (typeof parsed.detail === 'string') message = parsed.detail;
      else if (typeof parsed.message === 'string') message = parsed.message;
      else message = res.statusText || `请求失败（${res.status}）`;
    } catch {
      // Keep the plain-text response when the server did not return JSON.
    }
    if (res.status === 401) {
      message = path === '/api/auth/login' ? '管理令牌不正确，请重新输入。' : '管理会话已过期，请重新登录。';
    } else if (res.status === 429) {
      message = '登录尝试过多，请稍后再试。';
    } else if (res.status === 503 && message.includes('ADMIN_TOKEN')) {
      message = '服务器尚未配置管理令牌，请先完成服务端配置。';
    }
    throw new ApiError(res.status, message);
  }
  if (res.status === 204) return undefined as T;
  try {
    return await res.json() as T;
  } catch {
    throw new ApiError(res.status, '服务器返回了无法识别的数据。');
  }
}

export function isUnauthorizedError(error: unknown): error is ApiError {
  return error instanceof ApiError && error.status === 401;
}

// Admin session
export const loginAdmin = (token: string, signal?: AbortSignal) =>
  request<{ authenticated: boolean }>('/api/auth/login', {
    method: 'POST',
    body: JSON.stringify({ token }),
    signal,
  });

export const getAdminSession = (signal?: AbortSignal) =>
  request<{ authenticated: boolean }>('/api/auth/session', { signal });

export const logoutAdmin = (signal?: AbortSignal) =>
  request<{ authenticated?: boolean; status?: string }>('/api/auth/logout', {
    method: 'POST',
    signal,
  });

// News
export const getNews = (params?: { page?: number; page_size?: number; classification?: 'bullish' | 'bearish' | 'neutral' }, signal?: AbortSignal) => {
  const qs = new URLSearchParams();
  if (params?.page != null) qs.set('page', String(params.page));
  if (params?.page_size != null) qs.set('page_size', String(params.page_size));
  if (params?.classification) qs.set('classification', params.classification);
  const query = qs.toString() ? `?${qs}` : '';
  return request<NewsListResponse>(`/api/news${query}`, { signal });
};

export const getNewsById = (id: number, signal?: AbortSignal) =>
  request<NewsItem>(`/api/news/${id}`, { signal });

export const fetchNews = (signal?: AbortSignal) =>
  request<{ status: string; new_items: number }>('/api/news/fetch', { method: 'POST', signal });

export interface NewsSourceStatus {
  source: string
  group: string | null
  enabled: boolean
  configured: boolean
  interval_seconds: number
  last_success: string | null
  last_error: string | null
  consecutive_failures: number
  next_attempt_at: string | null
}

export const getNewsSources = (signal?: AbortSignal) =>
  request<{ sources: NewsSourceStatus[] }>('/api/news/sources', { signal });

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

export const retryFailedAnalysis = (newsId: number, signal?: AbortSignal) =>
  request<{ status: string; count: number; news_id: number }>(
    `/api/analysis/retry-failed?news_id=${encodeURIComponent(newsId)}`,
    { method: 'POST', signal },
  );

export const getAnalysisStats = (signal?: AbortSignal) =>
  request<AnalysisStats>('/api/analysis/stats', { signal });

// Model market scenario (the backend keeps its legacy /x-sentiment route)
export const getModelMarketScenario = async (signal?: AbortSignal): Promise<XSentiment | null> => {
  const res = await request<{ data: XSentiment | null }>('/api/x-sentiment', { signal });
  return res.data;
};

export const refreshModelMarketScenario = (signal?: AbortSignal) =>
  request<import('../types').RefreshXSentimentResponse>('/api/x-sentiment/refresh', { method: 'POST', signal });

export const getModelMarketScenarioHistory = async (signal?: AbortSignal): Promise<XSentiment[]> => {
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

export interface SettingsUpdatePayload {
  default_llm_provider?: 'openai' | 'anthropic' | 'grok' | 'ollama'
  default_llm_model?: string
  ollama_base_url?: string
  analysis_batch_size?: number
  x_sentiment_interval?: number
}

export const updateSettings = (settings: SettingsUpdatePayload, signal?: AbortSignal) =>
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
  marketOpen: boolean | null
  type: 'index' | 'commodity'
  source?: string | null
  as_of?: string | null
  stale?: boolean
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
  volume: number | null
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
  source?: string | null
  as_of?: string | null
  period?: string | null
}

export const getCandles = (symbol: string, timeframe = '1D', signal?: AbortSignal) =>
  request<CandleData>(`/api/quotes/${encodeURIComponent(symbol)}/candles?timeframe=${timeframe}`, { signal })

// Profile (fundamentals)
export interface AssetProfile {
  symbol: string
  name: string
  shortName: string
  description: string | null
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
  source?: string | null
  as_of?: string | null
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
  source?: string | null
  as_of?: string | null
}

export const getAssetSentiment = (symbol: string, days = 7, signal?: AbortSignal) =>
  request<AssetSentiment>(`/api/quotes/${encodeURIComponent(symbol)}/sentiment?days=${days}`, { signal })

// Calendar
export interface CalendarResponse {
  events: CalendarEvent[]
  count: number
  analyzed?: number
  source?: string
  stale?: boolean
  as_of?: string | null
  last_success?: string | null
  last_error?: string | null
  analysis_capability?: import('../types').PaidCapability
}

export interface CalendarAnalysisResult {
  event_id: string
  title: string
  title_zh: string
  stock_impact: 'bullish' | 'bearish' | 'neutral'
  commodity_impact: 'bullish' | 'bearish' | 'neutral'
  explanation: string
}

export interface CalendarAnalysisJob {
  job_id: string
  status: 'pending' | 'queued' | 'in_progress' | 'completed' | 'failed' | 'insufficient_context' | 'budget_blocked'
  model: string
  reasoning: 'none' | 'low' | 'medium' | 'high' | 'xhigh' | 'max'
  event_count: number
  analyzed: number
  submitted_at: string | null
  updated_at: string
  completed_at: string | null
  error_code: string | null
  retry_after: number | null
  result: CalendarAnalysisResult[] | null
  created?: boolean
}

export const getCalendar = (signal?: AbortSignal) =>
  request<CalendarResponse>('/api/calendar', { signal });

export const analyzeCalendar = (signal?: AbortSignal) =>
  request<CalendarAnalysisJob>('/api/calendar/analyze?force=true', { method: 'POST', signal });

export const getCalendarAnalysisJob = (jobId: string, signal?: AbortSignal) =>
  request<CalendarAnalysisJob>(`/api/calendar/analyze/${encodeURIComponent(jobId)}`, { signal });

const waitForAbortableDelay = (milliseconds: number, signal?: AbortSignal) =>
  new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException('Aborted', 'AbortError'));
      return;
    }
    const timer = window.setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, milliseconds);
    const onAbort = () => {
      window.clearTimeout(timer);
      reject(new DOMException('Aborted', 'AbortError'));
    };
    signal?.addEventListener('abort', onAbort, { once: true });
  });

export async function waitForCalendarAnalysis(
  initialJob: CalendarAnalysisJob,
  signal?: AbortSignal,
): Promise<CalendarAnalysisJob> {
  let job = initialJob;
  const deadline = Date.now() + 30 * 60 * 1000;
  while (job.status === 'pending' || job.status === 'queued' || job.status === 'in_progress') {
    if (Date.now() >= deadline) {
      throw new Error('日历分析仍在后台运行，请稍后刷新查看。');
    }
    const hidden = typeof document !== 'undefined' && document.visibilityState === 'hidden';
    const delaySeconds = hidden ? 15 : Math.max(1, Math.min(15, job.retry_after ?? 2));
    await waitForAbortableDelay(delaySeconds * 1000, signal);
    job = await getCalendarAnalysisJob(job.job_id, signal);
  }
  if (job.status === 'completed') return job;
  if (job.status === 'budget_blocked') {
    throw new Error('今日日历分析额度已用完，请等待额度恢复。');
  }
  if (job.status === 'insufficient_context') {
    throw new Error('当前没有可分析的经济日历事件。');
  }
  throw new Error('日历分析未能完成，请稍后重试。');
}
