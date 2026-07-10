import { useCallback, useEffect, useRef, useState } from 'react'
import { getNews, getAnalysisStats, getNewsSources, triggerAnalysis, fetchNews, type MarketQuote } from '../../services/api'
import { useApi } from '../../hooks/useApi'
import { usePolling } from '../../hooks/usePolling'
import NewsCard from './NewsCard'
import MarketSidebar from '../layout/MarketSidebar'
import LoadingSpinner from '../common/LoadingSpinner'
import AssetDetailModal from '../markets/AssetDetailModal'
import { useAdminSession } from '../../context/AdminSessionContext'
import { useSearchParams } from 'react-router-dom'
import { toLocalTime } from '../../utils/time'

type Filter = 'all' | 'bullish' | 'bearish' | 'neutral'

const FILTER_LABELS: Record<Filter, string> = {
  all: '全部',
  bullish: '看多',
  bearish: '看空',
  neutral: '中性',
}

const PAGE_SIZE = 25

export default function NewsFeed() {
  const [searchParams, setSearchParams] = useSearchParams()
  const rawFilter = searchParams.get('filter')
  const filter: Filter = rawFilter === 'bullish' || rawFilter === 'bearish' || rawFilter === 'neutral' ? rawFilter : 'all'
  const rawPage = Number(searchParams.get('page') || '1')
  const page = Number.isInteger(rawPage) && rawPage > 0 ? rawPage : 1
  const [selectedQuote, setSelectedQuote] = useState<MarketQuote | null>(null)
  const [selectedTicker, setSelectedTicker] = useState<{ symbol: string; name?: string } | null>(null)
  const [fetching, setFetching] = useState(false)
  const [analyzing, setAnalyzing] = useState(false)
  const [actionMessage, setActionMessage] = useState<string | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)
  const actionControllerRef = useRef<AbortController | null>(null)
  const { checking: sessionChecking, requireAdmin, handleExpiredSession } = useAdminSession()

  const newsApi = useApi((signal) => getNews({
    page,
    page_size: PAGE_SIZE,
    classification: filter === 'all' ? undefined : filter,
  }, signal), [page, filter])
  const statsApi = useApi((signal) => getAnalysisStats(signal), [])
  const sourcesApi = useApi((signal) => getNewsSources(signal), [])

  useEffect(() => () => actionControllerRef.current?.abort(), [])

  const refetchAll = useCallback(() => {
    newsApi.refetch()
    statsApi.refetch()
  }, [newsApi, statsApi])

  usePolling(refetchAll, 30000, true)

  const handleFetch = async () => {
    if (!requireAdmin('抓取新闻需要管理员登录。')) return
    actionControllerRef.current?.abort()
    const controller = new AbortController()
    actionControllerRef.current = controller
    setFetching(true)
    setActionError(null)
    setActionMessage(null)
    try {
      const result = await fetchNews(controller.signal)
      setActionMessage(`抓取完成，新增 ${result.new_items} 条新闻。`)
      refetchAll()
    } catch (error) {
      if (error instanceof DOMException && error.name === 'AbortError') return
      if (!handleExpiredSession(error, '管理会话已过期，请重新登录后抓取新闻。')) {
        setActionError(error instanceof Error ? error.message : '新闻抓取失败，请稍后重试。')
      }
    } finally {
      if (actionControllerRef.current === controller) setFetching(false)
    }
  }

  const handleAnalyze = async () => {
    if (!requireAdmin('运行新闻分析需要管理员登录。')) return
    actionControllerRef.current?.abort()
    const controller = new AbortController()
    actionControllerRef.current = controller
    setAnalyzing(true)
    setActionError(null)
    setActionMessage(null)
    try {
      await triggerAnalysis(controller.signal)
      setActionMessage('分析任务已提交，列表会自动更新。')
    } catch (error) {
      if (error instanceof DOMException && error.name === 'AbortError') return
      if (!handleExpiredSession(error, '管理会话已过期，请重新登录后运行分析。')) {
        setActionError(error instanceof Error ? error.message : '分析任务提交失败，请稍后重试。')
      }
    } finally {
      if (actionControllerRef.current === controller) setAnalyzing(false)
    }
  }

  const news = newsApi.data?.items ?? []
  const filtered = news
  const totalPages = Math.max(1, Math.ceil((newsApi.data?.total ?? 0) / PAGE_SIZE))
  const enabledSources = sourcesApi.data?.sources.filter((source) => source.enabled) ?? []
  const unhealthySources = enabledSources.filter((source) => !source.configured || source.consecutive_failures > 0)

  const updateQuery = (nextFilter: Filter, nextPage: number) => {
    const next = new URLSearchParams(searchParams)
    if (nextFilter === 'all') next.delete('filter')
    else next.set('filter', nextFilter)
    if (nextPage <= 1) next.delete('page')
    else next.set('page', String(nextPage))
    setSearchParams(next)
  }

  return (
    <div className="flex min-h-screen">
      {/* Main content */}
      <main className="flex-1 xl:mr-80 p-4 md:p-6 lg:p-8 space-y-8" id="main-content">
        {/* Hero header */}
        <section className="space-y-4">
          <div className="inline-flex items-center gap-2 px-3 py-1 bg-secondary-container dark:bg-violet-900/30 text-on-secondary-container dark:text-violet-300 rounded-full text-xs font-bold tracking-wide">
            <span className="material-symbols-outlined text-[14px]">auto_awesome</span>
            每日情报
          </div>
          <h1 className="text-3xl lg:text-5xl font-extrabold font-headline tracking-tight text-on-surface dark:text-slate-50">
            全球市场脉搏
          </h1>
          <p className="text-base lg:text-lg text-on-surface-variant dark:text-slate-400 max-w-2xl leading-relaxed">
            模型辅助的宏观新闻情绪分析，帮助核对市场线索
          </p>
        </section>

        {/* Actions + Filter */}
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="flex gap-2">
            {(['all', 'bullish', 'bearish', 'neutral'] as Filter[]).map((f) => (
              <button
                key={f}
                type="button"
                aria-pressed={filter === f}
                onClick={() => updateQuery(f, 1)}
                className={`px-4 py-1.5 rounded-full text-xs font-bold transition-all ${
                  filter === f
                    ? f === 'bullish'
                      ? 'bg-tertiary-container text-on-tertiary-container'
                      : f === 'bearish'
                      ? 'bg-error-container text-on-error-container'
                      : 'bg-primary text-on-primary dark:bg-violet-600 dark:text-white'
                    : 'bg-surface-container dark:bg-slate-700 text-on-surface-variant dark:text-slate-400 hover:bg-surface-container-high'
                }`}
              >
                {FILTER_LABELS[f]}
              </button>
            ))}
          </div>

          <div className="flex gap-2">
            <button
              type="button"
              onClick={handleFetch}
              disabled={sessionChecking || fetching || analyzing}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-surface-container-lowest dark:bg-slate-800 border border-slate-200/50 dark:border-slate-700 rounded-lg text-xs font-bold text-on-surface-variant dark:text-slate-400 hover:bg-surface-container dark:hover:bg-slate-700 transition-all"
            >
              <span className={`material-symbols-outlined text-[16px] ${fetching ? 'animate-spin' : ''}`} aria-hidden="true">sync</span>
              {fetching ? '抓取中…' : '抓取新闻'}
            </button>
            <button
              type="button"
              onClick={handleAnalyze}
              disabled={sessionChecking || analyzing || fetching}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-primary dark:bg-violet-600 text-on-primary rounded-lg text-xs font-bold hover:bg-primary-dim dark:hover:bg-violet-700 transition-all active:scale-95"
            >
              <span className="material-symbols-outlined text-[16px]" aria-hidden="true">psychology</span>
              {analyzing ? '提交中…' : '运行分析'}
            </button>
          </div>
        </div>

        {(actionMessage || actionError) && (
          <p className={`rounded-xl px-4 py-3 text-sm ${actionError ? 'bg-red-50 text-red-700 dark:bg-red-950/30 dark:text-red-300' : 'bg-emerald-50 text-emerald-700 dark:bg-emerald-950/30 dark:text-emerald-300'}`} role={actionError ? 'alert' : 'status'} aria-live="polite">
            {actionError || actionMessage}
          </p>
        )}

        <details className="rounded-xl bg-surface-container-lowest p-4 text-sm dark:bg-slate-800">
          <summary className="cursor-pointer font-bold text-on-surface dark:text-white">
            数据源状态：启用 {enabledSources.length} 个{unhealthySources.length > 0 ? `，${unhealthySources.length} 个需检查` : ''}
          </summary>
          <div className="mt-3 grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
            {sourcesApi.error && <p className="text-error dark:text-red-400" role="alert">数据源状态加载失败</p>}
            {enabledSources.map((source) => (
              <div key={source.source} className="rounded-lg bg-surface-container p-3 text-xs dark:bg-slate-700">
                <div className="flex items-center justify-between gap-2">
                  <span className="font-bold" translate="no">{source.source}</span>
                  <span className={source.configured && source.consecutive_failures === 0 ? 'text-emerald-700 dark:text-emerald-400' : 'text-error dark:text-red-400'}>
                    {source.configured && source.consecutive_failures === 0 ? '正常' : '需检查'}
                  </span>
                </div>
                <p className="mt-1 text-on-surface-variant dark:text-slate-400">
                  最近成功：{source.last_success ? toLocalTime(source.last_success) : '暂无记录'}
                </p>
              </div>
            ))}
            {!sourcesApi.loading && !sourcesApi.error && enabledSources.length === 0 && <p className="text-on-surface-variant">暂无已启用数据源</p>}
          </div>
        </details>

        {/* News feed */}
        <section className="space-y-6">
          {newsApi.loading && filtered.length === 0 ? (
            <LoadingSpinner className="py-20" label="新闻加载中" />
          ) : newsApi.error && news.length === 0 ? (
            <div className="rounded-2xl bg-red-50 p-8 text-center text-red-700 dark:bg-red-950/30 dark:text-red-300" role="alert">
              <p className="font-bold">新闻加载失败</p>
              <p className="mt-2 text-sm">{newsApi.error}</p>
              <button type="button" onClick={newsApi.refetch} className="mt-4 rounded-lg bg-red-700 px-4 py-2 text-sm font-bold text-white">重新加载</button>
            </div>
          ) : filtered.length === 0 ? (
            <div className="text-center py-20 text-on-surface-variant dark:text-slate-500">
              <span className="material-symbols-outlined text-5xl mb-4 block opacity-30">newspaper</span>
              <p className="font-semibold">本页没有符合条件的新闻</p>
              <p className="text-sm mt-1">可切换筛选条件或翻到其他页面</p>
            </div>
          ) : (
            filtered.map((item) => (
              <NewsCard
                key={item.id}
                item={item}
                onTickerClick={(ticker, name) => setSelectedTicker({ symbol: ticker, name })}
                onRetryQueued={refetchAll}
              />
            ))
          )}
        </section>

        {newsApi.data && newsApi.data.total > 0 && (
          <nav className="flex items-center justify-center gap-4" aria-label="新闻分页">
            <button type="button" disabled={page <= 1 || newsApi.loading} onClick={() => updateQuery(filter, page - 1)} className="rounded-lg border border-slate-300 px-4 py-2 text-sm font-bold disabled:opacity-40 dark:border-slate-700">上一页</button>
            <span className="text-sm text-on-surface-variant dark:text-slate-400">第 {page} / {totalPages} 页</span>
            <button type="button" disabled={page >= totalPages || newsApi.loading} onClick={() => updateQuery(filter, page + 1)} className="rounded-lg border border-slate-300 px-4 py-2 text-sm font-bold disabled:opacity-40 dark:border-slate-700">下一页</button>
          </nav>
        )}

        {/* Stats footer */}
        {statsApi.data && (
          <div className="flex items-center gap-6 text-xs text-on-surface-variant dark:text-slate-500 pt-4">
            <span>最近 {statsApi.data.window_days ?? 7} 日已分析：<strong className="text-on-surface dark:text-slate-300">{statsApi.data.total_analyzed}</strong></span>
            {statsApi.data.avg_sentiment !== undefined && (
              <span>平均情绪：<strong className={statsApi.data.avg_sentiment > 0 ? 'text-tertiary dark:text-emerald-400' : statsApi.data.avg_sentiment < 0 ? 'text-error dark:text-red-400' : 'text-on-surface-variant dark:text-slate-400'}>
                {statsApi.data.avg_sentiment > 0 ? '+' : ''}{statsApi.data.avg_sentiment.toFixed(1)}
              </strong></span>
            )}
          </div>
        )}
      </main>

      {/* Right sidebar */}
      <MarketSidebar stats={statsApi.data} onQuoteClick={(q) => setSelectedQuote(q)} />

      {/* Asset Detail Modals */}
      {selectedQuote && (
        <AssetDetailModal quote={selectedQuote} onClose={() => setSelectedQuote(null)} />
      )}
      {selectedTicker && (
        <AssetDetailModal symbol={selectedTicker.symbol} symbolName={selectedTicker.name} onClose={() => setSelectedTicker(null)} />
      )}
    </div>
  )
}
