import { useCallback, useState } from 'react'
import { getNews, getAnalysisStats, triggerAnalysis, fetchNews, type MarketQuote } from '../../services/api'
import { useApi } from '../../hooks/useApi'
import { usePolling } from '../../hooks/usePolling'
import NewsCard from './NewsCard'
import MarketSidebar from '../layout/MarketSidebar'
import LoadingSpinner from '../common/LoadingSpinner'
import AssetDetailModal from '../markets/AssetDetailModal'

type Filter = 'all' | 'bullish' | 'bearish'

const FILTER_LABELS: Record<Filter, string> = {
  all: '全部',
  bullish: '看多',
  bearish: '看空',
}

export default function NewsFeed() {
  const [filter, setFilter] = useState<Filter>('all')
  const [selectedQuote, setSelectedQuote] = useState<MarketQuote | null>(null)
  const [selectedTicker, setSelectedTicker] = useState<{ symbol: string; name?: string } | null>(null)

  const newsApi = useApi((signal) => getNews({ page_size: 25 }, signal), [])
  const statsApi = useApi((signal) => getAnalysisStats(signal), [])

  const refetchAll = useCallback(() => {
    newsApi.refetch()
    statsApi.refetch()
  }, [newsApi, statsApi])

  usePolling(refetchAll, 30000, true)

  const handleFetch = async () => {
    await fetchNews()
    setTimeout(refetchAll, 2000)
  }

  const handleAnalyze = async () => {
    await triggerAnalysis()
    setTimeout(refetchAll, 3000)
  }

  const news = newsApi.data?.items ?? []
  const filtered = filter === 'all'
    ? news
    : news.filter((n) => n.analysis?.classification === filter)

  return (
    <div className="flex min-h-screen">
      {/* Main content */}
      <div className="flex-1 xl:mr-80 p-4 md:p-6 lg:p-8 space-y-8">
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
            AI驱动的宏观新闻情绪分析，洞察市场趋势
          </p>
        </section>

        {/* Actions + Filter */}
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="flex gap-2">
            {(['all', 'bullish', 'bearish'] as Filter[]).map((f) => (
              <button
                key={f}
                onClick={() => setFilter(f)}
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
                {f !== 'all' && statsApi.data && (
                  <span className="ml-1.5 opacity-70">
                    ({f === 'bullish' ? statsApi.data.bullish_count : statsApi.data.bearish_count})
                  </span>
                )}
              </button>
            ))}
          </div>

          <div className="flex gap-2">
            <button
              onClick={handleFetch}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-surface-container-lowest dark:bg-slate-800 border border-slate-200/50 dark:border-slate-700 rounded-lg text-xs font-bold text-on-surface-variant dark:text-slate-400 hover:bg-surface-container dark:hover:bg-slate-700 transition-all"
            >
              <span className="material-symbols-outlined text-[16px]">sync</span>
              抓取新闻
            </button>
            <button
              onClick={handleAnalyze}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-primary dark:bg-violet-600 text-on-primary rounded-lg text-xs font-bold hover:bg-primary-dim dark:hover:bg-violet-700 transition-all active:scale-95"
            >
              <span className="material-symbols-outlined text-[16px]">psychology</span>
              AI分析
            </button>
          </div>
        </div>

        {/* News feed */}
        <section className="space-y-6">
          {newsApi.loading && filtered.length === 0 ? (
            <LoadingSpinner className="py-20" />
          ) : filtered.length === 0 ? (
            <div className="text-center py-20 text-on-surface-variant dark:text-slate-500">
              <span className="material-symbols-outlined text-5xl mb-4 block opacity-30">newspaper</span>
              <p className="font-semibold">暂无新闻</p>
              <p className="text-sm mt-1">点击「抓取新闻」获取最新资讯</p>
            </div>
          ) : (
            filtered.map((item) => (
              <NewsCard
                key={item.id}
                item={item}
                onTickerClick={(ticker, name) => setSelectedTicker({ symbol: ticker, name })}
              />
            ))
          )}
        </section>

        {/* Stats footer */}
        {statsApi.data && (
          <div className="flex items-center gap-6 text-xs text-on-surface-variant dark:text-slate-500 pt-4">
            <span>已分析: <strong className="text-on-surface dark:text-slate-300">{statsApi.data.total_analyzed}</strong></span>
            {statsApi.data.avg_sentiment !== undefined && (
              <span>平均情绪: <strong className={statsApi.data.avg_sentiment > 0 ? 'text-tertiary dark:text-emerald-400' : 'text-error dark:text-red-400'}>
                {statsApi.data.avg_sentiment > 0 ? '+' : ''}{statsApi.data.avg_sentiment.toFixed(1)}
              </strong></span>
            )}
          </div>
        )}
      </div>

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
