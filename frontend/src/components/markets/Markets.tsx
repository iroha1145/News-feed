import { useApi } from '../../hooks/useApi'
import { usePolling } from '../../hooks/usePolling'
import { getMarketQuotes, getCalendar, getAnalysisStats, getNews, getXSentiment, type MarketQuote } from '../../services/api'
import type { AnalysisStats, CalendarEvent, XSentiment, NewsItem } from '../../types'
import FearGreedGauge from '../sentiment/FearGreedGauge'
import LoadingSpinner from '../common/LoadingSpinner'
import { useCallback, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { toLocalTime } from '../../utils/time'
import AssetDetailModal from './AssetDetailModal'

function seededBars(seed: string, count = 7, min = 30, spread = 70) {
  let hash = 0
  for (let i = 0; i < seed.length; i += 1) {
    hash = (hash * 31 + seed.charCodeAt(i)) >>> 0
  }

  return Array.from({ length: count }, (_, index) => {
    hash = (hash * 1664525 + 1013904223 + index) >>> 0
    return min + (hash % spread)
  })
}

export default function Markets() {
  const quotesApi = useApi<{ quotes: MarketQuote[] }>(() => getMarketQuotes(), [])
  const calendarApi = useApi<{ events: CalendarEvent[]; count: number }>(() => getCalendar(), [])
  const statsApi = useApi<AnalysisStats>(() => getAnalysisStats(), [])
  const newsApi = useApi<{ items: NewsItem[]; total: number }>(() => getNews({ page_size: 5 }), [])
  const xApi = useApi<XSentiment | null>(() => getXSentiment(), [])

  const [selectedQuote, setSelectedQuote] = useState<MarketQuote | null>(null)

  const refetch = useCallback(() => {
    quotesApi.refetch()
    statsApi.refetch()
  }, [quotesApi, statsApi])

  usePolling(refetch, 120_000)

  const quotes = quotesApi.data?.quotes ?? []
  const indices = quotes.filter(q => q.type === 'index')
  const commodities = quotes.filter(q => q.type === 'commodity')
  const events = calendarApi.data?.events?.slice(0, 4) ?? []
  const stats = statsApi.data
  const news = newsApi.data?.items?.slice(0, 3) ?? []
  const xData = xApi.data

  const indexSparkBars = useMemo(
    () => Object.fromEntries(indices.map((quote) => [quote.symbol, seededBars(quote.symbol)])),
    [indices],
  )

  const fearGreed = xData?.fear_greed_estimate ?? (stats ? Math.round(50 + (stats.avg_sentiment ?? 0) * 5) : 50)

  if (quotesApi.loading && !quotesApi.data) {
    return <LoadingSpinner className="py-20" />
  }

  return (
    <div className="flex gap-0">
      {/* Main Content */}
      <main className="flex-1 xl:mr-80 p-4 md:p-6 lg:p-8 space-y-8">
        {/* Market Index Cards — matching stitch design */}
        <section>
          <div className="flex items-center justify-between mb-6">
            <h1 className="text-3xl font-extrabold font-headline dark:text-white">市场总览</h1>
            <span className="text-xs text-on-surface-variant dark:text-slate-400">
              {new Date().toLocaleString('zh-CN', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })}
            </span>
          </div>
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            {indices.map((q) => {
              const price = q.price ?? q.previousClose ?? 0
              const pct = q.changePercent ?? 0
              const isPos = pct > 0
              const isNeg = pct < 0
              if (price === 0) return null
              return (
                <button type="button" key={q.symbol} className="w-full text-left bg-surface-container-lowest dark:bg-slate-800 rounded-2xl p-5 space-y-3 cursor-pointer hover:shadow-lg hover:-translate-y-0.5 transition-all active:scale-[0.98]" onClick={() => setSelectedQuote(q)}>
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-bold text-on-surface-variant dark:text-slate-400 uppercase tracking-wider">{q.label}</span>
                    <span className={`text-xs font-bold px-2 py-0.5 rounded-full ${
                      isPos ? 'bg-tertiary-container text-on-tertiary-container' :
                      isNeg ? 'bg-error-container text-on-error-container' :
                      'bg-surface-container text-on-surface-variant dark:bg-slate-700 dark:text-slate-400'
                    }`}>
                      {isPos ? '+' : ''}{pct.toFixed(2)}%
                    </span>
                  </div>
                  <p className="text-2xl font-black dark:text-white tracking-tight">
                    {price.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                  </p>
                  {/* Mini bar chart visualization */}
                  <div className="flex items-end gap-0.5 h-8">
                    {(indexSparkBars[q.symbol] ?? seededBars(q.symbol)).map((h, i) => {
                      const last = i === 6
                      return (
                        <div
                          key={i}
                          className={`flex-1 rounded-sm ${
                            last
                              ? isPos ? 'bg-tertiary dark:bg-emerald-500' : isNeg ? 'bg-error dark:bg-red-500' : 'bg-slate-300'
                              : isPos ? 'bg-tertiary/30 dark:bg-emerald-500/30' : isNeg ? 'bg-error/30 dark:bg-red-500/30' : 'bg-slate-200 dark:bg-slate-700'
                          }`}
                          style={{ height: `${h}%` }}
                        />
                      )
                    })}
                  </div>
                </button>
              )
            })}
          </div>
        </section>

        {/* Featured News — Hero Card */}
        {news.length > 0 && (
          <section>
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-xl font-extrabold font-headline dark:text-white">宏观新闻速递</h2>
              <Link to="/news" className="text-xs font-bold text-primary dark:text-violet-400 hover:underline">
                查看全部 →
              </Link>
            </div>

            {/* Featured article */}
            {(() => {
              const featured = news[0]
              const analysis = featured.analysis
              return (
                <div className="bg-surface-container-lowest dark:bg-slate-800 rounded-2xl p-6 space-y-4 mb-4">
                  <div className="flex items-center gap-3 text-xs font-bold text-on-surface-variant dark:text-slate-400 uppercase tracking-widest">
                    <span className="text-primary dark:text-violet-400">{featured.source}</span>
                    <span className="w-1 h-1 bg-outline-variant rounded-full" />
                    <span>{toLocalTime(featured.published_at)}</span>
                  </div>
                  <h3 className="text-xl font-bold font-headline leading-tight dark:text-white">
                    {analysis?.title_zh || featured.title}
                  </h3>
                  <p className="text-sm text-on-surface-variant dark:text-slate-400 leading-relaxed line-clamp-3">
                    {analysis?.headline_summary || featured.summary}
                  </p>
                  {analysis && (
                    <div className="flex flex-wrap gap-2">
                      {analysis.classification && (
                        <span className={`text-xs font-bold px-3 py-1 rounded-lg ${
                          analysis.classification === 'bullish' ? 'bg-tertiary-container text-on-tertiary-container' :
                          analysis.classification === 'bearish' ? 'bg-error-container text-on-error-container' :
                          'bg-surface-container-high text-on-surface-variant dark:bg-slate-700 dark:text-slate-300'
                        }`}>
                          {analysis.classification === 'bullish' ? '看多' : analysis.classification === 'bearish' ? '看空' : '中性'}
                        </span>
                      )}
                      {analysis.affected_sectors?.slice(0, 2).map(s => (
                        <span key={s} className="text-xs font-bold px-3 py-1 rounded-lg bg-surface-container dark:bg-slate-700 text-on-surface-variant dark:text-slate-300">
                          {s}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              )
            })()}

            {/* More news cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {news.slice(1, 3).map((item) => {
                const analysis = item.analysis
                return (
                  <Link key={item.id} to="/news" className="bg-surface-container-lowest dark:bg-slate-800 rounded-2xl p-5 space-y-3 hover:shadow-lg transition-all group">
                    {analysis?.classification && (
                      <span className={`text-[10px] font-black px-2.5 py-1 rounded-lg ${
                        analysis.classification === 'bullish' ? 'bg-tertiary-container text-on-tertiary-container' :
                        analysis.classification === 'bearish' ? 'bg-error-container text-on-error-container' :
                        'bg-surface-container-high text-on-surface-variant dark:bg-slate-700 dark:text-slate-300'
                      }`}>
                        {analysis.classification === 'bullish' ? '看多' : analysis.classification === 'bearish' ? '看空' : '中性'}
                      </span>
                    )}
                    <h4 className="font-bold text-sm leading-tight dark:text-white group-hover:text-primary dark:group-hover:text-violet-400 transition-colors line-clamp-2">
                      {analysis?.title_zh || item.title}
                    </h4>
                    <p className="text-xs text-on-surface-variant dark:text-slate-400 line-clamp-2">
                      {analysis?.headline_summary || item.summary}
                    </p>
                    <span className="text-[10px] text-on-surface-variant dark:text-slate-500">
                      {item.source} · {toLocalTime(item.published_at)}
                    </span>
                  </Link>
                )
              })}
            </div>
          </section>
        )}

        {/* Commodity Overview */}
        {commodities.length > 0 && (
          <section>
            <h2 className="text-xl font-extrabold font-headline mb-4 dark:text-white">大宗商品</h2>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              {commodities.map(q => {
                const price = q.price ?? q.previousClose ?? 0
                const pct = q.changePercent ?? 0
                const isPos = pct > 0
                const isNeg = pct < 0
                if (price === 0) return null
                return (
                  <button type="button" key={q.symbol} className="w-full text-left bg-surface-container-lowest dark:bg-slate-800 rounded-2xl p-5 space-y-2 cursor-pointer hover:shadow-lg hover:-translate-y-0.5 transition-all active:scale-[0.98]" onClick={() => setSelectedQuote(q)}>
                    <div className="flex items-center gap-3">
                      <div className="w-10 h-10 rounded-xl bg-surface-container dark:bg-slate-700 flex items-center justify-center">
                        <span className="material-symbols-outlined text-amber-500">
                          {q.name.includes('Gold') ? 'diamond' : q.name.includes('Oil') ? 'oil_barrel' : 'toll'}
                        </span>
                      </div>
                      <div className="flex-1">
                        <p className="font-bold text-sm dark:text-white">{q.label}</p>
                        <p className="text-xs text-on-surface-variant dark:text-slate-400">{q.name}</p>
                      </div>
                    </div>
                    <div className="flex items-end justify-between pt-2">
                      <span className="text-xl font-black dark:text-white">
                        ${price.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                      </span>
                      <span className={`text-sm font-bold ${isPos ? 'text-tertiary dark:text-emerald-400' : isNeg ? 'text-error dark:text-red-400' : 'text-slate-400'}`}>
                        {isPos ? '+' : ''}{pct.toFixed(2)}%
                      </span>
                    </div>
                  </button>
                )
              })}
            </div>
          </section>
        )}
      </main>

      {/* Right Sidebar — Macro Calendar + Sentiment + Commodities */}
      <aside className="hidden xl:block fixed right-0 top-16 w-80 h-[calc(100vh-64px)] p-6 bg-surface-container-low dark:bg-slate-900 overflow-y-auto custom-scrollbar z-40">
        <div className="space-y-8">
          {/* Macro Calendar */}
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="text-xs font-black font-headline tracking-widest uppercase text-on-surface-variant dark:text-slate-400 flex items-center gap-2">
                <span className="material-symbols-outlined text-primary dark:text-violet-400 text-lg">calendar_today</span>
                经济日历
              </h3>
            </div>
            {events.length > 0 ? (
              <div className="space-y-2">
                {events.map((ev, i) => {
                  const d = new Date(ev.date)
                  return (
                    <div key={i} className="flex gap-3 p-3 bg-surface-container-lowest dark:bg-slate-800 rounded-xl">
                      <div className="text-center flex-shrink-0 w-12">
                        <p className="text-[10px] text-on-surface-variant dark:text-slate-500">{d.toLocaleString('zh-CN', { hour: '2-digit', minute: '2-digit' })}</p>
                        <p className="text-lg font-black dark:text-white">{d.getDate()}</p>
                        <p className="text-[10px] font-bold text-on-surface-variant dark:text-slate-500 uppercase">{d.toLocaleString('zh-CN', { month: 'short' })}</p>
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-semibold truncate dark:text-white">{ev.title}</p>
                        <p className={`text-[10px] font-bold ${
                          ev.impact === 'high' ? 'text-error dark:text-red-400' :
                          ev.impact === 'medium' ? 'text-amber-600 dark:text-amber-400' :
                          'text-slate-400'
                        }`}>
                          影响: {ev.impact === 'high' ? '高' : ev.impact === 'medium' ? '中' : '低'}
                        </p>
                      </div>
                    </div>
                  )
                })}
              </div>
            ) : (
              <p className="text-sm text-on-surface-variant dark:text-slate-500 italic">加载中...</p>
            )}
          </div>

          {/* Market Sentiment Index */}
          <div className="bg-surface-container-lowest dark:bg-slate-800 p-6 rounded-2xl text-center space-y-4">
            <h3 className="text-xs font-black font-headline tracking-widest uppercase text-on-surface-variant dark:text-slate-400">
              市场情绪指数
            </h3>
            <div className="bg-gradient-to-br from-slate-900 via-violet-950 to-slate-900 rounded-2xl p-6">
              <FearGreedGauge value={fearGreed} />
            </div>
            {stats && (
              <p className="text-xs text-on-surface-variant dark:text-slate-400">
                已分析 <strong className="dark:text-white">{stats.total_analyzed}</strong> 条新闻 · 
                看多 <strong className="text-tertiary dark:text-emerald-400">{stats.bullish_count}</strong> · 
                看空 <strong className="text-error dark:text-red-400">{stats.bearish_count}</strong>
              </p>
            )}
          </div>

          {/* Commodity Impact */}
          {commodities.length > 0 && (
            <div className="bg-surface-container-lowest dark:bg-slate-800 p-5 rounded-2xl space-y-4">
              <h3 className="text-xs font-black font-headline tracking-[0.2em] uppercase text-on-surface-variant dark:text-slate-400">
                Commodity Impact
              </h3>
              <div className="space-y-3">
                {commodities.map(q => {
                  const price = q.price ?? q.previousClose ?? 0
                  const pct = q.changePercent ?? 0
                  const isPos = pct > 0
                  const isNeg = pct < 0
                  const pairMap: Record<string, string> = { 'GC=F': 'XAU/USD', 'SI=F': 'XAG/USD', 'CL=F': 'WTI/USD' }
                  const iconColorMap: Record<string, string> = { 'GC=F': 'bg-amber-400 text-white', 'SI=F': 'bg-slate-400 text-white', 'CL=F': 'bg-violet-500 text-white' }
                  const pair = pairMap[q.symbol] ?? q.symbol
                  const iconColor = iconColorMap[q.symbol] ?? 'bg-slate-400 text-white'
                  return (
                    <div key={q.symbol} className="flex items-center gap-3 p-3.5 bg-surface-container dark:bg-slate-700/50 rounded-xl">
                      <div className={`w-9 h-9 rounded-full ${iconColor} flex items-center justify-center flex-shrink-0`}>
                        <span className="material-symbols-outlined text-lg">
                          {q.name.includes('Gold') ? 'star' : q.name.includes('Oil') ? 'local_gas_station' : 'hexagon'}
                        </span>
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-bold dark:text-white">{q.name}</p>
                        <p className="text-[10px] text-on-surface-variant dark:text-slate-500 font-medium">{pair}</p>
                      </div>
                      <div className="text-right flex-shrink-0">
                        <p className={`text-sm font-bold ${isPos ? 'text-emerald-600 dark:text-emerald-400' : isNeg ? 'text-red-600 dark:text-red-400' : 'text-slate-400'}`}>
                          {isPos ? '+' : ''}{pct.toFixed(2)}%
                        </p>
                        <p className="text-xs text-on-surface-variant dark:text-slate-400 font-medium">
                          {price > 0 ? price.toLocaleString(undefined, { maximumFractionDigits: 2 }) : '—'}
                        </p>
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>
          )}
        </div>
      </aside>

      {/* Asset Detail Modal */}
      {selectedQuote && (
        <AssetDetailModal
          quote={selectedQuote}
          onClose={() => setSelectedQuote(null)}
        />
      )}
    </div>
  )
}
