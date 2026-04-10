import { useMemo, useState } from 'react'
import { useApi } from '../../hooks/useApi'
import { usePolling } from '../../hooks/usePolling'
import { getAnalysisStats, getXSentiment, getCalendar, getMarketQuotes, getLatestAnalyses, getNews, analyzeCalendar, type MarketQuote } from '../../services/api'
import type { AnalysisStats, XSentiment, CalendarEvent, Analysis, NewsItem } from '../../types'
import FearGreedGauge from './FearGreedGauge'
import LoadingSpinner from '../common/LoadingSpinner'
import { Link } from 'react-router-dom'
import { toLocalTime } from '../../utils/time'

function seededBars(seed: string, count = 7, min = 25, spread = 75) {
  let hash = 0
  for (let i = 0; i < seed.length; i += 1) {
    hash = (hash * 31 + seed.charCodeAt(i)) >>> 0
  }

  return Array.from({ length: count }, (_, index) => {
    hash = (hash * 1664525 + 1013904223 + index) >>> 0
    return min + (hash % spread)
  })
}

export default function SentimentDashboard() {
  const statsApi = useApi<AnalysisStats>(() => getAnalysisStats(), [])
  const xApi = useApi<XSentiment | null>(() => getXSentiment(), [])
  const calendarApi = useApi<{ events: CalendarEvent[]; count: number }>(() => getCalendar(), [])
  const quotesApi = useApi<{ quotes: MarketQuote[] }>(() => getMarketQuotes(), [])
  const analysesApi = useApi<Analysis[]>(() => getLatestAnalyses(1), [])
  const newsApi = useApi<{ items: NewsItem[]; total: number }>(() => getNews({ page_size: 4 }), [])

  const [calendarExpanded, setCalendarExpanded] = useState(false)
  const [calendarAnalyzing, setCalendarAnalyzing] = useState(false)

  usePolling(() => { statsApi.refetch(); xApi.refetch() }, 60_000)

  const stats = statsApi.data
  const xData = xApi.data
  const quotes = quotesApi.data?.quotes ?? []
  const indices = quotes.filter(q => q.type === 'index')
  const commodities = quotes.filter(q => q.type === 'commodity')
  const allEvents = calendarApi.data?.events ?? []
  const events = calendarExpanded ? allEvents : allEvents.slice(0, 3)
  const latestAnalysis = analysesApi.data?.[0]
  const news = newsApi.data?.items ?? []
  const indexSparkBars = useMemo(
    () => Object.fromEntries(indices.map((quote) => [quote.symbol, seededBars(quote.symbol)])),
    [indices],
  )
  const fearGreed = xData?.fear_greed_estimate ?? (stats ? Math.round(50 + (stats.avg_sentiment ?? 0) * 5) : 50)

  const handleAnalyzeCalendar = async () => {
    setCalendarAnalyzing(true)
    try {
      await analyzeCalendar()
      calendarApi.refetch()
    } catch { /* ignore */ }
    setCalendarAnalyzing(false)
  }

  // Parse affected stocks from latest analysis
  const affectedStocks: Array<{ ticker: string; impact_score: number; reason: string }> =
    latestAnalysis ? (() => {
      try {
        const raw = latestAnalysis.affected_stocks
        return typeof raw === 'string' ? JSON.parse(raw) : Array.isArray(raw) ? raw : []
      } catch { return [] }
    })() : []

  const sentimentLabel = fearGreed >= 70 ? '贪婪' : fearGreed >= 55 ? '乐观' : fearGreed >= 45 ? '中性' : fearGreed >= 30 ? '恐惧' : '极度恐惧'

  if (statsApi.loading && !stats) return <LoadingSpinner className="py-20" />

  return (
    <main className="flex-1 lg:ml-0 p-4 md:p-6 lg:p-8">
      <div className="max-w-7xl mx-auto space-y-8">
        {/* Market Index Cards */}
        <div className="grid grid-cols-2 xl:grid-cols-4 gap-4">
          {indices.map((q) => {
            const price = q.price ?? q.previousClose ?? 0
            const pct = q.changePercent ?? 0
            const isPos = pct > 0
            const isNeg = pct < 0
            if (price === 0) return null
            return (
              <div key={q.symbol} className="bg-surface-container-lowest dark:bg-slate-800 p-5 rounded-xl shadow-sm hover:shadow-md transition-shadow">
                <div className="flex justify-between items-start mb-3">
                  <div>
                    <p className="text-xs font-bold text-on-surface-variant dark:text-slate-400 tracking-wider uppercase">{q.label}</p>
                    <h4 className="text-2xl font-extrabold font-headline dark:text-white">{price.toLocaleString(undefined, { maximumFractionDigits: 2 })}</h4>
                  </div>
                  <span className={`text-[10px] px-2 py-1 rounded font-bold ${
                    isPos ? 'bg-tertiary-container text-on-tertiary-container' :
                    isNeg ? 'bg-error-container text-on-error-container' :
                    'bg-surface-container text-on-surface-variant'
                  }`}>
                    {isPos ? '+' : ''}{pct.toFixed(2)}%
                  </span>
                </div>
                <div className="h-10 w-full flex items-end gap-0.5">
                  {(indexSparkBars[q.symbol] ?? seededBars(q.symbol)).map((h, i) => {
                    const last = i === 6
                    return (
                      <div key={i}
                        className={`flex-1 rounded-sm ${
                          last ? (isPos ? 'bg-tertiary' : isNeg ? 'bg-error' : 'bg-slate-300') :
                          (isPos ? 'bg-tertiary/20' : isNeg ? 'bg-error/20' : 'bg-slate-200 dark:bg-slate-700')
                        }`}
                        style={{ height: `${h}%` }}
                      />
                    )
                  })}
                </div>
              </div>
            )
          })}
        </div>

        {/* Main Grid: Sentiment Analysis + Sidebar */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8 items-start">
          {/* Left Column: Event Analysis + Headlines */}
          <div className="lg:col-span-2 space-y-8">
            {/* Macro Event Analysis Card */}
            <section className="glass-panel bg-surface-container-lowest dark:bg-slate-800 p-6 md:p-8 rounded-[2rem] shadow-2xl shadow-slate-200/50 dark:shadow-none">
              <div className="flex flex-wrap justify-between items-start mb-6 gap-4">
                <div>
                  <span className="inline-block bg-primary/10 dark:bg-violet-500/20 text-primary dark:text-violet-400 text-[10px] font-bold px-3 py-1 rounded-full uppercase tracking-widest mb-2">
                    宏观事件分析
                  </span>
                  <h2 className="text-2xl md:text-3xl font-extrabold font-headline dark:text-white tracking-tight">
                    {latestAnalysis?.title_zh || latestAnalysis?.headline_summary || '最新市场分析'}
                  </h2>
                </div>
                <div className="bg-surface-container-low dark:bg-slate-700 p-3 rounded-2xl flex items-center gap-2">
                  <span className="material-symbols-outlined text-primary dark:text-violet-400 material-symbols-filled">auto_awesome</span>
                  <span className="text-xs font-bold text-primary dark:text-violet-400 uppercase">AI 洞察</span>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                {/* Left: Sentiment Gauge + Summary */}
                <div className="space-y-6">
                  {/* Horizontal Sentiment Gauge */}
                  <div>
                    <div className="flex justify-between items-end mb-3">
                      <span className="text-sm font-bold text-on-surface-variant dark:text-slate-400">市场情绪仪表</span>
                      <span className={`text-xl font-black ${
                        fearGreed >= 60 ? 'text-tertiary dark:text-emerald-400' :
                        fearGreed >= 40 ? 'text-primary dark:text-violet-400' :
                        'text-error dark:text-red-400'
                      }`}>{sentimentLabel}</span>
                    </div>
                    <div className="relative h-6 w-full rounded-full sentiment-gradient overflow-hidden">
                      <div
                        className="absolute top-0 bottom-0 w-1.5 bg-white border-2 border-slate-900 dark:border-white rounded-full shadow-lg z-10"
                        style={{ left: `${fearGreed}%` }}
                      />
                    </div>
                    <div className="flex justify-between mt-2 px-1">
                      <span className="text-[10px] font-bold text-on-surface-variant dark:text-slate-500 uppercase">极度恐惧</span>
                      <span className="text-[10px] font-bold text-on-surface-variant dark:text-slate-500 uppercase">中性</span>
                      <span className="text-[10px] font-bold text-on-surface-variant dark:text-slate-500 uppercase">极度贪婪</span>
                    </div>
                  </div>

                  {/* Sentiment Summary */}
                  <div className="space-y-3">
                    <h5 className="text-xs font-bold text-on-surface-variant dark:text-slate-400 uppercase tracking-widest">情绪摘要</h5>
                    <p className="text-sm text-on-surface-variant dark:text-slate-300 leading-relaxed">
                      {latestAnalysis?.headline_summary || xData?.key_narratives?.[0] || '运行AI分析以生成市场情绪摘要。'}
                    </p>
                  </div>
                </div>

                {/* Right: Impacted Asset Clusters */}
                <div className="space-y-4">
                  <h5 className="text-xs font-bold text-on-surface-variant dark:text-slate-400 uppercase tracking-widest">受影响资产</h5>
                  <div className="grid grid-cols-2 gap-3">
                    {affectedStocks.slice(0, 4).map((stock) => {
                      const isPos = stock.impact_score > 0
                      return (
                        <button type="button" key={stock.ticker} className="w-full text-left bg-surface-container-low dark:bg-slate-700 p-4 rounded-2xl hover:bg-white dark:hover:bg-slate-600 transition-colors cursor-pointer group">
                          <div className="flex justify-between items-center mb-1">
                            <span className="font-bold text-sm dark:text-white">{stock.ticker}</span>
                            <span className={`material-symbols-outlined text-lg material-symbols-filled ${isPos ? 'text-tertiary dark:text-emerald-400' : 'text-error dark:text-red-400'}`}>
                              {isPos ? 'trending_up' : 'trending_down'}
                            </span>
                          </div>
                          <p className="text-[10px] text-on-surface-variant dark:text-slate-400 group-hover:text-primary dark:group-hover:text-violet-400 transition-colors line-clamp-1">
                            {stock.reason}
                          </p>
                          <div className={`mt-2 h-1 rounded-full overflow-hidden ${isPos ? 'bg-tertiary-container dark:bg-emerald-900/30' : 'bg-error-container/30 dark:bg-red-900/30'}`}>
                            <div className={`h-full ${isPos ? 'bg-tertiary dark:bg-emerald-500' : 'bg-error dark:bg-red-500'}`}
                              style={{ width: `${Math.min(Math.abs(stock.impact_score), 100)}%` }} />
                          </div>
                        </button>
                      )
                    })}
                    {affectedStocks.length === 0 && (
                      <p className="col-span-2 text-sm text-on-surface-variant dark:text-slate-500 italic">运行AI分析后将显示受影响资产</p>
                    )}
                  </div>
                </div>
              </div>

              {/* Action buttons */}
              <div className="mt-8 pt-6 border-t border-slate-200/50 dark:border-slate-700 flex flex-wrap gap-4">
                <Link to="/analysis" className="flex items-center gap-2 px-5 py-3 bg-slate-900 dark:bg-violet-600 text-white rounded-xl text-sm font-bold shadow-lg active:scale-95 transition-all">
                  <span className="material-symbols-outlined text-lg">description</span> 查看详细报告
                </Link>
                <Link to="/news" className="flex items-center gap-2 px-5 py-3 bg-white dark:bg-slate-700 text-slate-700 dark:text-slate-200 border border-slate-200 dark:border-slate-600 rounded-xl text-sm font-bold hover:bg-slate-50 dark:hover:bg-slate-600 active:scale-95 transition-all">
                  <span className="material-symbols-outlined text-lg">newspaper</span> 浏览新闻流
                </Link>
              </div>
            </section>

            {/* Global Macro Headlines */}
            <div className="space-y-4">
              <h3 className="text-xl font-bold font-headline dark:text-white px-2">全球宏观头条</h3>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {news.slice(0, 4).map((item) => (
                  <Link key={item.id} to="/news" className="bg-surface-container-low dark:bg-slate-800 p-5 rounded-2xl flex gap-4 hover:-translate-y-1 transition-transform cursor-pointer">
                    <div className="w-14 h-14 bg-slate-200 dark:bg-slate-700 rounded-xl flex-shrink-0 flex items-center justify-center">
                      <span className="material-symbols-outlined text-slate-400 dark:text-slate-500 text-2xl">
                        {item.analysis?.classification === 'bullish' ? 'trending_up' : item.analysis?.classification === 'bearish' ? 'trending_down' : 'public'}
                      </span>
                    </div>
                    <div className="min-w-0">
                      <h4 className="font-bold text-sm mb-1 leading-tight dark:text-white line-clamp-2">
                        {item.analysis?.title_zh || item.title}
                      </h4>
                      <p className="text-[10px] text-on-surface-variant dark:text-slate-500 font-medium">
                        {item.source} · {toLocalTime(item.published_at)}
                      </p>
                    </div>
                  </Link>
                ))}
              </div>
            </div>
          </div>

          {/* Right Sidebar Widgets */}
          <div className="space-y-6">
            {/* Market Sentiment Ring */}
            <section className="bg-surface-container-lowest dark:bg-slate-800 p-6 rounded-[2rem] shadow-xl shadow-slate-200/50 dark:shadow-none">
              <h3 className="text-sm font-black uppercase tracking-[0.15em] text-on-surface-variant dark:text-slate-400 mb-6">市场情绪指数</h3>
              <div className="text-center space-y-4">
                <div className="bg-gradient-to-br from-slate-900 via-violet-950 to-slate-900 rounded-2xl p-6">
                  <FearGreedGauge value={fearGreed} />
                </div>
                {stats && (
                  <p className="text-xs text-on-surface-variant dark:text-slate-400 leading-relaxed px-4">
                    已分析 <strong className="dark:text-white">{stats.total_analyzed}</strong> 条新闻，
                    看多 <strong className="text-tertiary dark:text-emerald-400">{stats.bullish_count}</strong>，
                    看空 <strong className="text-error dark:text-red-400">{stats.bearish_count}</strong>
                  </p>
                )}
              </div>
            </section>

            {/* Commodity Impact */}
            <section className="bg-surface-container-lowest dark:bg-slate-800 p-6 rounded-[2rem] shadow-xl shadow-slate-200/50 dark:shadow-none">
              <h3 className="text-sm font-black uppercase tracking-[0.15em] text-on-surface-variant dark:text-slate-400 mb-4">大宗商品影响</h3>
              <div className="space-y-3">
                {commodities.map(q => {
                  const price = q.price ?? q.previousClose ?? 0
                  const pct = q.changePercent ?? 0
                  const isPos = pct > 0
                  const isNeg = pct < 0
                  return (
                    <div key={q.symbol} className="flex items-center justify-between p-3 bg-surface-container-low dark:bg-slate-700 rounded-xl">
                      <div className="flex items-center gap-3">
                        <span className="material-symbols-outlined text-amber-500 material-symbols-filled">
                          {q.name.includes('Gold') ? 'stars' : q.name.includes('Oil') ? 'local_fire_department' : 'stars'}
                        </span>
                        <div>
                          <p className="text-xs font-bold dark:text-white">{q.label}</p>
                          <p className="text-[10px] text-on-surface-variant dark:text-slate-500">{q.name}</p>
                        </div>
                      </div>
                      <div className="text-right">
                        <p className={`text-xs font-black ${isPos ? 'text-tertiary dark:text-emerald-400' : isNeg ? 'text-error dark:text-red-400' : 'text-slate-400'}`}>
                          {isPos ? '+' : ''}{pct.toFixed(2)}%
                        </p>
                        <p className="text-[10px] text-on-surface-variant dark:text-slate-400">
                          {price > 0 ? price.toLocaleString(undefined, { maximumFractionDigits: 2 }) : '—'}
                        </p>
                      </div>
                    </div>
                  )
                })}
                {commodities.length === 0 && <p className="text-sm text-on-surface-variant italic">加载中...</p>}
              </div>
            </section>

            {/* Macro Calendar */}
            <section className="bg-surface-container-lowest dark:bg-slate-800 p-6 rounded-[2rem] shadow-xl shadow-slate-200/50 dark:shadow-none">
              <div className="flex justify-between items-center mb-6">
                <h3 className="text-sm font-black uppercase tracking-[0.15em] text-on-surface-variant dark:text-slate-400">宏观日历</h3>
                <span className="material-symbols-outlined text-primary dark:text-violet-400 text-lg">event</span>
              </div>
              <div className="space-y-5">
                {events.map((ev, i) => {
                  const isHigh = ev.impact === 'high'
                  return (
                    <div key={i} className={`relative pl-6 border-l-2 ${isHigh ? 'border-primary/40 dark:border-violet-500/40' : 'border-slate-200 dark:border-slate-700'}`}>
                      <div className={`absolute -left-[5px] top-0 w-2 h-2 rounded-full ${isHigh ? 'bg-primary dark:bg-violet-500' : 'bg-slate-300 dark:bg-slate-600'}`} />
                      <p className={`text-[10px] font-black uppercase mb-1 ${isHigh ? 'text-primary dark:text-violet-400' : 'text-on-surface-variant dark:text-slate-500'}`}>
                        {ev.date} · {ev.country}
                      </p>
                      <p className="text-sm font-bold dark:text-white leading-tight">{ev.title_zh || ev.title}</p>
                      <p className="text-[10px] text-on-surface-variant dark:text-slate-500 mt-1">
                        影响: <span className={`font-bold ${isHigh ? 'text-error dark:text-red-400' : ev.impact === 'medium' ? 'text-amber-600 dark:text-amber-400' : 'text-slate-400'}`}>
                          {isHigh ? '高' : ev.impact === 'medium' ? '中' : '低'}
                        </span>
                      </p>
                      {ev.explanation && (
                        <p className="text-[10px] text-on-surface-variant dark:text-slate-400 mt-1 italic">{ev.explanation}</p>
                      )}
                      {ev.stock_impact && (
                        <span className={`inline-block mt-1 text-[9px] font-bold px-1.5 py-0.5 rounded ${
                          ev.stock_impact === 'bullish' ? 'bg-tertiary-container/50 text-on-tertiary-container' :
                          ev.stock_impact === 'bearish' ? 'bg-error-container/50 text-on-error-container' :
                          'bg-surface-container text-on-surface-variant'
                        }`}>
                          {ev.stock_impact === 'bullish' ? '利好股市' : ev.stock_impact === 'bearish' ? '利空股市' : '影响中性'}
                        </span>
                      )}
                    </div>
                  )
                })}
              </div>
              {allEvents.length > 3 && (
                <button
                  onClick={() => setCalendarExpanded(!calendarExpanded)}
                  className="w-full mt-4 py-2.5 border border-slate-100 dark:border-slate-700 rounded-xl text-xs font-bold text-on-surface-variant dark:text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-700 transition-colors uppercase tracking-widest"
                >
                  {calendarExpanded ? '收起' : `完整日程 (${allEvents.length})`}
                </button>
              )}
              <button
                onClick={handleAnalyzeCalendar}
                disabled={calendarAnalyzing}
                className="w-full mt-2 py-2.5 bg-primary/10 dark:bg-violet-500/10 text-primary dark:text-violet-400 rounded-xl text-xs font-bold hover:bg-primary/20 dark:hover:bg-violet-500/20 transition-colors uppercase tracking-widest disabled:opacity-50"
              >
                {calendarAnalyzing ? '分析中...' : '🤖 AI 分析日历'}
              </button>
            </section>
          </div>
        </div>
      </div>
    </main>
  )
}
