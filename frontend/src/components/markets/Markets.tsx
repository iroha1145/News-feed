import { useApi } from '../../hooks/useApi'
import { usePolling } from '../../hooks/usePolling'
import { getMarketQuotes, getCalendar, getAnalysisStats, getNews, type CalendarResponse, type MarketQuote } from '../../services/api'
import type { AnalysisStats, NewsItem } from '../../types'
import FearGreedGauge from '../sentiment/FearGreedGauge'
import LoadingSpinner from '../common/LoadingSpinner'
import { useCallback, useState } from 'react'
import { Link } from 'react-router-dom'
import { toLocalTime } from '../../utils/time'
import AssetDetailModal from './AssetDetailModal'
import { getNewsSentimentIndex } from '../../utils/sentiment'
import { safeExternalUrl } from '../../utils/url'

export default function Markets() {
  const quotesApi = useApi<{ quotes: MarketQuote[] }>((signal) => getMarketQuotes(signal), [])
  const calendarApi = useApi<CalendarResponse>((signal) => getCalendar(signal), [])
  const statsApi = useApi<AnalysisStats>((signal) => getAnalysisStats(signal), [])
  const newsApi = useApi<{ items: NewsItem[]; total: number }>((signal) => getNews({ page_size: 5 }, signal), [])

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
  const sentiment = getNewsSentimentIndex(stats)
  const quoteAsOf = quotes.find((quote) => quote.as_of)?.as_of

  if (quotesApi.loading && !quotesApi.data) {
    return <LoadingSpinner className="py-20" />
  }

  if (quotesApi.error && !quotesApi.data) {
    return (
      <main className="p-8 text-center" role="alert">
        <h1 className="text-2xl font-bold dark:text-white">行情暂时无法加载</h1>
        <p className="mt-3 text-sm text-on-surface-variant dark:text-slate-400">{quotesApi.error}</p>
        <button type="button" onClick={quotesApi.refetch} className="mt-5 rounded-xl bg-primary px-5 py-2.5 text-sm font-bold text-white">重新加载</button>
      </main>
    )
  }

  return (
    <div className="flex gap-0">
      {/* Main Content */}
      <main id="main-content" className="flex-1 xl:mr-80 p-4 md:p-6 lg:p-8 space-y-8">
        {/* Market Index Cards — matching stitch design */}
        <section>
          <div className="flex items-center justify-between mb-6">
            <h1 className="text-3xl font-extrabold font-headline dark:text-white">市场总览</h1>
            <span className="text-xs text-on-surface-variant dark:text-slate-400">
              {quoteAsOf ? `行情时间 ${toLocalTime(quoteAsOf)}` : '行情时间未提供'}
            </span>
          </div>
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            {indices.map((q) => {
              const price = q.price
              const pct = q.changePercent
              const hasChange = pct != null
              const isPos = hasChange && pct > 0
              const isNeg = hasChange && pct < 0
              return (
                <button type="button" key={q.symbol} className="w-full text-left bg-surface-container-lowest dark:bg-slate-800 rounded-2xl p-5 space-y-3 cursor-pointer hover:shadow-lg hover:-translate-y-0.5 transition-all active:scale-[0.98]" onClick={() => setSelectedQuote(q)}>
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-bold text-on-surface-variant dark:text-slate-400 uppercase tracking-wider">{q.label}</span>
                    <span className={`text-xs font-bold px-2 py-0.5 rounded-full ${
                      isPos ? 'bg-tertiary-container text-on-tertiary-container' :
                      isNeg ? 'bg-error-container text-on-error-container' :
                      'bg-surface-container text-on-surface-variant dark:bg-slate-700 dark:text-slate-400'
                    }`}>
                      {hasChange ? `${isPos ? '+' : ''}${pct.toFixed(2)}%` : '涨跌暂无'}
                    </span>
                  </div>
                  <p className="text-2xl font-black dark:text-white tracking-tight">
                    {price != null ? price.toLocaleString(undefined, { maximumFractionDigits: 2 }) : '暂无报价'}
                  </p>
                  <div className="flex items-center justify-between text-[10px] text-on-surface-variant dark:text-slate-500">
                    <span>{q.marketOpen === true ? '交易中' : q.marketOpen === false ? '已收盘' : '交易状态未知'}</span>
                    <span>{q.source || '数据源未标注'}</span>
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
              const articleUrl = safeExternalUrl(featured.url)
              return (
                <div className="bg-surface-container-lowest dark:bg-slate-800 rounded-2xl p-6 space-y-4 mb-4">
                  <div className="flex items-center gap-3 text-xs font-bold text-on-surface-variant dark:text-slate-400 uppercase tracking-widest">
                    <span className="text-primary dark:text-violet-400">{featured.source}</span>
                    <span className="w-1 h-1 bg-outline-variant rounded-full" />
                    <span>{toLocalTime(featured.published_at)}</span>
                  </div>
                  {articleUrl ? <a href={articleUrl} target="_blank" rel="noopener noreferrer" className="block hover:text-primary dark:hover:text-violet-400">
                    <h3 className="text-xl font-bold font-headline leading-tight dark:text-white">
                      {analysis?.title_zh || featured.title}
                    </h3>
                  </a> : <h3 className="text-xl font-bold font-headline leading-tight dark:text-white">{analysis?.title_zh || featured.title}</h3>}
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
                  <div className="flex flex-wrap gap-3 text-xs font-bold">
                    {articleUrl && <a href={articleUrl} target="_blank" rel="noopener noreferrer" className="text-primary hover:underline dark:text-violet-400">阅读原文 ↗</a>}
                    {analysis && <Link to={`/analysis/${featured.id}`} className="text-on-surface-variant hover:underline dark:text-slate-400">查看分析依据</Link>}
                  </div>
                </div>
              )
            })()}

            {/* More news cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {news.slice(1, 3).map((item) => {
                const analysis = item.analysis
                const articleUrl = safeExternalUrl(item.url)
                return (
                  <article key={item.id} className="bg-surface-container-lowest dark:bg-slate-800 rounded-2xl p-5 space-y-3 hover:shadow-lg transition-shadow group">
                    {analysis?.classification && (
                      <span className={`text-[10px] font-black px-2.5 py-1 rounded-lg ${
                        analysis.classification === 'bullish' ? 'bg-tertiary-container text-on-tertiary-container' :
                        analysis.classification === 'bearish' ? 'bg-error-container text-on-error-container' :
                        'bg-surface-container-high text-on-surface-variant dark:bg-slate-700 dark:text-slate-300'
                      }`}>
                        {analysis.classification === 'bullish' ? '看多' : analysis.classification === 'bearish' ? '看空' : '中性'}
                      </span>
                    )}
                    {articleUrl ? <a href={articleUrl} target="_blank" rel="noopener noreferrer">
                      <h4 className="font-bold text-sm leading-tight dark:text-white group-hover:text-primary dark:group-hover:text-violet-400 transition-colors line-clamp-2">
                        {analysis?.title_zh || item.title}
                      </h4>
                    </a> : <h4 className="font-bold text-sm leading-tight dark:text-white line-clamp-2">{analysis?.title_zh || item.title}</h4>}
                    <p className="text-xs text-on-surface-variant dark:text-slate-400 line-clamp-2">
                      {analysis?.headline_summary || item.summary}
                    </p>
                    <span className="text-[10px] text-on-surface-variant dark:text-slate-500">
                      {item.source} · {toLocalTime(item.published_at)}
                    </span>
                    <div className="flex gap-3 text-[10px] font-bold">
                      {articleUrl && <a href={articleUrl} target="_blank" rel="noopener noreferrer" className="text-primary dark:text-violet-400">原文 ↗</a>}
                      {analysis && <Link to={`/analysis/${item.id}`} className="text-on-surface-variant dark:text-slate-400">分析依据</Link>}
                    </div>
                  </article>
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
                const price = q.price
                const pct = q.changePercent
                const isPos = pct != null && pct > 0
                const isNeg = pct != null && pct < 0
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
                        {price != null ? `$${price.toLocaleString(undefined, { maximumFractionDigits: 2 })}` : '暂无报价'}
                      </span>
                      <span className={`text-sm font-bold ${isPos ? 'text-tertiary dark:text-emerald-400' : isNeg ? 'text-error dark:text-red-400' : 'text-slate-400'}`}>
                        {pct != null ? `${isPos ? '+' : ''}${pct.toFixed(2)}%` : '涨跌暂无'}
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
            {calendarApi.data?.stale && (
              <p className="rounded-lg bg-amber-50 px-3 py-2 text-[10px] leading-relaxed text-amber-800 dark:bg-amber-950/30 dark:text-amber-300" role="status">
                日历上游暂不可用，当前显示最近一次有效数据{calendarApi.data.as_of ? `（${toLocalTime(calendarApi.data.as_of)}）` : ''}。
              </p>
            )}
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
            ) : calendarApi.loading ? (
              <p className="text-sm text-on-surface-variant dark:text-slate-500 italic" role="status">日历加载中…</p>
            ) : calendarApi.error ? (
              <p className="text-sm text-error dark:text-red-400" role="alert">经济日历加载失败</p>
            ) : (
              <p className="text-sm text-on-surface-variant dark:text-slate-500">近期暂无事件</p>
            )}
          </div>

          {/* Market Sentiment Index */}
          <div className="bg-surface-container-lowest dark:bg-slate-800 p-6 rounded-2xl text-center space-y-4">
            <h3 className="text-xs font-black font-headline tracking-widest uppercase text-on-surface-variant dark:text-slate-400">
              市场情绪指数
            </h3>
            <div className="bg-gradient-to-br from-slate-900 via-violet-950 to-slate-900 rounded-2xl p-6">
              <FearGreedGauge value={sentiment.value} label={sentiment.label} />
            </div>
            {stats && (
              <p className="text-xs text-on-surface-variant dark:text-slate-400">
                已分析 <strong className="dark:text-white">{stats.total_analyzed}</strong> 条新闻 · 
                看多 <strong className="text-tertiary dark:text-emerald-400">{stats.bullish_count}</strong> · 
                看空 <strong className="text-error dark:text-red-400">{stats.bearish_count}</strong>
              </p>
            )}
            <p className="text-[10px] leading-relaxed text-on-surface-variant dark:text-slate-500">
              来源：{sentiment.source}<br />窗口：{sentiment.window}
            </p>
          </div>

          {/* Commodity Impact */}
          {commodities.length > 0 && (
            <div className="bg-surface-container-lowest dark:bg-slate-800 p-5 rounded-2xl space-y-4">
              <h3 className="text-xs font-black font-headline tracking-[0.2em] uppercase text-on-surface-variant dark:text-slate-400">
                大宗商品行情
              </h3>
              <div className="space-y-3">
                {commodities.map(q => {
                  const price = q.price
                  const pct = q.changePercent
                  const isPos = pct != null && pct > 0
                  const isNeg = pct != null && pct < 0
                  const iconColorMap: Record<string, string> = { 'GC=F': 'bg-amber-400 text-white', 'SI=F': 'bg-slate-400 text-white', 'CL=F': 'bg-violet-500 text-white' }
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
                        <p className="text-[10px] text-on-surface-variant dark:text-slate-500 font-medium">{q.source || '数据源未标注'}</p>
                      </div>
                      <div className="text-right flex-shrink-0">
                        <p className={`text-sm font-bold ${isPos ? 'text-emerald-600 dark:text-emerald-400' : isNeg ? 'text-red-600 dark:text-red-400' : 'text-slate-400'}`}>
                          {pct != null ? `${isPos ? '+' : ''}${pct.toFixed(2)}%` : '—'}
                        </p>
                        <p className="text-xs text-on-surface-variant dark:text-slate-400 font-medium">
                          {price != null ? price.toLocaleString(undefined, { maximumFractionDigits: 2 }) : '暂无报价'}
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
