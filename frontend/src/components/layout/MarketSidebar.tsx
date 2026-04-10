import { useApi } from '../../hooks/useApi'
import { getMarketQuotes, type MarketQuote } from '../../services/api'
import type { AnalysisStats } from '../../types'

interface MarketSidebarProps {
  stats?: AnalysisStats | null
  onQuoteClick?: (quote: MarketQuote) => void
}

export default function MarketSidebar({ stats, onQuoteClick }: MarketSidebarProps) {
  const quotesApi = useApi((signal) => getMarketQuotes(signal), [])
  const quotes = quotesApi.data?.quotes ?? []

  const indices = quotes.filter(q => q.type === 'index')
  const commodities = quotes.filter(q => q.type === 'commodity')

  const fearGreed = stats ? (() => {
    const { bullish_count = 0, bearish_count = 0, neutral_count = 0 } = stats
    const total = bullish_count + bearish_count + neutral_count
    if (total === 0) return 50
    return Math.round((bullish_count / total) * 100)
  })() : 50

  const sentimentLabel = fearGreed >= 70 ? '贪婪' : fearGreed >= 55 ? '乐观' : fearGreed >= 45 ? '中性' : fearGreed >= 30 ? '恐惧' : '极度恐惧'

  return (
    <aside className="hidden xl:block fixed right-0 top-16 w-80 h-[calc(100vh-64px)] p-6 bg-surface-container-low dark:bg-slate-900 overflow-y-auto custom-scrollbar z-40">
      <div className="space-y-8">
        {/* Market Indices */}
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="text-xs font-black font-headline tracking-widest uppercase text-on-surface-variant dark:text-slate-400">
              热门行情
            </h3>
            <span className="material-symbols-outlined text-slate-400 text-lg">bolt</span>
          </div>

          <div className="space-y-3">
            {(indices.length > 0 ? indices : quotes).map((quote) => {
              const price = quote.price ?? quote.previousClose ?? 0
              const pct = quote.changePercent ?? 0
              const isPos = pct > 0
              const isNeg = pct < 0
              const isClosed = !quote.marketOpen
              const low52 = quote.yearLow ?? price * 0.7
              const high52 = quote.yearHigh ?? price * 1.15
              const range = high52 - low52
              const position = range > 0 ? ((price - low52) / range) * 100 : 50

              if (price === 0) return null

              return (
                <button type="button" key={quote.symbol} className="w-full text-left bg-surface-container-lowest dark:bg-slate-800 p-4 rounded-xl space-y-3 cursor-pointer hover:shadow-md hover:-translate-y-0.5 transition-all active:scale-[0.98]" onClick={() => onQuoteClick?.(quote)}>
                  <div className="flex justify-between items-center">
                    <div>
                      <span className="font-bold text-sm dark:text-slate-100">{quote.label || quote.name}</span>
                      {isClosed && (
                        <span className="ml-2 text-[9px] font-bold text-on-surface-variant/60 dark:text-slate-500 bg-surface-container dark:bg-slate-700 px-1.5 py-0.5 rounded">已收盘</span>
                      )}
                    </div>
                    <div className="text-right">
                      <p className="text-xs font-bold dark:text-slate-200">
                        {price.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                      </p>
                      <span className={`text-[11px] font-bold ${isPos ? 'text-tertiary dark:text-emerald-400' : isNeg ? 'text-error dark:text-red-400' : 'text-slate-400'}`}>
                        {isPos ? '+' : ''}{pct.toFixed(2)}%
                      </span>
                    </div>
                  </div>
                  {/* 52-week range */}
                  <div className="space-y-1">
                    <div className="flex justify-between text-[9px] text-on-surface-variant dark:text-slate-500">
                      <span>{low52.toLocaleString(undefined, { maximumFractionDigits: 0 })}</span>
                      <span className="text-on-surface-variant/50 dark:text-slate-600">52周范围</span>
                      <span>{high52.toLocaleString(undefined, { maximumFractionDigits: 0 })}</span>
                    </div>
                    <div className="relative w-full h-1.5 bg-surface-container dark:bg-slate-700 rounded-full">
                      <div
                        className={`absolute top-1/2 -translate-y-1/2 w-2.5 h-2.5 rounded-full border-2 border-white dark:border-slate-800 shadow-sm ${
                          isPos ? 'bg-tertiary dark:bg-emerald-500' : isNeg ? 'bg-error dark:bg-red-500' : 'bg-slate-400'
                        }`}
                        style={{ left: `calc(${Math.min(Math.max(position, 3), 97)}% - 5px)` }}
                      />
                    </div>
                  </div>
                </button>
              )
            })}
          </div>
        </div>

        {/* Sentiment Gauge */}
        <div className="bg-surface-container-lowest dark:bg-slate-800 p-6 rounded-2xl shadow-sm space-y-4">
          <h3 className="text-xs font-black font-headline text-on-surface dark:text-slate-100">综合情绪</h3>
          <div className="relative pt-6 pb-2">
            <div className="h-2 w-full sentiment-gradient rounded-full" />
            <div className="absolute top-[22px] flex flex-col items-center -translate-x-1/2" style={{ left: `${fearGreed}%` }}>
              <div className="w-4 h-4 rounded-full bg-white border-2 border-primary shadow-lg shadow-primary/20" />
              <span className="text-[10px] font-bold mt-1 text-on-surface dark:text-slate-300 whitespace-nowrap">
                {fearGreed} ({sentimentLabel})
              </span>
            </div>
          </div>
          <div className="flex justify-between text-[10px] font-bold text-on-surface-variant dark:text-slate-500 tracking-wider uppercase">
            <span>恐惧</span><span>中性</span><span>贪婪</span>
          </div>
          {stats && (
            <div className="grid grid-cols-3 gap-2 pt-2">
              {[
                { label: '看多', count: stats.bullish_count, color: 'text-tertiary dark:text-emerald-400' },
                { label: '中性', count: stats.neutral_count, color: 'text-slate-500' },
                { label: '看空', count: stats.bearish_count, color: 'text-error dark:text-red-400' },
              ].map(({ label, count, color }) => (
                <div key={label} className="text-center">
                  <p className={`text-lg font-black ${color}`}>{count}</p>
                  <p className="text-[10px] font-bold text-on-surface-variant dark:text-slate-500">{label}</p>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Commodities */}
        {commodities.length > 0 && (
          <div className="space-y-4">
            <h3 className="text-xs font-black font-headline tracking-widest uppercase text-on-surface-variant dark:text-slate-400">大宗商品</h3>
            <div className="space-y-3">
              {commodities.map(q => {
                const price = q.price ?? q.previousClose ?? 0
                const pct = q.changePercent ?? 0
                const isPos = pct > 0
                const isNeg = pct < 0
                if (price === 0) return null
                return (
                  <button type="button" key={q.symbol} className="w-full flex items-center gap-4 p-4 rounded-xl bg-surface-container-lowest dark:bg-slate-800 cursor-pointer hover:shadow-md hover:-translate-y-0.5 transition-all active:scale-[0.98]" onClick={() => onQuoteClick?.(q)}>
                    <div className="w-10 h-10 rounded-xl bg-surface-container dark:bg-slate-700 flex items-center justify-center">
                      <span className="material-symbols-outlined text-amber-500">
                        {q.name.includes('Gold') ? 'diamond' : q.name.includes('Oil') ? 'oil_barrel' : 'toll'}
                      </span>
                    </div>
                    <div className="flex-1">
                      <p className="font-bold text-sm dark:text-white">{q.label}</p>
                      <p className="text-xs text-on-surface-variant dark:text-slate-400">
                        ${price.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                      </p>
                    </div>
                    <span className={`text-sm font-bold ${isPos ? 'text-tertiary dark:text-emerald-400' : isNeg ? 'text-error dark:text-red-400' : 'text-slate-400'}`}>
                      {isPos ? '+' : ''}{pct.toFixed(2)}%
                    </span>
                  </button>
                )
              })}
            </div>
          </div>
        )}
      </div>
    </aside>
  )
}
