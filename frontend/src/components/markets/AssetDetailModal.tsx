import { useState, useEffect, useCallback, useRef } from 'react'
import { useApi } from '../../hooks/useApi'
import {
  getCandles, getAssetProfile, getAssetSentiment,
  type MarketQuote, type CandleData, type AssetProfile, type AssetSentiment,
} from '../../services/api'
import { toLocalTime } from '../../utils/time'

interface AssetDetailModalProps {
  // Either a full MarketQuote or just a symbol identifier
  quote?: MarketQuote
  symbol?: string       // e.g. "AMD", "NVDA", "GC=F"
  symbolName?: string   // e.g. "AMD", "NVIDIA Corp."
  onClose: () => void
}

const TIMEFRAMES = [
  { value: '1D', label: '近5日', interval: '15分钟' },
  { value: '1W', label: '近1月', interval: '1小时' },
  { value: '1M', label: '近6月', interval: '日线' },
  { value: '1Y', label: '近1年', interval: '日线' },
] as const
type Timeframe = typeof TIMEFRAMES[number]['value']

// ── SVG Candlestick renderer ────────────────────────────────────
const CHART_W = 800
const CHART_H = 300
const PAD = { top: 10, right: 10, bottom: 10, left: 10 }

function CandlestickChart({ data }: { data: CandleData }) {
  const { candles, ema20, sma50 } = data
  if (!candles.length) return <div className="h-64 md:h-80 flex items-center justify-center text-on-surface-variant dark:text-slate-500 text-sm">暂无数据</div>

  const allPrices = candles.flatMap(c => [c.high, c.low])
  const minP = Math.min(...allPrices)
  const maxP = Math.max(...allPrices)
  const range = maxP - minP || 1
  const padded = range * 0.08
  const low = minP - padded
  const high = maxP + padded
  const yRange = high - low

  const toY = (v: number) => PAD.top + (1 - (v - low) / yRange) * (CHART_H - PAD.top - PAD.bottom)
  const n = candles.length
  const barW = Math.max(2, Math.min(14, (CHART_W - PAD.left - PAD.right) / n * 0.6))
  const step = (CHART_W - PAD.left - PAD.right) / n
  const toX = (i: number) => PAD.left + step * i + step / 2

  const yTicks = Array.from({ length: 5 }, (_, i) => {
    const val = high - (yRange * i) / 4
    return { val, y: toY(val) }
  })

  const timeIdx = new Map(candles.map((c, i) => [c.time, i]))

  const maPath = (pts: { time: string; value: number }[]) => {
    const mapped = pts
      .map(p => ({ x: timeIdx.get(p.time), y: p.value }))
      .filter((p): p is { x: number; y: number } => p.x !== undefined)
    if (mapped.length < 2) return ''
    return mapped.map((p, i) => `${i === 0 ? 'M' : 'L'}${toX(p.x).toFixed(1)},${toY(p.y).toFixed(1)}`).join(' ')
  }

  return (
    <div className="w-full h-64 md:h-80 relative">
      <svg className="w-full h-full" viewBox={`0 0 ${CHART_W} ${CHART_H}`} preserveAspectRatio="none">
        {yTicks.map((t, i) => (
          <line key={i} x1={0} x2={CHART_W} y1={t.y} y2={t.y} stroke="currentColor" className="text-outline-variant/15 dark:text-slate-700/50" strokeWidth={1} />
        ))}
        {sma50.length > 1 && (
          <path d={maPath(sma50)} fill="none" stroke="#4953ac" strokeWidth={2.5} strokeDasharray="6,4" />
        )}
        {ema20.length > 1 && (
          <path d={maPath(ema20)} fill="none" stroke="#6a1cf6" strokeWidth={2.5} />
        )}
        {candles.map((c, i) => {
          const x = toX(i)
          const isUp = c.close >= c.open
          const color = isUp ? '#006a28' : '#b41340'
          const bodyTop = toY(Math.max(c.open, c.close))
          const bodyBot = toY(Math.min(c.open, c.close))
          const bodyH = Math.max(1, bodyBot - bodyTop)
          return (
            <g key={i}>
              <line x1={x} x2={x} y1={toY(c.high)} y2={toY(c.low)} stroke={color} strokeWidth={1.2} />
              <rect x={x - barW / 2} y={bodyTop} width={barW} height={bodyH} fill={color} rx={1} />
            </g>
          )
        })}
      </svg>
      <div className="absolute left-1 top-0 bottom-0 flex flex-col justify-between text-[10px] font-bold text-on-surface-variant/50 dark:text-slate-500 pointer-events-none py-1">
        {yTicks.map((t, i) => (
          <span key={i} className="tabular-nums">
            {t.val >= 10000 ? t.val.toLocaleString(undefined, { maximumFractionDigits: 0 })
             : t.val >= 100 ? t.val.toLocaleString(undefined, { maximumFractionDigits: 1 })
             : t.val.toLocaleString(undefined, { maximumFractionDigits: 2 })}
          </span>
        ))}
      </div>
    </div>
  )
}

// ── Format helpers ──────────────────────────────────────────────
function fmtLargeNum(n: number | null | undefined): string {
  if (n == null) return '—'
  if (n >= 1e12) return `$${(n / 1e12).toFixed(2)}T`
  if (n >= 1e9) return `$${(n / 1e9).toFixed(2)}B`
  if (n >= 1e6) return `$${(n / 1e6).toFixed(1)}M`
  return `$${n.toLocaleString()}`
}

function fmtPrice(n: number | null | undefined): string {
  if (n == null) return '—'
  return `$${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}

function fmtVolume(n: number | null | undefined): string {
  if (n == null) return '—'
  if (n >= 1e9) return `${(n / 1e9).toFixed(2)}B`
  if (n >= 1e6) return `${(n / 1e6).toFixed(2)}M`
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K`
  return n.toLocaleString()
}

function fmtCompact(n: number | null | undefined): string {
  if (n == null) return '—'
  return `$${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}

// ── Derive effective symbol from props ──────────────────────────
function deriveTypeLabel(sym: string, quoteType?: string): string {
  if (quoteType === 'index') return '指数'
  if (quoteType === 'commodity') return '大宗商品'
  if (sym.startsWith('^') || sym.endsWith('.SS') || sym.endsWith('.SZ')) return '指数'
  if (sym.includes('=F')) return '大宗商品'
  return '股票'
}

// ── Main Modal ──────────────────────────────────────────────────
export default function AssetDetailModal({ quote, symbol: symbolProp, symbolName, onClose }: AssetDetailModalProps) {
  // Derive the effective symbol & display name from whichever props are provided
  const effectiveSymbol = quote?.symbol ?? symbolProp ?? ''
  const effectiveName = quote?.name ?? symbolName ?? effectiveSymbol
  const effectiveLabel = quote?.label ?? symbolName ?? effectiveSymbol

  const [timeframe, setTimeframe] = useState<Timeframe>('1D')
  const [visible, setVisible] = useState(false)
  const closeButtonRef = useRef<HTMLButtonElement>(null)
  const dialogRef = useRef<HTMLDivElement>(null)
  const previousFocusRef = useRef<HTMLElement | null>(null)

  const candleApi = useApi<CandleData>((signal) => getCandles(effectiveSymbol, timeframe, signal), [effectiveSymbol, timeframe])
  const profileApi = useApi<AssetProfile>((signal) => getAssetProfile(effectiveSymbol, signal), [effectiveSymbol])
  const sentimentApi = useApi<AssetSentiment>((signal) => getAssetSentiment(effectiveSymbol, 7, signal), [effectiveSymbol])

  useEffect(() => {
    previousFocusRef.current = document.activeElement instanceof HTMLElement ? document.activeElement : null
    requestAnimationFrame(() => {
      setVisible(true)
      closeButtonRef.current?.focus()
    })
    const onEsc = (e: KeyboardEvent) => { if (e.key === 'Escape') handleClose() }
    document.addEventListener('keydown', onEsc)
    const previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    return () => {
      document.removeEventListener('keydown', onEsc)
      document.body.style.overflow = previousOverflow
      previousFocusRef.current?.focus()
    }
  }, [])

  const handleClose = useCallback(() => {
    setVisible(false)
    setTimeout(onClose, 200)
  }, [onClose])

  const price = quote?.price ?? null
  const pct = quote?.changePercent ?? null
  const change = quote?.change ?? null
  const isPos = pct != null && pct > 0
  const isNeg = pct != null && pct < 0

  const profile = profileApi.data
  const yearLow = profile?.year_low ?? quote?.yearLow ?? null
  const yearHigh = profile?.year_high ?? quote?.yearHigh ?? null
  const hasYearRange = price != null && yearLow != null && yearHigh != null && yearHigh > yearLow
  const yearProgress = hasYearRange ? Math.min(100, Math.max(0, ((price - yearLow) / (yearHigh - yearLow)) * 100)) : null

  const typeLabel = deriveTypeLabel(effectiveSymbol, quote?.type)
  const description = profile?.description
    ? (profile.description.length > 120 ? profile.description.slice(0, 120) + '…' : profile.description)
    : `${effectiveName} 的市场数据与新闻分析。`
  const selectedTimeframe = TIMEFRAMES.find((item) => item.value === timeframe) ?? TIMEFRAMES[0]
  const showFundamentals = typeLabel === '股票'

  return (
    <div
      className={`fixed inset-0 z-[100] flex items-center justify-center p-4 md:p-6 transition-all duration-200 ${
        visible ? 'bg-inverse-surface/10 backdrop-blur-md' : 'bg-transparent'
      }`}
      role="presentation"
      onClick={(e) => { if (e.target === e.currentTarget) handleClose() }}
    >
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby="asset-detail-title"
        onKeyDown={(event) => {
          if (event.key !== 'Tab') return
          const focusable = dialogRef.current?.querySelectorAll<HTMLElement>('button:not([disabled]), a[href], [tabindex]:not([tabindex="-1"])')
          if (!focusable?.length) return
          const first = focusable[0]
          const last = focusable[focusable.length - 1]
          if (event.shiftKey && document.activeElement === first) {
            event.preventDefault()
            last.focus()
          } else if (!event.shiftKey && document.activeElement === last) {
            event.preventDefault()
            first.focus()
          }
        }}
        className={`glass-modal w-full max-w-6xl max-h-[90vh] rounded-[2rem] shadow-2xl flex flex-col overflow-hidden overscroll-contain relative border border-white/40 dark:border-slate-700/40 transition-all duration-200 ${
          visible ? 'opacity-100 scale-100' : 'opacity-0 scale-95'
        }`}
      >
        {/* Close */}
        <button
          ref={closeButtonRef}
          type="button"
          aria-label="关闭资产详情"
          onClick={handleClose}
          className="absolute top-5 right-6 text-on-surface-variant hover:text-primary dark:text-slate-400 dark:hover:text-violet-400 transition-colors p-2 z-50"
        >
          <span className="material-symbols-outlined text-2xl">close</span>
        </button>

        {/* Content */}
        <div className="overflow-y-auto custom-scrollbar p-6 md:p-10 space-y-8">

          {/* ── Header ───────────────────────────── */}
          <div className="flex flex-col md:flex-row md:items-end justify-between gap-4">
            <div className="space-y-1">
              <div className="flex items-center gap-3 flex-wrap">
                <span className="bg-primary/10 text-primary dark:bg-violet-500/20 dark:text-violet-300 px-3 py-1 rounded-full text-[10px] font-bold tracking-widest uppercase font-headline">
                  {typeLabel}
                </span>
                <h1 id="asset-detail-title" className="text-2xl md:text-3xl font-extrabold font-headline tracking-tight text-on-surface dark:text-white">
                  {effectiveName} ({effectiveLabel})
                </h1>
              </div>
              <p className="text-on-surface-variant dark:text-slate-400 font-medium text-sm md:text-base">
                {description}
              </p>
            </div>
            <div className="text-left md:text-right flex-shrink-0">
              <div className="text-3xl md:text-4xl font-black font-headline text-on-surface dark:text-white tabular-nums">
                {price != null ? price.toLocaleString(undefined, { maximumFractionDigits: 2 }) : '暂无现价'}
              </div>
              {pct != null && change != null ? <div className={`flex items-center md:justify-end gap-1 font-bold text-sm ${
                isPos ? 'text-tertiary dark:text-emerald-400' : isNeg ? 'text-error dark:text-red-400' : 'text-on-surface-variant'
              }`}>
                <span className="material-symbols-outlined text-sm">
                  {isPos ? 'trending_up' : isNeg ? 'trending_down' : 'trending_flat'}
                </span>
                <span>{isPos ? '+' : ''}{pct.toFixed(2)}%（{isPos ? '+' : ''}${Math.abs(change).toFixed(2)}）</span>
              </div> : <p className="mt-1 text-xs text-on-surface-variant dark:text-slate-500">涨跌数据暂无</p>}
              <p className="mt-2 text-[10px] text-on-surface-variant dark:text-slate-500">
                {quote?.source || profile?.source || '数据源未标注'} · {quote?.as_of || profile?.as_of ? toLocalTime(quote?.as_of || profile?.as_of || '') : '更新时间未提供'}
              </p>
            </div>
          </div>

          {/* ── Chart + Sentiment ────────────────── */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            {/* Chart */}
            <div className="lg:col-span-2 bg-surface-container-lowest dark:bg-slate-800 rounded-3xl p-5 md:p-6 shadow-sm border border-outline-variant/10 dark:border-slate-700/30">
              <div className="flex items-center justify-between mb-6 flex-wrap gap-3">
                <div className="flex gap-2">
                  {TIMEFRAMES.map((item) => (
                    <button
                      type="button"
                      key={item.value}
                      onClick={() => setTimeframe(item.value)}
                      aria-pressed={timeframe === item.value}
                      className={`px-4 py-1.5 rounded-full text-xs font-bold transition-all duration-200 ${
                        timeframe === item.value
                          ? 'bg-primary text-on-primary shadow-sm'
                          : 'text-on-surface-variant dark:text-slate-400 hover:bg-surface-container dark:hover:bg-slate-700'
                      }`}
                    >
                      {item.label}
                    </button>
                  ))}
                </div>
                <div className="flex gap-5 text-[10px] font-bold text-on-surface-variant dark:text-slate-400 uppercase tracking-tight">
                  <span className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-primary" /> 20期指数均线</span>
                  <span className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-secondary" /> 50期简单均线</span>
                </div>
              </div>

              {candleApi.loading && !candleApi.data ? (
                <div className="h-64 md:h-80 flex items-center justify-center">
                  <div className="w-6 h-6 border-2 border-primary border-t-transparent rounded-full animate-spin" />
                </div>
              ) : candleApi.data ? (
                <>
                  <CandlestickChart data={candleApi.data} />
                  <p className="mt-2 text-[10px] text-on-surface-variant dark:text-slate-500">
                    范围：{selectedTimeframe.label} · 采样：{selectedTimeframe.interval} · 来源：{candleApi.data.source || '数据源未标注'} · {candleApi.data.as_of ? `更新于 ${toLocalTime(candleApi.data.as_of)}` : '更新时间未提供'}
                  </p>
                </>
              ) : (
                <div className="h-64 md:h-80 flex items-center justify-center text-on-surface-variant dark:text-slate-500 text-sm">
                  加载失败
                </div>
              )}
            </div>

            {/* Oracle Sentiment */}
            <div className="rounded-[2rem] p-6 md:p-8 flex flex-col justify-between text-white bg-gradient-to-br from-[#6a1cf6] to-[#4953ac] shadow-2xl shadow-primary/20 relative overflow-hidden">
              <div className="absolute -top-10 -right-10 w-40 h-40 bg-white/10 rounded-full blur-3xl" />
              <div className="relative z-10 flex flex-col h-full">
                <div className="flex items-center gap-3 mb-6">
                  <div className="w-10 h-10 bg-white/20 rounded-xl flex items-center justify-center backdrop-blur-md">
                    <span className="material-symbols-outlined text-white text-lg">psychology</span>
                  </div>
                  <h3 className="font-headline font-extrabold text-lg tracking-tight">相关新闻情绪</h3>
                </div>
                {sentimentApi.loading && !sentimentApi.data ? (
                  <div className="flex-1 flex items-center justify-center">
                    <div className="w-6 h-6 border-2 border-white/60 border-t-transparent rounded-full animate-spin" />
                  </div>
                ) : sentimentApi.data && sentimentApi.data.score != null ? (
                  <>
                    <div className="space-y-4 flex-1">
                      <div className="flex justify-between items-end mb-1">
                        <span className="text-[10px] font-bold text-white/70 uppercase tracking-[0.15em]">
                          {{ bullish: '新闻情绪偏多', bearish: '新闻情绪偏空', neutral: '新闻情绪中性' }[sentimentApi.data.signal ?? ''] ?? '新闻情绪中性'}
                        </span>
                        <span className="text-3xl font-black font-headline leading-none">{sentimentApi.data.score}%</span>
                      </div>
                      <div className="relative h-2.5 bg-white/20 rounded-full overflow-hidden">
                        <div
                          className="absolute top-0 left-0 h-full bg-white rounded-full shadow-[0_0_15px_rgba(255,255,255,0.6)] transition-all duration-500"
                          style={{ width: `${sentimentApi.data.score}%` }}
                        />
                      </div>
                      <p className="text-white/90 font-medium leading-relaxed text-sm pt-1">
                        {sentimentApi.data.description}
                      </p>
                      <div className="flex gap-4 text-[10px] font-bold text-white/60 pt-1">
                        <span>看多 <strong className="text-white">{sentimentApi.data.bullish}</strong></span>
                        <span>看空 <strong className="text-white">{sentimentApi.data.bearish}</strong></span>
                        <span>中性 <strong className="text-white">{sentimentApi.data.neutral}</strong></span>
                      </div>
                    </div>
                    <div className="flex gap-2 mt-6 flex-wrap">
                      {sentimentApi.data.tags.map((tag) => (
                        <span key={tag} className="px-3 py-1.5 bg-white/20 backdrop-blur-md rounded-lg text-[10px] font-bold tracking-widest uppercase border border-white/10">
                          {tag}
                        </span>
                      ))}
                    </div>
                    <p className="mt-3 text-[10px] text-white/50">来源：{sentimentApi.data.source || '已分析新闻'} · {sentimentApi.data.as_of ? toLocalTime(sentimentApi.data.as_of) : '更新时间未提供'}</p>
                  </>
                ) : (
                  <div className="flex-1 flex flex-col items-center justify-center gap-3 text-center">
                    <span className="material-symbols-outlined text-white/40 text-4xl">analytics</span>
                    <p className="text-white/60 text-sm font-medium leading-relaxed">
                      {sentimentApi.error ? '情绪数据加载失败' : '暂无情绪数据'}<br />
                      <span className="text-[10px] text-white/40">基于近 7 日相关新闻分析</span>
                    </p>
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* ── Bottom: Financial Metrics + Constituents ── */}
          <div className="grid grid-cols-1 gap-8">
            {/* Left 2/3: OHLV cards + Market Statistics */}
            <div className="space-y-6">
              {/* OHLV Quick Cards */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                {[
                  { label: '开盘', value: fmtPrice(profile?.open) },
                  { label: '最高', value: fmtPrice(profile?.day_high) },
                  { label: '最低', value: fmtPrice(profile?.day_low) },
                  { label: '成交量', value: fmtVolume(profile?.last_volume ?? profile?.avg_volume) },
                ].map((item) => (
                  <div key={item.label} className="bg-white dark:bg-slate-800 rounded-3xl p-5 shadow-sm border border-surface-container-low dark:border-slate-700/30 hover:-translate-y-0.5 hover:shadow-md transition-all">
                    <p className="text-[11px] font-bold text-on-surface-variant/70 dark:text-slate-400 uppercase tracking-widest mb-1">{item.label}</p>
                    <p className="text-xl font-bold text-on-surface dark:text-white tracking-tight font-headline">
                      {profile ? item.value : profileApi.error ? '加载失败' : <Skeleton />}
                    </p>
                  </div>
                ))}
              </div>

              {/* Market Statistics Table */}
              <div className="bg-surface-container-low dark:bg-slate-800/60 rounded-3xl p-6 md:p-8">
                <h3 className="font-headline font-bold text-lg mb-6 dark:text-white">市场统计</h3>
                {!showFundamentals && (
                  <p className="mb-5 rounded-xl bg-surface-container px-4 py-3 text-sm text-on-surface-variant dark:bg-slate-700 dark:text-slate-300">
                    指数与大宗商品没有可直接比较的公司基本面；为避免把交易所交易基金代理数据误作目标资产数据，此处不展示市值、市盈率和股息率。
                  </p>
                )}
                {profileApi.error && <p className="mb-5 text-sm text-error dark:text-red-400" role="alert">基本面数据加载失败：{profileApi.error}</p>}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-x-12 gap-y-5">
                  {showFundamentals && <>
                  <div className="flex justify-between items-center py-2 border-b border-surface-variant/30 dark:border-slate-700/40">
                    <span className="text-sm text-on-surface-variant dark:text-slate-400 font-medium">总市值</span>
                    <span className="text-sm font-bold text-on-surface dark:text-white">
                      {profile ? fmtLargeNum(profile.market_cap) : <Skeleton />}
                    </span>
                  </div>
                  <div className="flex justify-between items-center py-2 border-b border-surface-variant/30 dark:border-slate-700/40">
                    <span className="text-sm text-on-surface-variant dark:text-slate-400 font-medium">市盈率</span>
                    <span className="text-sm font-bold text-on-surface dark:text-white">
                      {profile ? (profile.pe_ratio != null ? profile.pe_ratio.toFixed(2) : '—') : <Skeleton />}
                    </span>
                  </div>
                  <div className="flex justify-between items-center py-2 border-b border-surface-variant/30 dark:border-slate-700/40">
                    <span className="text-sm text-on-surface-variant dark:text-slate-400 font-medium">股息率</span>
                    <span className="text-sm font-bold text-on-surface dark:text-white">
                      {profile ? (profile.dividend_yield != null ? `${(profile.dividend_yield * 100).toFixed(2)}%` : '—') : <Skeleton />}
                    </span>
                  </div>
                  <div className="flex justify-between items-center py-2 border-b border-surface-variant/30 dark:border-slate-700/40">
                    <span className="text-sm text-on-surface-variant dark:text-slate-400 font-medium">平均成交量</span>
                    <span className="text-sm font-bold text-on-surface dark:text-white">
                      {profile ? fmtVolume(profile.avg_volume) : <Skeleton />}
                    </span>
                  </div>
                  </>}
                  {/* 52-Week Range — full width */}
                  <div className="md:col-span-2">
                    <div className="flex justify-between items-center mb-3">
                      <span className="text-sm text-on-surface-variant dark:text-slate-400 font-medium">52周区间</span>
                      {hasYearRange ? <div className="flex gap-4">
                        <span className="text-xs font-bold dark:text-white">{fmtCompact(yearLow)}</span>
                        <span className="text-xs font-bold dark:text-white">{fmtCompact(yearHigh)}</span>
                      </div> : <span className="text-xs text-on-surface-variant dark:text-slate-500">暂无数据</span>}
                    </div>
                    {hasYearRange && yearProgress != null && <div className="w-full h-2 bg-surface-variant/40 dark:bg-slate-700 rounded-full relative overflow-hidden">
                      <div
                        className="absolute h-full bg-primary rounded-full shadow-[0_0_10px_rgba(106,28,246,0.3)]"
                        style={{ left: 0, width: `${yearProgress}%` }}
                      />
                    </div>}
                  </div>
                </div>
              </div>
            </div>
          </div>

        </div>
      </div>
    </div>
  )
}

function Skeleton() {
  return <span className="inline-block w-16 h-5 bg-surface-container-high dark:bg-slate-700 rounded animate-pulse" />
}
