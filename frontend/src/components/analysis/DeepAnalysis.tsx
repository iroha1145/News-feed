import { useParams, Link } from 'react-router-dom'
import { useApi } from '../../hooks/useApi'
import { getAnalysisByNewsId, getAnalyses, getNews, triggerAnalysis } from '../../services/api'
import type { Analysis, NewsItem } from '../../types'
import LoadingSpinner from '../common/LoadingSpinner'
import SentimentChip from '../common/SentimentChip'
import NewsImage from '../news/NewsImage'
import { useState } from 'react'
import AssetDetailModal from '../markets/AssetDetailModal'
import { parseUtcDate } from '../../utils/time'
import { getRealImageUrl } from '../../utils/image'

export default function DeepAnalysis() {
  const { id } = useParams<{ id: string }>()
  const [triggerMsg, setTriggerMsg] = useState<string | null>(null)
  const [selectedTicker, setSelectedTicker] = useState<{ symbol: string; name?: string } | null>(null)

  const selectedNewsId = id ? parseInt(id) : null

  // ── Detail mode: fetch by news_id directly ──
  const directApi = useApi<{ analysis: Analysis; news: NewsItem | null }>(
    () => selectedNewsId ? getAnalysisByNewsId(selectedNewsId) : Promise.reject('no id'),
    [selectedNewsId]
  )

  // ── List mode (no id): load latest analyses ──
  const analysesApi = useApi<{ items: Analysis[]; total: number }>(
    () => selectedNewsId ? Promise.resolve({ items: [], total: 0 }) : getAnalyses({ page: 1, page_size: 20 }),
    [selectedNewsId]
  )
  const newsApi = useApi<{ items: NewsItem[]; total: number }>(
    () => selectedNewsId ? Promise.resolve({ items: [], total: 0 }) : getNews(),
    [selectedNewsId]
  )

  // Detail mode uses directApi only; list mode uses analysesApi
  const selectedAnalysis = selectedNewsId
    ? directApi.data?.analysis ?? null
    : (analysesApi.data?.items ?? [])[0] ?? null

  const matchedNews = selectedNewsId
    ? directApi.data?.news ?? null
    : selectedAnalysis
    ? (newsApi.data?.items ?? []).find(n => n.id === selectedAnalysis.news_id) ?? null
    : null

  const analyses = analysesApi.data?.items ?? []
  const newsItems = newsApi.data?.items ?? []

  const isLoading = selectedNewsId ? directApi.loading : analysesApi.loading

  const handleTrigger = async () => {
    setTriggerMsg('分析已触发...')
    try {
      await triggerAnalysis()
      setTriggerMsg('分析正在后台运行，稍后刷新查看')
      setTimeout(() => {
        if (selectedNewsId) directApi.refetch()
        else analysesApi.refetch()
        setTriggerMsg(null)
      }, 5000)
    } catch {
      setTriggerMsg('触发分析失败')
    }
  }

  if (isLoading) {
    return <LoadingSpinner className="py-20" />
  }

  // No analysis found (directApi returned 404, or list is empty)
  if (!selectedAnalysis) {
    return (
      <div className="flex-1 p-6 md:p-8">
        <div className="max-w-2xl mx-auto text-center py-20">
          <span className="material-symbols-outlined text-6xl text-primary/30 dark:text-violet-400/30 mb-6 block">
            psychology
          </span>
          <h2 className="text-2xl font-extrabold font-headline mb-4 dark:text-white">
            {selectedNewsId ? '未找到分析' : '暂无分析'}
          </h2>
          <p className="text-on-surface-variant dark:text-slate-400 mb-8 leading-relaxed">
            {selectedNewsId
              ? 'This news article hasn\'t been analyzed yet. Trigger an analysis to get AI-powered insights.'
              : '运行分析引擎，获取AI深度分析。'}
          </p>
          <div className="flex justify-center gap-4">
            <button
              onClick={handleTrigger}
              className="bg-gradient-to-r from-primary to-primary-container text-white px-6 py-3 rounded-xl font-bold text-sm hover:shadow-lg hover:shadow-primary/20 active:scale-95 transition-all"
            >
              <span className="material-symbols-outlined text-sm align-middle mr-2">auto_awesome</span>
              Run Analysis
            </button>
            <Link
              to="/news"
              className="px-6 py-3 rounded-xl font-bold text-sm border border-surface-container dark:border-slate-700 text-on-surface-variant dark:text-slate-400 hover:bg-surface-container dark:hover:bg-slate-800 transition-all"
            >
              Back to News
            </Link>
          </div>
          {triggerMsg && (
            <p className="mt-4 text-sm text-primary dark:text-violet-400 animate-pulse">{triggerMsg}</p>
          )}
        </div>
      </div>
    )
  }

  const classification = selectedAnalysis.classification as 'bullish' | 'bearish' | 'neutral'
  const isBullish = classification === 'bullish'
  const isBearish = classification === 'bearish'

  // Parse JSON strings from backend
  const affectedStocks = safeParseJson<Array<{ ticker: string; impact_score: number; reason: string }>>(selectedAnalysis.affected_stocks) ?? []
  const keyFactors = safeParseJson<string[]>(selectedAnalysis.key_factors) ?? []
  const affectedCommodities = safeParseJson<Array<{ name: string; impact_score: number; reason: string }>>(selectedAnalysis.affected_commodities) ?? []
  const affectedSectors = safeParseJson<string[]>(selectedAnalysis.affected_sectors) ?? []

  // 置信度 score
  const confidence = selectedAnalysis.confidence ?? 0

  return (
    <div className="xl:grid xl:grid-cols-[1fr_20rem] gap-0">
      {/* Main Content */}
      <main className="min-w-0 p-4 md:p-6 lg:p-8 space-y-8">
        {/* Breadcrumb */}
        <div className="flex items-center gap-2 text-xs text-on-surface-variant dark:text-slate-500">
          <Link to="/news" className="hover:text-primary dark:hover:text-violet-400 transition-colors">新闻</Link>
          <span className="material-symbols-outlined text-[14px]">chevron_right</span>
          <span className="text-on-surface dark:text-slate-300">深度分析</span>
        </div>

        {/* Hero */}
        <section className="space-y-6">
          <div className="flex flex-wrap items-center gap-3">
            <SentimentChip classification={classification} score={selectedAnalysis.overall_sentiment} />
            <span className="text-xs font-bold text-on-surface-variant dark:text-slate-400 uppercase tracking-wider">
              分析时间: {selectedAnalysis.analyzed_at ? parseUtcDate(selectedAnalysis.analyzed_at).toLocaleString('zh-CN') : ''}
            </span>
            {selectedAnalysis.llm_provider && (
              <span className="text-[10px] font-bold text-on-surface-variant dark:text-slate-500 bg-surface-container dark:bg-slate-800 px-2 py-1 rounded-full uppercase">
                {selectedAnalysis.llm_provider} / {selectedAnalysis.llm_model}
              </span>
            )}
          </div>

          <h1 className="text-xl md:text-2xl lg:text-3xl font-extrabold font-headline tracking-tight leading-tight dark:text-white break-words">
            {selectedAnalysis.title_zh || matchedNews?.title || '市场分析'}
          </h1>
          {matchedNews?.title && selectedAnalysis.title_zh && selectedAnalysis.title_zh !== matchedNews.title && (
            <p className="text-sm text-on-surface-variant dark:text-slate-400">{matchedNews.title}</p>
          )}

          {/* News Image — filter out generic publisher logos */}
          {(() => {
            const heroImg = getRealImageUrl(matchedNews?.image_url)
            return heroImg ? (
              <div className="w-full h-48 md:h-64 rounded-2xl overflow-hidden">
                <NewsImage
                  src={heroImg}
                  alt={matchedNews?.title ?? ''}
                  className="w-full h-full"
                />
              </div>
            ) : null
          })()}

          {/* Summary */}
          <div className="bg-surface-container-lowest dark:bg-slate-900 rounded-2xl p-6 md:p-8">
            <p className="text-on-surface-variant dark:text-slate-300 leading-relaxed text-lg">
              {selectedAnalysis.headline_summary}
            </p>
          </div>
        </section>

        {/* 置信度 Score */}
        <section className="bg-surface-container-lowest dark:bg-slate-900 rounded-2xl p-6">
          <div className="flex items-center gap-3 mb-4">
            <span className="material-symbols-outlined text-primary dark:text-violet-400">verified</span>
            <h3 className="font-bold font-headline dark:text-white">AI置信度</h3>
          </div>
          <div className="flex items-center gap-6">
            <div className="relative w-24 h-24">
              <svg className="w-24 h-24 transform -rotate-90" viewBox="0 0 36 36">
                <path
                  className="text-surface-container dark:text-slate-700"
                  d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="3"
                />
                <path
                  className={isBullish ? 'text-tertiary dark:text-emerald-400' : isBearish ? 'text-error dark:text-red-400' : 'text-amber-500'}
                  d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="3"
                  strokeDasharray={`${confidence}, 100`}
                  strokeLinecap="round"
                />
              </svg>
              <div className="absolute inset-0 flex items-center justify-center">
                <span className="text-2xl font-black dark:text-white">{confidence}</span>
              </div>
            </div>
            <div className="flex-1">
              <p className="font-bold dark:text-white mb-1">
                {confidence >= 80 ? '高置信度' : confidence >= 50 ? '中等置信度' : '低置信度'}
              </p>
              <p className="text-sm text-on-surface-variant dark:text-slate-400 leading-relaxed">
                {isBullish
                  ? 'The analysis engine identifies a strong positive signal in the underlying data. Multiple corroborating factors suggest upward momentum.'
                  : isBearish
                  ? '多个指标检测到警告信号。分析表明应保持谨慎，存在潜在下行风险。'
                  : '各指标信号混合。市场方向不明确，需持续关注。'}
              </p>
            </div>
          </div>
        </section>

        {/* 关键因素 */}
        {keyFactors.length > 0 && (
          <section className="bg-surface-container-lowest dark:bg-slate-900 rounded-2xl p-6 md:p-8">
            <div className="flex items-center gap-3 mb-6">
              <span className="material-symbols-outlined text-primary dark:text-violet-400">checklist</span>
              <h3 className="font-bold font-headline dark:text-white">关键因素</h3>
            </div>
            <div className="space-y-3">
              {keyFactors.map((factor, i) => (
                <div key={i} className="flex gap-3 items-start">
                  <div className={`mt-1 w-6 h-6 rounded-lg flex items-center justify-center flex-shrink-0 text-xs font-bold ${
                    isBullish
                      ? 'bg-tertiary-container text-on-tertiary-container'
                      : isBearish
                      ? 'bg-error-container text-on-error-container'
                      : 'bg-surface-container dark:bg-slate-700 text-on-surface-variant dark:text-slate-400'
                  }`}>
                    {i + 1}
                  </div>
                  <p className="text-sm text-on-surface-variant dark:text-slate-300 leading-relaxed">{factor}</p>
                </div>
              ))}
            </div>
          </section>
        )}

        {/* Logic Chain */}
        {selectedAnalysis.logic_chain && (
          <section className="bg-surface-container-lowest dark:bg-slate-900 rounded-2xl p-6 md:p-8">
            <div className="flex items-center gap-3 mb-4">
              <span className="material-symbols-outlined text-primary dark:text-violet-400">timeline</span>
              <h3 className="font-bold font-headline dark:text-white">AI深度解读</h3>
            </div>
            <div className="prose prose-sm dark:prose-invert max-w-none">
              <p className="text-on-surface-variant dark:text-slate-300 leading-relaxed whitespace-pre-line">
                {selectedAnalysis.logic_chain}
              </p>
            </div>
          </section>
        )}

        {/* 受影响股票 */}
        {affectedStocks.length > 0 && (
          <section>
            <h3 className="font-bold font-headline mb-4 dark:text-white">受影响股票</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {affectedStocks.map((stock) => {
                const positive = stock.impact_score > 0
                return (
                  <button
                    key={stock.ticker}
                    type="button"
                    className="w-full text-left bg-surface-container-lowest dark:bg-slate-900 rounded-xl p-4 space-y-2 cursor-pointer hover:shadow-lg hover:-translate-y-0.5 transition-all active:scale-[0.98]"
                    onClick={() => setSelectedTicker({ symbol: stock.ticker, name: stock.ticker })}
                  >
                    <div className="flex items-center justify-between">
                      <span className="font-mono font-bold dark:text-white">{stock.ticker}</span>
                      <span className={`font-bold text-sm ${positive ? 'text-tertiary dark:text-emerald-400' : 'text-error dark:text-red-400'}`}>
                        {positive ? '▲' : '▼'} Impact: {Math.abs(stock.impact_score)}
                      </span>
                    </div>
                    <div className="h-1.5 bg-surface-container dark:bg-slate-700 rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full transition-all ${positive ? 'bg-tertiary dark:bg-emerald-500' : 'bg-error dark:bg-red-500'}`}
                        style={{ width: `${Math.min(Math.abs(stock.impact_score) * 10, 100)}%` }}
                      />
                    </div>
                    <p className="text-xs text-on-surface-variant dark:text-slate-400">{stock.reason}</p>
                  </button>
                )
              })}
            </div>
          </section>
        )}

        {/* 近期分析 List (when no specific ID) */}
        {!selectedNewsId && analyses.length > 1 && (
          <section>
            <h3 className="text-xl font-extrabold font-headline mb-6 dark:text-white">近期分析</h3>
            <div className="space-y-3">
              {analyses.slice(1, 10).map((a) => {
                const aNews = newsItems.find(n => n.id === a.news_id)
                return (
                  <Link
                    key={a.id}
                    to={`/analysis/${a.news_id}`}
                    className="flex items-center gap-4 p-4 bg-surface-container-lowest dark:bg-slate-900 rounded-xl hover:shadow-md transition-all group"
                  >
                    {getRealImageUrl(aNews?.image_url) && (
                      <div className="w-16 h-16 rounded-xl overflow-hidden flex-shrink-0">
                        <NewsImage
                          src={getRealImageUrl(aNews?.image_url)}
                          alt={a.headline_summary || ''}
                          className="w-full h-full"
                        />
                      </div>
                    )}
                    <div className="flex-1 min-w-0">
                      <p className="font-semibold text-sm truncate group-hover:text-primary dark:text-white dark:group-hover:text-violet-400 transition-colors">
                        {a.headline_summary || aNews?.title || `Analysis #${a.id}`}
                      </p>
                      <p className="text-xs text-on-surface-variant dark:text-slate-400 mt-1 truncate">
                        {a.headline_summary?.slice(0, 100)}...
                      </p>
                    </div>
                    <SentimentChip
                      classification={a.classification as 'bullish' | 'bearish' | 'neutral'}
                      size="sm"
                    />
                  </Link>
                )
              })}
            </div>
          </section>
        )}
      </main>

      {/* Right Sidebar */}
      <aside className="hidden xl:block h-[calc(100vh-64px)] sticky top-16 p-6 overflow-y-auto custom-scrollbar bg-surface-container-low dark:bg-slate-900/50 border-l border-surface-container dark:border-slate-800">
        <div className="space-y-8">
          {/* 板块影响 */}
          {affectedSectors.length > 0 && (
            <div className="space-y-4">
              <h3 className="text-sm font-black font-headline tracking-widest uppercase text-on-surface-variant dark:text-slate-400">
                板块影响
              </h3>
              <div className="space-y-3">
                {affectedSectors.map((sector, i) => {
                  // Map overall_sentiment (-100..100) to a 0-100% bar
                  const sent = selectedAnalysis.overall_sentiment ?? 0
                  const barPct = Math.min(100, Math.max(5, Math.round((sent + 100) / 2)))
                  return (
                    <div
                      key={i}
                      className="bg-surface-container-lowest dark:bg-slate-800 p-4 rounded-xl"
                    >
                      <div className="flex items-center justify-between mb-2">
                        <span className="text-sm font-semibold dark:text-white">{sector}</span>
                        <span className={`text-xs font-bold ${isBullish ? 'text-tertiary dark:text-emerald-400' : isBearish ? 'text-error dark:text-red-400' : 'text-slate-400'}`}>
                          {sent > 0 ? '+' : ''}{sent}
                        </span>
                      </div>
                      <div className="h-1.5 bg-surface-container dark:bg-slate-700 rounded-full overflow-hidden">
                        <div
                          className={`h-full rounded-full ${isBullish ? 'bg-tertiary dark:bg-emerald-500' : isBearish ? 'bg-error dark:bg-red-500' : 'bg-slate-400'}`}
                          style={{ width: `${barPct}%` }}
                        />
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>
          )}

          {/* 大宗商品影响 */}
          {affectedCommodities.length > 0 && (
            <div className="space-y-4">
              <h3 className="text-sm font-black font-headline tracking-widest uppercase text-on-surface-variant dark:text-slate-400">
                大宗商品影响
              </h3>
              <div className="space-y-3">
                {affectedCommodities.map((c, i) => {
                  const n = c.name.toLowerCase()
                  const isPositive = c.impact_score > 0
                  return (
                    <div key={i} className="flex items-center gap-3 p-3 bg-surface-container-lowest dark:bg-slate-800 rounded-xl">
                      <span className="material-symbols-outlined text-amber-500">
                        {n.includes('oil') || n.includes('crude') ? 'oil_barrel' :
                         n.includes('gold') ? 'diamond' :
                         n.includes('wheat') || n.includes('grain') || n.includes('corn') ? 'grain' :
                         n.includes('silver') ? 'toll' : 'monitoring'}
                      </span>
                      <div className="flex-1">
                        <span className="text-sm font-semibold dark:text-slate-300">{c.name}</span>
                        {c.reason && <p className="text-xs text-on-surface-variant dark:text-slate-500 mt-0.5">{c.reason}</p>}
                      </div>
                      <span className={`text-sm font-bold ${isPositive ? 'text-tertiary dark:text-emerald-400' : 'text-error dark:text-red-400'}`}>
                        {isPositive ? '▲' : '▼'} {Math.abs(c.impact_score)}
                      </span>
                    </div>
                  )
                })}
              </div>
            </div>
          )}

          {/* 关键指标 */}
          <div className="space-y-4">
            <h3 className="text-sm font-black font-headline tracking-widest uppercase text-on-surface-variant dark:text-slate-400">
              关键指标
            </h3>
            <div className="space-y-3">
              {[
                { label: '情绪分数', value: selectedAnalysis.overall_sentiment?.toFixed(1) ?? '—', icon: 'monitoring' },
                { label: '置信度', value: `${confidence}%`, icon: 'verified' },
                { label: '分类', value: classification.charAt(0).toUpperCase() + classification.slice(1), icon: 'label' },
              ].map(m => (
                <div key={m.label} className="flex items-center gap-3 p-3 bg-surface-container-lowest dark:bg-slate-800 rounded-xl">
                  <span className="material-symbols-outlined text-primary dark:text-violet-400 text-xl">{m.icon}</span>
                  <div className="flex-1">
                    <p className="text-xs text-on-surface-variant dark:text-slate-400">{m.label}</p>
                    <p className="font-bold text-sm dark:text-white">{m.value}</p>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Actions */}
          <div className="space-y-3">
            <button
              onClick={handleTrigger}
              className="w-full py-3 bg-gradient-to-r from-primary to-primary-container text-white rounded-xl text-xs font-bold hover:shadow-lg active:scale-95 transition-all"
            >
              <span className="material-symbols-outlined text-sm align-middle mr-1">auto_awesome</span>
              Run New Analysis
            </button>
            {triggerMsg && (
              <p className="text-xs text-primary dark:text-violet-400 text-center animate-pulse">{triggerMsg}</p>
            )}
            <Link
              to="/news"
              className="block w-full py-3 border border-surface-container dark:border-slate-700 text-on-surface-variant dark:text-slate-400 rounded-xl text-xs font-bold text-center hover:bg-surface-container dark:hover:bg-slate-800 transition-all"
            >
              Back to News Feed
            </Link>
          </div>
        </div>
      </aside>

      {/* Asset Detail Modal */}
      {selectedTicker && (
        <AssetDetailModal
          symbol={selectedTicker.symbol}
          symbolName={selectedTicker.name}
          onClose={() => setSelectedTicker(null)}
        />
      )}
    </div>
  )
}

// Utility to safely parse JSON strings from backend
function safeParseJson<T>(value: unknown): T | null {
  if (value === null || value === undefined) return null
  if (typeof value === 'object') return value as T
  if (typeof value === 'string') {
    try {
      return JSON.parse(value)
    } catch {
      return null
    }
  }
  return null
}
