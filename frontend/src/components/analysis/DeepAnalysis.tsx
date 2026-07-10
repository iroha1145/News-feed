import { useParams, Link } from 'react-router-dom'
import { useApi } from '../../hooks/useApi'
import { getAnalysisByNewsId, getAnalyses, getNews, triggerAnalysis } from '../../services/api'
import type { Analysis, NewsItem } from '../../types'
import LoadingSpinner from '../common/LoadingSpinner'
import SentimentChip from '../common/SentimentChip'
import NewsImage from '../news/NewsImage'
import { useEffect, useRef, useState } from 'react'
import AssetDetailModal from '../markets/AssetDetailModal'
import { parseUtcDate } from '../../utils/time'
import { getRealImageUrl } from '../../utils/image'
import { useAdminSession } from '../../context/AdminSessionContext'
import { safeExternalUrl } from '../../utils/url'

export default function DeepAnalysis() {
  const { id } = useParams<{ id: string }>()
  const [triggerMsg, setTriggerMsg] = useState<string | null>(null)
  const [triggerError, setTriggerError] = useState<string | null>(null)
  const [triggering, setTriggering] = useState(false)
  const [visibleAnalyses, setVisibleAnalyses] = useState(10)
  const [selectedTicker, setSelectedTicker] = useState<{ symbol: string; name?: string } | null>(null)
  const actionControllerRef = useRef<AbortController | null>(null)
  const { checking: sessionChecking, requireAdmin, handleExpiredSession } = useAdminSession()

  const parsedId = id ? Number(id) : null
  const selectedNewsId = parsedId != null && Number.isInteger(parsedId) && parsedId > 0 ? parsedId : null

  useEffect(() => () => actionControllerRef.current?.abort(), [])

  // ── Detail mode: fetch by news_id directly ──
  const directApi = useApi<{ analysis: Analysis; news: NewsItem | null }>(
    (signal) => selectedNewsId ? getAnalysisByNewsId(selectedNewsId, signal) : Promise.reject('no id'),
    [selectedNewsId]
  )

  // ── List mode (no id): load latest analyses ──
  const analysesApi = useApi<{ items: Analysis[]; total: number }>(
    (signal) => selectedNewsId ? Promise.resolve({ items: [], total: 0 }) : getAnalyses({ page: 1, page_size: 50 }, signal),
    [selectedNewsId]
  )
  const newsApi = useApi<{ items: NewsItem[]; total: number }>(
    (signal) => selectedNewsId ? Promise.resolve({ items: [], total: 0 }) : getNews({ page: 1, page_size: 100 }, signal),
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
    if (!requireAdmin('提交新闻分析任务需要管理员登录。')) return
    actionControllerRef.current?.abort()
    const controller = new AbortController()
    actionControllerRef.current = controller
    setTriggering(true)
    setTriggerMsg(null)
    setTriggerError(null)
    try {
      await triggerAnalysis(controller.signal)
      setTriggerMsg('分析任务已提交，请稍后刷新页面查看结果。')
      if (selectedNewsId) directApi.refetch()
      else analysesApi.refetch()
    } catch (error) {
      if (error instanceof DOMException && error.name === 'AbortError') return
      if (!handleExpiredSession(error, '管理会话已过期，请重新登录后提交分析任务。')) {
        setTriggerError(error instanceof Error ? error.message : '分析任务提交失败，请稍后重试。')
      }
    } finally {
      if (actionControllerRef.current === controller) setTriggering(false)
    }
  }

  if (isLoading) {
    return <LoadingSpinner className="py-20" label="分析加载中" />
  }

  const activeError = selectedNewsId ? directApi.error : analysesApi.error

  // No analysis found (directApi returned 404, or list is empty)
  if (!selectedAnalysis) {
    return (
      <div className="flex-1 p-6 md:p-8">
        <div className="max-w-2xl mx-auto text-center py-20">
          <span className="material-symbols-outlined text-6xl text-primary/30 dark:text-violet-400/30 mb-6 block">
            psychology
          </span>
          <h1 className="text-2xl font-extrabold font-headline mb-4 dark:text-white">
            {activeError ? '分析暂时无法加载' : selectedNewsId ? '未找到分析' : '暂无分析'}
          </h1>
          <p className="text-on-surface-variant dark:text-slate-400 mb-8 leading-relaxed">
            {activeError
              ? activeError
              : selectedNewsId
              ? '这篇新闻尚未生成分析，管理员可提交最新未分析新闻批次。'
              : '运行分析引擎后，这里会显示新闻深度分析。'}
          </p>
          <div className="flex justify-center gap-4">
            <button
              onClick={handleTrigger}
              disabled={sessionChecking || triggering}
              className="bg-gradient-to-r from-primary to-primary-container text-white px-6 py-3 rounded-xl font-bold text-sm hover:shadow-lg hover:shadow-primary/20 active:scale-95 transition-all"
            >
              <span className="material-symbols-outlined text-sm align-middle mr-2">auto_awesome</span>
              {triggering ? '提交中…' : '提交分析任务'}
            </button>
            <Link
              to="/news"
              className="px-6 py-3 rounded-xl font-bold text-sm border border-surface-container dark:border-slate-700 text-on-surface-variant dark:text-slate-400 hover:bg-surface-container dark:hover:bg-slate-800 transition-all"
            >
              返回新闻
            </Link>
          </div>
          {triggerMsg && <p className="mt-4 text-sm text-primary dark:text-violet-400" role="status" aria-live="polite">{triggerMsg}</p>}
          {triggerError && <p className="mt-4 text-sm text-error dark:text-red-400" role="alert">{triggerError}</p>}
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
  const sourceUrl = safeExternalUrl(matchedNews?.url || selectedAnalysis.news_url)
  const sourceName = matchedNews?.source || selectedAnalysis.news_source || '来源未标注'
  const evidenceParts = [
    Boolean(selectedAnalysis.headline_summary),
    keyFactors.length > 0,
    Boolean(selectedAnalysis.logic_chain),
    affectedStocks.length > 0 || affectedSectors.length > 0 || affectedCommodities.length > 0,
  ]
  const evidenceCount = evidenceParts.filter(Boolean).length

  return (
    <div className="xl:grid xl:grid-cols-[1fr_20rem] gap-0">
      {/* Main Content */}
      <main id="main-content" className="min-w-0 p-4 md:p-6 lg:p-8 space-y-8">
        {/* Breadcrumb */}
        <div className="flex items-center gap-2 text-xs text-on-surface-variant dark:text-slate-500">
          <Link to="/news" className="hover:text-primary dark:hover:text-violet-400 transition-colors">新闻</Link>
          <span className="material-symbols-outlined text-[14px]">chevron_right</span>
          <span className="text-on-surface dark:text-slate-300">深度分析</span>
        </div>

        {/* Hero */}
        <section className="space-y-6">
          <div className="flex flex-wrap items-center gap-3">
            <SentimentChip classification={classification} score={Math.abs(selectedAnalysis.overall_sentiment)} />
            <span className="text-xs font-bold text-on-surface-variant dark:text-slate-400 uppercase tracking-wider">
              分析时间：{selectedAnalysis.analyzed_at ? parseUtcDate(selectedAnalysis.analyzed_at).toLocaleString('zh-CN') : '时间未提供'}
            </span>
            {selectedAnalysis.llm_provider && (
              <span className="text-[10px] font-bold text-on-surface-variant dark:text-slate-500 bg-surface-container dark:bg-slate-800 px-2 py-1 rounded-full uppercase">
                {selectedAnalysis.llm_provider} / {selectedAnalysis.llm_model}
              </span>
            )}
            <span className="rounded-full bg-surface-container px-2 py-1 text-[10px] font-bold text-on-surface-variant dark:bg-slate-800 dark:text-slate-400">
              分析依据完整度 {evidenceCount}/4
            </span>
          </div>

          <h1 className="text-xl md:text-2xl lg:text-3xl font-extrabold font-headline tracking-tight leading-tight dark:text-white break-words">
            {selectedAnalysis.title_zh || matchedNews?.title || '市场分析'}
          </h1>
          {matchedNews?.title && selectedAnalysis.title_zh && selectedAnalysis.title_zh !== matchedNews.title && (
            <p className="text-sm text-on-surface-variant dark:text-slate-400">{matchedNews.title}</p>
          )}
          <div className="flex flex-wrap items-center gap-3 text-xs font-bold text-on-surface-variant dark:text-slate-400">
            <span>新闻来源：{sourceName}</span>
            {sourceUrl ? (
              <a href={sourceUrl} target="_blank" rel="noopener noreferrer" className="text-primary hover:underline dark:text-violet-400">阅读原文 ↗</a>
            ) : (
              <span>原文链接未提供</span>
            )}
          </div>

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
            <h3 className="font-bold font-headline dark:text-white">模型置信度</h3>
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
                  ? '本次模型分析偏多；请结合原文、行情时间和其他资料核验。'
                  : isBearish
                  ? '本次模型分析偏空；请结合原文、行情时间和其他资料核验。'
                  : '本次模型分析为中性；现有新闻信息不足以支持明确方向。'}
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
              <h3 className="font-bold font-headline dark:text-white">模型深度解读</h3>
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
                const negative = stock.impact_score < 0
                return (
                  <button
                    key={stock.ticker}
                    type="button"
                    className="w-full text-left bg-surface-container-lowest dark:bg-slate-900 rounded-xl p-4 space-y-2 cursor-pointer hover:shadow-lg hover:-translate-y-0.5 transition-all active:scale-[0.98]"
                    onClick={() => setSelectedTicker({ symbol: stock.ticker, name: stock.ticker })}
                  >
                    <div className="flex items-center justify-between">
                      <span className="font-mono font-bold dark:text-white">{stock.ticker}</span>
                      <span className={`font-bold text-sm ${positive ? 'text-tertiary dark:text-emerald-400' : negative ? 'text-error dark:text-red-400' : 'text-on-surface-variant dark:text-slate-400'}`}>
                        {positive ? '▲' : negative ? '▼' : '•'} 影响分数：{Math.abs(stock.impact_score)}
                      </span>
                    </div>
                    <div className="h-1.5 bg-surface-container dark:bg-slate-700 rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full transition-all ${positive ? 'bg-tertiary dark:bg-emerald-500' : negative ? 'bg-error dark:bg-red-500' : 'bg-slate-400'}`}
                        style={{ width: `${Math.min(Math.abs(stock.impact_score), 100)}%` }}
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
              {analyses.slice(1, visibleAnalyses).map((a) => {
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
                        {a.headline_summary || aNews?.title || `分析 #${a.id}`}
                      </p>
                      <p className="text-xs text-on-surface-variant dark:text-slate-400 mt-1 truncate">
                        {a.headline_summary?.slice(0, 100)}…
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
            {visibleAnalyses < analyses.length && (
              <button type="button" onClick={() => setVisibleAnalyses((count) => count + 10)} className="mt-4 w-full rounded-xl border border-surface-container py-3 text-sm font-bold text-on-surface-variant hover:bg-surface-container dark:border-slate-700 dark:text-slate-400 dark:hover:bg-slate-800">
                加载更多分析
              </button>
            )}
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
                  return (
                    <div
                      key={i}
                      className="bg-surface-container-lowest dark:bg-slate-800 p-4 rounded-xl"
                    >
                      <span className="text-sm font-semibold dark:text-white">{sector}</span>
                      <p className="mt-1 text-[10px] text-on-surface-variant dark:text-slate-500">分析仅标记受影响板块，未生成独立板块分数。</p>
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
                  const isNegative = c.impact_score < 0
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
                      <span className={`text-sm font-bold ${isPositive ? 'text-tertiary dark:text-emerald-400' : isNegative ? 'text-error dark:text-red-400' : 'text-on-surface-variant dark:text-slate-400'}`}>
                        {isPositive ? '▲' : isNegative ? '▼' : '•'} {Math.abs(c.impact_score)}
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
              disabled={sessionChecking || triggering}
              className="w-full py-3 bg-gradient-to-r from-primary to-primary-container text-white rounded-xl text-xs font-bold hover:shadow-lg active:scale-95 transition-all"
            >
              <span className="material-symbols-outlined text-sm align-middle mr-1">auto_awesome</span>
              {triggering ? '提交中…' : '提交新分析'}
            </button>
            {triggerMsg && <p className="text-xs text-primary dark:text-violet-400 text-center" role="status" aria-live="polite">{triggerMsg}</p>}
            {triggerError && <p className="text-xs text-error dark:text-red-400 text-center" role="alert">{triggerError}</p>}
            <Link
              to="/news"
              className="block w-full py-3 border border-surface-container dark:border-slate-700 text-on-surface-variant dark:text-slate-400 rounded-xl text-xs font-bold text-center hover:bg-surface-container dark:hover:bg-slate-800 transition-all"
            >
              返回新闻流
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
