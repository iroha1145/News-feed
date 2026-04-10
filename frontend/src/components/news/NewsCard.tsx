import { Link } from 'react-router-dom'
import type { NewsItem } from '../../types'
import NewsImage from './NewsImage'
import SentimentChip from '../common/SentimentChip'
import { timeAgo } from '../../utils/time'
import { getRealImageUrl } from '../../utils/image'

interface NewsCardProps {
  item: NewsItem
  onTickerClick?: (ticker: string, name?: string) => void
}

export default function NewsCard({ item, onTickerClick }: NewsCardProps) {
  const analysis = item.analysis
  const classification = analysis?.classification
  const rawImageUrl = item.image_url || item.urlToImage
  const imageUrl = getRealImageUrl(rawImageUrl)
  const hasAnalysis = item.analysis_status === 'completed' && analysis
  const isPinned = item.is_pinned

  const titleContent = (
    <h2 className="text-lg font-bold font-headline leading-snug text-on-surface dark:text-slate-100 group-hover:text-primary dark:group-hover:text-violet-400 transition-colors duration-200">
      {item.title}
    </h2>
  )

  return (
    <article className={`group rounded-xl p-5 transition-all duration-200 ${
      isPinned
        ? 'bg-violet-50/50 dark:bg-violet-950/20 border-l-4 border-primary dark:border-violet-500 shadow-xl shadow-primary/10 dark:shadow-violet-900/20 hover:shadow-2xl hover:shadow-primary/10'
        : 'bg-white/70 dark:bg-slate-800/70 backdrop-blur-sm border border-slate-200/60 dark:border-slate-700/60 shadow-sm hover:shadow-lg hover:shadow-slate-200/50 dark:hover:shadow-slate-900/40'
    }`}>
      <div className="flex gap-4">
        {/* Text content - left */}
        <div className="flex-1 flex flex-col gap-2.5 min-w-0">
          {/* Source + Time + Pinned badge */}
          <div className="flex items-center gap-2 text-[11px] font-bold uppercase tracking-widest flex-wrap">
            <span className="text-primary dark:text-violet-400">{item.source}</span>
            <span className="w-1 h-1 bg-slate-300 dark:bg-slate-600 rounded-full" />
            <span className="text-slate-400 dark:text-slate-500">{timeAgo(item.published_at)}</span>
            {hasAnalysis && (
              <>
                <span className="w-1 h-1 bg-slate-300 dark:bg-slate-600 rounded-full" />
                <span className="text-emerald-500 dark:text-emerald-400">AI已分析</span>
              </>
            )}
            {isPinned && (
              <div className="flex items-center gap-1 px-2 py-0.5 bg-primary/10 dark:bg-violet-500/20 text-primary dark:text-violet-400 rounded-md text-[10px] font-black tracking-wider ml-auto">
                <span className="material-symbols-outlined text-[13px]" style={{ fontVariationSettings: "'FILL' 1" }}>push_pin</span>
                置顶
              </div>
            )}
          </div>

          {/* Title - linked if analysis exists */}
          {hasAnalysis ? (
            <Link to={`/analysis/${item.id}`} className="block">
              {titleContent}
            </Link>
          ) : (
            titleContent
          )}

          {/* Chinese title translation */}
          {analysis?.title_zh && analysis.title_zh !== item.title && (
            <p className="text-sm text-slate-500 dark:text-slate-400 leading-snug">
              {analysis.title_zh}
            </p>
          )}

          {/* Summary */}
          {item.summary && (
            <p className="text-slate-500 dark:text-slate-400 leading-relaxed text-sm line-clamp-2">
              {item.summary}
            </p>
          )}

          {/* Chips */}
          <div className="flex flex-wrap gap-2 pt-0.5">
            {classification && (
              <SentimentChip
                classification={classification}
                score={analysis ? Math.round(Math.abs(analysis.overall_sentiment)) : undefined}
              />
            )}
            {analysis?.affected_stocks?.slice(0, 2).map((stock) => (
              <div
                key={stock.ticker}
                className={onTickerClick ? 'cursor-pointer hover:opacity-80 active:scale-95 transition-all' : ''}
                onClick={(e) => {
                  if (onTickerClick) {
                    e.preventDefault()
                    e.stopPropagation()
                    onTickerClick(stock.ticker, stock.company)
                  }
                }}
              >
                <SentimentChip
                  classification={stock.impact_score > 0 ? 'bullish' : 'bearish'}
                  ticker={stock.ticker}
                  score={Math.round(Math.abs(stock.impact_score))}
                  size="sm"
                />
              </div>
            ))}
            {analysis?.affected_sectors?.slice(0, 2).map((sector) => (
              <div
                key={sector}
                className="flex items-center gap-1 bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 px-2.5 py-1 rounded-md text-[11px] font-bold uppercase tracking-wide"
              >
                <span className="material-symbols-outlined text-[12px]">token</span>
                {sector}
              </div>
            ))}
          </div>

          {/* Analysis excerpt */}
          {analysis?.headline_summary && (
            <p className="text-xs text-slate-400 dark:text-slate-500 italic">
              {analysis.headline_summary}
            </p>
          )}

          {/* Pending/processing status */}
          {(item.analysis_status === 'pending' || item.analysis_status === 'processing') && (
            <div className="pt-1">
              {item.analysis_status === 'pending' && (
                <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">
                  等待分析
                </span>
              )}
              {item.analysis_status === 'processing' && (
                <span className="flex items-center gap-1 text-[10px] font-bold text-primary uppercase tracking-wider">
                  <span className="w-1.5 h-1.5 bg-primary rounded-full animate-pulse" />
                  分析中...
                </span>
              )}
            </div>
          )}
        </div>

        {/* Image - right, only when imageUrl exists */}
        {imageUrl && (
          <div className="flex-shrink-0 w-36 h-28 md:w-40 md:h-28 rounded-lg overflow-hidden self-start">
            <NewsImage
              src={imageUrl}
              alt={item.title}
              className="w-full h-full"
            />
          </div>
        )}
      </div>
    </article>
  )
}
