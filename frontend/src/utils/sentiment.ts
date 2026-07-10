import type { AnalysisStats } from '../types'

export interface SentimentIndexMeta {
  value: number
  label: string
  source: string
  window: string
  sampleSize: number
}

export function clampPercent(value: number): number {
  if (!Number.isFinite(value)) return 50
  return Math.min(100, Math.max(0, value))
}

export function sentimentLabel(value: number): string {
  const normalized = clampPercent(value)
  if (normalized >= 80) return '极度乐观'
  if (normalized >= 60) return '乐观'
  if (normalized >= 40) return '中性'
  if (normalized >= 20) return '谨慎'
  return '极度谨慎'
}

/** Map the analysis score (-100..100) onto the display index (0..100). */
export function analysisScoreToIndex(score: number | null | undefined): number {
  return clampPercent(50 + (score ?? 0) / 2)
}

/**
 * The same definition is used throughout the product. It intentionally excludes
 * model-generated market scenarios, which are displayed separately as non-live context.
 */
export function getNewsSentimentIndex(stats?: AnalysisStats | null): SentimentIndexMeta {
  const value = analysisScoreToIndex(stats?.avg_sentiment)
  const sampleSize = stats?.total_analyzed ?? 0
  return {
    value,
    label: sampleSize > 0 ? sentimentLabel(value) : '暂无样本',
    source: '已入库新闻分析',
    window: sampleSize > 0 ? `最近 ${stats?.window_days ?? 7} 日` : '等待新闻分析',
    sampleSize,
  }
}
