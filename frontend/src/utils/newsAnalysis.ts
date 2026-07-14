import type { AnalysisStatus } from '../types'

export const INSUFFICIENT_CONTEXT_NOTICE =
  '现有信息不足以支持可靠结论；页面保留可审计的分析线索，请结合原文核对。'

interface NewsAnalysisState {
  analysis_status?: AnalysisStatus
  analysis?: unknown
}

export interface NewsAnalysisPresentation {
  hasAnalysisDetails: boolean
  isInsufficientContext: boolean
  statusLabel: '模型已分析' | '上下文不足' | null
}

export function getNewsAnalysisPresentation(
  item: NewsAnalysisState,
): NewsAnalysisPresentation {
  const isCompleted = item.analysis_status === 'completed'
  const isInsufficientContext = item.analysis_status === 'insufficient_context'
  return {
    hasAnalysisDetails: Boolean(item.analysis) && (isCompleted || isInsufficientContext),
    isInsufficientContext,
    statusLabel: isInsufficientContext
      ? '上下文不足'
      : isCompleted
        ? '模型已分析'
        : null,
  }
}
