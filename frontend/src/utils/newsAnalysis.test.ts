import { describe, expect, it } from 'vitest'
import type { Analysis } from '../types'
import {
  getNewsAnalysisPresentation,
  INSUFFICIENT_CONTEXT_NOTICE,
} from './newsAnalysis'

const storedAnalysis = {} as Analysis

describe('新闻分析状态展示', () => {
  it('让上下文不足的已有分析进入完整详情，但不伪装成正常完成', () => {
    const presentation = getNewsAnalysisPresentation({
      analysis_status: 'insufficient_context',
      analysis: storedAnalysis,
    })

    expect(presentation).toEqual({
      hasAnalysisDetails: true,
      isInsufficientContext: true,
      statusLabel: '上下文不足',
    })
    expect(presentation.statusLabel).not.toBe('模型已分析')
  })

  it('只有实际返回分析内容时才开放详情入口', () => {
    expect(getNewsAnalysisPresentation({
      analysis_status: 'insufficient_context',
      analysis: null,
    })).toMatchObject({
      hasAnalysisDetails: false,
      isInsufficientContext: true,
      statusLabel: '上下文不足',
    })
  })

  it('保持普通完成和处理中状态的原有含义', () => {
    expect(getNewsAnalysisPresentation({
      analysis_status: 'completed',
      analysis: storedAnalysis,
    })).toMatchObject({
      hasAnalysisDetails: true,
      isInsufficientContext: false,
      statusLabel: '模型已分析',
    })
    expect(getNewsAnalysisPresentation({
      analysis_status: 'processing',
      analysis: storedAnalysis,
    }).hasAnalysisDetails).toBe(false)
  })

  it('使用同时适合空影响与低置信度结果的中性说明', () => {
    expect(INSUFFICIENT_CONTEXT_NOTICE).toContain('信息不足')
    expect(INSUFFICIENT_CONTEXT_NOTICE).toContain('可审计的分析线索')
    expect(INSUFFICIENT_CONTEXT_NOTICE).not.toContain('股票线索')
  })
})
