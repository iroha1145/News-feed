import { describe, expect, it } from 'vitest'
import {
  analysisScoreToIndex,
  clampPercent,
  getNewsSentimentIndex,
  sentimentLabel,
} from './sentiment'

describe('新闻情绪指数', () => {
  it('把 -100 到 100 的分析分数线性映射到 0 到 100', () => {
    expect(analysisScoreToIndex(-100)).toBe(0)
    expect(analysisScoreToIndex(-50)).toBe(25)
    expect(analysisScoreToIndex(0)).toBe(50)
    expect(analysisScoreToIndex(50)).toBe(75)
    expect(analysisScoreToIndex(100)).toBe(100)
  })

  it('对异常范围和非有限数值做安全夹紧', () => {
    expect(clampPercent(-12)).toBe(0)
    expect(clampPercent(112)).toBe(100)
    expect(clampPercent(Number.NaN)).toBe(50)
  })

  it('在中文标签边界使用一致口径', () => {
    expect(sentimentLabel(0)).toBe('极度谨慎')
    expect(sentimentLabel(19.99)).toBe('极度谨慎')
    expect(sentimentLabel(20)).toBe('谨慎')
    expect(sentimentLabel(39.99)).toBe('谨慎')
    expect(sentimentLabel(40)).toBe('中性')
    expect(sentimentLabel(59.99)).toBe('中性')
    expect(sentimentLabel(60)).toBe('乐观')
    expect(sentimentLabel(79.99)).toBe('乐观')
    expect(sentimentLabel(80)).toBe('极度乐观')
  })

  it('把服务端统计窗口和样本量带入展示说明', () => {
    const result = getNewsSentimentIndex({
      window_days: 7,
      total_analyzed: 12,
      avg_sentiment: 20,
      bullish_count: 6,
      bearish_count: 2,
      neutral_count: 4,
    })
    expect(result).toMatchObject({
      value: 60,
      label: '乐观',
      source: '已入库新闻分析',
      window: '最近 7 日',
      sampleSize: 12,
    })
  })
})
