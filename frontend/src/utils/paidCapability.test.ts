import { describe, expect, it } from 'vitest'
import { paidCapabilityEnabled, paidCapabilityLabel } from './paidCapability'

describe('paid capability controls', () => {
  it.each(['disabled', 'budget_configuration_required', 'budget_blocked', undefined] as const)(
    'keeps %s disabled',
    (capability) => expect(paidCapabilityEnabled(capability)).toBe(false),
  )

  it('enables only an explicitly enabled capability', () => {
    expect(paidCapabilityEnabled('enabled')).toBe(true)
  })

  it('explains disabled and missing-budget states', () => {
    const labels = { enabled: '运行分析', disabled: '分析已关闭' }
    expect(paidCapabilityLabel('disabled', labels)).toBe('分析已关闭')
    expect(paidCapabilityLabel('budget_configuration_required', labels)).toBe('配置预算后启用')
    expect(paidCapabilityLabel('enabled', labels)).toBe('运行分析')
  })
})
