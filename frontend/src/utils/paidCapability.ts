import type { PaidCapability } from '../types'

export function paidCapabilityEnabled(capability: PaidCapability | undefined): boolean {
  return capability === 'enabled'
}

export function paidCapabilityLabel(
  capability: PaidCapability | undefined,
  labels: { enabled: string; disabled: string },
): string {
  if (capability === 'budget_configuration_required' || capability === 'budget_blocked') {
    return '配置预算后启用'
  }
  if (capability === 'disabled') return labels.disabled
  return labels.enabled
}
