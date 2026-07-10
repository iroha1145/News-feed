/**
 * Shared UTC time parsing utilities.
 *
 * Backend may emit ISO strings without a timezone suffix (e.g. "2026-03-22T09:59:48").
 * These are always UTC but the browser will treat them as local time unless we
 * explicitly append "Z".  The helpers below normalise any such string before
 * handing it to `new Date()`.
 */

/** Return true when the string already carries an explicit timezone marker. */
function hasTimezone(s: string): boolean {
  // Ends with 'Z'
  if (s.endsWith('Z') || s.endsWith('z')) return true
  // Contains an explicit UTC offset like +08:00, -05:00, +0800, -0530
  // We look after the 'T' separator to avoid matching the '-' in the date part.
  const tIdx = s.indexOf('T')
  if (tIdx === -1) return false
  const timePart = s.slice(tIdx + 1)
  return /[+-]\d{2}(:\d{2})?$/.test(timePart)
}

/** Parse a date string as UTC, appending 'Z' when no timezone is present. */
export function parseUtcDate(dateStr: string | null | undefined): Date {
  if (!dateStr) return new Date(Number.NaN)
  const normalized = hasTimezone(dateStr) ? dateStr : dateStr + 'Z'
  return new Date(normalized)
}

/** Human-friendly relative time string (Chinese). */
export function timeAgo(dateStr: string | null | undefined): string {
  const date = parseUtcDate(dateStr)
  if (Number.isNaN(date.getTime())) return '时间未知'
  const now = new Date()
  const diff = Math.floor((now.getTime() - date.getTime()) / 1000)
  if (diff < 0) return '刚刚'
  if (diff < 60) return '刚刚'
  if (diff < 3600) return `${Math.floor(diff / 60)} 分钟前`
  if (diff < 86400) return `${Math.floor(diff / 3600)} 小时前`
  if (diff < 172800) return '昨天'
  return `${Math.floor(diff / 86400)} 天前`
}

/** Format a date string to concise Chinese local time. */
export function toLocalTime(dateStr: string | null | undefined): string {
  const date = parseUtcDate(dateStr)
  if (Number.isNaN(date.getTime())) return '时间未知'
  return date.toLocaleString('zh-CN', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}
