import { useEffect, useRef, useState, type FormEvent } from 'react'
import { useAdminSession } from '../../context/AdminSessionContext'

export default function AdminLoginModal() {
  const {
    loginOpen,
    loginReason,
    busy,
    error,
    closeLogin,
    login,
    clearError,
  } = useAdminSession()
  const [token, setToken] = useState('')
  const inputRef = useRef<HTMLInputElement>(null)
  const dialogRef = useRef<HTMLElement>(null)
  const previousFocusRef = useRef<HTMLElement | null>(null)

  useEffect(() => {
    if (!loginOpen) {
      setToken('')
      return
    }
    previousFocusRef.current = document.activeElement instanceof HTMLElement ? document.activeElement : null
    const previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    requestAnimationFrame(() => inputRef.current?.focus())
    return () => {
      document.body.style.overflow = previousOverflow
      previousFocusRef.current?.focus()
    }
  }, [loginOpen])

  if (!loginOpen) return null

  const handleSubmit = async (event: FormEvent) => {
    event.preventDefault()
    if (!token.trim() || busy) return
    try {
      await login(token.trim())
    } catch {
      inputRef.current?.focus()
    }
  }

  return (
    <div
      className="fixed inset-0 z-[120] flex items-center justify-center bg-slate-950/45 p-4 backdrop-blur-sm"
      role="presentation"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) closeLogin()
      }}
    >
      <section
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby="admin-login-title"
        aria-describedby="admin-login-description"
        onKeyDown={(event) => {
          if (event.key === 'Escape' && !busy) {
            closeLogin()
            return
          }
          if (event.key !== 'Tab') return
          const focusable = dialogRef.current?.querySelectorAll<HTMLElement>('button:not([disabled]), input:not([disabled]), [tabindex]:not([tabindex="-1"])')
          if (!focusable?.length) return
          const first = focusable[0]
          const last = focusable[focusable.length - 1]
          if (event.shiftKey && document.activeElement === first) {
            event.preventDefault()
            last.focus()
          } else if (!event.shiftKey && document.activeElement === last) {
            event.preventDefault()
            first.focus()
          }
        }}
        className="w-full max-w-sm rounded-2xl bg-white p-6 shadow-2xl dark:bg-slate-900"
      >
        <div className="mb-5 flex items-start justify-between gap-4">
          <div>
            <h2 id="admin-login-title" className="font-headline text-xl font-extrabold text-slate-900 dark:text-white">
              管理员登录
            </h2>
            <p id="admin-login-description" className="mt-2 text-sm leading-relaxed text-slate-600 dark:text-slate-400">
              {loginReason || '登录后可抓取新闻、运行分析和分析经济日历。'}
            </p>
          </div>
          <button
            type="button"
            aria-label="关闭管理员登录"
            onClick={closeLogin}
            disabled={busy}
            className="rounded-lg p-2 text-slate-500 hover:bg-slate-100 hover:text-slate-900 disabled:opacity-50 dark:hover:bg-slate-800 dark:hover:text-white"
          >
            <span className="material-symbols-outlined" aria-hidden="true">close</span>
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label htmlFor="admin-token" className="mb-2 block text-sm font-bold text-slate-700 dark:text-slate-300">
              管理令牌
            </label>
            <input
              ref={inputRef}
              id="admin-token"
              name="admin_token"
              type="password"
              value={token}
              onChange={(event) => {
                setToken(event.target.value)
                if (error) clearError()
              }}
              autoComplete="current-password"
              spellCheck={false}
              placeholder="输入管理令牌…"
              className="w-full rounded-xl border border-slate-300 bg-white px-4 py-3 text-sm text-slate-900 outline-none focus-visible:ring-2 focus-visible:ring-violet-500 dark:border-slate-700 dark:bg-slate-950 dark:text-white"
            />
          </div>

          {error && (
            <p role="alert" aria-live="polite" className="rounded-lg bg-red-50 px-3 py-2 text-sm text-red-700 dark:bg-red-950/40 dark:text-red-300">
              {error}
            </p>
          )}

          <button
            type="submit"
            disabled={busy || !token.trim()}
            className="flex w-full items-center justify-center gap-2 rounded-xl bg-violet-700 px-4 py-3 text-sm font-bold text-white hover:bg-violet-800 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {busy && <span className="h-4 w-4 animate-spin rounded-full border-2 border-white/40 border-t-white" aria-hidden="true" />}
            {busy ? '登录中…' : '登录'}
          </button>
        </form>
      </section>
    </div>
  )
}
