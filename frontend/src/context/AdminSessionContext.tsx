import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react'
import {
  getAdminSession,
  isUnauthorizedError,
  loginAdmin,
  logoutAdmin,
} from '../services/api'

interface AdminSessionContextValue {
  authenticated: boolean
  checking: boolean
  busy: boolean
  error: string | null
  loginOpen: boolean
  loginReason: string | null
  openLogin: (reason?: string) => void
  closeLogin: () => void
  login: (token: string) => Promise<void>
  logout: () => Promise<void>
  requireAdmin: (reason?: string) => boolean
  handleExpiredSession: (error: unknown, reason?: string) => boolean
  clearError: () => void
}

const AdminSessionContext = createContext<AdminSessionContextValue | null>(null)

function messageFromError(error: unknown, fallback: string) {
  return error instanceof Error && error.message ? error.message : fallback
}

export function AdminSessionProvider({ children }: { children: ReactNode }) {
  const [authenticated, setAuthenticated] = useState(false)
  const [checking, setChecking] = useState(true)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [loginOpen, setLoginOpen] = useState(false)
  const [loginReason, setLoginReason] = useState<string | null>(null)
  const actionControllerRef = useRef<AbortController | null>(null)

  useEffect(() => {
    const controller = new AbortController()
    getAdminSession(controller.signal)
      .then((session) => setAuthenticated(Boolean(session.authenticated)))
      .catch((sessionError: unknown) => {
        if (sessionError instanceof DOMException && sessionError.name === 'AbortError') return
        setAuthenticated(false)
      })
      .finally(() => setChecking(false))
    return () => controller.abort()
  }, [])

  useEffect(() => () => actionControllerRef.current?.abort(), [])

  const openLogin = useCallback((reason?: string) => {
    setLoginReason(reason ?? null)
    setError(null)
    setLoginOpen(true)
  }, [])

  const closeLogin = useCallback(() => {
    if (busy) return
    setLoginOpen(false)
    setLoginReason(null)
    setError(null)
  }, [busy])

  const login = useCallback(async (token: string) => {
    actionControllerRef.current?.abort()
    const controller = new AbortController()
    actionControllerRef.current = controller
    setBusy(true)
    setError(null)
    try {
      const session = await loginAdmin(token, controller.signal)
      if (!session.authenticated) throw new Error('登录未成功，请检查管理令牌。')
      setAuthenticated(true)
      setLoginOpen(false)
      setLoginReason(null)
    } catch (loginError) {
      setAuthenticated(false)
      setError(messageFromError(loginError, '登录失败，请检查管理令牌后重试。'))
      throw loginError
    } finally {
      setBusy(false)
    }
  }, [])

  const logout = useCallback(async () => {
    actionControllerRef.current?.abort()
    const controller = new AbortController()
    actionControllerRef.current = controller
    setBusy(true)
    setError(null)
    try {
      await logoutAdmin(controller.signal)
      setAuthenticated(false)
    } catch (logoutError) {
      setError(messageFromError(logoutError, '退出失败，请稍后重试。'))
      throw logoutError
    } finally {
      setBusy(false)
    }
  }, [])

  const requireAdmin = useCallback((reason?: string) => {
    if (authenticated) return true
    openLogin(reason ?? '此操作需要管理员登录。')
    return false
  }, [authenticated, openLogin])

  const handleExpiredSession = useCallback((sessionError: unknown, reason?: string) => {
    if (!isUnauthorizedError(sessionError)) return false
    setAuthenticated(false)
    openLogin(reason ?? '管理会话已过期，请重新登录。')
    return true
  }, [openLogin])

  const value = useMemo<AdminSessionContextValue>(() => ({
    authenticated,
    checking,
    busy,
    error,
    loginOpen,
    loginReason,
    openLogin,
    closeLogin,
    login,
    logout,
    requireAdmin,
    handleExpiredSession,
    clearError: () => setError(null),
  }), [
    authenticated,
    busy,
    checking,
    closeLogin,
    error,
    handleExpiredSession,
    login,
    loginOpen,
    loginReason,
    logout,
    openLogin,
    requireAdmin,
  ])

  return <AdminSessionContext.Provider value={value}>{children}</AdminSessionContext.Provider>
}

export function useAdminSession() {
  const value = useContext(AdminSessionContext)
  if (!value) throw new Error('useAdminSession 必须在 AdminSessionProvider 内使用。')
  return value
}
