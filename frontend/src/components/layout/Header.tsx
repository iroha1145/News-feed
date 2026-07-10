import { Link, useLocation } from 'react-router-dom'
import { useTheme } from '../../hooks/useTheme'
import { useAdminSession } from '../../context/AdminSessionContext'

export default function Header() {
  const { toggle } = useTheme()
  const { authenticated, checking, busy, error, openLogin, logout } = useAdminSession()
  const location = useLocation()

  const isActive = (path: string) => {
    if (path === '/') return location.pathname === '/'
    return location.pathname.startsWith(path)
  }

  return (
    <header className="bg-slate-50/80 dark:bg-slate-950/80 backdrop-blur-2xl sticky top-0 z-50 shadow-xl shadow-slate-900/5">
      <div className="flex items-center justify-between w-full px-6 py-4 max-w-[1920px] mx-auto">
        <div className="flex items-center gap-8">
          <Link to="/" className="text-2xl font-extrabold tracking-tighter text-slate-900 dark:text-slate-50 font-headline">
            MacroLens
          </Link>
          <nav className="hidden md:flex gap-6 items-center" aria-label="页首导航">
            <Link
              to="/"
              aria-current={isActive('/') ? 'page' : undefined}
              className={`font-headline font-semibold tracking-tight transition-colors ${
                isActive('/')
                  ? 'text-violet-700 dark:text-violet-400 font-bold border-b-2 border-violet-700 dark:border-violet-400 pb-1'
                  : 'text-slate-500 dark:text-slate-400 hover:text-slate-800 dark:hover:text-slate-200'
              }`}
            >
              市场
            </Link>
            <Link
              to="/news"
              aria-current={location.pathname === '/news' ? 'page' : undefined}
              className={`font-headline font-semibold tracking-tight transition-colors ${
                location.pathname === '/news'
                  ? 'text-violet-700 dark:text-violet-400 font-bold border-b-2 border-violet-700 dark:border-violet-400 pb-1'
                  : 'text-slate-500 dark:text-slate-400 hover:text-slate-800 dark:hover:text-slate-200'
              }`}
            >
              新闻
            </Link>
          </nav>
        </div>
        <div className="flex items-center gap-2">
          {error && <span className="hidden max-w-48 truncate text-[10px] text-error md:inline" role="alert" title={error}>{error}</span>}
          <Link
            to="/sentiment"
            aria-label="打开市场情绪"
            aria-current={isActive('/sentiment') ? 'page' : undefined}
            className="p-2 text-slate-500 dark:text-slate-400 hover:bg-slate-100/50 dark:hover:bg-slate-800/50 rounded-lg transition-colors duration-300"
            title="市场情绪"
          >
            <span className="material-symbols-outlined" aria-hidden="true">monitoring</span>
          </Link>
          <button
            type="button"
            disabled={checking || busy}
            onClick={() => {
              if (authenticated) void logout().catch(() => undefined)
              else openLogin('登录后可执行新闻抓取和分析任务。')
            }}
            className="flex items-center gap-1.5 rounded-lg px-2 py-2 text-xs font-bold text-slate-600 transition-colors hover:bg-slate-100/70 disabled:opacity-50 dark:text-slate-300 dark:hover:bg-slate-800/70"
            aria-label={authenticated ? '退出管理员登录' : '管理员登录'}
          >
            <span className="material-symbols-outlined text-[19px]" aria-hidden="true">
              {authenticated ? 'verified_user' : 'lock'}
            </span>
            <span className="hidden sm:inline">
              {checking ? '确认中…' : authenticated ? '管理员' : '登录'}
            </span>
          </button>
          <button
            type="button"
            onClick={toggle}
            aria-label="切换明暗主题"
            className="p-2 text-slate-500 dark:text-slate-400 hover:bg-slate-100/50 dark:hover:bg-slate-800/50 rounded-lg transition-colors duration-300"
            title="切换主题"
          >
            <span className="material-symbols-outlined dark:hidden" aria-hidden="true">dark_mode</span>
            <span className="material-symbols-outlined hidden dark:inline" aria-hidden="true">light_mode</span>
          </button>
        </div>
      </div>
      <div className="bg-slate-200/40 dark:bg-slate-800/40 h-px w-full" />
    </header>
  )
}
