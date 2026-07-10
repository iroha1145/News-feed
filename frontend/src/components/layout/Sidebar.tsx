import { Link, useLocation } from 'react-router-dom'

const navItems = [
  { icon: 'show_chart', label: '市场总览', path: '/' },
  { icon: 'newspaper', label: '新闻流', path: '/news' },
  { icon: 'psychology', label: '市场情绪', path: '/sentiment' },
  { icon: 'auto_awesome', label: '深度分析', path: '/analysis' },
]

export default function Sidebar() {
  const location = useLocation()

  const isActive = (path: string) => {
    if (path === '/') return location.pathname === '/'
    return location.pathname.startsWith(path)
  }

  return (
    <aside className="hidden lg:flex flex-col gap-4 p-6 w-64 fixed left-0 top-16 h-[calc(100vh-64px)] overflow-y-auto bg-slate-100 dark:bg-slate-900 custom-scrollbar z-40">
      <div className="mb-4">
        <h2 className="text-lg font-black text-violet-700 dark:text-violet-400 font-headline">
          宏观洞察
        </h2>
        <p className="text-xs font-semibold text-slate-500 dark:text-slate-400 tracking-widest">
          智能分析引擎
        </p>
      </div>

      <nav className="flex flex-col gap-1" aria-label="主导航">
        {navItems.map((item) => {
          const active = isActive(item.path)
          return (
            <Link
              key={item.label}
              to={item.path}
              aria-current={active ? 'page' : undefined}
              className={`p-3 flex items-center gap-3 rounded-xl transition-all duration-200 font-headline text-sm font-semibold hover:translate-x-1 ${
                active
                  ? 'bg-white dark:bg-slate-800 text-violet-700 dark:text-violet-400 shadow-sm'
                  : 'text-slate-500 dark:text-slate-400 hover:bg-white/50 dark:hover:bg-slate-800/50'
              }`}
            >
              <span className="material-symbols-outlined" aria-hidden="true">{item.icon}</span>
              {item.label}
            </Link>
          )
        })}
      </nav>
    </aside>
  )
}
