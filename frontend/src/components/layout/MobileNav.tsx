import { Link, useLocation } from 'react-router-dom'

const items = [
  { icon: 'show_chart', label: '市场', path: '/' },
  { icon: 'newspaper', label: '新闻', path: '/news' },
  { icon: 'monitoring', label: '情绪', path: '/sentiment' },
  { icon: 'auto_awesome', label: '分析', path: '/analysis' },
]

export default function MobileNav() {
  const location = useLocation()

  const isActive = (path: string) => {
    if (path === '/') return location.pathname === '/'
    return location.pathname.startsWith(path)
  }

  return (
    <nav aria-label="移动端主导航" className="lg:hidden fixed bottom-0 left-0 w-full bg-white/90 dark:bg-slate-950/90 backdrop-blur-xl border-t border-slate-200/50 dark:border-slate-800/50 min-h-16 flex items-center justify-around px-4 z-50" style={{ paddingBottom: 'env(safe-area-inset-bottom)' }}>
      {items.map((item) => {
        const active = isActive(item.path)
        return (
          <Link
            key={item.label}
            to={item.path}
            aria-current={active ? 'page' : undefined}
            className={`flex flex-col items-center gap-1 transition-colors ${
              active
                ? 'text-violet-700 dark:text-violet-400'
                : 'text-slate-400 dark:text-slate-500'
            }`}
          >
            <span className="material-symbols-outlined text-[22px]" aria-hidden="true">{item.icon}</span>
            <span className="text-[10px] font-bold">{item.label}</span>
          </Link>
        )
      })}
    </nav>
  )
}
