import type { ReactNode } from 'react'
import Header from './Header'
import Sidebar from './Sidebar'
import MobileNav from './MobileNav'

interface LayoutProps {
  children: ReactNode
}

export default function Layout({ children }: LayoutProps) {
  return (
    <div className="min-h-screen bg-surface dark:bg-slate-950 text-on-surface dark:text-slate-100">
      <a href="#main-content" className="fixed left-4 top-2 z-[200] -translate-y-20 rounded-lg bg-primary px-4 py-2 text-sm font-bold text-white focus:translate-y-0">
        跳到主要内容
      </a>
      <Header />
      <div className="flex">
        <Sidebar />
        <div className="flex-1 lg:ml-64 pb-16 lg:pb-0 min-w-0">
          {children}
        </div>
      </div>
      <MobileNav />
    </div>
  )
}
