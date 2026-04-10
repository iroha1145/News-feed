import React from 'react'

interface ErrorBoundaryProps {
  children: React.ReactNode
}

interface ErrorBoundaryState {
  hasError: boolean
}

export default class ErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorBoundaryState> {
  state: ErrorBoundaryState = { hasError: false }

  static getDerivedStateFromError(): ErrorBoundaryState {
    return { hasError: true }
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error('Unhandled React render error', error, errorInfo)
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen flex items-center justify-center bg-surface dark:bg-slate-950 p-6">
          <div className="max-w-md w-full rounded-2xl bg-surface-container-lowest dark:bg-slate-900 p-8 shadow-xl text-center space-y-4">
            <span className="material-symbols-outlined text-5xl text-error dark:text-red-400">error</span>
            <h1 className="text-2xl font-extrabold font-headline dark:text-white">应用遇到问题</h1>
            <p className="text-sm text-on-surface-variant dark:text-slate-400 leading-relaxed">
              页面发生了意外错误。请刷新重试；如果问题持续，请联系管理员并查看控制台日志。
            </p>
          </div>
        </div>
      )
    }

    return this.props.children
  }
}
