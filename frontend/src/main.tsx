import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './index.css'

function applyTheme(mode: 'auto' | 'light' | 'dark') {
  const isDark =
    mode === 'dark' ||
    (mode === 'auto' && window.matchMedia('(prefers-color-scheme: dark)').matches)
  document.documentElement.classList.toggle('dark', isDark)
}

const saved = (localStorage.getItem('theme') as 'auto' | 'light' | 'dark') ?? 'auto'
applyTheme(saved)

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
