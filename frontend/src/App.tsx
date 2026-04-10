import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Layout from './components/layout/Layout'
import ErrorBoundary from './components/common/ErrorBoundary'
import Markets from './components/markets/Markets'
import NewsFeed from './components/news/NewsFeed'
import SentimentDashboard from './components/sentiment/SentimentDashboard'
import DeepAnalysis from './components/analysis/DeepAnalysis'

export default function App() {
  useEffect(() => {
    const media = window.matchMedia('(prefers-color-scheme: dark)')
    const handler = () => {
      const saved = localStorage.getItem('theme') ?? 'auto'
      if (saved === 'auto') {
        document.documentElement.classList.toggle('dark', media.matches)
      }
    }
    media.addEventListener('change', handler)
    return () => media.removeEventListener('change', handler)
  }, [])

  return (
    <ErrorBoundary>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Layout><Markets /></Layout>} />
          <Route path="/news" element={<Layout><NewsFeed /></Layout>} />
          <Route path="/sentiment" element={<Layout><SentimentDashboard /></Layout>} />
          <Route path="/analysis/:id?" element={<Layout><DeepAnalysis /></Layout>} />
        </Routes>
      </BrowserRouter>
    </ErrorBoundary>
  )
}
