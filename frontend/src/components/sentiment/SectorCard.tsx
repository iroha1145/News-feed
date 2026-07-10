interface SectorCardProps {
  name: string
  score: number
  count: number
}

export default function SectorCard({ name, score, count }: SectorCardProps) {
  const isPositive = score > 0
  const isNeutral = Math.abs(score) < 10

  const bgColor = isNeutral
    ? 'bg-surface-container dark:bg-slate-700'
    : isPositive
    ? 'bg-tertiary-container/30 dark:bg-emerald-900/20 border border-tertiary-container/50 dark:border-emerald-800/30'
    : 'bg-error-container/20 dark:bg-red-900/20 border border-error-container/30 dark:border-red-800/30'

  const scoreColor = isNeutral
    ? 'text-on-surface-variant dark:text-slate-400'
    : isPositive
    ? 'text-tertiary dark:text-emerald-400'
    : 'text-error dark:text-red-400'

  const icon = isNeutral ? 'trending_flat' : isPositive ? 'trending_up' : 'trending_down'

  return (
    <div className={`p-4 rounded-xl transition-shadow hover:shadow-md ${bgColor}`}>
      <div className="flex items-start justify-between mb-3">
        <div>
          <p className="text-xs font-bold text-on-surface-variant dark:text-slate-400 uppercase tracking-wider">{name}</p>
          <p className="text-[10px] text-on-surface-variant/60 dark:text-slate-500">{count} 篇文章</p>
        </div>
        <span className={`material-symbols-outlined ${scoreColor}`}>{icon}</span>
      </div>
      <div className="flex items-baseline gap-1">
        <span className={`text-2xl font-black ${scoreColor}`}>
          {isPositive ? '+' : ''}{score.toFixed(0)}
        </span>
        <span className="text-xs text-on-surface-variant dark:text-slate-500">/ 100</span>
      </div>
      <div className="mt-2 h-1.5 bg-surface-container dark:bg-slate-700 rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all ${isPositive ? 'bg-tertiary dark:bg-emerald-500' : 'bg-error dark:bg-red-500'}`}
          style={{ width: `${Math.min(Math.abs(score), 100)}%` }}
        />
      </div>
    </div>
  )
}
