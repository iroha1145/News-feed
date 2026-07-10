interface FearGreedGaugeProps {
  value: number
  label?: string
}

export default function FearGreedGauge({ value, label }: FearGreedGaugeProps) {
  const clampedValue = Math.min(100, Math.max(0, value))
  const radius = 70
  const circumference = 2 * Math.PI * radius
  const offset = circumference - (clampedValue / 100) * circumference

  const sentimentLabel = label || (
    clampedValue >= 80 ? '极度乐观' :
    clampedValue >= 60 ? '乐观' :
    clampedValue >= 40 ? '中性' :
    clampedValue >= 20 ? '谨慎' : '极度谨慎'
  )

  const color = clampedValue >= 60 ? '#5cfd80' : clampedValue >= 40 ? '#ac8eff' : '#f74b6d'

  return (
    <div className="flex flex-col items-center gap-4">
      <div className="relative inline-block">
        <svg width="180" height="180" className="transform -rotate-90" role="img" aria-label={`新闻情绪指数 ${Math.round(clampedValue)}，${sentimentLabel}`}>
          <circle cx="90" cy="90" r={radius} fill="none" stroke="rgba(255,255,255,0.15)" strokeWidth="12" />
          <circle
            cx="90"
            cy="90"
            r={radius}
            fill="none"
            stroke={color}
            strokeWidth="12"
            strokeDasharray={circumference}
            strokeDashoffset={offset}
            strokeLinecap="round"
            style={{ transition: 'stroke-dashoffset 1s ease, stroke 0.5s ease' }}
          />
        </svg>
        <div className="absolute inset-0 flex flex-col items-center justify-center rotate-90">
          <span className="text-5xl font-black text-white tracking-tighter drop-shadow-lg">
            {Math.round(clampedValue)}
          </span>
          <span className="text-xs font-black uppercase tracking-widest mt-1 drop-shadow" style={{ color }}>
            {sentimentLabel}
          </span>
        </div>
      </div>
    </div>
  )
}
