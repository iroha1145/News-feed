export default function LoadingSpinner({ className = '', label = '加载中' }: { className?: string; label?: string }) {
  return (
    <div className={`flex items-center justify-center ${className}`} role="status" aria-label={label}>
      <div className="w-6 h-6 border-2 border-primary/20 border-t-primary rounded-full animate-spin" aria-hidden="true" />
    </div>
  )
}
