import { useEffect, useState } from 'react'

interface NewsImageProps {
  src: string | null | undefined
  alt: string
  className?: string
}

export default function NewsImage({ src, alt, className = '' }: NewsImageProps) {
  const [error, setError] = useState(false)

  useEffect(() => setError(false), [src])

  if (!src || error) {
    return (
      <div className={`bg-gradient-to-br from-primary/20 to-primary-container/20 dark:from-violet-900/30 dark:to-violet-800/20 flex items-center justify-center ${className}`}>
        <span className="material-symbols-outlined text-primary/40 dark:text-violet-400/40 text-3xl" aria-hidden="true">
          article
        </span>
      </div>
    )
  }

  return (
    <img
      src={src}
      alt={alt}
      width={640}
      height={360}
      loading="lazy"
      decoding="async"
      referrerPolicy="no-referrer"
      className={`object-cover ${className}`}
      onError={() => setError(true)}
    />
  )
}
