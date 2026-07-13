import { describe, expect, it } from 'vitest'
import newsFeedSource from './NewsFeed.tsx?raw'

describe('新闻页窄屏布局', () => {
  it('允许主内容在弹性布局中收缩到手机视口宽度', () => {
    const mainTag = newsFeedSource.match(/<main\b[^>]*id="main-content"[^>]*>/)?.[0]
    const className = mainTag?.match(/className="([^"]+)"/)?.[1]

    expect(mainTag).toBeDefined()
    expect(className?.split(/\s+/)).toContain('min-w-0')
  })
})
