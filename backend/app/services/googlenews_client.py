import logging
import xml.etree.ElementTree as ET
from typing import Optional
from email.utils import parsedate_to_datetime

import httpx

from app.utils.http import log_http_failure
from app.utils.news_text import clean_news_text

logger = logging.getLogger(__name__)

# Multiple search queries for broad financial coverage
RSS_FEEDS = [
    {
        "url": "https://news.google.com/rss/search?q=stock+market+OR+S%26P+500+OR+nasdaq+OR+fed+rate&hl=en-US&gl=US&ceid=US:en",
        "label": "stocks",
    },
    {
        "url": "https://news.google.com/rss/search?q=gold+price+OR+silver+OR+precious+metals+OR+commodities&hl=en-US&gl=US&ceid=US:en",
        "label": "commodities",
    },
    {
        "url": "https://news.google.com/rss/search?q=economy+OR+inflation+OR+GDP+OR+unemployment+OR+trade+war&hl=en-US&gl=US&ceid=US:en",
        "label": "macro",
    },
]


def _parse_rss_item(item: ET.Element) -> Optional[dict]:
    title_el = item.find("title")
    link_el = item.find("link")
    pub_el = item.find("pubDate")
    source_el = item.find("source")

    title = clean_news_text(title_el.text if title_el is not None else None, empty="") or ""
    url = clean_news_text(link_el.text if link_el is not None else None, empty="") or ""
    if not title or not url:
        return None

    # Remove " - SourceName" suffix that Google News appends
    source_name = clean_news_text(source_el.text if source_el is not None else None, empty="Google News") or "Google News"

    # Parse RFC 2822 date
    published_at = ""
    if pub_el is not None and pub_el.text:
        try:
            dt = parsedate_to_datetime(pub_el.text)
            published_at = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            published_at = pub_el.text

    # Clean title - remove source suffix
    if f" - {source_name}" in title:
        title = title.rsplit(f" - {source_name}", 1)[0].strip()

    return {
        "source": f"google/{source_name}",
        "title": title,
        "summary": None,
        "url": url,
        "image_url": None,
        "published_at": published_at,
    }


async def fetch_google_news() -> list[dict]:
    """Fetch financial news from Google News RSS. Free, no API key needed."""
    results: list[dict] = []
    seen_titles: set[str] = set()
    successful_feeds = 0
    errors: list[str] = []

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            for feed in RSS_FEEDS:
                try:
                    resp = await client.get(
                        feed["url"],
                        headers={"User-Agent": "MacroLens/1.0 (news aggregator)"},
                    )
                    resp.raise_for_status()
                    tree = ET.fromstring(resp.text)
                    items = tree.findall(".//item")
                    successful_feeds += 1

                    count = 0
                    for item in items[:30]:  # Cap per feed
                        parsed = _parse_rss_item(item)
                        if parsed and parsed["title"] not in seen_titles:
                            seen_titles.add(parsed["title"])
                            results.append(parsed)
                            count += 1

                    logger.info(f"Google News [{feed['label']}]: {count} items")
                except Exception as e:
                    errors.append(log_http_failure(logger, f"Google News [{feed['label']}]", e, endpoint=feed["url"]))

    except Exception as e:
        errors.append(log_http_failure(logger, "Google News", e, warning=False))

    logger.info(f"Google News total: {len(results)} items")
    if successful_feeds == 0 and errors:
        raise RuntimeError("All Google News feeds failed")
    return results
