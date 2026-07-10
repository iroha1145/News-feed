import logging
import xml.etree.ElementTree as ET
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import Optional

import httpx

from app.utils.http import log_http_failure
from app.utils.news_text import clean_news_text

logger = logging.getLogger(__name__)

SA_FEEDS = {
    "breaking": "https://seekingalpha.com/market_currents.xml",
    "daily": "https://seekingalpha.com/tag/wall-st-breakfast.xml",
}


def _parse_sa_item(item: ET.Element, feed_kind: str = "breaking") -> Optional[dict]:
    title_el = item.find("title")
    link_el = item.find("link")
    pub_el = item.find("pubDate")

    title = clean_news_text(title_el.text if title_el is not None else None, empty="") or ""
    url = clean_news_text(link_el.text if link_el is not None else None, empty="") or ""
    if not title or not url:
        return None

    tickers = []
    for category in item.findall("category"):
        if "symbol" in category.get("domain", "") and category.text:
            tickers.append(category.text.upper())
    summary = f"[{', '.join(tickers[:6])}]" if tickers else None

    published_at = ""
    if pub_el is not None and pub_el.text:
        try:
            parsed = parsedate_to_datetime(pub_el.text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            published_at = parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        except (TypeError, ValueError, OverflowError):
            published_at = clean_news_text(pub_el.text, empty="") or ""

    return {
        "source": f"seekingalpha/{feed_kind}",
        "title": title,
        "summary": summary,
        "url": url,
        "image_url": None,
        "published_at": published_at,
    }


async def fetch_seekingalpha_feed(feed_kind: str) -> list[dict]:
    if feed_kind not in SA_FEEDS:
        raise ValueError(f"Unknown Seeking Alpha feed: {feed_kind}")
    feed_url = SA_FEEDS[feed_kind]

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                feed_url,
                headers={"User-Agent": "MacroLens/1.0 (RSS reader)"},
            )
            response.raise_for_status()
            tree = ET.fromstring(response.text)
    except Exception as exc:
        log_http_failure(logger, f"Seeking Alpha [{feed_kind}]", exc, endpoint=feed_url)
        raise RuntimeError(f"Seeking Alpha {feed_kind} feed failed") from exc

    results: list[dict] = []
    seen_urls: set[str] = set()
    for element in tree.findall(".//item"):
        parsed = _parse_sa_item(element, feed_kind)
        if parsed and parsed["url"] not in seen_urls:
            seen_urls.add(parsed["url"])
            results.append(parsed)

    logger.info("Seeking Alpha [%s]: %s items", feed_kind, len(results))
    return results


async def fetch_seekingalpha_breaking() -> list[dict]:
    return await fetch_seekingalpha_feed("breaking")


async def fetch_seekingalpha_daily() -> list[dict]:
    return await fetch_seekingalpha_feed("daily")


async def fetch_seekingalpha_news() -> list[dict]:
    """Compatibility helper that fetches both feeds independently."""
    results = await fetch_seekingalpha_breaking()
    results.extend(await fetch_seekingalpha_daily())
    return results
