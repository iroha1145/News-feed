import logging
from typing import Optional

import httpx

from app.utils.http import log_http_failure
from app.utils.news_text import clean_news_text

logger = logging.getLogger(__name__)

BASE_URL = "https://api.massive.com/v2/reference/news"


def _parse_item(item: dict) -> Optional[dict]:
    title = clean_news_text(item.get("title"), empty="") or ""
    url = clean_news_text(item.get("article_url"), empty="") or ""
    if not title or not url:
        return None

    published_at = item.get("published_utc", "")
    # Ensure UTC suffix
    if published_at and not published_at.endswith("Z") and "+" not in published_at:
        published_at += "Z"

    publisher = item.get("publisher", {})
    source_name = publisher.get("name", "Massive")

    tickers = item.get("tickers", [])
    # Build summary from description + tickers
    desc = clean_news_text(item.get("description"), empty="") or ""
    if tickers:
        desc = f"[{', '.join(tickers[:5])}] {desc}"

    return {
        "source": f"massive/{source_name}",
        "title": title,
        "summary": desc[:500] if desc else None,
        "url": url,
        "image_url": item.get("image_url"),
        "published_at": published_at,
    }


async def fetch_massive_news(api_key: str) -> list[dict]:
    if not api_key:
        logger.debug("Massive API key not set; skipping")
        return []

    results: list[dict] = []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                BASE_URL,
                params={"limit": 50, "sort": "published_utc", "order": "desc"},
                headers={"Authorization": f"Bearer {api_key}", "User-Agent": "MacroLens/1.0"},
            )
            resp.raise_for_status()
            data = resp.json()

            for item in data.get("results", []):
                parsed = _parse_item(item)
                if parsed:
                    results.append(parsed)

        logger.info(f"Massive: fetched {len(results)} items")
    except Exception as e:
        log_http_failure(logger, "Massive", e, endpoint=BASE_URL, secrets=(api_key,), warning=False)
        raise RuntimeError("Massive request failed") from e

    return results
