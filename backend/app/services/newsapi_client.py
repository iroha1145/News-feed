import logging
from typing import Optional

import httpx

from app.utils.http import log_http_failure
from app.utils.news_text import clean_news_text

logger = logging.getLogger(__name__)

BASE_URL = "https://newsapi.org/v2"


def _parse_item(item: dict, source_label: str) -> Optional[dict]:
    title = clean_news_text(item.get("title"), empty="") or ""
    url = clean_news_text(item.get("url"), empty="") or ""
    if not title or not url or title == "[Removed]":
        return None

    source_name = ""
    if isinstance(item.get("source"), dict):
        source_name = item["source"].get("name") or ""

    return {
        "source": f"newsapi/{source_name or source_label}",
        "title": title,
        "summary": clean_news_text(item.get("description")),
        "url": url,
        "image_url": item.get("urlToImage") or None,
        "published_at": item.get("publishedAt") or None,
    }


async def fetch_newsapi_news(api_key: str) -> list[dict]:
    if not api_key:
        logger.warning("NewsAPI key not set; skipping")
        return []

    endpoints = [
        {
            "url": f"{BASE_URL}/top-headlines",
            "params": {"category": "business", "language": "en", "pageSize": 50},
            "label": "top-headlines",
        },
        {
            "url": f"{BASE_URL}/everything",
            "params": {
                "q": "stocks OR gold OR silver OR economy OR fed",
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 50,
            },
            "label": "everything",
        },
    ]

    results: list[dict] = []
    successful_requests = 0
    errors: list[str] = []
    headers = {"X-Api-Key": api_key, "User-Agent": "MacroLens/1.0"}

    async with httpx.AsyncClient(timeout=15) as client:
        for endpoint in endpoints:
            try:
                response = await client.get(endpoint["url"], params=endpoint["params"], headers=headers)
                response.raise_for_status()
                data = response.json()
                if not isinstance(data, dict) or not isinstance(data.get("articles"), list):
                    raise ValueError("NewsAPI returned an invalid payload")
                articles = data["articles"]
                successful_requests += 1
                for article in articles:
                    parsed = _parse_item(article, endpoint["label"])
                    if parsed:
                        results.append(parsed)
                logger.info(f"NewsAPI [{endpoint['label']}]: fetched {len(articles)} items")
            except Exception as e:
                errors.append(log_http_failure(logger, f"NewsAPI [{endpoint['label']}]", e, endpoint=endpoint["url"], secrets=(api_key,)))

    if successful_requests == 0 and errors:
        raise RuntimeError("All NewsAPI requests failed")
    return results
