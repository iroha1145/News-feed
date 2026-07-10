import logging
from typing import Optional

import httpx

from app.utils.http import log_http_failure
from app.utils.news_text import clean_news_text

logger = logging.getLogger(__name__)

BASE_URL = "https://gnews.io/api/v4"


def _parse_item(item: dict, source_label: str) -> Optional[dict]:
    title = clean_news_text(item.get("title"), empty="") or ""
    url = clean_news_text(item.get("url"), empty="") or ""
    if not title or not url:
        return None

    source_name = ""
    if isinstance(item.get("source"), dict):
        source_name = item["source"].get("name") or ""

    return {
        "source": f"gnews/{source_name or source_label}",
        "title": title,
        "summary": clean_news_text(item.get("description")),
        "url": url,
        "image_url": item.get("image") or None,
        "published_at": item.get("publishedAt") or None,
    }


async def fetch_gnews_news(api_key: str) -> list[dict]:
    if not api_key:
        logger.warning("GNews API key not set; skipping")
        return []

    endpoints = [
        {
            "url": f"{BASE_URL}/top-headlines",
            "params": {"category": "business", "lang": "en", "max": 10},
            "label": "top-headlines",
        },
        {
            "url": f"{BASE_URL}/search",
            "params": {"q": "economy stocks gold", "lang": "en", "max": 10},
            "label": "search",
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
                    raise ValueError("GNews returned an invalid payload")
                articles = data["articles"]
                successful_requests += 1
                for article in articles:
                    parsed = _parse_item(article, endpoint["label"])
                    if parsed:
                        results.append(parsed)
                logger.info(f"GNews [{endpoint['label']}]: fetched {len(articles)} items")
            except Exception as e:
                errors.append(log_http_failure(logger, f"GNews [{endpoint['label']}]", e, endpoint=endpoint["url"], secrets=(api_key,)))

    if successful_requests == 0 and errors:
        raise RuntimeError("All GNews requests failed")
    return results
