import logging
from datetime import datetime, timezone
from typing import Optional

import httpx

from app.utils.http import log_http_failure
from app.utils.news_text import clean_news_text

logger = logging.getLogger(__name__)

BASE_URL = "https://finnhub.io/api/v1"


def _parse_item(item: dict) -> Optional[dict]:
    """Normalize a Finnhub news item to our internal schema."""
    title = clean_news_text(item.get("headline"), empty="") or ""
    url = clean_news_text(item.get("url"), empty="") or ""
    if not title or not url:
        return None

    published_ts = item.get("datetime")
    published_at = None
    if published_ts:
        try:
            published_at = datetime.fromtimestamp(int(published_ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")
        except (ValueError, OSError):
            published_at = None

    return {
        "source": f"finnhub/{item.get('source', 'unknown')}",
        "title": title,
        "summary": clean_news_text(item.get("summary")),
        "url": url,
        "image_url": item.get("image") or None,
        "published_at": published_at,
    }


async def fetch_finnhub_news(api_key: str) -> list[dict]:
    if not api_key:
        logger.warning("Finnhub API key not set; skipping")
        return []

    categories = ["general", "forex", "merger"]
    results: list[dict] = []
    successful_requests = 0
    errors: list[str] = []
    headers = {"X-Finnhub-Token": api_key, "User-Agent": "MacroLens/1.0"}

    async with httpx.AsyncClient(timeout=15) as client:
        # Market news categories
        for category in categories:
            try:
                response = await client.get(
                    f"{BASE_URL}/news",
                    params={"category": category},
                    headers=headers,
                )
                response.raise_for_status()
                items = response.json()
                if not isinstance(items, list):
                    raise ValueError("Finnhub returned an invalid news payload")
                successful_requests += 1
                for item in items:
                    parsed = _parse_item(item)
                    if parsed:
                        results.append(parsed)
                logger.info(f"Finnhub [{category}]: fetched {len(items)} items")
            except Exception as e:
                errors.append(log_http_failure(logger, f"Finnhub [{category}]", e, endpoint=f"{BASE_URL}/news", secrets=(api_key,)))

        # Company news for key tickers (more real-time)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for symbol in ["AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "GOOGL", "GLD", "SPY"]:
            try:
                response = await client.get(
                    f"{BASE_URL}/company-news",
                    params={"symbol": symbol, "from": today, "to": today},
                    headers=headers,
                )
                response.raise_for_status()
                items = response.json()
                if not isinstance(items, list):
                    raise ValueError("Finnhub returned an invalid company-news payload")
                successful_requests += 1
                for item in items[:10]:  # cap per symbol
                    parsed = _parse_item(item)
                    if parsed:
                        results.append(parsed)
            except Exception as e:
                errors.append(log_http_failure(logger, f"Finnhub [{symbol}]", e, endpoint=f"{BASE_URL}/company-news", secrets=(api_key,)))

        logger.info(f"Finnhub total: {len(results)} items")

    if successful_requests == 0 and errors:
        raise RuntimeError("All Finnhub requests failed")
    return results
