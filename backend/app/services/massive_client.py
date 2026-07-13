import logging
from typing import Optional, Sequence

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
    # Tickers remain structured metadata; they are not injected into summary.
    desc = clean_news_text(item.get("description"), empty="") or ""

    return {
        "source": f"massive/{source_name}",
        "title": title,
        "summary": desc[:500] if desc else None,
        "url": url,
        "image_url": item.get("image_url"),
        "published_at": published_at,
        "source_tickers": [str(value).upper() for value in tickers[:100] if value],
        "ticker_association_method": "provider_tag",
    }


async def fetch_massive_focus_news(
    focus_symbols: Sequence[str],
    *,
    api_key: str,
    client: httpx.AsyncClient,
    request_limit: int = 10,
) -> list[dict]:
    symbols: list[str] = []
    for value in focus_symbols:
        ticker = str(value or "").strip().upper().lstrip("$")
        if ticker and ticker not in symbols:
            symbols.append(ticker)
        if len(symbols) >= request_limit:
            break
    results: list[dict] = []
    for ticker in symbols:
        focused = await client.get(
            BASE_URL,
            params={"ticker": ticker, "limit": 10, "sort": "published_utc", "order": "desc"},
            headers={"Authorization": f"Bearer {api_key}", "User-Agent": "MacroLens/1.0"},
        )
        focused.raise_for_status()
        payload = focused.json()
        if not isinstance(payload, dict) or not isinstance(payload.get("results", []), list):
            raise ValueError("Massive returned an invalid focus-news payload")
        for item in payload.get("results", []):
            parsed = _parse_item(item)
            if parsed:
                if ticker not in parsed["source_tickers"]:
                    parsed["source_tickers"].append(ticker)
                results.append(parsed)
    return results


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

            # A bounded focus query supplements the hourly broad poll. The
            # scheduler cadence remains independent and no symbol is hard-coded.
            from app.config import settings
            from app.models.database import get_db
            from app.services.focus_context import latest_focus_context

            db = await get_db()
            try:
                snapshot = await latest_focus_context(db)
            finally:
                await db.close()
            symbols = [
                str(item.get("ticker") or "").strip().upper()
                for item in ((snapshot or {}).get("payload") or {}).get("symbols", [])
                if isinstance(item, dict) and item.get("ticker")
            ][: settings.massive_focus_request_limit]
            results.extend(
                await fetch_massive_focus_news(
                    symbols,
                    api_key=api_key,
                    client=client,
                    request_limit=settings.massive_focus_request_limit,
                )
            )

        logger.info(f"Massive: fetched {len(results)} items")
    except Exception as e:
        log_http_failure(logger, "Massive", e, endpoint=BASE_URL, secrets=(api_key,), warning=False)
        raise RuntimeError("Massive request failed") from e

    return results
