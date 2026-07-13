from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone
from typing import Optional, Sequence

import httpx

from app.utils.http import log_http_failure
from app.utils.news_text import clean_news_text

logger = logging.getLogger(__name__)

BASE_URL = "https://finnhub.io/api/v1"
_last_focus_fetch_monotonic: Optional[float] = None


def _parse_item(item: dict, *, queried_ticker: str | None = None) -> Optional[dict]:
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

    related = str(item.get("related") or "")
    source_tickers = [value.strip().upper() for value in related.split(",") if value.strip()]

    result = {
        "source": f"finnhub/{item.get('source', 'unknown')}",
        "title": title,
        "summary": clean_news_text(item.get("summary")),
        "url": url,
        "image_url": item.get("image") or None,
        "published_at": published_at,
        "source_tickers": source_tickers[:100],
        "ticker_association_method": "provider_tag",
    }
    if queried_ticker:
        ticker = queried_ticker.strip().upper().lstrip("$")
        if ticker and ticker not in result["source_tickers"]:
            result["source_tickers"].append(ticker)
        result["ticker_association_method"] = "company_endpoint"
        result["queried_ticker"] = ticker
    return result


async def fetch_finnhub_company_news(
    focus_symbols: Sequence[str],
    date_from: str | date,
    date_to: str | date,
    *,
    api_key: str,
    client: httpx.AsyncClient | None = None,
    request_limit: int | None = None,
) -> list[dict]:
    """Fetch a bounded, focus-driven company feed and preserve the query ticker."""

    if not api_key:
        return []
    start = date_from.isoformat() if isinstance(date_from, date) else str(date_from)
    end = date_to.isoformat() if isinstance(date_to, date) else str(date_to)
    limit = request_limit or 20
    symbols: list[str] = []
    for value in focus_symbols:
        symbol = str(value or "").strip().upper().lstrip("$")
        if symbol and symbol not in symbols:
            symbols.append(symbol)
        if len(symbols) >= limit:
            break
    owned = client is None
    client = client or httpx.AsyncClient(timeout=15)
    headers = {"X-Finnhub-Token": api_key, "User-Agent": "MacroLens/1.0"}
    results: list[dict] = []
    try:
        for symbol in symbols:
            response = await client.get(
                f"{BASE_URL}/company-news",
                params={"symbol": symbol, "from": start, "to": end},
                headers=headers,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                raise ValueError("Finnhub returned an invalid company-news payload")
            for item in payload[:10]:
                parsed = _parse_item(item, queried_ticker=symbol)
                if parsed:
                    results.append(parsed)
        return results
    finally:
        if owned:
            await client.aclose()


async def fetch_finnhub_news(api_key: str) -> list[dict]:
    global _last_focus_fetch_monotonic
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

        # Company news is driven by the last successful option-pro focus
        # snapshot; there is no hard-coded stock list.
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        try:
            from app.config import settings
            from app.models.database import get_db
            from app.services.focus_context import latest_focus_context

            db = await get_db()
            try:
                snapshot = await latest_focus_context(db)
            finally:
                await db.close()
            symbols = [
                str(item.get("ticker") or "")
                for item in ((snapshot or {}).get("payload") or {}).get("symbols", [])
                if isinstance(item, dict)
            ]
            if (
                _last_focus_fetch_monotonic is None
                or time.monotonic() - _last_focus_fetch_monotonic >= settings.finnhub_focus_interval
            ):
                company_items = await fetch_finnhub_company_news(
                    symbols, today, today, api_key=api_key, client=client,
                    request_limit=settings.finnhub_focus_request_limit,
                )
                results.extend(company_items)
                successful_requests += len(symbols[: settings.finnhub_focus_request_limit])
                _last_focus_fetch_monotonic = time.monotonic()
        except Exception as e:
            errors.append(log_http_failure(logger, "Finnhub focus company news", e, endpoint=f"{BASE_URL}/company-news", secrets=(api_key,)))

        logger.info(f"Finnhub total: {len(results)} items")

    if successful_requests == 0 and errors:
        raise RuntimeError("All Finnhub requests failed")
    return results
