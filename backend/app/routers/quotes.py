import asyncio
import logging
import os
import re
import time
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Optional

import numpy as np
import yfinance as yf
from fastapi import APIRouter, HTTPException, Query

from app.utils.http import safe_exception_message

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/quotes", tags=["quotes"])

YAHOO_SOURCE = "Yahoo Finance via yfinance"
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9^][A-Z0-9.^=_-]{0,19}$")

INDICES = {
    "^IXIC": {"name": "NASDAQ", "label": "纳斯达克"},
    "^GSPC": {"name": "S&P 500", "label": "标普500"},
    "^N225": {"name": "Nikkei 225", "label": "日经225"},
    "000001.SS": {"name": "上证指数", "label": "上证"},
}

COMMODITIES = {
    "GC=F": {"name": "Gold", "label": "黄金"},
    "CL=F": {"name": "Crude Oil", "label": "原油"},
    "SI=F": {"name": "Silver", "label": "白银"},
}

ALL_SYMBOLS = {**INDICES, **COMMODITIES}


class BoundedTTLCache:
    def __init__(self, max_size: int, ttl_seconds: int):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._items: OrderedDict[str, tuple[float, Any]] = OrderedDict()

    def get(self, key: str) -> Optional[Any]:
        item = self._items.get(key)
        if item is None:
            return None
        created_at, value = item
        if time.monotonic() - created_at >= self.ttl_seconds:
            self._items.pop(key, None)
            return None
        self._items.move_to_end(key)
        return value

    def set(self, key: str, value: Any) -> None:
        self._items[key] = (time.monotonic(), value)
        self._items.move_to_end(key)
        while len(self._items) > self.max_size:
            self._items.popitem(last=False)

    def __len__(self) -> int:
        return len(self._items)


_market_cache = BoundedTTLCache(max_size=1, ttl_seconds=120)
_candle_cache = BoundedTTLCache(max_size=64, ttl_seconds=300)
_profile_cache = BoundedTTLCache(max_size=128, ttl_seconds=600)
_yfinance_slots: Optional[asyncio.Semaphore] = None

_TF_MAP = {
    "1D": ("5d", "15m"),
    "1W": ("1mo", "1h"),
    "1M": ("6mo", "1d"),
    "1Y": ("1y", "1d"),
}


def _configure_yfinance_cache() -> None:
    cache_dir = os.getenv("YFINANCE_CACHE_DIR", "/tmp/macrolens-yfinance")
    try:
        os.makedirs(cache_dir, mode=0o700, exist_ok=True)
        yf.set_tz_cache_location(cache_dir)
    except Exception as exc:
        logger.warning("Unable to configure yfinance cache: %s", type(exc).__name__)


_configure_yfinance_cache()


def _as_of() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _run_yfinance(function, *args):
    global _yfinance_slots
    if _yfinance_slots is None:
        _yfinance_slots = asyncio.Semaphore(4)
    async with _yfinance_slots:
        return await asyncio.to_thread(function, *args)


def _validated_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not SYMBOL_PATTERN.fullmatch(normalized):
        raise HTTPException(status_code=400, detail="Unsupported symbol format")
    return normalized


def _finite_float(value: Any, digits: int = 2) -> Optional[float]:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(parsed):
        return None
    return round(parsed, digits)


def _finite_int(value: Any) -> Optional[int]:
    if isinstance(value, (int, np.integer)) and not isinstance(value, (bool, np.bool_)):
        return int(value)
    parsed = _finite_float(value, digits=6)
    return int(parsed) if parsed is not None else None


def _safe_attr(obj: Any, name: str) -> Any:
    try:
        return getattr(obj, name)
    except Exception:
        return None


def _first_present(*values: Any) -> Any:
    return next((value for value in values if value is not None), None)


def _empty_quote(symbol: str, meta: dict, as_of: str) -> dict:
    return {
        "symbol": symbol,
        "name": meta["name"],
        "label": meta["label"],
        "price": None,
        "change": None,
        "changePercent": None,
        "previousClose": None,
        "yearLow": None,
        "yearHigh": None,
        "marketOpen": None,
        "type": "commodity" if symbol in COMMODITIES else "index",
        "source": YAHOO_SOURCE,
        "as_of": as_of,
    }


def _fetch_market_quotes_sync() -> dict:
    as_of = _as_of()
    tickers = yf.Tickers(" ".join(ALL_SYMBOLS))
    quotes = []
    for symbol, meta in ALL_SYMBOLS.items():
        try:
            ticker = tickers.tickers[symbol]
            info = ticker.fast_info
            price = _finite_float(_safe_attr(info, "last_price"))
            previous = _finite_float(_safe_attr(info, "previous_close"))
            change = round(price - previous, 2) if price is not None and previous is not None else None
            change_percent = (
                round(change / previous * 100, 2)
                if change is not None and previous not in (None, 0)
                else None
            )
            quotes.append({
                "symbol": symbol,
                "name": meta["name"],
                "label": meta["label"],
                "price": price,
                "change": change,
                "changePercent": change_percent,
                "previousClose": previous,
                "yearLow": _finite_float(_safe_attr(info, "year_low")),
                "yearHigh": _finite_float(_safe_attr(info, "year_high")),
                # yfinance does not provide a reliable cross-market open/closed signal here.
                "marketOpen": None,
                "type": "commodity" if symbol in COMMODITIES else "index",
                "source": YAHOO_SOURCE,
                "as_of": as_of,
            })
        except Exception as exc:
            logger.warning("Failed to fetch %s: %s", symbol, safe_exception_message(exc))
            quotes.append(_empty_quote(symbol, meta, as_of))
    return {"quotes": quotes, "source": YAHOO_SOURCE, "as_of": as_of}


@router.get("")
async def get_market_quotes():
    cached = _market_cache.get("all")
    if cached is not None:
        return cached
    try:
        result = await _run_yfinance(_fetch_market_quotes_sync)
    except Exception as exc:
        logger.warning("Failed to fetch market quotes: %s", safe_exception_message(exc))
        as_of = _as_of()
        result = {
            "quotes": [_empty_quote(symbol, meta, as_of) for symbol, meta in ALL_SYMBOLS.items()],
            "source": YAHOO_SOURCE,
            "as_of": as_of,
        }
    _market_cache.set("all", result)
    return result


def _fetch_candles_sync(symbol: str, timeframe: str) -> dict:
    period, interval = _TF_MAP[timeframe]
    frame = yf.Ticker(symbol).history(period=period, interval=interval)
    if frame.empty:
        raise LookupError("No candle data available")

    if getattr(frame.index, "tz", None) is not None:
        frame.index = frame.index.tz_convert(None)

    close = frame["Close"]
    ema20 = close.ewm(span=min(20, len(close)), min_periods=1, adjust=False).mean()
    sma50 = close.rolling(window=min(50, len(close)), min_periods=1).mean()
    candles = []
    ema_points = []
    sma_points = []

    for idx, row in frame.iterrows():
        timestamp = idx.isoformat()
        open_price = _finite_float(row.get("Open"))
        high = _finite_float(row.get("High"))
        low = _finite_float(row.get("Low"))
        close_price = _finite_float(row.get("Close"))
        if None in (open_price, high, low, close_price):
            continue
        candles.append({
            "time": timestamp,
            "open": open_price,
            "high": high,
            "low": low,
            "close": close_price,
            "volume": _finite_int(row.get("Volume")),
        })
        ema_value = _finite_float(ema20.loc[idx])
        sma_value = _finite_float(sma50.loc[idx])
        if ema_value is not None:
            ema_points.append({"time": timestamp, "value": ema_value})
        if sma_value is not None:
            sma_points.append({"time": timestamp, "value": sma_value})

    if not candles:
        raise LookupError("No valid candle data available")
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "candles": candles,
        "ema20": ema_points,
        "sma50": sma_points,
        "source": YAHOO_SOURCE,
        "as_of": _as_of(),
    }


@router.get("/{symbol:path}/candles")
async def get_candles(
    symbol: str,
    timeframe: str = Query("1D", pattern="^(1D|1W|1M|1Y)$"),
):
    normalized = _validated_symbol(symbol)
    cache_key = f"{normalized}:{timeframe}"
    cached = _candle_cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        result = await _run_yfinance(_fetch_candles_sync, normalized, timeframe)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.warning(
            "Candle fetch failed for %s: %s",
            normalized,
            safe_exception_message(exc),
        )
        raise HTTPException(status_code=502, detail="Market data provider unavailable") from exc
    _candle_cache.set(cache_key, result)
    return result


def _fetch_profile_sync(symbol: str) -> dict:
    ticker = yf.Ticker(symbol)
    info = ticker.info or {}
    try:
        fast_info = ticker.fast_info
    except Exception:
        fast_info = None

    meta = ALL_SYMBOLS.get(symbol)
    display_name = meta["name"] if meta else symbol
    open_price = _first_present(info.get("open"), info.get("regularMarketOpen"), _safe_attr(fast_info, "open"))
    day_high = _first_present(info.get("dayHigh"), info.get("regularMarketDayHigh"), _safe_attr(fast_info, "day_high"))
    day_low = _first_present(info.get("dayLow"), info.get("regularMarketDayLow"), _safe_attr(fast_info, "day_low"))
    description = info.get("longBusinessSummary") or info.get("description")
    if not isinstance(description, str) or not description.strip():
        description = None
    short_name = info.get("shortName")
    if not isinstance(short_name, str) or not short_name.strip():
        short_name = display_name

    return {
        "symbol": symbol,
        "name": display_name,
        "shortName": short_name,
        "description": description,
        "market_cap": _finite_int(info.get("marketCap")),
        "pe_ratio": _finite_float(_first_present(info.get("trailingPE"), info.get("forwardPE"))),
        "dividend_yield": _finite_float(info.get("dividendYield"), digits=6),
        "avg_volume": _finite_int(_first_present(info.get("averageVolume"), info.get("averageDailyVolume10Day"))),
        "open": _finite_float(open_price),
        "day_high": _finite_float(day_high),
        "day_low": _finite_float(day_low),
        "last_volume": _finite_int(_first_present(_safe_attr(fast_info, "last_volume"), info.get("volume"), info.get("regularMarketVolume"))),
        "year_low": _finite_float(_first_present(info.get("fiftyTwoWeekLow"), _safe_attr(fast_info, "year_low"))),
        "year_high": _finite_float(_first_present(info.get("fiftyTwoWeekHigh"), _safe_attr(fast_info, "year_high"))),
        "fifty_day_avg": _finite_float(_first_present(info.get("fiftyDayAverage"), _safe_attr(fast_info, "fifty_day_average"))),
        "two_hundred_day_avg": _finite_float(_first_present(info.get("twoHundredDayAverage"), _safe_attr(fast_info, "two_hundred_day_average"))),
        "beta": _finite_float(info.get("beta")),
        "source": YAHOO_SOURCE,
        "as_of": _as_of(),
    }


@router.get("/{symbol:path}/profile")
async def get_profile(symbol: str):
    normalized = _validated_symbol(symbol)
    cached = _profile_cache.get(normalized)
    if cached is not None:
        return cached
    try:
        result = await _run_yfinance(_fetch_profile_sync, normalized)
    except Exception as exc:
        logger.warning(
            "Profile fetch failed for %s: %s",
            normalized,
            safe_exception_message(exc),
        )
        raise HTTPException(status_code=502, detail="Market data provider unavailable") from exc
    _profile_cache.set(normalized, result)
    return result


@router.get("/{symbol:path}/sentiment")
async def get_asset_sentiment_api(
    symbol: str,
    days: int = Query(7, ge=1, le=90),
):
    normalized = _validated_symbol(symbol)
    try:
        from app.models.database import get_asset_sentiment, get_db

        db = await get_db()
        try:
            result = await get_asset_sentiment(db, normalized, days=days)
        finally:
            await db.close()
        return {
            "symbol": normalized,
            "days": days,
            **result,
            "source": "MacroLens analyzed news",
            "as_of": _as_of(),
        }
    except Exception as exc:
        logger.warning(
            "Sentiment aggregation failed for %s: %s",
            normalized,
            safe_exception_message(exc),
        )
        return {
            "symbol": normalized,
            "days": days,
            "score": None,
            "total": 0,
            "bullish": 0,
            "bearish": 0,
            "neutral": 0,
            "signal": None,
            "description": None,
            "tags": [],
            "source": "MacroLens analyzed news",
            "as_of": _as_of(),
        }


@router.get("/{symbol:path}/constituents")
async def get_constituents(symbol: str):
    """No static weights are returned because index membership and weights change over time."""
    normalized = _validated_symbol(symbol)
    return {
        "symbol": normalized,
        "constituents": [],
        "source": None,
        "as_of": None,
    }
