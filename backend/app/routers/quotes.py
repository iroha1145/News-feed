import logging
import time
from typing import Optional

import numpy as np
import yfinance as yf
from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/quotes", tags=["quotes"])

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

# Proxy ETFs for fundamental data (indices/commodities lack market_cap, PE, yield)
_ETF_PROXY: dict[str, str] = {
    "^IXIC": "QQQ",
    "^GSPC": "SPY",
    "^N225": "EWJ",
    "000001.SS": "ASHR",
    "GC=F": "GLD",
    "SI=F": "SLV",
    "CL=F": "USO",
}

# Cache to avoid hitting API too frequently
_cache: dict = {"data": None, "ts": 0}
CACHE_TTL = 120  # 2 minutes


def _is_market_open(info) -> bool:
    """Heuristic: if last_price != previous_close, market likely moved today."""
    try:
        return abs(info.last_price - info.previous_close) > 0.001
    except Exception:
        return False


@router.get("")
async def get_market_quotes():
    """Fetch market quotes using yfinance with 52-week range data."""
    now = time.time()
    if _cache["data"] and (now - _cache["ts"]) < CACHE_TTL:
        return _cache["data"]

    try:
        symbols_str = " ".join(ALL_SYMBOLS.keys())
        tickers = yf.Tickers(symbols_str)

        quotes = []
        for symbol, meta in ALL_SYMBOLS.items():
            try:
                t = tickers.tickers[symbol]
                info = t.fast_info
                price = info.last_price
                prev = info.previous_close
                change = price - prev if price and prev else None
                change_pct = (change / prev * 100) if change and prev else None
                market_open = _is_market_open(info)

                quotes.append({
                    "symbol": symbol,
                    "name": meta["name"],
                    "label": meta["label"],
                    "price": round(price, 2) if price else None,
                    "change": round(change, 2) if change is not None else None,
                    "changePercent": round(change_pct, 2) if change_pct is not None else None,
                    "previousClose": round(prev, 2) if prev else None,
                    "yearLow": round(info.year_low, 2) if info.year_low else None,
                    "yearHigh": round(info.year_high, 2) if info.year_high else None,
                    "marketOpen": market_open,
                    "type": "commodity" if symbol in COMMODITIES else "index",
                })
            except Exception as e:
                logger.warning(f"Failed to fetch {symbol}: {e}")
                quotes.append({
                    "symbol": symbol,
                    "name": meta["name"],
                    "label": meta["label"],
                    "price": None,
                    "change": None,
                    "changePercent": None,
                    "previousClose": None,
                    "yearLow": None,
                    "yearHigh": None,
                    "marketOpen": False,
                    "type": "commodity" if symbol in COMMODITIES else "index",
                })

        result = {"quotes": quotes}
        _cache["data"] = result
        _cache["ts"] = now
        return result

    except Exception as e:
        logger.warning(f"Failed to fetch market quotes: {e}")
        return {
            "quotes": [
                {
                    "symbol": s,
                    "name": ALL_SYMBOLS[s]["name"],
                    "label": ALL_SYMBOLS[s]["label"],
                    "price": None,
                    "change": None,
                    "changePercent": None,
                    "previousClose": None,
                    "yearLow": None,
                    "yearHigh": None,
                    "marketOpen": False,
                    "type": "commodity" if s in COMMODITIES else "index",
                }
                for s in ALL_SYMBOLS
            ]
        }


# ── Candles (OHLCV + EMA/SMA) ──────────────────────────────────

# Map frontend timeframe → yfinance (period, interval)
_TF_MAP = {
    "1D": ("5d", "15m"),
    "1W": ("1mo", "1h"),
    "1M": ("6mo", "1d"),
    "1Y": ("1y", "1d"),
}

_candle_cache: dict = {}
_CANDLE_TTL = 300  # 5 min


@router.get("/{symbol:path}/candles")
async def get_candles(
    symbol: str,
    timeframe: str = Query("1D", regex="^(1D|1W|1M|1Y)$"),
):
    """Return OHLCV candles + EMA-20 / SMA-50 for a given symbol & timeframe."""

    cache_key = f"{symbol}:{timeframe}"
    now = time.time()
    cached = _candle_cache.get(cache_key)
    if cached and (now - cached["ts"]) < _CANDLE_TTL:
        return cached["data"]

    period, interval = _TF_MAP[timeframe]

    try:
        t = yf.Ticker(symbol)
        df = t.history(period=period, interval=interval)

        if df.empty:
            raise HTTPException(404, "No candle data available")

        # Drop timezone info for JSON serialisation
        df.index = df.index.tz_localize(None) if df.index.tz is None else df.index.tz_convert(None)

        # Compute moving averages on Close
        close = df["Close"]
        ema20 = close.ewm(span=min(20, len(close)), min_periods=1, adjust=False).mean()
        sma50 = close.rolling(window=min(50, len(close)), min_periods=1).mean()

        candles = []
        ema_points = []
        sma_points = []

        for idx, row in df.iterrows():
            ts = idx.isoformat()
            candles.append({
                "time": ts,
                "open": round(float(row["Open"]), 2),
                "high": round(float(row["High"]), 2),
                "low": round(float(row["Low"]), 2),
                "close": round(float(row["Close"]), 2),
                "volume": int(row["Volume"]),
            })
            if not np.isnan(ema20[idx]):
                ema_points.append({"time": ts, "value": round(float(ema20[idx]), 2)})
            if not np.isnan(sma50[idx]):
                sma_points.append({"time": ts, "value": round(float(sma50[idx]), 2)})

        result = {
            "symbol": symbol,
            "timeframe": timeframe,
            "candles": candles,
            "ema20": ema_points,
            "sma50": sma_points,
        }
        _candle_cache[cache_key] = {"data": result, "ts": now}
        return result

    except HTTPException:
        raise
    except Exception:
        logger.exception("Candle fetch failed for %s", symbol)
        raise HTTPException(500, "Failed to fetch candle data")


# ── Profile (fundamentals) ──────────────────────────────────────

_profile_cache: dict = {}
_PROFILE_TTL = 600  # 10 min


@router.get("/{symbol:path}/profile")
async def get_profile(symbol: str):
    """Return fundamental profile data for a given symbol."""
    now = time.time()
    cached = _profile_cache.get(symbol)
    if cached and (now - cached["ts"]) < _PROFILE_TTL:
        return cached["data"]

    # Derive name/type for both known and arbitrary symbols
    meta = ALL_SYMBOLS.get(symbol)
    display_name = meta["name"] if meta else symbol

    try:
        t = yf.Ticker(symbol)
        info = t.info or {}

        # fast_info is more reliable for OHLV on indices
        try:
            fi = t.fast_info
            fi_open = fi.open
            fi_high = fi.day_high
            fi_low = fi.day_low
            fi_vol = fi.last_volume
            fi_year_low = fi.year_low
            fi_year_high = fi.year_high
            fi_50d = fi.fifty_day_average if hasattr(fi, "fifty_day_average") else None
            fi_200d = fi.two_hundred_day_average if hasattr(fi, "two_hundred_day_average") else None
        except Exception:
            fi_open = fi_high = fi_low = fi_vol = fi_year_low = fi_year_high = fi_50d = fi_200d = None

        # Fetch proxy ETF fundamentals if the symbol itself lacks them
        etf_info: dict = {}
        proxy = _ETF_PROXY.get(symbol)
        if proxy and not info.get("marketCap"):
            try:
                etf_info = yf.Ticker(proxy).info or {}
            except Exception:
                pass

        def _r(v):
            return round(float(v), 2) if v is not None else None

        result = {
            "symbol": symbol,
            "name": display_name,
            "shortName": info.get("shortName", display_name),
            "description": info.get("longBusinessSummary") or info.get("description", ""),
            "market_cap": info.get("marketCap") or etf_info.get("totalAssets") or etf_info.get("marketCap"),
            "pe_ratio": info.get("trailingPE") or info.get("forwardPE") or etf_info.get("trailingPE"),
            "dividend_yield": info.get("dividendYield") or etf_info.get("yield") or etf_info.get("dividendYield"),
            "avg_volume": info.get("averageVolume") or info.get("averageDailyVolume10Day"),
            "open": _r(info.get("open") or info.get("regularMarketOpen") or fi_open),
            "day_high": _r(info.get("dayHigh") or info.get("regularMarketDayHigh") or fi_high),
            "day_low": _r(info.get("dayLow") or info.get("regularMarketDayLow") or fi_low),
            "last_volume": int(fi_vol) if fi_vol else (info.get("volume") or info.get("regularMarketVolume")),
            "year_low": _r(info.get("fiftyTwoWeekLow") or fi_year_low),
            "year_high": _r(info.get("fiftyTwoWeekHigh") or fi_year_high),
            "fifty_day_avg": _r(info.get("fiftyDayAverage") or fi_50d),
            "two_hundred_day_avg": _r(info.get("twoHundredDayAverage") or fi_200d),
            "beta": info.get("beta") or etf_info.get("beta3Year") or etf_info.get("beta"),
            "etf_proxy": proxy,
        }

        _profile_cache[symbol] = {"data": result, "ts": now}
        return result

    except Exception:
        logger.exception("Profile fetch failed for %s", symbol)
        raise HTTPException(500, "Failed to fetch profile")


# ── Asset Sentiment (aggregated from analyses) ──────────────────

@router.get("/{symbol:path}/sentiment")
async def get_asset_sentiment_api(
    symbol: str,
    days: int = Query(7, ge=1, le=90),
):
    """Aggregate news sentiment for a given asset over the past N days."""
    try:
        from app.models.database import get_db, get_asset_sentiment
        db = await get_db()
        try:
            result = await get_asset_sentiment(db, symbol, days=days)
            return {"symbol": symbol, "days": days, **result}
        finally:
            await db.close()
    except Exception as e:
        logger.warning(f"Sentiment aggregation failed for {symbol}: {e}")
        # Return empty/null sentiment for unknown or unsupported symbols
        return {
            "symbol": symbol,
            "days": days,
            "score": None,
            "total": 0,
            "bullish": 0,
            "bearish": 0,
            "neutral": 0,
            "signal": None,
            "description": None,
            "tags": [],
        }


# ── Top Constituents (index weight contributors) ────────────────

_CONSTITUENTS: dict[str, list[dict]] = {
    "^IXIC": [
        {"ticker": "AAPL", "name": "Apple Inc.", "weight": 12.4},
        {"ticker": "MSFT", "name": "Microsoft Corp.", "weight": 10.2},
        {"ticker": "NVDA", "name": "NVIDIA Corp.", "weight": 6.8},
        {"ticker": "AMZN", "name": "Amazon.com Inc.", "weight": 5.6},
        {"ticker": "META", "name": "Meta Platforms", "weight": 4.3},
    ],
    "^GSPC": [
        {"ticker": "AAPL", "name": "Apple Inc.", "weight": 7.1},
        {"ticker": "MSFT", "name": "Microsoft Corp.", "weight": 6.8},
        {"ticker": "NVDA", "name": "NVIDIA Corp.", "weight": 5.2},
        {"ticker": "AMZN", "name": "Amazon.com Inc.", "weight": 3.8},
        {"ticker": "GOOG", "name": "Alphabet Inc.", "weight": 3.5},
    ],
    "^N225": [
        {"ticker": "TM", "name": "Toyota Motor", "weight": 4.8},
        {"ticker": "SONY", "name": "Sony Group", "weight": 3.2},
        {"ticker": "6758.T", "name": "Sony Group (TSE)", "weight": 3.2},
        {"ticker": "7203.T", "name": "Toyota Motor (TSE)", "weight": 4.8},
        {"ticker": "8306.T", "name": "Mitsubishi UFJ", "weight": 2.5},
    ],
}

_const_cache: dict = {}
_CONST_TTL = 300  # 5 min


@router.get("/{symbol:path}/constituents")
async def get_constituents(symbol: str):
    """Return top weight contributors with live change % for an index."""
    mapping = _CONSTITUENTS.get(symbol)
    if not mapping:
        return {"symbol": symbol, "constituents": []}

    now = time.time()
    cached = _const_cache.get(symbol)
    if cached and (now - cached["ts"]) < _CONST_TTL:
        return cached["data"]

    try:
        tickers_str = " ".join(c["ticker"] for c in mapping)
        tks = yf.Tickers(tickers_str)
        result_list = []

        for c in mapping:
            try:
                t = tks.tickers.get(c["ticker"])
                if t:
                    fi = t.fast_info
                    prc = fi.last_price
                    prev = fi.previous_close
                    chg_pct = ((prc - prev) / prev * 100) if prc and prev else None
                else:
                    chg_pct = None
            except Exception:
                chg_pct = None

            result_list.append({
                "ticker": c["ticker"],
                "name": c["name"],
                "weight": c["weight"],
                "changePercent": round(chg_pct, 2) if chg_pct is not None else None,
            })

        result = {"symbol": symbol, "constituents": result_list}
        _const_cache[symbol] = {"data": result, "ts": now}
        return result

    except Exception:
        logger.exception("Constituents fetch failed for %s", symbol)
        raise HTTPException(500, "Failed to fetch constituents")
