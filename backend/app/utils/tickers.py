from __future__ import annotations

import re
from typing import Any


PUBLIC_TICKER_PATTERN = re.compile(r"[A-Z0-9][A-Z0-9.^/_-]{0,19}")


def normalize_ticker(value: Any) -> str:
    ticker = str(value or "").strip().upper().lstrip("$")
    return ticker if PUBLIC_TICKER_PATTERN.fullmatch(ticker) else ""
