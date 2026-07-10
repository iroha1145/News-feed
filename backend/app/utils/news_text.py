"""Small, lossless text normalization helpers for news ingestion."""

from __future__ import annotations

import html
import re
from typing import Optional


_WHITESPACE_RE = re.compile(r"\s+")
_ZERO_WIDTH_RE = re.compile("[\u200b\u200c\u200d\ufeff]")


def clean_news_text(value: object, *, empty: Optional[str] = None) -> Optional[str]:
    """Normalize transport noise while preserving short or low-context text."""
    if value is None:
        return empty
    text = html.unescape(str(value))
    text = _ZERO_WIDTH_RE.sub("", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text or empty
