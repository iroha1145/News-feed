"""HTTP logging helpers that never expose credentials or query strings."""

from __future__ import annotations

import logging
import re
from typing import Iterable
from urllib.parse import urlsplit, urlunsplit

import httpx


_URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
_SECRET_FIELD_RE = re.compile(
    r"(?i)(api[_-]?key|apikey|token|authorization|x-api-key|x-finnhub-token)"
    r"(?:[\"']?\s*[=:]\s*[\"']?)([^\s,&;\"'}]+)"
)
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+")


def safe_url(value: object) -> str:
    """Return an endpoint URL without query parameters, fragments, or credentials."""
    try:
        parts = urlsplit(str(value))
    except Exception:
        return "<invalid-url>"

    if not parts.scheme or not parts.netloc:
        return parts.path or "<unknown-endpoint>"

    try:
        hostname = parts.hostname or ""
        if ":" in hostname and not hostname.startswith("["):
            hostname = f"[{hostname}]"
        port = f":{parts.port}" if parts.port else ""
    except ValueError:
        return urlunsplit((parts.scheme, "<invalid-host>", parts.path, "", ""))
    return urlunsplit((parts.scheme, f"{hostname}{port}", parts.path, "", ""))


def redact_text(value: object, secrets: Iterable[str] = ()) -> str:
    """Remove URLs' query strings and common credential forms from arbitrary text."""
    text = str(value)
    for secret in secrets:
        if secret:
            text = text.replace(str(secret), "[REDACTED]")
    text = _URL_RE.sub(lambda match: safe_url(match.group(0)), text)
    text = _BEARER_RE.sub("Bearer [REDACTED]", text)
    return _SECRET_FIELD_RE.sub(r"\1=[REDACTED]", text)


def safe_exception_message(exc: BaseException, *, secrets: Iterable[str] = ()) -> str:
    """Build a useful error message without echoing request URLs or credentials."""
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code} at {safe_url(exc.request.url)}"
    if isinstance(exc, httpx.RequestError):
        endpoint = safe_url(exc.request.url) if exc.request is not None else "<unknown-endpoint>"
        return f"{type(exc).__name__} at {endpoint}"
    return redact_text(f"{type(exc).__name__}: {exc}", secrets)


def log_http_failure(
    logger: logging.Logger,
    source: str,
    exc: BaseException,
    *,
    endpoint: object | None = None,
    secrets: Iterable[str] = (),
    warning: bool = True,
) -> str:
    """Log and return a sanitized source failure description."""
    detail = safe_exception_message(exc, secrets=secrets)
    if endpoint is not None and " at " not in detail:
        detail = f"{detail} at {safe_url(endpoint)}"
    message = f"{source}: {detail}"
    (logger.warning if warning else logger.error)(message)
    return message
