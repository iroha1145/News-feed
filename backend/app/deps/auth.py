from __future__ import annotations

import hashlib
import logging
import secrets
import time
from dataclasses import dataclass
from typing import Optional

from fastapi import Header, HTTPException, Request, status

from app.config import settings

logger = logging.getLogger(__name__)

SESSION_COOKIE_NAME = "macrolens_admin_session"
MAX_SESSIONS = 1024


@dataclass(frozen=True)
class AdminSession:
    expires_at: float


_sessions: dict[str, AdminSession] = {}


def _session_key(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _purge_expired_sessions(now: Optional[float] = None) -> None:
    current = now if now is not None else time.time()
    expired = [key for key, session in _sessions.items() if session.expires_at <= current]
    for key in expired:
        _sessions.pop(key, None)


def admin_token_is_valid(candidate: Optional[str]) -> bool:
    configured = settings.admin_token
    if not configured or candidate is None:
        return False
    return secrets.compare_digest(candidate, configured)


def create_admin_session() -> tuple[str, int]:
    _purge_expired_sessions()
    token = secrets.token_urlsafe(32)
    ttl_seconds = settings.session_ttl_seconds
    _sessions[_session_key(token)] = AdminSession(expires_at=time.time() + ttl_seconds)
    while len(_sessions) > MAX_SESSIONS:
        oldest_key = min(_sessions, key=lambda key: _sessions[key].expires_at)
        _sessions.pop(oldest_key, None)
    return token, ttl_seconds


def revoke_admin_session(token: Optional[str]) -> None:
    if token:
        _sessions.pop(_session_key(token), None)


def session_is_valid(token: Optional[str]) -> bool:
    if not token:
        return False
    now = time.time()
    _purge_expired_sessions(now)
    session = _sessions.get(_session_key(token))
    return bool(session and session.expires_at > now)


async def require_admin(
    request: Request,
    x_admin_token: Optional[str] = Header(default=None, alias="X-Admin-Token"),
) -> None:
    if not settings.admin_token:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Server misconfigured: ADMIN_TOKEN is not set",
        )

    if admin_token_is_valid(x_admin_token):
        return

    cookie_token = request.cookies.get(SESSION_COOKIE_NAME)
    if session_is_valid(cookie_token):
        return

    logger.warning("Rejected unauthenticated admin request")
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Unauthorized",
    )
