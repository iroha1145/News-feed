import ipaddress
import time
from collections import deque

from fastapi import APIRouter, HTTPException, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field

from app.config import settings
from app.deps.auth import (
    SESSION_COOKIE_NAME,
    admin_token_is_valid,
    create_admin_session,
    revoke_admin_session,
    session_is_valid,
)

router = APIRouter(prefix="/api/auth", tags=["auth"])

LOGIN_WINDOW_SECONDS = 5 * 60
LOGIN_MAX_ATTEMPTS = 5
_login_attempts: dict[str, deque[float]] = {}


class LoginRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    token: str = Field(min_length=1, max_length=4096)


def _client_key(request: Request) -> str:
    real_ip = request.headers.get("x-real-ip", "").strip()
    if real_ip:
        try:
            return str(ipaddress.ip_address(real_ip))
        except ValueError:
            pass
    return request.client.host if request.client else "unknown"


def _attempt_bucket(client_key: str, now: float) -> deque[float]:
    cutoff = now - LOGIN_WINDOW_SECONDS
    attempts = _login_attempts.setdefault(client_key, deque())
    while attempts and attempts[0] <= cutoff:
        attempts.popleft()

    # Bound memory if callers rotate addresses over a long-running process.
    if len(_login_attempts) > 1024:
        stale = [key for key, values in _login_attempts.items() if not values or values[-1] <= cutoff]
        for key in stale:
            _login_attempts.pop(key, None)
        while len(_login_attempts) > 1024:
            oldest_key = min(
                _login_attempts,
                key=lambda key: _login_attempts[key][-1] if _login_attempts[key] else 0,
            )
            _login_attempts.pop(oldest_key, None)
    return attempts


def _secure_cookie(request: Request) -> bool:
    forwarded_proto = request.headers.get("x-forwarded-proto", "").split(",", 1)[0].strip()
    return settings.session_cookie_secure or request.url.scheme == "https" or forwarded_proto == "https"


@router.post("/login")
async def login(body: LoginRequest, request: Request, response: Response):
    if not settings.admin_token:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Server misconfigured: ADMIN_TOKEN is not set",
        )

    now = time.time()
    client_key = _client_key(request)
    attempts = _attempt_bucket(client_key, now)
    if len(attempts) >= LOGIN_MAX_ATTEMPTS:
        retry_after = max(1, int(LOGIN_WINDOW_SECONDS - (now - attempts[0])))
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many login attempts",
            headers={"Retry-After": str(retry_after)},
        )

    if not admin_token_is_valid(body.token):
        attempts.append(now)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid admin token")

    _login_attempts.pop(client_key, None)
    session_token, max_age = create_admin_session()
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_token,
        max_age=max_age,
        httponly=True,
        secure=_secure_cookie(request),
        samesite="lax",
        path="/",
    )
    return {"authenticated": True, "expires_in": max_age}


@router.get("/session")
async def get_session(request: Request):
    authenticated = session_is_valid(request.cookies.get(SESSION_COOKIE_NAME))
    return {"authenticated": authenticated}


@router.post("/logout")
async def logout(request: Request, response: Response):
    revoke_admin_session(request.cookies.get(SESSION_COOKIE_NAME))
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path="/",
        httponly=True,
        secure=_secure_cookie(request),
        samesite="lax",
    )
    return {"authenticated": False}
