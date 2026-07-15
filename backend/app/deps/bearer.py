from __future__ import annotations

import secrets

from fastapi import HTTPException, Request, status

from app.config import settings


async def require_owner_token(request: Request) -> None:
    expected = settings.internal_api_token
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "internal_token_not_configured"},
        )

    values = request.headers.getlist("authorization")
    if len(values) != 1:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "invalid_bearer_token"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    scheme, separator, provided = values[0].partition(" ")
    valid = (
        bool(separator)
        and scheme.casefold() == "bearer"
        and bool(provided)
        and secrets.compare_digest(provided, expected)
    )
    if not valid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "invalid_bearer_token"},
            headers={"WWW-Authenticate": "Bearer"},
        )
