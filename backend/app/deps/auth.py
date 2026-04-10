import logging

from fastapi import Header, HTTPException, status

from app.config import settings

logger = logging.getLogger(__name__)


async def require_admin(x_admin_token: str | None = Header(default=None, alias="X-Admin-Token")) -> None:
    if not settings.admin_token:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Server misconfigured: ADMIN_TOKEN is not set",
        )
    if x_admin_token != settings.admin_token:
        logger.warning("Rejected admin request due to invalid token")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
        )
