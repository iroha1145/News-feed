from __future__ import annotations

import hashlib
import hmac
import ipaddress
import re
import secrets
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Literal
from urllib.parse import parse_qsl, quote

import aiosqlite
from fastapi import Depends, Request

from app.config import settings
from app.models.database import get_db

HEADER_KEY_ID = "X-Optix-Key-Id"
HEADER_TIMESTAMP = "X-Optix-Timestamp"
HEADER_NONCE = "X-Optix-Nonce"
HEADER_CONTENT_HASH = "X-Optix-Content-SHA256"
HEADER_SIGNATURE = "X-Optix-Signature"

EMPTY_BODY_SHA256 = hashlib.sha256(b"").hexdigest()
HEX_64 = re.compile(r"^[0-9a-f]{64}$")
KEY_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
NONCE = re.compile(r"^[A-Za-z0-9._~-]{16,128}$")


class IntegrationAPIError(Exception):
    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        retry_after_seconds: int | None = None,
        resync_from: datetime | None = None,
        server_time: datetime | None = None,
        latest_window_days: int | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds
        self.resync_from = resync_from
        self.server_time = server_time
        self.latest_window_days = latest_window_days


@dataclass(frozen=True)
class IntegrationPrincipal:
    key_id: str
    scope: Literal["read", "action"]
    source_ip: str


_failure_buckets: dict[str, deque[float]] = defaultdict(deque)
AUTH_FAILURE_LIMIT = 60
AUTH_FAILURE_WINDOW_SECONDS = 60


def _record_auth_failure(source_ip: str) -> None:
    now = time.monotonic()
    bucket = _failure_buckets[source_ip]
    while bucket and bucket[0] <= now - AUTH_FAILURE_WINDOW_SECONDS:
        bucket.popleft()
    if len(bucket) >= AUTH_FAILURE_LIMIT:
        raise IntegrationAPIError(
            429,
            "authentication_rate_limited",
            "Too many authentication failures.",
            retryable=True,
            retry_after_seconds=AUTH_FAILURE_WINDOW_SECONDS,
        )
    bucket.append(now)
    if len(_failure_buckets) > 10_000:
        for key in list(_failure_buckets)[:1_000]:
            if not _failure_buckets[key] or _failure_buckets[key][-1] <= now - AUTH_FAILURE_WINDOW_SECONDS:
                _failure_buckets.pop(key, None)


def canonical_query(raw_query: bytes | str) -> str:
    if isinstance(raw_query, bytes):
        raw_query = raw_query.decode("ascii", errors="strict")
    pairs = parse_qsl(raw_query, keep_blank_values=True, strict_parsing=False, encoding="utf-8", errors="strict")
    encoded = [
        (quote(key, safe="-._~", encoding="utf-8", errors="strict"),
         quote(value, safe="-._~", encoding="utf-8", errors="strict"))
        for key, value in pairs
    ]
    encoded.sort(key=lambda pair: (pair[0], pair[1]))
    return "&".join(f"{key}={value}" for key, value in encoded)


def canonical_path(raw_path: bytes | str) -> str:
    if isinstance(raw_path, str):
        raw_path = raw_path.encode("ascii", errors="strict")
    # Starlette 0.38's TestClient copied httpx.URL.raw_path verbatim, including
    # the query suffix. ASGI servers provide only the path here; accept both
    # shapes while continuing to sign the query separately below.
    return raw_path.split(b"?", 1)[0].decode("ascii", errors="strict")


def canonical_string(
    method: str,
    path: str,
    query: str,
    timestamp: str,
    nonce: str,
    body_sha256: str,
) -> str:
    return "\n".join((method.upper(), path, query, timestamp, nonce, body_sha256))


def calculate_signature(secret: str, canonical: str) -> str:
    return hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).hexdigest()


def _parse_networks(raw: str) -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]:
    networks = []
    for value in raw.split(","):
        value = value.strip()
        if value:
            try:
                networks.append(ipaddress.ip_network(value, strict=False))
            except ValueError as exc:
                raise IntegrationAPIError(503, "invalid_server_configuration", "Allowed network configuration is invalid.") from exc
    return tuple(networks)


def _in_networks(address: ipaddress._BaseAddress, networks: Iterable[ipaddress._BaseNetwork]) -> bool:
    return any(address.version == network.version and address in network for network in networks)


def _source_ip(request: Request) -> tuple[str, bool]:
    direct = request.client.host if request.client else ""
    try:
        direct_ip = ipaddress.ip_address(direct)
    except ValueError as exc:
        raise IntegrationAPIError(403, "source_forbidden", "The request source is not allowed.") from exc

    trusted = _parse_networks(settings.option_pro_trusted_proxy_cidrs)
    is_trusted_proxy = _in_networks(direct_ip, trusted)
    if not is_trusted_proxy:
        return str(direct_ip), False

    chain: list[ipaddress._BaseAddress] = []
    for value in request.headers.get("X-Forwarded-For", "").split(","):
        value = value.strip()
        if value:
            try:
                chain.append(ipaddress.ip_address(value))
            except ValueError as exc:
                raise IntegrationAPIError(403, "source_forbidden", "The proxy source chain is invalid.") from exc
    chain.append(direct_ip)
    while len(chain) > 1 and _in_networks(chain[-1], trusted):
        chain.pop()
    return str(chain[-1]), True


def _is_secure(request: Request, trusted_proxy: bool, source_ip: str) -> bool:
    if request.url.scheme == "https":
        return True
    if trusted_proxy and request.headers.get("X-Forwarded-Proto", "").split(",")[0].strip().lower() == "https":
        return True
    if settings.option_pro_allow_local_http:
        try:
            return ipaddress.ip_address(source_ip).is_loopback
        except ValueError:
            return False
    return False


def _key_material(key_id: str) -> tuple[Literal["read", "action"], tuple[str, ...]] | None:
    if key_id and key_id == settings.option_pro_read_key_id and settings.option_pro_read_secret:
        return "read", tuple(
            value for value in (settings.option_pro_read_secret, settings.option_pro_previous_read_secret) if value
        )
    if key_id and key_id == settings.option_pro_action_key_id and settings.option_pro_action_secret:
        return "action", tuple(
            value for value in (settings.option_pro_action_secret, settings.option_pro_previous_action_secret) if value
        )
    return None


async def _consume_nonce(db: aiosqlite.Connection, key_id: str, nonce: str, now: datetime) -> None:
    expires_at = now + timedelta(seconds=settings.option_pro_nonce_ttl_seconds)
    try:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("DELETE FROM integration_nonces WHERE expires_at <= ?", (now.isoformat(),))
        await db.execute(
            "INSERT INTO integration_nonces(key_id, nonce, received_at, expires_at) VALUES (?, ?, ?, ?)",
            (key_id, nonce, now.isoformat(), expires_at.isoformat()),
        )
        await db.commit()
    except aiosqlite.IntegrityError as exc:
        await db.rollback()
        raise IntegrationAPIError(401, "nonce_replayed", "The request nonce has already been used.") from exc
    except Exception:
        await db.rollback()
        raise


async def authenticate_request(request: Request, required_scope: Literal["read", "action"]) -> IntegrationPrincipal:
    source_ip, trusted_proxy = _source_ip(request)
    try:
        if not _is_secure(request, trusted_proxy, source_ip):
            raise IntegrationAPIError(403, "https_required", "HTTPS is required for this integration.")

        key_id = request.headers.get(HEADER_KEY_ID, "")
        timestamp = request.headers.get(HEADER_TIMESTAMP, "")
        nonce = request.headers.get(HEADER_NONCE, "")
        supplied_hash = request.headers.get(HEADER_CONTENT_HASH, "")
        supplied_signature = request.headers.get(HEADER_SIGNATURE, "")
        if not KEY_ID.fullmatch(key_id) or not NONCE.fullmatch(nonce):
            raise IntegrationAPIError(401, "invalid_authentication_headers", "Authentication headers are invalid.")
        if not timestamp.isascii() or not timestamp.isdigit() or len(timestamp) > 12:
            raise IntegrationAPIError(401, "invalid_timestamp", "The request timestamp is invalid.")
        if not HEX_64.fullmatch(supplied_hash) or not HEX_64.fullmatch(supplied_signature):
            raise IntegrationAPIError(401, "invalid_authentication_headers", "Authentication headers are invalid.")

        now = datetime.now(timezone.utc)
        if abs(now.timestamp() - int(timestamp)) > settings.option_pro_signature_clock_skew_seconds:
            raise IntegrationAPIError(401, "timestamp_outside_window", "The request timestamp is outside the allowed window.")

        body = await request.body()
        actual_hash = hashlib.sha256(body).hexdigest()
        if not secrets.compare_digest(actual_hash, supplied_hash):
            raise IntegrationAPIError(401, "body_hash_mismatch", "The request body digest is invalid.")

        material = _key_material(key_id)
        if material is None:
            raise IntegrationAPIError(401, "invalid_signature", "The request signature is invalid.")
        scope, candidate_secrets = material

        raw_path = request.scope.get("raw_path") or request.url.path.encode("ascii")
        path = canonical_path(raw_path)
        query = canonical_query(request.scope.get("query_string", b""))
        canonical = canonical_string(request.method, path, query, timestamp, nonce, supplied_hash)
        valid_signature = any(
            secrets.compare_digest(calculate_signature(secret, canonical), supplied_signature)
            for secret in candidate_secrets
        )
        if not valid_signature:
            raise IntegrationAPIError(401, "invalid_signature", "The request signature is invalid.")

        allowed = _parse_networks(settings.option_pro_allowed_cidrs)
        if not allowed:
            raise IntegrationAPIError(
                503,
                "invalid_server_configuration",
                "The integration source allow-list is not configured.",
            )
        if not _in_networks(ipaddress.ip_address(source_ip), allowed):
            raise IntegrationAPIError(403, "source_forbidden", "The request source is not allowed.")
        if required_scope == "action" and scope != "action":
            raise IntegrationAPIError(403, "insufficient_scope", "The action scope is required.")

        db = await get_db()
        try:
            await _consume_nonce(db, key_id, nonce, now)
        finally:
            await db.close()
        return IntegrationPrincipal(key_id=key_id, scope=scope, source_ip=source_ip)
    except IntegrationAPIError:
        _record_auth_failure(source_ip)
        raise


async def require_read(request: Request) -> IntegrationPrincipal:
    return await authenticate_request(request, "read")


async def require_action(request: Request) -> IntegrationPrincipal:
    return await authenticate_request(request, "action")


ReadPrincipal = Depends(require_read)
ActionPrincipal = Depends(require_action)
