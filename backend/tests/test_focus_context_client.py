from __future__ import annotations

import asyncio
import json
import socket
import ssl
from datetime import datetime, timezone

import httpx
import pytest
from pydantic import ValidationError

from app.config import Settings, settings
from app.services import focus_context
from app.services.focus_context import (
    FOCUS_SCHEMA_SHA256,
    FOCUS_SCHEMA_VERSION,
    FocusClientError,
)


def run(coro):
    return asyncio.run(coro)


def focus_payload() -> bytes:
    now = datetime.now(timezone.utc).isoformat()
    return json.dumps(
        {
            "schema_version": FOCUS_SCHEMA_VERSION,
            "schema_sha256": FOCUS_SCHEMA_SHA256,
            "revision": 4,
            "as_of": now,
            "data_through": now,
            "market_session": "regular",
            "universe_version": "test-universe-4",
            "symbols": [
                {
                    "ticker": "NVDA",
                    "validation_status": "canonical",
                    "universe_reasons": ["dollar_volume_top20"],
                    "dollar_volume_rank": 1,
                    "dollar_volume": 1_500_000_000.0,
                    "dollar_volume_basis": "intraday_completed_bars",
                    "as_of": now,
                    "data_through": now,
                    "data_quality": 0.95,
                    "source_status": "active",
                    "data_source": "test-feed",
                }
            ],
            "major_market_symbols": ["SPY"],
            "warnings": [],
        },
        separators=(",", ":"),
    ).encode()


@pytest.fixture(autouse=True)
def isolated_focus_client(monkeypatch):
    focus_context._reset_focus_circuits()
    monkeypatch.setattr(settings, "option_pro_focus_base_url", "https://option.example")
    monkeypatch.setattr(settings, "option_pro_focus_key_id", "focus-read")
    monkeypatch.setattr(settings, "option_pro_focus_secret", "s" * 48)
    monkeypatch.setattr(settings, "option_pro_focus_verify_tls", True)
    monkeypatch.setattr(settings, "option_pro_focus_ca_bundle", "")
    monkeypatch.setattr(settings, "option_pro_focus_connect_timeout_seconds", 1.0)
    monkeypatch.setattr(settings, "option_pro_focus_read_timeout_seconds", 1.0)
    monkeypatch.setattr(settings, "option_pro_focus_timeout_seconds", 2)
    monkeypatch.setattr(settings, "option_pro_focus_max_response_bytes", 1_048_576)
    monkeypatch.setattr(settings, "option_pro_focus_max_attempts", 3)
    monkeypatch.setattr(settings, "option_pro_focus_retry_backoff_seconds", 0)
    monkeypatch.setattr(settings, "option_pro_focus_circuit_failure_threshold", 3)
    monkeypatch.setattr(settings, "option_pro_focus_circuit_reset_seconds", 60)
    yield
    focus_context._reset_focus_circuits()


def test_focus_settings_require_https_origin_and_tls_verification():
    with pytest.raises(ValidationError, match="HTTPS origin"):
        Settings(_env_file=None, option_pro_focus_base_url="https://option.example/api")
    with pytest.raises(ValidationError, match="VERIFY_TLS"):
        Settings(
            _env_file=None,
            option_pro_focus_base_url="https://option.example",
            option_pro_focus_verify_tls=False,
        )


def test_owned_focus_client_disables_environment_proxy_and_redirects(monkeypatch):
    captured: dict = {}
    sentinel = object()

    def constructor(**kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(focus_context, "_tls_context", lambda: ssl.create_default_context())
    monkeypatch.setattr(focus_context.httpx, "AsyncClient", constructor)
    assert focus_context._create_focus_client() is sentinel
    assert captured["base_url"] == "https://option.example"
    assert captured["trust_env"] is False
    assert captured["follow_redirects"] is False
    assert isinstance(captured["verify"], ssl.SSLContext)


def test_focus_client_rejects_redirect_without_following_it():
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(302, headers={"location": "https://attacker.example"})

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(FocusClientError) as raised:
                await focus_context._fetch_focus_context(client)
        assert raised.value.category == "redirect"

    run(scenario())
    assert seen == [
        "https://option.example/api/integrations/macrolens/v1/focus-context"
    ]


@pytest.mark.parametrize(
    ("status_code", "category"),
    [(401, "auth_401"), (403, "auth_403"), (429, "rate_limited"), (503, "upstream_5xx")],
)
def test_focus_client_classifies_http_failures(monkeypatch, status_code, category):
    monkeypatch.setattr(settings, "option_pro_focus_max_attempts", 1)

    async def scenario():
        transport = httpx.MockTransport(lambda _request: httpx.Response(status_code))
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(FocusClientError) as raised:
                await focus_context._fetch_focus_context(client)
        assert raised.value.category == category

    run(scenario())


def test_focus_client_rejects_non_json_and_oversized_response(monkeypatch):
    async def rejected(response: httpx.Response, category: str):
        transport = httpx.MockTransport(lambda _request: response)
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(FocusClientError) as raised:
                await focus_context._fetch_focus_context(client)
        assert raised.value.category == category

    run(
        rejected(
            httpx.Response(200, headers={"content-type": "text/html"}, content=b"{}"),
            "content_type",
        )
    )
    monkeypatch.setattr(settings, "option_pro_focus_max_response_bytes", 32)
    run(
        rejected(
            httpx.Response(
                200,
                headers={"content-type": "application/json"},
                content=b"x" * 33,
            ),
            "oversized_response",
        )
    )


def test_focus_client_retries_with_a_fresh_nonce_and_redacted_log(caplog):
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(503, content=b"upstream secret should not be logged")
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            content=focus_payload(),
        )

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            context = await focus_context._fetch_focus_context(client)
        assert context.revision == 4

    run(scenario())
    assert len(requests) == 2
    nonces = [request.headers[focus_context.HEADER_NONCE] for request in requests]
    assert len(set(nonces)) == 2
    assert "secret" not in caplog.text
    assert "option.example" not in caplog.text


def test_focus_client_classifies_dns_and_tls_without_exposing_exception_text():
    request = httpx.Request("GET", "https://option.example")
    dns_error = httpx.ConnectError("credential=must-not-leak", request=request)
    dns_error.__cause__ = socket.gaierror("private host detail")
    tls_error = httpx.ConnectError("certificate path must-not-leak", request=request)
    tls_error.__cause__ = ssl.SSLError("private certificate detail")
    assert focus_context._transport_error(dns_error).category == "dns"
    assert focus_context._transport_error(tls_error).category == "tls"
    assert "must-not-leak" not in str(focus_context._transport_error(dns_error))


def test_focus_client_total_timeout_bounds_retries(monkeypatch):
    monkeypatch.setattr(settings, "option_pro_focus_timeout_seconds", 0.02)
    monkeypatch.setattr(settings, "option_pro_focus_retry_backoff_seconds", 0.01)

    async def slow_sleep(_delay: float):
        await asyncio.sleep(1)

    async def scenario():
        transport = httpx.MockTransport(lambda _request: httpx.Response(503))
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(FocusClientError) as raised:
                await focus_context._fetch_focus_context(client, sleeper=slow_sleep)
        assert raised.value.category == "total_timeout"

    run(scenario())


def test_focus_client_circuit_opens_after_bounded_failures(monkeypatch):
    monkeypatch.setattr(settings, "option_pro_focus_circuit_failure_threshold", 2)
    origin = "https://option.example"
    assert focus_context._circuit_permits(origin)
    focus_context._record_circuit_failure(origin)
    assert focus_context._circuit_permits(origin)
    focus_context._record_circuit_failure(origin)
    assert not focus_context._circuit_permits(origin)
