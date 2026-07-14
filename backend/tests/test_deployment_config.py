from __future__ import annotations

from pathlib import Path
import logging

from app.utils.http import SENSITIVE_NETWORK_LOGGERS, configure_safe_network_logging


ROOT = Path(__file__).resolve().parents[2]


def test_uvicorn_disables_untrusted_proxy_and_query_access_logs():
    dockerfile = (ROOT / "backend" / "Dockerfile").read_text(encoding="utf-8")
    assert '"--no-proxy-headers"' in dockerfile
    assert '"--no-access-log"' in dockerfile


def test_frontend_never_proxies_or_logs_the_signed_integration_api():
    nginx = (ROOT / "frontend" / "nginx.conf").read_text(encoding="utf-8")
    assert "access_log off;" in nginx
    assert "location ^~ /api/integrations/option-pro/" in nginx
    assert "return 404;" in nginx


def test_compose_exposes_frontend_and_backend_on_loopback_only():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert '"127.0.0.1:8000:8000"' in compose
    assert '"127.0.0.1:3000:8080"' in compose


def test_backend_and_worker_share_model_database_and_retention_environment():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert compose.count("<<: *analysis-runtime-environment") == 2
    for key in (
        "DATABASE_URL",
        "DEFAULT_LLM_MODEL",
        "OPENAI_REASONING",
        "OPENAI_EXECUTION_MODE",
        "NEWS_IMPACT_PROMPT_VERSION",
        "CALENDAR_ANALYSIS_SCHEMA_VERSION",
        "CALENDAR_LLM_MANUAL_ENABLED",
        "X_SENTIMENT_ENABLED",
        "ANALYSIS_RETENTION_LIMIT",
        "NEWS_RETENTION_DAYS",
        "X_SENTIMENT_RETENTION_DAYS",
        "HOT_CYCLE_MAX_EVENTS",
        "HOT_CYCLE_MAX_FOCUS_SYMBOLS",
        "HOTSPOT_PREPARATION_RETENTION_DAYS",
        "MARKET_FOCUS_COMPLETED_RETENTION_DAYS",
        "MARKET_FOCUS_FAILED_RETENTION_DAYS",
        "FOCUS_SNAPSHOT_RETENTION_DAYS",
        "FOCUS_SNAPSHOT_FULL_RESOLUTION_DAYS",
        "FOCUS_SNAPSHOT_DAILY_ROLLUP_ENABLED",
        "CATALYST_CONTEXT_SUPPORT_TARGET",
        "EVENT_MEMBER_RETENTION_DAYS",
        "PROJECTION_RETRY_RETENTION_DAYS",
        "PROJECTION_RETRY_MAX_ATTEMPTS",
        "ANALYSIS_WORKER_QUICK_CHECK_INTERVAL_SECONDS",
    ):
        assert f"  {key}: ${{{key}:-" in compose


def test_data_init_can_traverse_legacy_and_migrated_data_directories():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    data_init = compose.split("  data-init:", 1)[1].split("\n  backend:", 1)[0]
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )

    assert 'user: "0:10001"' in data_init
    assert "cap_drop:\n      - ALL" in data_init
    assert "cap_add:\n      - CHOWN" in data_init
    assert "scripts/verify-data-init.sh" in workflow

    verifier = (ROOT / "scripts" / "verify-data-init.sh").read_text(
        encoding="utf-8"
    )
    assert 'sudo chown "$owner_uid:$owner_gid" "${files[@]}"' in verifier
    assert "sudo stat -c '%u:%g'" in verifier
    assert '"$data_dir"/*' not in verifier


def test_worker_healthcheck_caches_only_the_expensive_integrity_probe():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    worker = compose.split("  analysis-worker:", 1)[1].split("\n  frontend:", 1)[0]
    healthcheck = (ROOT / "backend" / "app" / "worker_healthcheck.py").read_text(
        encoding="utf-8"
    )

    assert "ANALYSIS_WORKER_QUICK_CHECK_INTERVAL_SECONDS:-1800" in compose
    assert 'test: ["CMD", "python", "-m", "app.worker_healthcheck"]' in worker
    assert "timeout: 120s" in worker
    assert "PRAGMA quick_check" in healthcheck
    assert 'Path("/tmp/macrolens-analysis-worker-quick-check.ok")' in healthcheck


def test_ci_starts_and_smokes_all_runtime_services_without_external_sources():
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    assert "--wait --wait-timeout 180 backend analysis-worker frontend" in workflow
    assert "CALENDAR_FETCH_INTERVAL_SECONDS=86400" in workflow
    assert "http://127.0.0.1:3000/api/news" in workflow
    assert "/api/integrations/option-pro/v1/health?probe=redacted" in workflow
    assert "docker compose down --remove-orphans --volumes" in workflow


def test_network_client_loggers_cannot_emit_provider_request_identifiers():
    previous = {
        name: logging.getLogger(name).level for name in SENSITIVE_NETWORK_LOGGERS
    }
    try:
        for name in SENSITIVE_NETWORK_LOGGERS:
            logging.getLogger(name).setLevel(logging.INFO)
        configure_safe_network_logging()
        assert all(
            logging.getLogger(name).getEffectiveLevel() >= logging.WARNING
            for name in SENSITIVE_NETWORK_LOGGERS
        )
    finally:
        for name, level in previous.items():
            logging.getLogger(name).setLevel(level)
