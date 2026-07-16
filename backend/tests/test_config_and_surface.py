from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

from app.config import ConfigurationError, _resolve_project_root, load_settings


ROOT = Path(__file__).resolve().parents[2]


def _config_text(*, extra: str = "") -> str:
    return f"""
[storage]
database_path = "data/test.db"
news_retention_days = 30
change_retention_days = 60
calendar_snapshot_retention_days = 30
retention_interval_seconds = 600

[calendar]
interval_seconds = 600

[sources.google]
enabled = true
interval_seconds = 900
{extra}
"""


def test_personal_toml_is_loaded_by_standard_library(tmp_path):
    path = tmp_path / "personal.toml"
    path.write_text(_config_text(), encoding="utf-8")
    loaded = load_settings(
        path,
        environ={"INTERNAL_API_TOKEN": "internal", "DATA_DIR": str(tmp_path)},
    )
    assert loaded.database_path == tmp_path / "macrolens.db"
    assert loaded.source("google").interval_seconds == 900
    assert loaded.internal_api_token == "internal"
    assert not any(
        hasattr(loaded, name)
        for name in ("model", "reasoning", "llm", "provider", "openai_api_key")
    )


def test_project_root_supports_packaged_container_layout(tmp_path):
    module_file = tmp_path / "runtime" / "app" / "config.py"
    config_file = tmp_path / "runtime" / "config" / "personal.toml"
    module_file.parent.mkdir(parents=True)
    config_file.parent.mkdir(parents=True)
    module_file.touch()
    config_file.touch()

    assert _resolve_project_root(module_file) == tmp_path / "runtime"


def test_personal_toml_rejects_unknown_settings(tmp_path):
    path = tmp_path / "personal.toml"
    path.write_text(_config_text(extra="unexpected = true"), encoding="utf-8")
    with pytest.raises(ConfigurationError, match="unknown sources.google setting"):
        load_settings(path, environ={})


def test_runtime_configuration_contains_no_model_settings():
    paths = [
        ROOT / "config/personal.toml",
        ROOT / ".env.example",
        ROOT / "secrets.env.example",
        ROOT / "docker-compose.yml",
        ROOT / "docker-compose.personal.yml",
        ROOT / "backend/requirements.txt",
    ]
    forbidden = re.compile(r"openai|\bgpt\b|\bllm\b|reasoning|model[_-]|provider[_-]|hmac|focus", re.I)
    for path in paths:
        assert forbidden.search(path.read_text(encoding="utf-8")) is None, path


def test_environment_examples_keep_machine_and_secret_inputs_separate():
    machine_declarations = {
        match.group(1)
        for match in re.finditer(
            r"^([A-Z][A-Z0-9_]*)=", (ROOT / ".env.example").read_text(encoding="utf-8"), re.M
        )
    }
    secret_declarations = {
        match.group(1)
        for match in re.finditer(
            r"^([A-Z][A-Z0-9_]*)=",
            (ROOT / "secrets.env.example").read_text(encoding="utf-8"),
            re.M,
        )
    }
    assert machine_declarations == {"HOST_BIND", "PORT"}
    assert secret_declarations == {
        "INTERNAL_API_TOKEN",
        "FINNHUB_API_KEY",
        "MASSIVE_API_KEY",
        "NEWSAPI_API_KEY",
        "GNEWS_API_KEY",
        "DATA_DIR",
    }
    ignored = set((ROOT / ".gitignore").read_text(encoding="utf-8").splitlines())
    assert {"secrets.env", "secrets.env.bak.*", ".secrets.env.lock", ".venv/"} <= ignored


def test_no_browser_or_action_surface_is_packaged():
    frontend = ROOT / "frontend"
    assert not frontend.exists() or not any(path.is_file() for path in frontend.rglob("*"))
    runtime_files = sorted((ROOT / "backend/app").rglob("*.py"))
    forbidden = re.compile(
        r"sessionStorage|localStorage|action[_-]?(?:api|key)|capabilit|"
        r"require_(?:owner|admin|expensive)|openai|\bllm\b|focus[_-]?pull",
        re.I,
    )
    for path in runtime_files:
        assert forbidden.search(path.read_text(encoding="utf-8")) is None, path


def test_personal_configuration_stays_small_and_has_no_dead_service_knobs():
    with (ROOT / "config/personal.toml").open("rb") as handle:
        raw = tomllib.load(handle)

    def leaf_count(value):
        if isinstance(value, dict):
            return sum(leaf_count(child) for child in value.values())
        return 1

    assert "service" not in raw
    assert "retention_batch_size" not in raw["storage"]
    assert set(raw["calendar"]) == {"interval_seconds"}
    assert leaf_count(raw) < 40


def _compose_services(path: Path) -> list[str]:
    services: list[str] = []
    in_services = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line == "services:":
            in_services = True
            continue
        if in_services and line and not line.startswith(" "):
            break
        match = re.fullmatch(r"  ([a-zA-Z0-9_-]+):", line)
        if in_services and match:
            services.append(match.group(1))
    return services


def test_both_compose_files_have_one_long_running_service():
    secret_keys = {
        "INTERNAL_API_TOKEN",
        "FINNHUB_API_KEY",
        "MASSIVE_API_KEY",
        "NEWSAPI_API_KEY",
        "GNEWS_API_KEY",
        "DATA_DIR",
    }
    for name in ("docker-compose.yml", "docker-compose.personal.yml"):
        path = ROOT / name
        assert _compose_services(path) == ["macrolens"]
        text = path.read_text(encoding="utf-8")
        assert '"${HOST_BIND:-127.0.0.1}:${PORT:-8000}:8000"' in text
        assert "- path: secrets.env" in text
        assert "- path: .env" not in text
        assert "\n    environment:" not in text
        for key in secret_keys:
            assert f"${{{key}" not in text
    dockerfile = (ROOT / "backend/Dockerfile").read_text(encoding="utf-8")
    assert '"--workers"' not in dockerfile
    assert "analysis-worker" not in dockerfile
    assert "frontend" not in dockerfile

    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    token_set = './personal.sh secrets set INTERNAL_API_TOKEN'
    data_dir_set = './personal.sh secrets set DATA_DIR'
    container_start = 'docker compose up -d --build --wait --wait-timeout 180 macrolens'
    assert 'printf \'%s\\n\' "$INTERNAL_API_TOKEN" | ' + token_set in workflow
    assert 'printf \'%s\\n\' "$DATA_DIR" | ' + data_dir_set in workflow
    assert './personal.sh secrets validate' in workflow
    assert workflow.index(token_set) < workflow.index(container_start)
    assert workflow.index(data_dir_set) < workflow.index(container_start)
