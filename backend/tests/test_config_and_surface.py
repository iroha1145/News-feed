from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

from app.config import ConfigurationError, load_settings


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
        environ={"MACROLENS_INTERNAL_TOKEN": "owner", "MACROLENS_DATA_DIR": str(tmp_path)},
    )
    assert loaded.database_path == tmp_path / "macrolens.db"
    assert loaded.source("google").interval_seconds == 900
    assert loaded.internal_api_token == "owner"
    assert not any(
        hasattr(loaded, name)
        for name in ("model", "reasoning", "llm", "provider", "openai_api_key")
    )


def test_personal_toml_rejects_unknown_settings(tmp_path):
    path = tmp_path / "personal.toml"
    path.write_text(_config_text(extra="unexpected = true"), encoding="utf-8")
    with pytest.raises(ConfigurationError, match="unknown sources.google setting"):
        load_settings(path, environ={})


def test_runtime_configuration_contains_no_model_settings():
    paths = [
        ROOT / "config/personal.toml",
        ROOT / ".env.example",
        ROOT / "docker-compose.yml",
        ROOT / "docker-compose.personal.yml",
        ROOT / "backend/requirements.txt",
    ]
    forbidden = re.compile(r"openai|\bgpt\b|\bllm\b|reasoning|model[_-]|provider[_-]|hmac|focus", re.I)
    for path in paths:
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
    assert _compose_services(ROOT / "docker-compose.yml") == ["macrolens"]
    assert _compose_services(ROOT / "docker-compose.personal.yml") == ["macrolens"]
    dockerfile = (ROOT / "backend/Dockerfile").read_text(encoding="utf-8")
    assert '"--workers"' not in dockerfile
    assert "analysis-worker" not in dockerfile
    assert "frontend" not in dockerfile
