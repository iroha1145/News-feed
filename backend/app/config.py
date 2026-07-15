from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "personal.toml"
INTERNAL_TOKEN_ENV = "MACROLENS_INTERNAL_TOKEN"


class ConfigurationError(ValueError):
    """Raised when the personal service configuration is invalid."""


@dataclass(frozen=True)
class SourceConfig:
    enabled: bool
    interval_seconds: int
    api_key_env: str | None = None


@dataclass(frozen=True)
class StorageConfig:
    database_path: Path
    news_retention_days: int
    change_retention_days: int
    calendar_snapshot_retention_days: int
    retention_interval_seconds: int


@dataclass(frozen=True)
class CalendarConfig:
    interval_seconds: int


@dataclass(frozen=True)
class PersonalSettings:
    config_path: Path
    storage: StorageConfig
    calendar: CalendarConfig
    sources: Mapping[str, SourceConfig]
    environment: Mapping[str, str]

    @property
    def database_path(self) -> Path:
        override = self.environment.get("MACROLENS_DATABASE_PATH", "").strip()
        if override:
            return Path(override).expanduser().resolve()
        data_dir = self.environment.get("MACROLENS_DATA_DIR", "").strip()
        if data_dir:
            return (Path(data_dir).expanduser() / "macrolens.db").resolve()
        return self.storage.database_path

    @property
    def calendar_cache_path(self) -> Path:
        return self.database_path.parent / "calendar_cache.json"

    @property
    def internal_api_token(self) -> str:
        return self.environment.get(INTERNAL_TOKEN_ENV, "").strip()

    def source(self, name: str) -> SourceConfig:
        try:
            return self.sources[name]
        except KeyError as exc:
            raise ConfigurationError(f"unknown source: {name}") from exc

    def api_key(self, name: str) -> str:
        source = self.source(name)
        if not source.api_key_env:
            return ""
        return self.environment.get(source.api_key_env, "").strip()


def _mapping(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{path} must be a TOML table")
    return value


def _known_keys(table: Mapping[str, Any], allowed: set[str], path: str) -> None:
    unknown = sorted(set(table) - allowed)
    if unknown:
        raise ConfigurationError(f"unknown {path} setting: {unknown[0]}")


def _boolean(table: Mapping[str, Any], key: str, path: str) -> bool:
    value = table.get(key)
    if not isinstance(value, bool):
        raise ConfigurationError(f"{path}.{key} must be true or false")
    return value


def _integer(
    table: Mapping[str, Any],
    key: str,
    path: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    value = table.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigurationError(f"{path}.{key} must be an integer")
    if not minimum <= value <= maximum:
        raise ConfigurationError(
            f"{path}.{key} must be between {minimum} and {maximum}"
        )
    return value


def _text(table: Mapping[str, Any], key: str, path: str) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"{path}.{key} must be non-empty text")
    return value.strip()


def load_settings(
    path: str | Path | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> PersonalSettings:
    environment = dict(os.environ if environ is None else environ)
    configured_path = path or environment.get("MACROLENS_CONFIG_PATH") or DEFAULT_CONFIG_PATH
    config_path = Path(configured_path).expanduser().resolve()
    try:
        with config_path.open("rb") as handle:
            raw = tomllib.load(handle)
    except FileNotFoundError as exc:
        raise ConfigurationError(f"configuration file not found: {config_path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigurationError(f"invalid TOML configuration: {exc}") from exc

    _known_keys(raw, {"storage", "calendar", "sources"}, "root")
    storage = _mapping(raw.get("storage"), "storage")
    calendar = _mapping(raw.get("calendar"), "calendar")
    source_tables = _mapping(raw.get("sources"), "sources")

    _known_keys(
        storage,
        {
            "database_path",
            "news_retention_days",
            "change_retention_days",
            "calendar_snapshot_retention_days",
            "retention_interval_seconds",
        },
        "storage",
    )
    _known_keys(
        calendar,
        {"interval_seconds"},
        "calendar",
    )

    database_value = Path(_text(storage, "database_path", "storage")).expanduser()
    if not database_value.is_absolute():
        database_value = (PROJECT_ROOT / database_value).resolve()

    sources: dict[str, SourceConfig] = {}
    for name, raw_source in source_tables.items():
        if not isinstance(name, str) or not name.replace("_", "").isalnum():
            raise ConfigurationError("source names may contain letters, numbers, and underscores")
        source = _mapping(raw_source, f"sources.{name}")
        _known_keys(source, {"enabled", "interval_seconds", "api_key_env"}, f"sources.{name}")
        api_key_env = source.get("api_key_env")
        if api_key_env is not None:
            if not isinstance(api_key_env, str) or not api_key_env.strip():
                raise ConfigurationError(f"sources.{name}.api_key_env must be non-empty text")
            api_key_env = api_key_env.strip()
        sources[name] = SourceConfig(
            enabled=_boolean(source, "enabled", f"sources.{name}"),
            interval_seconds=_integer(
                source,
                "interval_seconds",
                f"sources.{name}",
                minimum=30,
                maximum=86_400,
            ),
            api_key_env=api_key_env,
        )
    if not sources:
        raise ConfigurationError("at least one source must be configured")

    return PersonalSettings(
        config_path=config_path,
        storage=StorageConfig(
            database_path=database_value,
            news_retention_days=_integer(
                storage, "news_retention_days", "storage", minimum=0, maximum=3_650
            ),
            change_retention_days=_integer(
                storage, "change_retention_days", "storage", minimum=1, maximum=3_650
            ),
            calendar_snapshot_retention_days=_integer(
                storage,
                "calendar_snapshot_retention_days",
                "storage",
                minimum=1,
                maximum=3_650,
            ),
            retention_interval_seconds=_integer(
                storage,
                "retention_interval_seconds",
                "storage",
                minimum=60,
                maximum=86_400,
            ),
        ),
        calendar=CalendarConfig(
            interval_seconds=_integer(
                calendar, "interval_seconds", "calendar", minimum=60, maximum=86_400
            ),
        ),
        sources=sources,
        environment=environment,
    )


settings = load_settings()
