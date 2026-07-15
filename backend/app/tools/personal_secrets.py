from __future__ import annotations

import argparse
import fcntl
import getpass
import json
import os
import re
import stat
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, TextIO


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SECRETS_PATH = REPOSITORY_ROOT / "secrets.env"
SECRET_KEYS = (
    "INTERNAL_API_TOKEN",
    "FINNHUB_API_KEY",
    "MASSIVE_API_KEY",
    "NEWSAPI_API_KEY",
    "GNEWS_API_KEY",
    "DATA_DIR",
)

_ENV_KEY = re.compile(r"^[A-Z][A-Z0-9_]*$")
_SAFE_TOKEN = re.compile(r"^[!-~]+$")
_UNSAFE_ENV_CHARACTERS = frozenset("#'\"\\$")


def secrets_path() -> Path:
    return DEFAULT_SECRETS_PATH


def _parse_values(stream: TextIO) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in stream:
        line = raw_line.rstrip("\r\n")
        if not line or line.lstrip().startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not _ENV_KEY.fullmatch(key):
            raise ValueError("secrets.env contains an invalid declaration")
        if key not in SECRET_KEYS:
            raise ValueError("secrets.env contains an unsupported key")
        if key in values:
            raise ValueError("secrets.env contains a duplicate key")
        if value:
            values[key] = value
    return values


def _open_regular_file(path: Path, *, require_private: bool) -> int:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("secrets.env is not a regular file")
        if require_private and stat.S_IMODE(metadata.st_mode) != 0o600:
            raise ValueError("secrets.env permissions must be 0600 before update")
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _read_values(path: Path, *, require_private: bool = False) -> dict[str, str]:
    try:
        descriptor = _open_regular_file(path, require_private=require_private)
    except FileNotFoundError:
        return {}
    with os.fdopen(descriptor, "r", encoding="utf-8", closefd=True) as stream:
        return _parse_values(stream)


def _format_valid(key: str, value: str) -> bool:
    if key not in SECRET_KEYS or not value:
        return False
    if key == "DATA_DIR":
        return bool(
            len(value) <= 4096
            and Path(value).is_absolute()
            and all(ord(character) >= 32 and ord(character) != 127 for character in value)
            and not any(character in value for character in _UNSAFE_ENV_CHARACTERS)
        )
    return bool(
        8 <= len(value) <= 8192
        and _SAFE_TOKEN.fullmatch(value)
        and not any(character in value for character in _UNSAFE_ENV_CHARACTERS)
    )


def _normalized_value(key: str, value: str) -> str:
    if key not in SECRET_KEYS:
        raise ValueError("unsupported Secret key")
    if not _format_valid(key, value):
        if key == "DATA_DIR":
            raise ValueError("DATA_DIR must be a safe absolute directory path")
        raise ValueError("Secret value has an unsupported format")
    return value


def _validated_values(values: dict[str, str]) -> dict[str, str]:
    unknown = set(values) - set(SECRET_KEYS)
    if unknown:
        raise ValueError("unsupported Secret key")
    normalized: dict[str, str] = {}
    for key in SECRET_KEYS:
        value = values.get(key, "")
        if value:
            normalized[key] = _normalized_value(key, value)
    return normalized


def _serialize(values: dict[str, str]) -> bytes:
    safe_values = _validated_values(values)
    return "".join(
        f"{key}={safe_values[key]}\n" for key in SECRET_KEYS if key in safe_values
    ).encode("utf-8")


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_real_directory(path: Path) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.parent.is_symlink() or not path.parent.is_dir():
        raise ValueError("secrets.env directory must be a real directory")


def _lock_path(path: Path) -> Path:
    return path.with_name(f".{path.name}.lock")


@contextmanager
def _exclusive_secret_lock(path: Path) -> Iterator[None]:
    """Lock the complete read-modify-write transaction in the same directory."""

    _ensure_real_directory(path)
    lock_path = _lock_path(path)
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(
            lock_path,
            os.O_RDWR | os.O_CREAT | os.O_EXCL | no_follow,
            0o600,
        )
        try:
            os.fchmod(descriptor, 0o600)
        except BaseException:
            os.close(descriptor)
            raise
    except FileExistsError:
        descriptor = os.open(lock_path, os.O_RDWR | no_follow)

    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("Secret update lock is not a regular file")
        if stat.S_IMODE(metadata.st_mode) != 0o600:
            raise ValueError("Secret update lock permissions must be 0600")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        locked_metadata = os.fstat(descriptor)
        if not stat.S_ISREG(locked_metadata.st_mode):
            raise ValueError("Secret update lock is not a regular file")
        if stat.S_IMODE(locked_metadata.st_mode) != 0o600:
            raise ValueError("Secret update lock permissions must be 0600")
        visible = os.stat(lock_path, follow_symlinks=False)
        if (
            not stat.S_ISREG(visible.st_mode)
            or visible.st_dev != locked_metadata.st_dev
            or visible.st_ino != locked_metadata.st_ino
        ):
            raise ValueError("Secret update lock changed while being acquired")
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _private_temporary_file(path: Path, *, purpose: str) -> tuple[int, Path]:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.{purpose}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
    except BaseException:
        os.close(descriptor)
        temporary.unlink(missing_ok=True)
        raise
    return descriptor, temporary


def _atomic_private_backup(path: Path) -> Path:
    source_descriptor = _open_regular_file(path, require_private=True)
    backup_descriptor, temporary = _private_temporary_file(path, purpose="backup")
    try:
        with os.fdopen(source_descriptor, "rb", closefd=True) as source:
            with os.fdopen(backup_descriptor, "wb", closefd=True) as target:
                while chunk := source.read(64 * 1024):
                    target.write(chunk)
                target.flush()
                os.fsync(target.fileno())

        backup: Path | None = None
        for _attempt in range(64):
            candidate = path.with_name(f"{path.name}.bak.{time.time_ns()}")
            try:
                os.link(temporary, candidate, follow_symlinks=False)
            except FileExistsError:
                continue
            backup = candidate
            break
        if backup is None:
            raise FileExistsError("could not allocate a unique Secret backup")
        temporary.unlink()
        _sync_directory(path.parent)
        return backup
    except BaseException:
        for descriptor in (source_descriptor, backup_descriptor):
            try:
                os.close(descriptor)
            except OSError:
                pass
        temporary.unlink(missing_ok=True)
        raise


def _atomic_write_locked(values: dict[str, str], path: Path) -> Path | None:
    payload = _serialize(values)
    backup: Path | None = None
    try:
        os.stat(path, follow_symlinks=False)
    except FileNotFoundError:
        pass
    else:
        backup = _atomic_private_backup(path)

    descriptor, temporary = _private_temporary_file(path, purpose="write")
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _sync_directory(path.parent)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        temporary.unlink(missing_ok=True)
        raise
    return backup


def atomic_write(values: dict[str, str], path: Path) -> Path | None:
    with _exclusive_secret_lock(path):
        return _atomic_write_locked(values, path)


def _mutate_secret(path: Path, key: str, value: str | None) -> tuple[Path | None, bool]:
    if key not in SECRET_KEYS:
        raise ValueError("unsupported Secret key")
    normalized = None if value is None else _normalized_value(key, value)
    with _exclusive_secret_lock(path):
        values = _read_values(path, require_private=True)
        if normalized is None:
            if key not in values:
                return None, False
            values.pop(key)
        else:
            if values.get(key) == normalized:
                return None, False
            values[key] = normalized
        return _atomic_write_locked(values, path), True


def _read_secret() -> str:
    if sys.stdin.isatty():
        value = getpass.getpass("Secret value: ")
    else:
        value = sys.stdin.readline().rstrip("\r\n")
    if not value:
        raise ValueError("Secret value cannot be empty")
    return value


def status_report(path: Path) -> dict[str, dict[str, bool]]:
    values = _read_values(path)
    return {key: {"configured": bool(values.get(key))} for key in SECRET_KEYS}


def _file_report(path: Path) -> dict[str, bool]:
    try:
        metadata = os.stat(path, follow_symlinks=False)
    except FileNotFoundError:
        return {"exists": False, "regular_file": True, "permission_0600": True}
    regular_file = stat.S_ISREG(metadata.st_mode)
    return {
        "exists": True,
        "regular_file": regular_file,
        "permission_0600": regular_file and stat.S_IMODE(metadata.st_mode) == 0o600,
    }


def _local_validation_state(*, ok: bool, reason: str) -> dict[str, bool | str]:
    return {
        "connection_checked": False,
        "connection_skipped": True,
        "connection_ok": ok,
        "reason": reason,
    }


def validate_report(path: Path) -> dict[str, object]:
    file_report = _file_report(path)
    values = _read_values(path) if file_report["regular_file"] else {}
    secret_report: dict[str, dict[str, bool | str]] = {}
    for key in SECRET_KEYS:
        value = values.get(key, "")
        configured = bool(value)
        format_valid = configured and _format_valid(key, value)
        item: dict[str, bool | str] = {
            "configured": configured,
            "format_valid": format_valid,
        }
        if not configured:
            item.update(_local_validation_state(ok=False, reason="not_configured"))
        elif not file_report["permission_0600"]:
            item.update(
                _local_validation_state(ok=False, reason="file_permissions_invalid")
            )
        elif not format_valid:
            item.update(_local_validation_state(ok=False, reason="format_invalid"))
        else:
            item.update(_local_validation_state(ok=True, reason="local_validation_only"))
        secret_report[key] = item
    return {"file": file_report, "secrets": secret_report}


def _validation_succeeded(report: dict[str, object]) -> bool:
    file_report = report.get("file")
    secrets = report.get("secrets")
    if not isinstance(file_report, dict) or not (
        file_report.get("regular_file") and file_report.get("permission_0600")
    ):
        return False
    if not isinstance(secrets, dict):
        return False
    return all(
        isinstance(item, dict)
        and (not item.get("configured") or bool(item.get("format_valid")))
        for item in secrets.values()
    )


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(2, "personal_secrets: invalid arguments\n")


def _parser() -> argparse.ArgumentParser:
    parser = _SafeArgumentParser(description="Manage server-only MacroLens Secrets.")
    commands = parser.add_subparsers(
        dest="command",
        required=True,
        parser_class=_SafeArgumentParser,
    )
    commands.add_parser("status")
    set_parser = commands.add_parser("set")
    set_parser.add_argument("key")
    remove_parser = commands.add_parser("remove")
    remove_parser.add_argument("key")
    commands.add_parser("validate")
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    path = secrets_path()
    try:
        if arguments.command == "status":
            print(json.dumps(status_report(path), sort_keys=True))
            return 0
        if arguments.command == "validate":
            report = validate_report(path)
            print(json.dumps(report, sort_keys=True))
            return 0 if _validation_succeeded(report) else 1

        key = arguments.key
        if key not in SECRET_KEYS:
            raise ValueError("unsupported Secret key")
        value = _read_secret() if arguments.command == "set" else None
        _backup, changed = _mutate_secret(path, key, value)
        print(
            json.dumps(
                {
                    "changed": changed,
                    "configured": arguments.command == "set",
                    "key": key,
                },
                sort_keys=True,
            )
        )
        return 0
    except (EOFError, KeyboardInterrupt):
        print("Secret update cancelled.", file=sys.stderr)
        return 2
    except (OSError, UnicodeError, ValueError) as exc:
        print(f"Secret update failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
