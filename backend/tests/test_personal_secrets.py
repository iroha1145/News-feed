from __future__ import annotations

import io
import json
import multiprocessing
import os
import stat
import subprocess
from pathlib import Path

import pytest

from app.tools import personal_secrets


ROOT = Path(__file__).resolve().parents[2]
EXPECTED_KEYS = {
    "INTERNAL_API_TOKEN",
    "FINNHUB_API_KEY",
    "MASSIVE_API_KEY",
    "NEWSAPI_API_KEY",
    "GNEWS_API_KEY",
    "DATA_DIR",
}


def _set_stdin(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setattr(personal_secrets.sys, "stdin", io.StringIO(value + "\n"))


def _read_serialized(path: Path) -> dict[str, str]:
    with path.open(encoding="utf-8") as stream:
        return personal_secrets._parse_values(stream)


def _process_mutation(
    path_text: str,
    key: str,
    value: str | None,
    start,
    ready,
    result,
) -> None:
    ready.set()
    start.wait()
    try:
        personal_secrets._mutate_secret(Path(path_text), key, value)
    except BaseException as exc:
        result.put((False, type(exc).__name__))
    else:
        result.put((True, key))


def test_macrolens_secret_allowlist_is_exact() -> None:
    assert set(personal_secrets.SECRET_KEYS) == EXPECTED_KEYS
    assert len(personal_secrets.SECRET_KEYS) == 6


def test_invalid_key_is_rejected_before_input_and_never_echoed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "secrets.env"
    invalid_key = "private-value-mistaken-for-key"
    monkeypatch.setattr(personal_secrets, "DEFAULT_SECRETS_PATH", path)

    class UnreadableInput(io.StringIO):
        def readline(self, *_args, **_kwargs):
            pytest.fail("invalid key attempted to read a Secret value")

    monkeypatch.setattr(personal_secrets.sys, "stdin", UnreadableInput())
    assert personal_secrets.main(["set", invalid_key]) == 2
    output = capsys.readouterr()
    assert invalid_key not in output.out + output.err
    assert not path.exists()


def test_set_is_private_atomic_and_never_echoes_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "secrets.env"
    monkeypatch.setattr(personal_secrets, "DEFAULT_SECRETS_PATH", path)
    first = "internal-private-sentinel"
    _set_stdin(monkeypatch, first)

    assert personal_secrets.main(["set", "INTERNAL_API_TOKEN"]) == 0
    output = capsys.readouterr()
    assert first not in output.out + output.err
    assert json.loads(output.out) == {
        "changed": True,
        "configured": True,
        "key": "INTERNAL_API_TOKEN",
    }
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert _read_serialized(path) == {"INTERNAL_API_TOKEN": first}
    assert list(tmp_path.glob("secrets.env.bak.*")) == []

    second = "finnhub-private-sentinel"
    _set_stdin(monkeypatch, second)
    assert personal_secrets.main(["set", "FINNHUB_API_KEY"]) == 0
    output = capsys.readouterr()
    assert second not in output.out + output.err
    backups = list(tmp_path.glob("secrets.env.bak.*"))
    assert len(backups) == 1
    assert stat.S_IMODE(backups[0].stat().st_mode) == 0o600
    assert _read_serialized(backups[0]) == {"INTERNAL_API_TOKEN": first}
    lock = personal_secrets._lock_path(path)
    assert lock.parent == path.parent
    assert stat.S_IMODE(lock.stat().st_mode) == 0o600


def test_noop_set_and_remove_do_not_rewrite_or_back_up(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "secrets.env"
    monkeypatch.setattr(personal_secrets, "DEFAULT_SECRETS_PATH", path)
    personal_secrets.atomic_write({"NEWSAPI_API_KEY": "newsapi-private-value"}, path)
    original_inode = path.stat().st_ino
    _set_stdin(monkeypatch, "newsapi-private-value")

    assert personal_secrets.main(["set", "NEWSAPI_API_KEY"]) == 0
    assert json.loads(capsys.readouterr().out)["changed"] is False
    assert path.stat().st_ino == original_inode
    assert list(tmp_path.glob("secrets.env.bak.*")) == []

    assert personal_secrets.main(["remove", "GNEWS_API_KEY"]) == 0
    assert json.loads(capsys.readouterr().out)["changed"] is False
    assert path.stat().st_ino == original_inode
    assert list(tmp_path.glob("secrets.env.bak.*")) == []


def test_status_contains_only_configured_booleans(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "secrets.env"
    secret = "massive-status-private-sentinel"
    personal_secrets.atomic_write({"MASSIVE_API_KEY": secret}, path)
    monkeypatch.setattr(personal_secrets, "DEFAULT_SECRETS_PATH", path)

    assert personal_secrets.main(["status"]) == 0
    output = capsys.readouterr()
    assert secret not in output.out + output.err
    report = json.loads(output.out)
    assert set(report) == EXPECTED_KEYS
    assert report["MASSIVE_API_KEY"] == {"configured": True}
    assert report["GNEWS_API_KEY"] == {"configured": False}
    assert all(set(item) == {"configured"} for item in report.values())
    assert all(isinstance(item["configured"], bool) for item in report.values())


def test_validate_uses_only_format_and_local_file_checks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "secrets.env"
    values = {
        "INTERNAL_API_TOKEN": "internal-local-validation",
        "FINNHUB_API_KEY": "finnhub-local-validation",
        "MASSIVE_API_KEY": "massive-local-validation",
        "NEWSAPI_API_KEY": "newsapi-local-validation",
        "GNEWS_API_KEY": "gnews-local-validation",
        "DATA_DIR": "/app/data",
    }
    personal_secrets.atomic_write(values, path)
    monkeypatch.setattr(personal_secrets, "DEFAULT_SECRETS_PATH", path)

    assert personal_secrets.main(["validate"]) == 0
    output = capsys.readouterr()
    assert all(value not in output.out + output.err for value in values.values())
    assert "http://" not in output.out
    assert "https://" not in output.out
    report = json.loads(output.out)
    assert report["file"] == {
        "exists": True,
        "regular_file": True,
        "permission_0600": True,
    }
    for item in report["secrets"].values():
        assert item["configured"] is True
        assert item["format_valid"] is True
        assert item["connection_checked"] is False
        assert item["connection_skipped"] is True
        assert item["connection_ok"] is True
        assert item["reason"] == "local_validation_only"


def test_validate_rejects_public_file_without_leaking_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "secrets.env"
    secret = "gnews-public-file-sentinel"
    path.write_text(f"GNEWS_API_KEY={secret}\n", encoding="utf-8")
    path.chmod(0o644)
    monkeypatch.setattr(personal_secrets, "DEFAULT_SECRETS_PATH", path)

    assert personal_secrets.main(["validate"]) == 1
    output = capsys.readouterr()
    assert secret not in output.out + output.err
    report = json.loads(output.out)
    assert report["file"]["permission_0600"] is False
    assert report["secrets"]["GNEWS_API_KEY"]["reason"] == "file_permissions_invalid"


@pytest.mark.parametrize(
    ("key", "unsafe"),
    (
        ("INTERNAL_API_TOKEN", "short"),
        ("FINNHUB_API_KEY", "private-${HOME}-value"),
        ("DATA_DIR", "relative/data"),
    ),
)
def test_set_rejects_unsafe_values_without_echoing_or_writing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    key: str,
    unsafe: str,
) -> None:
    path = tmp_path / "secrets.env"
    monkeypatch.setattr(personal_secrets, "DEFAULT_SECRETS_PATH", path)
    _set_stdin(monkeypatch, unsafe)

    assert personal_secrets.main(["set", key]) == 2
    output = capsys.readouterr()
    assert unsafe not in output.out + output.err
    assert not path.exists()
    assert list(tmp_path.glob("secrets.env.bak.*")) == []


def test_updates_reject_symlinks_and_non_private_existing_files(tmp_path: Path) -> None:
    target = tmp_path / "target.env"
    target.write_text("GNEWS_API_KEY=target-private-value\n", encoding="utf-8")
    target.chmod(0o600)
    linked = tmp_path / "secrets.env"
    linked.symlink_to(target)

    with pytest.raises(OSError):
        personal_secrets.atomic_write(
            {"GNEWS_API_KEY": "replacement-private-value"}, linked
        )
    assert "target-private-value" in target.read_text(encoding="utf-8")
    assert list(tmp_path.glob("secrets.env.bak.*")) == []

    linked.unlink()
    linked.write_text("GNEWS_API_KEY=world-readable-value\n", encoding="utf-8")
    linked.chmod(0o644)
    with pytest.raises(ValueError, match="0600"):
        personal_secrets.atomic_write(
            {"GNEWS_API_KEY": "replacement-private-value"}, linked
        )
    assert stat.S_IMODE(linked.stat().st_mode) == 0o644
    assert list(tmp_path.glob("secrets.env.bak.*")) == []


def test_update_lock_rejects_symlinks_and_unsafe_permissions(tmp_path: Path) -> None:
    symlink_dir = tmp_path / "symlink-lock"
    symlink_dir.mkdir()
    path = symlink_dir / "secrets.env"
    lock = personal_secrets._lock_path(path)
    target = symlink_dir / "lock-target"
    target.write_text("", encoding="utf-8")
    target.chmod(0o600)
    lock.symlink_to(target)
    with pytest.raises(OSError):
        personal_secrets.atomic_write(
            {"INTERNAL_API_TOKEN": "internal-lock-symlink"}, path
        )

    lock.unlink()
    lock.write_text("", encoding="utf-8")
    lock.chmod(0o644)
    with pytest.raises(ValueError, match="0600"):
        personal_secrets.atomic_write(
            {"INTERNAL_API_TOKEN": "internal-lock-permission"}, path
        )


def test_failed_replacement_keeps_complete_original_and_backup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "secrets.env"
    original = {"INTERNAL_API_TOKEN": "internal-original-private"}
    personal_secrets.atomic_write(original, path)
    original_payload = path.read_bytes()

    def fail_replace(_source, _destination):
        raise OSError("simulated replacement failure")

    monkeypatch.setattr(personal_secrets.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated replacement failure"):
        personal_secrets.atomic_write(
            {**original, "FINNHUB_API_KEY": "finnhub-replacement-value"}, path
        )

    assert path.read_bytes() == original_payload
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    backups = list(tmp_path.glob("secrets.env.bak.*"))
    assert len(backups) == 1
    assert backups[0].read_bytes() == original_payload
    assert stat.S_IMODE(backups[0].stat().st_mode) == 0o600
    assert list(tmp_path.glob(".*.tmp")) == []


def test_concurrent_mutations_preserve_independent_updates(tmp_path: Path) -> None:
    path = tmp_path / "secrets.env"
    personal_secrets.atomic_write(
        {
            "GNEWS_API_KEY": "gnews-remove-private",
            "DATA_DIR": "/app/data-before",
        },
        path,
    )
    operations = (
        ("FINNHUB_API_KEY", "finnhub-concurrent-private"),
        ("MASSIVE_API_KEY", "massive-concurrent-private"),
        ("GNEWS_API_KEY", None),
        ("DATA_DIR", "/app/data-after"),
    )
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    result = context.Queue()
    processes = []
    ready_events = []
    for key, value in operations:
        ready = context.Event()
        process = context.Process(
            target=_process_mutation,
            args=(str(path), key, value, start, ready, result),
        )
        process.start()
        processes.append(process)
        ready_events.append(ready)

    assert all(event.wait(timeout=5) for event in ready_events)
    start.set()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    outcomes = [result.get(timeout=2) for _ in operations]
    assert all(outcome[0] is True for outcome in outcomes), outcomes

    values = _read_serialized(path)
    assert values["FINNHUB_API_KEY"] == "finnhub-concurrent-private"
    assert values["MASSIVE_API_KEY"] == "massive-concurrent-private"
    assert values["DATA_DIR"] == "/app/data-after"
    assert "GNEWS_API_KEY" not in values


@pytest.mark.parametrize(("running", "expect_recreate"), ((True, True), (False, False)))
def test_shell_keeps_value_off_arguments_and_only_recreates_running_service(
    tmp_path: Path,
    running: bool,
    expect_recreate: bool,
) -> None:
    fake_python = tmp_path / "fake-python"
    fake_python.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" > \"$ARGS_FILE\"\n"
        "IFS= read -r value\n"
        "[ -n \"$value\" ] && printf 'received\\n' > \"$STDIN_FILE\"\n"
        "printf '{\"changed\": true, \"configured\": true, \"key\": \"FINNHUB_API_KEY\"}\\n'\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o700)
    fake_docker = tmp_path / "docker"
    fake_docker.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" >> \"$DOCKER_LOG\"\n"
        "case \"$*\" in\n"
        "  *' ps --status running -q macrolens')\n"
        "    [ \"${MACROLENS_RUNNING:-0}\" = 1 ] && printf 'container-id\\n'\n"
        "    ;;\n"
        "esac\n"
        "exit 0\n",
        encoding="utf-8",
    )
    fake_docker.chmod(0o700)
    args_file = tmp_path / "args"
    stdin_file = tmp_path / "stdin"
    docker_log = tmp_path / "docker.log"
    secret = "shell-stdin-private-sentinel"
    environment = {
        **os.environ,
        "ARGS_FILE": str(args_file),
        "DOCKER_LOG": str(docker_log),
        "MACROLENS_RUNNING": "1" if running else "0",
        "PATH": f"{tmp_path}:{os.environ.get('PATH', '')}",
        "PYTHON_BIN": str(fake_python),
        "STDIN_FILE": str(stdin_file),
    }

    completed = subprocess.run(
        [str(ROOT / "personal.sh"), "secrets", "set", "FINNHUB_API_KEY"],
        cwd=ROOT,
        env=environment,
        input=secret + "\n",
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0
    assert secret not in completed.stdout + completed.stderr
    assert secret not in args_file.read_text(encoding="utf-8")
    assert stdin_file.read_text(encoding="utf-8") == "received\n"
    docker_commands = docker_log.read_text(encoding="utf-8").splitlines()
    assert any("ps --status running -q macrolens" in line for line in docker_commands)
    recreate_commands = [line for line in docker_commands if "force-recreate" in line]
    assert bool(recreate_commands) is expect_recreate
    if recreate_commands:
        assert recreate_commands == [
            f"compose -f {ROOT / 'docker-compose.personal.yml'} "
            "up -d --no-deps --no-build --force-recreate macrolens"
        ]


def test_shell_rejects_a_secret_value_argument() -> None:
    script = (ROOT / "personal.sh").read_text(encoding="utf-8")
    assert stat.S_IMODE((ROOT / "personal.sh").stat().st_mode) & 0o111
    assert '"$python_bin" -m app.tools.personal_secrets "$command_name" "$key"' in script
    assert "Secret values must be entered through standard input." in script
    assert "umask 077" in script

    completed = subprocess.run(
        [
            str(ROOT / "personal.sh"),
            "secrets",
            "set",
            "FINNHUB_API_KEY",
            "must-not-be-an-argument",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 2
    assert "standard input" in completed.stderr
    assert "must-not-be-an-argument" not in completed.stdout + completed.stderr
