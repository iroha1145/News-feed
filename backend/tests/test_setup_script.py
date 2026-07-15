from __future__ import annotations

import os
import re
import stat
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _dotenv_line(key: str, value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"{key}='{escaped}'"


def _fake_docker(tmp_path: Path) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    docker = bin_dir / "docker"
    docker.write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = info ]; then exit 0; fi\n"
        "if [ \"$1\" = compose ] && [ \"$2\" = version ]; then exit 0; fi\n"
        "exit 97\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    return bin_dir


def _run_setup(tmp_path: Path, env_file: Path, answers: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["ENV_FILE"] = str(env_file)
    env["PATH"] = f"{_fake_docker(tmp_path)}:{env['PATH']}"
    return subprocess.run(
        ["bash", str(ROOT / "setup.sh")],
        cwd=ROOT,
        env=env,
        input=answers,
        text=True,
        capture_output=True,
        timeout=20,
        check=False,
    )


def _keys(path: Path) -> set[str]:
    return {
        match.group(1)
        for line in path.read_text(encoding="utf-8").splitlines()
        if (match := re.match(r"^([A-Za-z_][A-Za-z0-9_]*)=", line))
    }


def test_setup_defaults_to_terra_without_printing_secret(tmp_path: Path):
    env_file = tmp_path / "fresh.env"
    secret = "sk-test-$fresh\\quote'kept"
    answers = "\n\n\n\n\n" + secret + "\n\nn\n"

    result = _run_setup(tmp_path, env_file, answers)

    assert result.returncode == 0, result.stderr
    assert secret not in result.stdout
    assert secret not in result.stderr
    generated = env_file.read_text(encoding="utf-8")
    assert "DEFAULT_LLM_PROVIDER='openai'" in generated
    assert "DEFAULT_LLM_MODEL='gpt-5.6-terra'" in generated
    assert _dotenv_line("OPENAI_API_KEY", secret) in generated
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600


def test_setup_allows_blank_model_key_when_paid_capabilities_are_disabled(tmp_path: Path):
    env_file = tmp_path / "readonly.env"
    answers = "\n\n\n\n\n\n\nn\n"

    result = _run_setup(tmp_path, env_file, answers)

    assert result.returncode == 0, result.stderr
    generated = env_file.read_text(encoding="utf-8")
    assert "OPENAI_API_KEY=''" in generated
    assert "NEWS_LLM_MANUAL_ENABLED='false'" in generated
    assert "CALENDAR_LLM_MANUAL_ENABLED='false'" in generated
    assert "HOT_CYCLE_ENABLED='false'" in generated
    assert "X_SENTIMENT_ENABLED='false'" in generated
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600


def test_setup_is_idempotent_preserves_special_values_and_future_keys(tmp_path: Path):
    env_file = tmp_path / "existing.env"
    secret = "sk-test-$value\\slash'quote"
    preserved = {
        "DEFAULT_LLM_PROVIDER": "openai",
        "DEFAULT_LLM_MODEL": "gpt-5.6-terra",
        "DEFAULT_LLM_API_KEY": secret,
        "OPENAI_API_KEY": secret,
        "OPENAI_REASONING": "xhigh",
        "OPENAI_EXECUTION_MODE": "worker_sync",
        "OPENAI_MAX_OUTPUT_TOKENS": "7777",
        "NEWS_LLM_MAX_QUEUED": "37",
        "NEWS_LLM_DAILY_JOB_LIMIT": "19",
        "NEWS_LLM_DAILY_OUTPUT_TOKEN_LIMIT": "88000",
        "HOT_CYCLE_MAX_EVENTS": "3",
        "HOT_CYCLE_MAX_FOCUS_SYMBOLS": "5",
        "MARKET_FOCUS_LEGACY_RECOVERY_AUTHORIZATIONS": (
            '[{"cycle_id":"mfc_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
            '"input_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",'
            '"created_at":"2026-07-15T08:00:00+00:00",'
            '"prompt_cache_key_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",'
            '"provider_base_url":"https://api.openai.com/v1","http_status":400,'
            '"error_type":"string_above_max_length","error_param":"prompt_cache_key",'
            '"authorized_at":"2026-07-15T09:00:00+00:00",'
            '"evidence_reference":"incident-20260715-openai-400"}]'
        ),
        "CALENDAR_LLM_MAX_QUEUED": "4",
        "CALENDAR_LLM_DAILY_JOB_LIMIT": "3",
        "CALENDAR_LLM_DAILY_OUTPUT_TOKEN_LIMIT": "44000",
        "OPTION_PRO_READ_SECRET": "r" * 32 + "$\\'",
        "OPTION_PRO_ACTION_SECRET": "a" * 32 + "$\\'",
        "OPTION_PRO_TRUSTED_PROXY_CIDRS": "127.0.0.1/32",
        "OPTION_PRO_ALLOW_LOCAL_HTTP": "false",
    }
    template_values: dict[str, str] = {}
    for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line)
        if match:
            template_values[match.group(1)] = match.group(2)
    template_values.update(preserved)
    env_file.write_text(
        "\n".join(_dotenv_line(key, value) for key, value in template_values.items())
        + "\nFUTURE_DEPLOYMENT_SECRET='future-$literal'\n",
        encoding="utf-8",
    )

    answers = "\n\n\n\n\n\n\nn\n"
    first = _run_setup(tmp_path, env_file, answers)
    second = _run_setup(tmp_path, env_file, answers)

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    combined_output = first.stdout + first.stderr + second.stdout + second.stderr
    for value in preserved.values():
        if len(value) >= 20:
            assert value not in combined_output
    generated = env_file.read_text(encoding="utf-8")
    for key, value in preserved.items():
        assert _dotenv_line(key, value) in generated
    assert "FUTURE_DEPLOYMENT_SECRET='future-$literal'" in generated
    assert _keys(ROOT / ".env.example") <= _keys(env_file)
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600


def test_setup_provider_switch_uses_new_provider_default_model(tmp_path: Path):
    env_file = tmp_path / "switch.env"
    env_file.write_text(
        "DEFAULT_LLM_PROVIDER='anthropic'\n"
        "DEFAULT_LLM_MODEL='claude-sonnet-4-6'\n"
        "ANTHROPIC_API_KEY='anthropic-existing-secret'\n",
        encoding="utf-8",
    )
    openai_secret = "openai-new-$secret"
    # Four source prompts, provider, OpenAI key, model, and start choice.
    answers = "\n\n\n\nopenai\n" + openai_secret + "\n\nn\n"

    result = _run_setup(tmp_path, env_file, answers)

    assert result.returncode == 0, result.stderr
    generated = env_file.read_text(encoding="utf-8")
    assert "DEFAULT_LLM_PROVIDER='openai'" in generated
    assert "DEFAULT_LLM_MODEL='gpt-5.6-terra'" in generated
    assert "DEFAULT_LLM_MODEL='claude-sonnet-4-6'" not in generated
    assert _dotenv_line("OPENAI_API_KEY", openai_secret) in generated
    assert openai_secret not in result.stdout + result.stderr
