#!/usr/bin/env bash
set -euo pipefail
umask 077

root="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
python_bin="${PYTHON_BIN:-$root/.venv/bin/python}"
if [ ! -x "$python_bin" ]; then
    python_bin="python3"
fi

if [ "${1:-}" != "secrets" ]; then
    echo "Usage: ./personal.sh secrets {status|set KEY|remove KEY|validate}" >&2
    exit 2
fi
shift

command_name="${1:-}"
key="${2:-}"
case "$command_name" in
    status|validate)
        [ "$#" -eq 1 ] || { echo "This command does not accept a value argument." >&2; exit 2; }
        PYTHONPATH="$root/backend${PYTHONPATH:+:$PYTHONPATH}" \
            "$python_bin" -m app.tools.personal_secrets "$command_name"
        exit 0
        ;;
    set|remove)
        [ "$#" -eq 2 ] || { echo "Secret values must be entered through standard input." >&2; exit 2; }
        result="$(
            PYTHONPATH="$root/backend${PYTHONPATH:+:$PYTHONPATH}" \
                "$python_bin" -m app.tools.personal_secrets "$command_name" "$key"
        )"
        printf '%s\n' "$result"
        ;;
    *)
        echo "Usage: ./personal.sh secrets {status|set KEY|remove KEY|validate}" >&2
        exit 2
        ;;
esac

case "$result" in
    *'"changed": true'*) ;;
    *) exit 0 ;;
esac

compose_file="$root/docker-compose.personal.yml"
if ! command -v docker >/dev/null 2>&1 || [ ! -f "$compose_file" ]; then
    exit 0
fi

if ! running="$(docker compose -f "$compose_file" ps --status running -q macrolens 2>/dev/null)"; then
    echo "Secret changed, but the MacroLens service state could not be checked." >&2
    exit 1
fi
if [ -n "$running" ]; then
    docker compose -f "$compose_file" up -d --no-deps --no-build --force-recreate macrolens
fi
