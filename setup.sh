#!/usr/bin/env bash

set -Eeuo pipefail
umask 077

ENV_FILE="${ENV_FILE:-.env}"
TEMP_ENV="${ENV_FILE}.tmp.$$"

cleanup() {
    rm -f "$TEMP_ENV"
}
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1; then
    echo "未检测到 Docker，请先安装并启动 Docker Desktop 或 Docker Engine。" >&2
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    echo "Docker 尚未运行，或当前账户无权访问 Docker。" >&2
    exit 1
fi

if docker compose version >/dev/null 2>&1; then
    COMPOSE=(docker compose)
else
    echo "未检测到新版 Docker Compose，请先安装 Docker Compose v2。" >&2
    exit 1
fi

existing_value() {
    local key="$1"
    [ -f "$ENV_FILE" ] || return 0
    awk -F= -v wanted="$key" '
        $1 == wanted {
            value = substr($0, index($0, "=") + 1)
            if (value ~ /^".*"$/ || value ~ /^'\''.*'\''$/) {
                value = substr(value, 2, length(value) - 2)
            }
            print value
            exit
        }
    ' "$ENV_FILE"
}

setting_or_default() {
    local key="$1"
    local fallback="$2"
    local value
    value="$(existing_value "$key")"
    printf '%s' "${value:-$fallback}"
}

set_variable() {
    local name="$1"
    local value="$2"
    printf -v "$name" '%s' "$value"
}

prompt_value() {
    local name="$1"
    local label="$2"
    local hint="${3:-}"
    local secret="${4:-false}"
    local required="${5:-false}"
    local current input

    current="$(existing_value "$name")"
    printf '%s' "$label"
    [ -n "$hint" ] && printf '（%s）' "$hint"
    [ -n "$current" ] && printf ' [已配置，回车保留]'
    printf ': '

    if [ "$secret" = "true" ]; then
        IFS= read -r -s input
        printf '\n'
    else
        IFS= read -r input
    fi

    if [ -z "$input" ]; then
        input="$current"
    fi
    if [ "$required" = "true" ] && [ -z "$input" ]; then
        echo "${label}不能为空。" >&2
        exit 1
    fi
    case "$input" in
        *$'\r'*|*$'\n'*)
            echo "$label 不能包含换行。" >&2
            exit 1
            ;;
    esac
    set_variable "$name" "$input"
}

write_env_value() {
    local key="$1"
    local value="$2"
    value=${value//\\/\\\\}
    value=${value//\"/\\\"}
    value=${value//\$/\$\$}
    printf '%s="%s"\n' "$key" "$value" >> "$TEMP_ENV"
}

echo "MacroLens 安装配置"
echo
echo "新闻源密钥均可留空；Google News 和 Seeking Alpha 会作为无密钥补充源。"
prompt_value FINNHUB_API_KEY "Finnhub API Key" "推荐的核心金融新闻源" true false
prompt_value MASSIVE_API_KEY "Massive API Key" "可选核心金融新闻源" true false
prompt_value NEWSAPI_API_KEY "NewsAPI API Key" "默认关闭，付费方案再启用" true false
prompt_value GNEWS_API_KEY "GNews API Key" "默认关闭，付费方案再启用" true false

echo
echo "模型分析配置"
current_provider="$(existing_value DEFAULT_LLM_PROVIDER)"
printf '提供方 [openai/anthropic/grok/ollama] [%s]: ' "${current_provider:-openai}"
IFS= read -r DEFAULT_LLM_PROVIDER
DEFAULT_LLM_PROVIDER="${DEFAULT_LLM_PROVIDER:-${current_provider:-openai}}"

case "$DEFAULT_LLM_PROVIDER" in
    openai)
        prompt_value OPENAI_API_KEY "OpenAI API Key" "" true true
        OPENAI_BASE_URL="$(existing_value OPENAI_BASE_URL)"
        OPENAI_BASE_URL="${OPENAI_BASE_URL:-https://api.openai.com/v1}"
        prompt_value DEFAULT_LLM_MODEL "模型名称" "默认 gpt-4o-mini" false false
        DEFAULT_LLM_MODEL="${DEFAULT_LLM_MODEL:-gpt-4o-mini}"
        DEFAULT_LLM_API_KEY="$OPENAI_API_KEY"
        ANTHROPIC_API_KEY="$(existing_value ANTHROPIC_API_KEY)"
        GROK_API_KEY="$(existing_value GROK_API_KEY)"
        ;;
    anthropic)
        prompt_value ANTHROPIC_API_KEY "Anthropic API Key" "" true true
        prompt_value DEFAULT_LLM_MODEL "模型名称" "默认 claude-sonnet-4-6" false false
        DEFAULT_LLM_MODEL="${DEFAULT_LLM_MODEL:-claude-sonnet-4-6}"
        DEFAULT_LLM_API_KEY="$ANTHROPIC_API_KEY"
        OPENAI_API_KEY="$(existing_value OPENAI_API_KEY)"
        OPENAI_BASE_URL="$(existing_value OPENAI_BASE_URL)"
        OPENAI_BASE_URL="${OPENAI_BASE_URL:-https://api.openai.com/v1}"
        GROK_API_KEY="$(existing_value GROK_API_KEY)"
        ;;
    grok)
        prompt_value GROK_API_KEY "Grok API Key" "" true true
        prompt_value DEFAULT_LLM_MODEL "模型名称" "默认 grok-4" false false
        DEFAULT_LLM_MODEL="${DEFAULT_LLM_MODEL:-grok-4}"
        DEFAULT_LLM_API_KEY="$GROK_API_KEY"
        OPENAI_API_KEY="$(existing_value OPENAI_API_KEY)"
        OPENAI_BASE_URL="$(existing_value OPENAI_BASE_URL)"
        OPENAI_BASE_URL="${OPENAI_BASE_URL:-https://api.openai.com/v1}"
        ANTHROPIC_API_KEY="$(existing_value ANTHROPIC_API_KEY)"
        ;;
    ollama)
        DEFAULT_LLM_API_KEY=""
        prompt_value DEFAULT_LLM_MODEL "本地模型名称" "默认 llama3" false false
        DEFAULT_LLM_MODEL="${DEFAULT_LLM_MODEL:-llama3}"
        OPENAI_API_KEY="$(existing_value OPENAI_API_KEY)"
        OPENAI_BASE_URL="$(existing_value OPENAI_BASE_URL)"
        OPENAI_BASE_URL="${OPENAI_BASE_URL:-https://api.openai.com/v1}"
        ANTHROPIC_API_KEY="$(existing_value ANTHROPIC_API_KEY)"
        GROK_API_KEY="$(existing_value GROK_API_KEY)"
        ;;
    *)
        echo "不支持的模型提供方：$DEFAULT_LLM_PROVIDER" >&2
        exit 1
        ;;
esac

GROK_BASE_URL="$(existing_value GROK_BASE_URL)"
GROK_BASE_URL="${GROK_BASE_URL:-https://api.x.ai/v1}"
GROK_MODEL="$(existing_value GROK_MODEL)"
GROK_MODEL="${GROK_MODEL:-grok-4}"
OLLAMA_BASE_URL="$(existing_value OLLAMA_BASE_URL)"
OLLAMA_BASE_URL="${OLLAMA_BASE_URL:-http://host.docker.internal:11434}"

ADMIN_TOKEN="$(existing_value ADMIN_TOKEN)"
if [ -z "$ADMIN_TOKEN" ]; then
    if command -v openssl >/dev/null 2>&1; then
        ADMIN_TOKEN="$(openssl rand -hex 32)"
    else
        ADMIN_TOKEN="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
    fi
fi

CORS_ORIGINS="$(existing_value CORS_ORIGINS)"
MACROLENS_DATA_DIR="$(existing_value MACROLENS_DATA_DIR)"
case "$MACROLENS_DATA_DIR" in
    /|/bin|/boot|/dev|/etc|/home|/lib|/lib64|/opt|/proc|/root|/run|/sbin|/srv|/sys|/tmp|/usr|/var)
        echo "MACROLENS_DATA_DIR 不能指向系统顶层目录。" >&2
        exit 1
        ;;
esac

if [ -f "$ENV_FILE" ]; then
    backup="${ENV_FILE}.backup.$(date '+%Y%m%d-%H%M%S')"
    cp "$ENV_FILE" "$backup"
    chmod 600 "$backup"
    echo "原配置已备份到 $backup"
fi

{
    echo "# MacroLens configuration"
    echo "# Generated at $(date '+%Y-%m-%d %H:%M:%S')"
} > "$TEMP_ENV"

write_env_value FINNHUB_API_KEY "$FINNHUB_API_KEY"
write_env_value MASSIVE_API_KEY "$MASSIVE_API_KEY"
write_env_value NEWSAPI_API_KEY "$NEWSAPI_API_KEY"
write_env_value GNEWS_API_KEY "$GNEWS_API_KEY"
write_env_value FINNHUB_NEWS_ENABLED "$(setting_or_default FINNHUB_NEWS_ENABLED "$([ -n "$FINNHUB_API_KEY" ] && echo true || echo false)")"
write_env_value FINNHUB_NEWS_INTERVAL "$(setting_or_default FINNHUB_NEWS_INTERVAL 300)"
write_env_value MASSIVE_NEWS_ENABLED "$(setting_or_default MASSIVE_NEWS_ENABLED "$([ -n "$MASSIVE_API_KEY" ] && echo true || echo false)")"
write_env_value MASSIVE_NEWS_INTERVAL "$(setting_or_default MASSIVE_NEWS_INTERVAL 300)"
write_env_value GOOGLE_NEWS_ENABLED "$(setting_or_default GOOGLE_NEWS_ENABLED true)"
write_env_value GOOGLE_NEWS_INTERVAL "$(setting_or_default GOOGLE_NEWS_INTERVAL 900)"
write_env_value SEEKINGALPHA_BREAKING_ENABLED "$(setting_or_default SEEKINGALPHA_BREAKING_ENABLED true)"
write_env_value SEEKINGALPHA_BREAKING_INTERVAL "$(setting_or_default SEEKINGALPHA_BREAKING_INTERVAL 300)"
write_env_value SEEKINGALPHA_DAILY_ENABLED "$(setting_or_default SEEKINGALPHA_DAILY_ENABLED true)"
write_env_value SEEKINGALPHA_DAILY_INTERVAL "$(setting_or_default SEEKINGALPHA_DAILY_INTERVAL 21600)"
write_env_value NEWSAPI_NEWS_ENABLED "$(setting_or_default NEWSAPI_NEWS_ENABLED false)"
write_env_value NEWSAPI_NEWS_INTERVAL "$(setting_or_default NEWSAPI_NEWS_INTERVAL 1800)"
write_env_value GNEWS_NEWS_ENABLED "$(setting_or_default GNEWS_NEWS_ENABLED false)"
write_env_value GNEWS_NEWS_INTERVAL "$(setting_or_default GNEWS_NEWS_INTERVAL 1800)"

write_env_value DEFAULT_LLM_PROVIDER "$DEFAULT_LLM_PROVIDER"
write_env_value DEFAULT_LLM_MODEL "$DEFAULT_LLM_MODEL"
write_env_value DEFAULT_LLM_API_KEY "$DEFAULT_LLM_API_KEY"
write_env_value OPENAI_API_KEY "$OPENAI_API_KEY"
write_env_value OPENAI_BASE_URL "$OPENAI_BASE_URL"
write_env_value ANTHROPIC_API_KEY "$ANTHROPIC_API_KEY"
write_env_value GROK_API_KEY "$GROK_API_KEY"
write_env_value GROK_MODEL "$GROK_MODEL"
write_env_value GROK_BASE_URL "$GROK_BASE_URL"
write_env_value OLLAMA_BASE_URL "$OLLAMA_BASE_URL"

write_env_value ADMIN_TOKEN "$ADMIN_TOKEN"
write_env_value SESSION_COOKIE_SECURE "$(setting_or_default SESSION_COOKIE_SECURE false)"
write_env_value SESSION_TTL_SECONDS "$(setting_or_default SESSION_TTL_SECONDS 28800)"
write_env_value ANALYSIS_BATCH_SIZE "$(setting_or_default ANALYSIS_BATCH_SIZE 10)"
write_env_value ANALYSIS_RETENTION_LIMIT "$(setting_or_default ANALYSIS_RETENTION_LIMIT 350)"
write_env_value X_SENTIMENT_INTERVAL "$(setting_or_default X_SENTIMENT_INTERVAL 21600)"
write_env_value CALENDAR_ANALYSIS_CACHE_TTL "$(setting_or_default CALENDAR_ANALYSIS_CACHE_TTL 3600)"
write_env_value NEWS_RETENTION_DAYS "$(existing_value NEWS_RETENTION_DAYS)"
write_env_value X_SENTIMENT_RETENTION_DAYS "$(existing_value X_SENTIMENT_RETENTION_DAYS)"
write_env_value DATABASE_URL "$(setting_or_default DATABASE_URL sqlite+aiosqlite:///data/macrolens.db)"
write_env_value CORS_ORIGINS "$CORS_ORIGINS"
write_env_value MACROLENS_DATA_DIR "$MACROLENS_DATA_DIR"

mv "$TEMP_ENV" "$ENV_FILE"
chmod 600 "$ENV_FILE"
trap - EXIT

echo
echo "配置已安全写入 ${ENV_FILE}，管理员令牌未显示在终端。"
printf '立即构建并启动服务？[Y/n]: '
IFS= read -r start_choice
if [ "${start_choice:-Y}" != "n" ] && [ "${start_choice:-Y}" != "N" ]; then
    "${COMPOSE[@]}" up -d --build
    echo "服务已启动：http://localhost:3000"
else
    echo "稍后可运行 docker compose up -d --build。"
fi
