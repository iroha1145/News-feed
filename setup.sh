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
    local line value decoded char next i length
    [ -f "$ENV_FILE" ] || return 0
    while IFS= read -r line || [ -n "$line" ]; do
        [[ "$line" == "$key="* ]] || continue
        value="${line#*=}"
        if [[ "$value" == \"*\" ]] && [[ "$value" == *\" ]]; then
            value="${value:1:${#value}-2}"
            decoded=""
            length=${#value}
            for ((i = 0; i < length; i++)); do
                char="${value:i:1}"
                next=""
                if ((i + 1 < length)); then
                    next="${value:i+1:1}"
                fi
                if [ "$char" = '\' ] && { [ "$next" = '\' ] || [ "$next" = '"' ]; }; then
                    decoded+="$next"
                    i=$((i + 1))
                elif [ "$char" = '$' ] && [ "$next" = '$' ]; then
                    decoded+='$'
                    i=$((i + 1))
                else
                    decoded+="$char"
                fi
            done
            value="$decoded"
        elif [[ "$value" == \'*\' ]] && [[ "$value" == *\' ]]; then
            value="${value:1:${#value}-2}"
            decoded=""
            length=${#value}
            for ((i = 0; i < length; i++)); do
                char="${value:i:1}"
                next=""
                if ((i + 1 < length)); then
                    next="${value:i+1:1}"
                fi
                if [ "$char" = '\' ] && { [ "$next" = '\' ] || [ "$next" = "'" ]; }; then
                    decoded+="$next"
                    i=$((i + 1))
                else
                    decoded+="$char"
                fi
            done
            value="$decoded"
        fi
        printf '%s' "$value"
        return 0
    done < "$ENV_FILE"
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
    local preserve_current="${6:-true}"
    local current input

    if [ "$preserve_current" = "true" ]; then
        current="$(existing_value "$name")"
    else
        current=""
    fi
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
    local escaped="" char i
    for ((i = 0; i < ${#value}; i++)); do
        char="${value:i:1}"
        case "$char" in
            \\|\') escaped+="\\$char" ;;
            *) escaped+="$char" ;;
        esac
    done
    # Single-quoted dotenv values are literal, so dollar signs cannot be
    # mistaken for Compose interpolation.  Backslashes and quotes are escaped
    # for both Docker Compose and pydantic-settings.
    printf "%s='%s'\n" "$key" "$escaped" >> "$TEMP_ENV"
}

append_unmanaged_values() {
    local line key
    [ -f "$ENV_FILE" ] || return 0
    while IFS= read -r line || [ -n "$line" ]; do
        [[ "$line" == *=* ]] || continue
        key="${line%%=*}"
        [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
        if ! grep -q "^${key}=" "$TEMP_ENV"; then
            # Keep future or deployment-specific settings byte-for-byte.  This
            # avoids silently deleting secrets that a newer release introduces.
            printf '%s\n' "$line" >> "$TEMP_ENV"
        fi
    done < "$ENV_FILE"
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
provider_changed=false
if [ "$DEFAULT_LLM_PROVIDER" != "${current_provider:-openai}" ]; then
    provider_changed=true
fi

case "$DEFAULT_LLM_PROVIDER" in
    openai)
        prompt_value OPENAI_API_KEY "OpenAI API Key" "付费能力关闭时可留空" true false
        OPENAI_BASE_URL="$(existing_value OPENAI_BASE_URL)"
        OPENAI_BASE_URL="${OPENAI_BASE_URL:-https://api.openai.com/v1}"
        prompt_value DEFAULT_LLM_MODEL "模型名称" "默认 gpt-5.6-terra" false false "$([ "$provider_changed" = false ] && echo true || echo false)"
        DEFAULT_LLM_MODEL="${DEFAULT_LLM_MODEL:-gpt-5.6-terra}"
        DEFAULT_LLM_API_KEY="$OPENAI_API_KEY"
        ANTHROPIC_API_KEY="$(existing_value ANTHROPIC_API_KEY)"
        GROK_API_KEY="$(existing_value GROK_API_KEY)"
        ;;
    anthropic)
        prompt_value ANTHROPIC_API_KEY "Anthropic API Key" "付费能力关闭时可留空" true false
        prompt_value DEFAULT_LLM_MODEL "模型名称" "默认 claude-sonnet-4-6" false false "$([ "$provider_changed" = false ] && echo true || echo false)"
        DEFAULT_LLM_MODEL="${DEFAULT_LLM_MODEL:-claude-sonnet-4-6}"
        DEFAULT_LLM_API_KEY="$ANTHROPIC_API_KEY"
        OPENAI_API_KEY="$(existing_value OPENAI_API_KEY)"
        OPENAI_BASE_URL="$(existing_value OPENAI_BASE_URL)"
        OPENAI_BASE_URL="${OPENAI_BASE_URL:-https://api.openai.com/v1}"
        GROK_API_KEY="$(existing_value GROK_API_KEY)"
        ;;
    grok)
        prompt_value GROK_API_KEY "Grok API Key" "付费能力关闭时可留空" true false
        prompt_value DEFAULT_LLM_MODEL "模型名称" "默认 grok-4" false false "$([ "$provider_changed" = false ] && echo true || echo false)"
        DEFAULT_LLM_MODEL="${DEFAULT_LLM_MODEL:-grok-4}"
        DEFAULT_LLM_API_KEY="$GROK_API_KEY"
        OPENAI_API_KEY="$(existing_value OPENAI_API_KEY)"
        OPENAI_BASE_URL="$(existing_value OPENAI_BASE_URL)"
        OPENAI_BASE_URL="${OPENAI_BASE_URL:-https://api.openai.com/v1}"
        ANTHROPIC_API_KEY="$(existing_value ANTHROPIC_API_KEY)"
        ;;
    ollama)
        DEFAULT_LLM_API_KEY=""
        prompt_value DEFAULT_LLM_MODEL "本地模型名称" "默认 llama3" false false "$([ "$provider_changed" = false ] && echo true || echo false)"
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
write_env_value MASSIVE_NEWS_INTERVAL "$(setting_or_default MASSIVE_NEWS_INTERVAL 3600)"
write_env_value FINNHUB_FOCUS_INTERVAL "$(setting_or_default FINNHUB_FOCUS_INTERVAL 1800)"
write_env_value MASSIVE_FOCUS_INTERVAL "$(setting_or_default MASSIVE_FOCUS_INTERVAL 2700)"
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
write_env_value OPENAI_ALLOW_CUSTOM_BASE_URL "$(setting_or_default OPENAI_ALLOW_CUSTOM_BASE_URL false)"
write_env_value OPENAI_ALLOW_LOCAL_HTTP "$(setting_or_default OPENAI_ALLOW_LOCAL_HTTP false)"
write_env_value OPENAI_REASONING "$(setting_or_default OPENAI_REASONING max)"
write_env_value OPENAI_EXECUTION_MODE "$(setting_or_default OPENAI_EXECUTION_MODE background)"
write_env_value OPENAI_SYNC_TIMEOUT_SECONDS "$(setting_or_default OPENAI_SYNC_TIMEOUT_SECONDS 900)"
write_env_value OPENAI_BACKGROUND_POLL_TIMEOUT_SECONDS "$(setting_or_default OPENAI_BACKGROUND_POLL_TIMEOUT_SECONDS 1800)"
write_env_value OPENAI_BACKGROUND_INITIAL_POLL_SECONDS "$(setting_or_default OPENAI_BACKGROUND_INITIAL_POLL_SECONDS 2)"
write_env_value OPENAI_BACKGROUND_MAX_POLL_SECONDS "$(setting_or_default OPENAI_BACKGROUND_MAX_POLL_SECONDS 15)"
write_env_value OPENAI_MAX_OUTPUT_TOKENS "$(setting_or_default OPENAI_MAX_OUTPUT_TOKENS 128000)"
write_env_value OPENAI_MAX_CONCURRENCY "$(setting_or_default OPENAI_MAX_CONCURRENCY 2)"
write_env_value OPENAI_MAX_RETRIES "$(setting_or_default OPENAI_MAX_RETRIES 0)"
write_env_value NEWS_IMPACT_PROMPT_VERSION "$(setting_or_default NEWS_IMPACT_PROMPT_VERSION news-impact-v2)"
write_env_value NEWS_IMPACT_SCHEMA_VERSION "$(setting_or_default NEWS_IMPACT_SCHEMA_VERSION news-impact-schema-v2)"
write_env_value NEWS_LLM_AUTO_ANALYZE_ENABLED "$(setting_or_default NEWS_LLM_AUTO_ANALYZE_ENABLED false)"
write_env_value NEWS_ITEM_MAX_OUTPUT_TOKENS "$(setting_or_default NEWS_ITEM_MAX_OUTPUT_TOKENS 32768)"
write_env_value NEWS_LLM_MAX_INFLIGHT "$(setting_or_default NEWS_LLM_MAX_INFLIGHT 2)"
write_env_value NEWS_LLM_MAX_QUEUED "$(setting_or_default NEWS_LLM_MAX_QUEUED 200)"
write_env_value NEWS_LLM_DAILY_JOB_LIMIT "$(existing_value NEWS_LLM_DAILY_JOB_LIMIT)"
write_env_value NEWS_LLM_DAILY_OUTPUT_TOKEN_LIMIT "$(existing_value NEWS_LLM_DAILY_OUTPUT_TOKEN_LIMIT)"
write_env_value NEWS_LLM_MANUAL_ENABLED "$(setting_or_default NEWS_LLM_MANUAL_ENABLED false)"
write_env_value NEWS_LLM_MANUAL_DAILY_JOB_LIMIT "$(existing_value NEWS_LLM_MANUAL_DAILY_JOB_LIMIT)"
write_env_value NEWS_LLM_MANUAL_DAILY_OUTPUT_TOKEN_LIMIT "$(existing_value NEWS_LLM_MANUAL_DAILY_OUTPUT_TOKEN_LIMIT)"
write_env_value HOT_CYCLE_ENABLED "$(setting_or_default HOT_CYCLE_ENABLED false)"
write_env_value HOT_CYCLE_SCHEDULE_ENABLED "$(setting_or_default HOT_CYCLE_SCHEDULE_ENABLED false)"
write_env_value HOT_CYCLE_TIMES_ET "$(setting_or_default HOT_CYCLE_TIMES_ET 08:00,12:00,16:00)"
write_env_value HOT_CYCLE_OPTIONAL_20_ET "$(setting_or_default HOT_CYCLE_OPTIONAL_20_ET false)"
write_env_value HOT_CYCLE_MANUAL_ENABLED "$(setting_or_default HOT_CYCLE_MANUAL_ENABLED false)"
write_env_value HOT_CYCLE_MAX_OUTPUT_TOKENS "$(setting_or_default HOT_CYCLE_MAX_OUTPUT_TOKENS 49152)"
write_env_value HOT_CYCLE_DAILY_JOB_LIMIT "$(existing_value HOT_CYCLE_DAILY_JOB_LIMIT)"
write_env_value HOT_CYCLE_DAILY_OUTPUT_TOKEN_LIMIT "$(existing_value HOT_CYCLE_DAILY_OUTPUT_TOKEN_LIMIT)"
write_env_value NEWS_LLM_MIN_CONTEXT_CHARS "$(setting_or_default NEWS_LLM_MIN_CONTEXT_CHARS 100)"
write_env_value NEWS_LLM_MIN_MARKET_RELEVANCE "$(setting_or_default NEWS_LLM_MIN_MARKET_RELEVANCE 35)"
write_env_value ANALYSIS_WORKER_POLL_SECONDS "$(setting_or_default ANALYSIS_WORKER_POLL_SECONDS 5)"
write_env_value ANALYSIS_WORKER_LEASE_SECONDS "$(setting_or_default ANALYSIS_WORKER_LEASE_SECONDS 120)"
write_env_value ANALYSIS_JOB_RETRY_COOLDOWN_SECONDS "$(setting_or_default ANALYSIS_JOB_RETRY_COOLDOWN_SECONDS 300)"
write_env_value CALENDAR_ANALYSIS_PROMPT_VERSION "$(setting_or_default CALENDAR_ANALYSIS_PROMPT_VERSION calendar-impact-v1)"
write_env_value CALENDAR_ANALYSIS_SCHEMA_VERSION "$(setting_or_default CALENDAR_ANALYSIS_SCHEMA_VERSION calendar-impact-schema-v1)"
write_env_value CALENDAR_LLM_MANUAL_ENABLED "$(setting_or_default CALENDAR_LLM_MANUAL_ENABLED false)"
write_env_value CALENDAR_LLM_MAX_INFLIGHT "$(setting_or_default CALENDAR_LLM_MAX_INFLIGHT 1)"
write_env_value CALENDAR_LLM_MAX_QUEUED "$(setting_or_default CALENDAR_LLM_MAX_QUEUED 10)"
write_env_value CALENDAR_MAX_OUTPUT_TOKENS "$(setting_or_default CALENDAR_MAX_OUTPUT_TOKENS 16384)"
write_env_value CALENDAR_LLM_DAILY_JOB_LIMIT "$(existing_value CALENDAR_LLM_DAILY_JOB_LIMIT)"
write_env_value CALENDAR_LLM_DAILY_OUTPUT_TOKEN_LIMIT "$(existing_value CALENDAR_LLM_DAILY_OUTPUT_TOKEN_LIMIT)"
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
write_env_value X_SENTIMENT_ENABLED "$(setting_or_default X_SENTIMENT_ENABLED false)"
write_env_value X_SENTIMENT_INTERVAL "$(setting_or_default X_SENTIMENT_INTERVAL 21600)"
write_env_value CALENDAR_ANALYSIS_CACHE_TTL "$(setting_or_default CALENDAR_ANALYSIS_CACHE_TTL 3600)"
write_env_value CALENDAR_FETCH_INTERVAL_SECONDS "$(setting_or_default CALENDAR_FETCH_INTERVAL_SECONDS 600)"
write_env_value NEWS_RETENTION_DAYS "$(existing_value NEWS_RETENTION_DAYS)"
write_env_value X_SENTIMENT_RETENTION_DAYS "$(existing_value X_SENTIMENT_RETENTION_DAYS)"
write_env_value NEWS_ITEM_RETENTION_DAYS "$(existing_value NEWS_ITEM_RETENTION_DAYS)"
write_env_value ANALYSIS_JOB_RETENTION_DAYS "$(existing_value ANALYSIS_JOB_RETENTION_DAYS)"
write_env_value ANALYSIS_REVISION_RETENTION_DAYS "$(existing_value ANALYSIS_REVISION_RETENTION_DAYS)"
write_env_value STOCK_IMPACT_RETENTION_DAYS "$(existing_value STOCK_IMPACT_RETENTION_DAYS)"
write_env_value EVENT_GROUP_RETENTION_DAYS "$(existing_value EVENT_GROUP_RETENTION_DAYS)"
write_env_value ANALYSIS_CYCLE_RETENTION_DAYS "$(existing_value ANALYSIS_CYCLE_RETENTION_DAYS)"
write_env_value CALENDAR_REVISION_RETENTION_DAYS "$(existing_value CALENDAR_REVISION_RETENTION_DAYS)"
write_env_value INTEGRATION_CHANGE_RETENTION_DAYS "$(existing_value INTEGRATION_CHANGE_RETENTION_DAYS)"
write_env_value HOTSPOT_PREPARATION_RETENTION_DAYS "$(setting_or_default HOTSPOT_PREPARATION_RETENTION_DAYS 90)"
write_env_value MARKET_FOCUS_COMPLETED_RETENTION_DAYS "$(setting_or_default MARKET_FOCUS_COMPLETED_RETENTION_DAYS 365)"
write_env_value MARKET_FOCUS_FAILED_RETENTION_DAYS "$(setting_or_default MARKET_FOCUS_FAILED_RETENTION_DAYS 30)"
write_env_value EVENT_MEMBER_RETENTION_DAYS "$(setting_or_default EVENT_MEMBER_RETENTION_DAYS 90)"
write_env_value PROJECTION_RETRY_RETENTION_DAYS "$(setting_or_default PROJECTION_RETRY_RETENTION_DAYS 30)"
write_env_value PROJECTION_RETRY_MAX_ATTEMPTS "$(setting_or_default PROJECTION_RETRY_MAX_ATTEMPTS 6)"
write_env_value DATABASE_URL "$(setting_or_default DATABASE_URL sqlite+aiosqlite:///data/macrolens.db)"
write_env_value CORS_ORIGINS "$CORS_ORIGINS"
write_env_value MACROLENS_DATA_DIR "$MACROLENS_DATA_DIR"
write_env_value MACROLENS_OPTION_PRO_CONTRACT_PATH "$(existing_value MACROLENS_OPTION_PRO_CONTRACT_PATH)"
write_env_value OPTION_PRO_READ_KEY_ID "$(existing_value OPTION_PRO_READ_KEY_ID)"
write_env_value OPTION_PRO_READ_SECRET "$(existing_value OPTION_PRO_READ_SECRET)"
write_env_value OPTION_PRO_ACTION_KEY_ID "$(existing_value OPTION_PRO_ACTION_KEY_ID)"
write_env_value OPTION_PRO_ACTION_SECRET "$(existing_value OPTION_PRO_ACTION_SECRET)"
write_env_value OPTION_PRO_PREVIOUS_READ_SECRET "$(existing_value OPTION_PRO_PREVIOUS_READ_SECRET)"
write_env_value OPTION_PRO_PREVIOUS_ACTION_SECRET "$(existing_value OPTION_PRO_PREVIOUS_ACTION_SECRET)"
write_env_value OPTION_PRO_ALLOWED_CIDRS "$(existing_value OPTION_PRO_ALLOWED_CIDRS)"
write_env_value OPTION_PRO_TRUSTED_PROXY_CIDRS "$(existing_value OPTION_PRO_TRUSTED_PROXY_CIDRS)"
write_env_value OPTION_PRO_SIGNATURE_CLOCK_SKEW_SECONDS "$(setting_or_default OPTION_PRO_SIGNATURE_CLOCK_SKEW_SECONDS 300)"
write_env_value OPTION_PRO_NONCE_TTL_SECONDS "$(setting_or_default OPTION_PRO_NONCE_TTL_SECONDS 600)"
write_env_value OPTION_PRO_SOURCE_STALE_AFTER_SECONDS "$(setting_or_default OPTION_PRO_SOURCE_STALE_AFTER_SECONDS 86400)"
write_env_value OPTION_PRO_ALLOW_LOCAL_HTTP "$(setting_or_default OPTION_PRO_ALLOW_LOCAL_HTTP false)"
write_env_value OPTION_PRO_FOCUS_BASE_URL "$(existing_value OPTION_PRO_FOCUS_BASE_URL)"
write_env_value OPTION_PRO_FOCUS_KEY_ID "$(existing_value OPTION_PRO_FOCUS_KEY_ID)"
write_env_value OPTION_PRO_FOCUS_SECRET "$(existing_value OPTION_PRO_FOCUS_SECRET)"
write_env_value OPTION_PRO_FOCUS_VERIFY_TLS "$(setting_or_default OPTION_PRO_FOCUS_VERIFY_TLS true)"
write_env_value OPTION_PRO_FOCUS_CA_BUNDLE "$(existing_value OPTION_PRO_FOCUS_CA_BUNDLE)"
write_env_value OPTION_PRO_FOCUS_INTERVAL_SECONDS "$(setting_or_default OPTION_PRO_FOCUS_INTERVAL_SECONDS 1800)"
write_env_value OPTION_PRO_FOCUS_CONNECT_TIMEOUT_SECONDS "$(setting_or_default OPTION_PRO_FOCUS_CONNECT_TIMEOUT_SECONDS 5)"
write_env_value OPTION_PRO_FOCUS_READ_TIMEOUT_SECONDS "$(setting_or_default OPTION_PRO_FOCUS_READ_TIMEOUT_SECONDS 10)"
write_env_value OPTION_PRO_FOCUS_TIMEOUT_SECONDS "$(setting_or_default OPTION_PRO_FOCUS_TIMEOUT_SECONDS 20)"
write_env_value OPTION_PRO_FOCUS_MAX_RESPONSE_BYTES "$(setting_or_default OPTION_PRO_FOCUS_MAX_RESPONSE_BYTES 1048576)"
write_env_value OPTION_PRO_FOCUS_MAX_ATTEMPTS "$(setting_or_default OPTION_PRO_FOCUS_MAX_ATTEMPTS 3)"
write_env_value OPTION_PRO_FOCUS_RETRY_BACKOFF_SECONDS "$(setting_or_default OPTION_PRO_FOCUS_RETRY_BACKOFF_SECONDS 0.25)"
write_env_value OPTION_PRO_FOCUS_CIRCUIT_FAILURE_THRESHOLD "$(setting_or_default OPTION_PRO_FOCUS_CIRCUIT_FAILURE_THRESHOLD 3)"
write_env_value OPTION_PRO_FOCUS_CIRCUIT_RESET_SECONDS "$(setting_or_default OPTION_PRO_FOCUS_CIRCUIT_RESET_SECONDS 60)"

append_unmanaged_values

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
