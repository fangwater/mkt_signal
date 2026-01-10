#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF2'
用法: scripts/deploy_fr_config_server.sh [trade|test]
                                       [--exchange <binance|okex|gate|bybit|bitget>]
                                       [--env-name <exchange>_fr_trade]
                                       [--target <path>]
                                       [--bind 0.0.0.0] [--port <default>]
                                       [--nginx-prefix /fr/<env-name>/config]
                                       [--nginx-port 4191]
                                       [--nginx-mapping-file $HOME/nginx_locations.txt]
                                       [--apply-nginx]

说明:
  - 部署 fr_config_server 到 $HOME/<exchange>_fr_<trade|test>/（或 --env-name / --target）。
  - exchange 可省略，会从 --env-name 或当前目录名推断（如 binance_fr_trade）。
  - 默认端口按交易所分配（okex=18011 gate=18021 binance=18031 bybit=18041 bitget=18051）。
  - 可选写入 nginx mapping（/fr/<env-name>/config）。
  - env-name/目标目录名必须匹配 <exchange>_fr_<suffix>（例如 binance_fr_trade / binance_fr_hf01）。

示例:
  scripts/deploy_fr_config_server.sh --exchange okex
  scripts/deploy_fr_config_server.sh trade --exchange binance --apply-nginx
  scripts/deploy_fr_config_server.sh --env-name okex_fr_trade --port 18011
EOF2
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_TYPE="trade"
EXCHANGE=""
ENV_NAME=""
TARGET_DIR=""

BIND="0.0.0.0"
PORT=""

NGINX_PREFIX=""
NGINX_PORT="4191"
NGINX_MAPPING_FILE=""
APPLY_NGINX="0"

normalize_exchange() {
  local ex="${1,,}"
  if [[ "$ex" == "okx" ]]; then
    ex="okex"
  fi
  echo "$ex"
}

normalize_env_name() {
  echo "$1" | tr 'A-Z' 'a-z'
}

require_fr_env_name() {
  local exchange="$1"
  local name="$2"
  if [[ ! "$name" =~ ^${exchange}_fr(_[a-z0-9][a-z0-9_-]*)?$ ]]; then
    echo "[ERROR] env-name must match ${exchange}_fr_<suffix> (got: ${name})" >&2
    exit 1
  fi
}

default_port_for_exchange() {
  case "$1" in
    okex) echo "18011" ;;
    gate) echo "18021" ;;
    binance) echo "18031" ;;
    bybit) echo "18041" ;;
    bitget) echo "18051" ;;
    *) echo "18011" ;;
  esac
}

infer_exchange_from_env_name() {
  local name="${1,,}"
  if [[ "$name" =~ ^([a-z0-9]+)[-_]fr([_-].*)?$ ]]; then
    echo "${BASH_REMATCH[1]}"
  fi
}

infer_exchange_from_cwd() {
  local name
  name="$(basename "$(pwd)")"
  infer_exchange_from_env_name "$name"
}

upsert_main_nginx_mapping() {
  local main_file begin_marker end_marker tmp
  if [[ -z "${NGINX_MAPPING_FILE}" ]]; then
    NGINX_MAPPING_FILE="$HOME/nginx_locations.txt"
  fi
  main_file="${NGINX_MAPPING_FILE}"

  if [[ ! -f "$main_file" ]]; then
    if [[ -f "${ROOT_DIR}/config/nginx_locations.txt" ]]; then
      mkdir -p "$(dirname "$main_file")" >/dev/null 2>&1 || true
      cp "${ROOT_DIR}/config/nginx_locations.txt" "$main_file"
      echo "[INFO] Initialized nginx mapping file: $main_file (from ${ROOT_DIR}/config/nginx_locations.txt)"
    else
      echo "[ERROR] Missing nginx mapping file: $main_file" >&2
      echo "[ERROR] Also missing template: ${ROOT_DIR}/config/nginx_locations.txt" >&2
      exit 1
    fi
  fi

  if [[ "${NGINX_PREFIX}" != /* ]]; then
    echo "[ERROR] --nginx-prefix must start with /: ${NGINX_PREFIX}" >&2
    exit 1
  fi

  local base_prefix="${NGINX_PREFIX%/}"
  local proxy_prefix="${base_prefix}/"

  begin_marker="# BEGIN managed: fr config ${base_prefix}"
  end_marker="# END managed: fr config ${base_prefix}"

  if grep -Fqx "$begin_marker" "$main_file" && ! grep -Fqx "$end_marker" "$main_file"; then
    echo "[ERROR] nginx_locations.txt has begin marker but missing end marker:" >&2
    echo "        ${begin_marker}" >&2
    echo "        (please fix the block manually and retry)" >&2
    exit 1
  fi

  tmp="$(mktemp)"
  awk -v begin="$begin_marker" \
      -v end="$end_marker" \
      -v prefix="$base_prefix" \
      -v proxy_prefix="$proxy_prefix" \
      -v port="$PORT" '
    BEGIN { in_block = 0; replaced = 0 }
    $0 == begin { in_block = 1; replaced = 1; next }
    in_block && $0 == end {
        in_block = 0;
        print begin;
        print "# fr config server (HTTP)";
        print prefix " http://127.0.0.1:" port "/";
        print proxy_prefix " http://127.0.0.1:" port "/";
        print end;
        next
    }
    in_block { next }
    {
        if (substr($0, 1, length(prefix)) == prefix && substr($0, length(prefix) + 1, 1) ~ /[[:space:]]/) {
            next
        }
        if (substr($0, 1, length(prefix) + 1) == (prefix "/") && substr($0, length(prefix) + 2, 1) ~ /[[:space:]]/) {
            next
        }
        print
    }
    END {
        if (!replaced) {
            print "";
            print begin;
            print "# fr config server (HTTP)";
            print prefix " http://127.0.0.1:" port "/";
            print proxy_prefix " http://127.0.0.1:" port "/";
            print end;
        }
    }
  ' "$main_file" >"$tmp"
  mv "$tmp" "$main_file"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    trade|test)
      ENV_TYPE="$1"
      shift
      ;;
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    --env-name)
      ENV_NAME="${2:-}"
      shift 2
      ;;
    --target)
      TARGET_DIR="${2:-}"
      shift 2
      ;;
    --bind)
      BIND="${2:-}"
      shift 2
      ;;
    --port)
      PORT="${2:-}"
      shift 2
      ;;
    --nginx-prefix)
      NGINX_PREFIX="${2:-}"
      shift 2
      ;;
    --nginx-port)
      NGINX_PORT="${2:-}"
      shift 2
      ;;
    --nginx-mapping-file)
      NGINX_MAPPING_FILE="${2:-}"
      shift 2
      ;;
    --apply-nginx)
      APPLY_NGINX="1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$EXCHANGE" ]]; then
  if [[ -n "$ENV_NAME" ]]; then
    EXCHANGE="$(infer_exchange_from_env_name "$ENV_NAME")"
  else
    EXCHANGE="$(infer_exchange_from_cwd)"
  fi
fi

EXCHANGE="$(normalize_exchange "$EXCHANGE")"
if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 需要使用 --exchange 指定交易所（例如 okex / binance / bybit ...）" >&2
  usage
  exit 1
fi

if [[ -z "$TARGET_DIR" ]]; then
  if [[ -n "$ENV_NAME" ]]; then
    ENV_NAME="$(normalize_env_name "$ENV_NAME")"
    TARGET_DIR="$HOME/${ENV_NAME}"
  else
    TARGET_DIR="$HOME/${EXCHANGE}_fr_${ENV_TYPE}"
  fi
fi

if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="$(basename "$TARGET_DIR")"
else
  ENV_NAME="$(normalize_env_name "$ENV_NAME")"
fi
ENV_NAME="$(normalize_env_name "$ENV_NAME")"
if [[ -n "$TARGET_DIR" ]]; then
  target_base="$(basename "$TARGET_DIR")"
  target_base="$(normalize_env_name "$target_base")"
  if [[ "$target_base" != "$ENV_NAME" ]]; then
    echo "[ERROR] --target basename must match env-name (${ENV_NAME}), got: ${target_base}" >&2
    exit 1
  fi
fi
require_fr_env_name "$EXCHANGE" "$ENV_NAME"

if [[ -z "$PORT" ]]; then
  PORT="$(default_port_for_exchange "$EXCHANGE")"
fi

if [[ -z "$NGINX_PREFIX" ]]; then
  NGINX_PREFIX="/fr/${ENV_NAME}/config"
fi

DEST_SCRIPT_DIR="$TARGET_DIR/scripts"
mkdir -p "$DEST_SCRIPT_DIR"
mkdir -p "$TARGET_DIR/config"

FILES=(
  "scripts/fr_config_server.py"
  "scripts/start_fr_config_server.sh"
  "scripts/stop_fr_config_server.sh"
  "scripts/sync_fr_risk_params.py"
  "scripts/sync_fr_rolling_metrics_params.py"
  "scripts/sync_funding_rate_thresholds.py"
  "scripts/sync_fr_symbol_lists.py"
  "scripts/sync_fr_strategy_params.py"
  "scripts/sync_fr_spread_thresholds.py"
)

for file in "${FILES[@]}"; do
  src="$ROOT_DIR/$file"
  if [[ ! -f "$src" ]]; then
    echo "[WARN] 跳过缺失文件: $src"
    continue
  fi
  rsync -a "$src" "$DEST_SCRIPT_DIR/"
done

for path in "$DEST_SCRIPT_DIR"/*.sh "$DEST_SCRIPT_DIR"/*.py; do
  [[ -f "$path" ]] && chmod +x "$path" 2>/dev/null || true
done

cat <<EOF > "$TARGET_DIR/config/fr_config_server.env"
HOST=${BIND}
PORT=${PORT}
DEFAULT_EXCHANGE=${EXCHANGE}
EOF

upsert_main_nginx_mapping
if [[ "${APPLY_NGINX}" == "1" ]]; then
  echo "[INFO] Applying nginx config (PORT=${NGINX_PORT}, MAPPING_FILE=${NGINX_MAPPING_FILE})"
  (
    cd "$ROOT_DIR"
    PORT="$NGINX_PORT" MAPPING_FILE="$NGINX_MAPPING_FILE" ./scripts/setup_nginx_4191.sh
  )
fi

echo "[INFO] 已部署 fr_config_server 脚本到 $DEST_SCRIPT_DIR"
echo "[INFO] 默认端口: ${PORT}"
echo "[INFO] 启动: cd $TARGET_DIR && PORT=${PORT} ./scripts/start_fr_config_server.sh"
echo "[INFO] 停止: cd $TARGET_DIR && ./scripts/stop_fr_config_server.sh"
