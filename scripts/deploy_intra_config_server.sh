#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
用法: scripts/deploy_intra_config_server.sh --env-name <exchange>-intra-<suffix>
                                            [--exchange <binance>]
                                            [--target <path>]
                                            [--bind 0.0.0.0] [--port <default>]
                                            [--nginx-prefix /intra/<env-name>/config]
                                            [--nginx-port 4191]
                                            [--nginx-mapping-file $HOME/nginx_locations.txt]
                                            [--apply-nginx]

说明:
  - 同所期现：部署 intra_config_server 到 $HOME/<env-name>/（或 --target）
  - env-name 必须匹配 <exchange>-intra-<suffix>，suffix 支持 arb01/arb02/arb03
  - exchange 可省略，会从 env-name 推断
  - 默认端口按 exchange + suffix 映射

示例:
  scripts/deploy_intra_config_server.sh --env-name binance-intra-arb01
  scripts/deploy_intra_config_server.sh --env-name binance-intra-arb01 --apply-nginx
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage; exit 0
fi

ENV_NAME=""
TARGET_DIR=""
EXCHANGE=""
BIND="0.0.0.0"
PORT=""
NGINX_PREFIX=""
NGINX_PORT="4191"
NGINX_MAPPING_FILE=""
APPLY_NGINX="0"

normalize_exchange() {
  local ex="${1,,}"
  [[ "$ex" == "okx" ]] && ex="okex"
  echo "$ex"
}

normalize_env_name() {
  echo "$1" | tr 'A-Z' 'a-z'
}

require_intra_env_name() {
  local name="$1"
  if [[ ! "$name" =~ ^[a-z0-9]+-intra-[a-z0-9][a-z0-9_-]*$ ]]; then
    echo "[ERROR] env-name must match <exchange>-intra-<suffix> (got: ${name})" >&2
    exit 1
  fi
}

intra_env_suffix() {
  local name="$1"
  if [[ "$name" =~ ^[a-z0-9]+[-_]intra[-_]([a-z0-9][a-z0-9_-]*)$ ]]; then
    echo "${BASH_REMATCH[1]}"
  fi
}

exchange_default_port() {
  local exchange="$1"
  local suffix="${2:-arb01}"
  case "$suffix" in
    arb01)
      case "$exchange" in
        binance) echo "19171" ;;
        okex)    echo "19181" ;;
        bybit)   echo "19191" ;;
        gate)    echo "19201" ;;
        bitget)  echo "19211" ;;
        *)       echo "19170" ;;
      esac
      ;;
    arb02)
      case "$exchange" in
        binance) echo "19172" ;;
        okex)    echo "19182" ;;
        bybit)   echo "19192" ;;
        gate)    echo "19202" ;;
        bitget)  echo "19212" ;;
        *)       echo "19172" ;;
      esac
      ;;
    arb03)
      case "$exchange" in
        binance) echo "19173" ;;
        okex)    echo "19183" ;;
        bybit)   echo "19193" ;;
        gate)    echo "19203" ;;
        bitget)  echo "19213" ;;
        *)       echo "19173" ;;
      esac
      ;;
    *)
      echo "[ERROR] unsupported intra suffix for config server: ${suffix}" >&2
      exit 1
      ;;
  esac
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
      echo "[INFO] Initialized nginx mapping file: $main_file"
    else
      echo "[ERROR] Missing nginx mapping file: $main_file" >&2
      exit 1
    fi
  fi
  if [[ "${NGINX_PREFIX}" != /* ]]; then
    echo "[ERROR] --nginx-prefix must start with /: ${NGINX_PREFIX}" >&2; exit 1
  fi
  local base_prefix="${NGINX_PREFIX%/}"
  local proxy_prefix="${base_prefix}/"
  begin_marker="# BEGIN managed: intra config ${base_prefix}"
  end_marker="# END managed: intra config ${base_prefix}"
  if grep -Fqx "$begin_marker" "$main_file" && ! grep -Fqx "$end_marker" "$main_file"; then
    echo "[ERROR] nginx_locations.txt has begin marker but missing end marker: ${begin_marker}" >&2
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
        print "# intra config server (HTTP)";
        print prefix " http://127.0.0.1:" port "/";
        print proxy_prefix " http://127.0.0.1:" port "/";
        print end;
        next
    }
    in_block { next }
    {
        if (substr($0, 1, length(prefix)) == prefix && substr($0, length(prefix) + 1, 1) ~ /[[:space:]]/) { next }
        if (substr($0, 1, length(prefix) + 1) == (prefix "/") && substr($0, length(prefix) + 2, 1) ~ /[[:space:]]/) { next }
        print
    }
    END {
        if (!replaced) {
            print "";
            print begin;
            print "# intra config server (HTTP)";
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
    --env-name)   ENV_NAME="${2:-}"; shift 2 ;;
    --target)     TARGET_DIR="${2:-}"; shift 2 ;;
    --exchange)   EXCHANGE="${2:-}"; shift 2 ;;
    --bind)       BIND="${2:-}"; shift 2 ;;
    --port)       PORT="${2:-}"; shift 2 ;;
    --nginx-prefix) NGINX_PREFIX="${2:-}"; shift 2 ;;
    --nginx-port)   NGINX_PORT="${2:-}"; shift 2 ;;
    --nginx-mapping-file) NGINX_MAPPING_FILE="${2:-}"; shift 2 ;;
    --apply-nginx) APPLY_NGINX="1"; shift ;;
    -h|--help)    usage; exit 0 ;;
    *)
      echo "[ERROR] 未知参数: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$ENV_NAME" ]]; then
  echo "[ERROR] 需要 --env-name <exchange>-intra-<suffix>" >&2
  usage; exit 1
fi

ENV_NAME="$(normalize_env_name "$ENV_NAME")"
require_intra_env_name "$ENV_NAME"
ENV_SUFFIX="$(intra_env_suffix "$ENV_NAME")"
case "$ENV_SUFFIX" in
  arb01|arb02|arb03) ;;
  trade)
    echo "[ERROR] intra suffix 'trade' is no longer supported; use arb01/arb02/arb03" >&2
    exit 1
    ;;
  *)
    echo "[ERROR] unsupported intra suffix: ${ENV_SUFFIX}; use arb01/arb02/arb03" >&2
    exit 1
    ;;
esac

if [[ -z "$EXCHANGE" ]]; then
  if [[ "$ENV_NAME" =~ ^([a-z0-9]+)-intra-[a-z0-9][a-z0-9_-]*$ ]]; then
    EXCHANGE="${BASH_REMATCH[1]}"
  fi
fi
EXCHANGE="$(normalize_exchange "$EXCHANGE")"

if [[ -z "$TARGET_DIR" ]]; then
  TARGET_DIR="$HOME/${ENV_NAME}"
fi
target_base="$(basename "$TARGET_DIR")"
target_base="$(normalize_env_name "$target_base")"
if [[ "$target_base" != "$ENV_NAME" ]]; then
  echo "[ERROR] --target basename must match env-name (${ENV_NAME}), got: ${target_base}" >&2
  exit 1
fi

if [[ -z "$PORT" ]]; then
  PORT="$(exchange_default_port "$EXCHANGE" "$ENV_SUFFIX")"
fi

if [[ -z "$NGINX_PREFIX" ]]; then
  NGINX_PREFIX="/intra/${ENV_NAME}/config"
fi

DEST_SCRIPT_DIR="$TARGET_DIR/scripts"
mkdir -p "$DEST_SCRIPT_DIR" "$TARGET_DIR/config"

FILES=(
  "scripts/intra_config_server.py"
  "scripts/arb_per_symbol_overrides.py"
  "scripts/start_intra_config_server.sh"
  "scripts/stop_intra_config_server.sh"
  "intra_scripts/sync_intra_risk_params.py"
  "intra_scripts/sync_intra_symbol_lists.py"
  "intra_scripts/sync_intra_strategy_params.py"
  "intra_scripts/sync_intra_funding_thresholds.py"
  "intra_scripts/sync_intra_spread_thresholds.py"
  "scripts/set_online_futures_leverage.py"
  "scripts/set_bybit_online_collateral.py"
  "scripts/rolling_metrics/sync_rolling_metrics_params.py"
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

cat <<EOF > "$TARGET_DIR/config/intra_config_server.env"
HOST=${BIND}
PORT=${PORT}
DEFAULT_EXCHANGE=${EXCHANGE}
DEFAULT_OPEN_VENUE=${EXCHANGE}-margin
DEFAULT_HEDGE_VENUE=${EXCHANGE}-futures
EOF

upsert_main_nginx_mapping
if [[ "${APPLY_NGINX}" == "1" ]]; then
  echo "[INFO] Applying nginx config (PORT=${NGINX_PORT}, MAPPING_FILE=${NGINX_MAPPING_FILE})"
  (
    cd "$ROOT_DIR"
    PORT="$NGINX_PORT" MAPPING_FILE="$NGINX_MAPPING_FILE" ./scripts/setup_nginx_4191.sh
  )
fi

echo "[INFO] 已部署 intra_config_server 脚本到 $DEST_SCRIPT_DIR"
echo "[INFO] exchange=${EXCHANGE} 端口=${PORT}"
echo "[INFO] 启动: cd $TARGET_DIR && PORT=${PORT} ./scripts/start_intra_config_server.sh"
echo "[INFO] 停止: cd $TARGET_DIR && ./scripts/stop_intra_config_server.sh"
