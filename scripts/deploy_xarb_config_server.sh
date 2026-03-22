#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
用法: scripts/deploy_xarb_config_server.sh --env-name <open>-<hedge>-xarb-<suffix>
                                         [--open-venue <okex-futures>]
                                         [--hedge-venue <binance-futures>]
                                         [--target <path>]
                                         [--bind 0.0.0.0] [--port <default>]
                                         [--nginx-prefix /xarb/<env-name>/config]
                                         [--nginx-port 4191]
                                         [--nginx-mapping-file $HOME/nginx_locations.txt]
                                         [--apply-nginx]

说明:
  - 部署 xarb_config_server 到 $HOME/<env-name>/（或 --target）。
  - env-name/目标目录名必须匹配 <open>-<hedge>-xarb-<suffix>，例如 okex-binance-xarb-trade。
  - open/hedge venue 可省略，会从 env-name 推断为 <exchange>-futures。
  - 默认端口按 open/hedge 组合映射：18100 + open_rank*10 + hedge_rank。
  - 可选写入 nginx mapping（/xarb/<env-name>/config）。

示例:
  scripts/deploy_xarb_config_server.sh --env-name okex-binance-xarb-trade
  scripts/deploy_xarb_config_server.sh --env-name okex-binance-xarb-trade --apply-nginx
  scripts/deploy_xarb_config_server.sh --env-name gate-binance-xarb-trade --port 18151
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_NAME=""
TARGET_DIR=""
OPEN_VENUE=""
HEDGE_VENUE=""
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

infer_pair_from_env_name() {
  local name="${1,,}"
  if [[ "$name" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]xarb([_-].*)?$ ]]; then
    echo "$(normalize_exchange "${BASH_REMATCH[1]}"),$(normalize_exchange "${BASH_REMATCH[2]}")"
  fi
}

require_xarb_env_name() {
  local name="$1"
  if [[ ! "$name" =~ ^[a-z0-9]+-[a-z0-9]+-xarb-[a-z0-9][a-z0-9_-]*$ ]]; then
    echo "[ERROR] env-name must match <open>-<hedge>-xarb-<suffix> (got: ${name})" >&2
    exit 1
  fi
}

ensure_xarb_venue() {
  local v="${1,,}"
  if [[ -z "$v" || ! "$v" =~ ^[a-z0-9]+-(margin|futures|spot|swap|perp|perpetual)$ ]]; then
    echo "[ERROR] 非法 xarb venue: $1" >&2
    exit 1
  fi
  echo "$v"
}

exchange_rank() {
  case "$1" in
    binance) echo "1" ;;
    okex) echo "2" ;;
    bybit) echo "3" ;;
    bitget) echo "4" ;;
    gate) echo "5" ;;
    *) echo "0" ;;
  esac
}

default_port_for_pair() {
  local open_ex="$1"
  local hedge_ex="$2"
  local open_rank hedge_rank
  open_rank="$(exchange_rank "$open_ex")"
  hedge_rank="$(exchange_rank "$hedge_ex")"
  if [[ "$open_rank" == "0" || "$hedge_rank" == "0" ]]; then
    echo "18111"
    return
  fi
  echo $((18100 + open_rank * 10 + hedge_rank))
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
  begin_marker="# BEGIN managed: xarb config ${base_prefix}"
  end_marker="# END managed: xarb config ${base_prefix}"

  if grep -Fqx "$begin_marker" "$main_file" && ! grep -Fqx "$end_marker" "$main_file"; then
    echo "[ERROR] nginx_locations.txt has begin marker but missing end marker:" >&2
    echo "        ${begin_marker}" >&2
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
        print "# xarb config server (HTTP)";
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
            print "# xarb config server (HTTP)";
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
    --env-name)
      ENV_NAME="${2:-}"
      shift 2
      ;;
    --target)
      TARGET_DIR="${2:-}"
      shift 2
      ;;
    --open-venue)
      OPEN_VENUE="${2:-}"
      shift 2
      ;;
    --hedge-venue)
      HEDGE_VENUE="${2:-}"
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
      echo "[ERROR] 未知参数: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$ENV_NAME" ]]; then
  echo "[ERROR] 需要使用 --env-name 指定部署环境名（例如 okex-binance-xarb-trade）" >&2
  usage
  exit 1
fi

ENV_NAME="$(normalize_env_name "$ENV_NAME")"
require_xarb_env_name "$ENV_NAME"

if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  if inferred="$(infer_pair_from_env_name "$ENV_NAME")" && [[ -n "$inferred" ]]; then
    if [[ "${inferred%%,*}" == "${inferred##*,}" ]]; then
      OPEN_VENUE="${OPEN_VENUE:-${inferred%%,*}-margin}"
      HEDGE_VENUE="${HEDGE_VENUE:-${inferred##*,}-futures}"
    else
      OPEN_VENUE="${OPEN_VENUE:-${inferred%%,*}-futures}"
      HEDGE_VENUE="${HEDGE_VENUE:-${inferred##*,}-futures}"
    fi
  fi
fi

OPEN_VENUE="$(ensure_xarb_venue "$OPEN_VENUE")"
HEDGE_VENUE="$(ensure_xarb_venue "$HEDGE_VENUE")"
if [[ "$OPEN_VENUE" == "$HEDGE_VENUE" ]]; then
  echo "[ERROR] xarb open/hedge venue 不能完全相同：open=${OPEN_VENUE} hedge=${HEDGE_VENUE}" >&2
  exit 1
fi

OPEN_EXCHANGE="$(normalize_exchange "${OPEN_VENUE%%-*}")"
HEDGE_EXCHANGE="$(normalize_exchange "${HEDGE_VENUE%%-*}")"

if [[ -z "$TARGET_DIR" ]]; then
  TARGET_DIR="$HOME/${ENV_NAME}"
fi
if [[ -n "$TARGET_DIR" ]]; then
  target_base="$(basename "$TARGET_DIR")"
  target_base="$(normalize_env_name "$target_base")"
  if [[ "$target_base" != "$ENV_NAME" ]]; then
    echo "[ERROR] --target basename must match env-name (${ENV_NAME}), got: ${target_base}" >&2
    exit 1
  fi
fi

if [[ -z "$PORT" ]]; then
  PORT="$(default_port_for_pair "$OPEN_EXCHANGE" "$HEDGE_EXCHANGE")"
fi

if [[ -z "$NGINX_PREFIX" ]]; then
  NGINX_PREFIX="/xarb/${ENV_NAME}/config"
fi

DEST_SCRIPT_DIR="$TARGET_DIR/scripts"
mkdir -p "$DEST_SCRIPT_DIR"
mkdir -p "$TARGET_DIR/config"

FILES=(
  "scripts/xarb_config_server.py"
  "scripts/start_xarb_config_server.sh"
  "scripts/stop_xarb_config_server.sh"
  "xarb_scripts/sync_xarb_risk_params.py"
  "xarb_scripts/sync_xarb_symbol_lists.py"
  "xarb_scripts/sync_xarb_strategy_params.py"
  "xarb_scripts/sync_xarb_funding_thresholds.py"
  "xarb_scripts/sync_xarb_spread_thresholds.py"
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

cat <<EOF > "$TARGET_DIR/config/xarb_config_server.env"
HOST=${BIND}
PORT=${PORT}
DEFAULT_EXCHANGE=${OPEN_EXCHANGE}
DEFAULT_OPEN_VENUE=${OPEN_VENUE}
DEFAULT_HEDGE_VENUE=${HEDGE_VENUE}
EOF

upsert_main_nginx_mapping
if [[ "${APPLY_NGINX}" == "1" ]]; then
  echo "[INFO] Applying nginx config (PORT=${NGINX_PORT}, MAPPING_FILE=${NGINX_MAPPING_FILE})"
  (
    cd "$ROOT_DIR"
    PORT="$NGINX_PORT" MAPPING_FILE="$NGINX_MAPPING_FILE" ./scripts/setup_nginx_4191.sh
  )
fi

echo "[INFO] 已部署 xarb_config_server 脚本到 $DEST_SCRIPT_DIR"
echo "[INFO] 默认端口: ${PORT}"
echo "[INFO] 默认 venues: open=${OPEN_VENUE} hedge=${HEDGE_VENUE}"
echo "[INFO] 启动: cd $TARGET_DIR && PORT=${PORT} ./scripts/start_xarb_config_server.sh"
echo "[INFO] 停止: cd $TARGET_DIR && ./scripts/stop_xarb_config_server.sh"
