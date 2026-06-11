#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
用法: scripts/deploy_tlen_config_server.sh --env-name <name>
                                         [--target <path>]
                                         [--bind 0.0.0.0] [--port 6322]
                                         [--default-venue <binance-futures>]
                                         [--redis-host 127.0.0.1]
                                         [--redis-port 6379]
                                         [--redis-db 0]
                                         [--redis-password <password>]
                                         [--nginx-prefix /shared/<env-name>/tlen]
                                         [--nginx-port 4191]
                                         [--nginx-mapping-file $HOME/nginx_locations.txt]
                                         [--apply-nginx]

说明:
  - 部署共享 tlen_config_server 到 $HOME/<env-name>/（或 --target）。
  - 适用于 model_pub / fusion_factor_pub 共用的阈值/amount/factor_plan/zscore 服务。
  - 默认监听 6322，对应 config/model_pub.toml 里的 tlen_server_base_url=http://127.0.0.1:6322。
  - 可选写入 nginx mapping（默认 /shared/<env-name>/tlen）。
  - env-name 仅要求是非空目录名，用于 JP2 这类主机级共享服务。

示例:
  scripts/deploy_tlen_config_server.sh --env-name tlen_config_shared
  scripts/deploy_tlen_config_server.sh --env-name tlen_config_shared --apply-nginx
  scripts/deploy_tlen_config_server.sh --env-name tlen_config_shared --default-venue binance-futures
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_NAME=""
TARGET_DIR=""
BIND="0.0.0.0"
PORT="6322"
DEFAULT_VENUE="binance-futures"
REDIS_HOST="127.0.0.1"
REDIS_PORT="6379"
REDIS_DB="0"
REDIS_PASSWORD=""
NGINX_PREFIX=""
NGINX_PORT="4191"
NGINX_MAPPING_FILE=""
APPLY_NGINX="0"

normalize_env_name() {
  echo "$1" | tr 'A-Z' 'a-z'
}

require_env_name() {
  local name="$1"
  if [[ -z "$name" || ! "$name" =~ ^[a-z0-9][a-z0-9._-]*$ ]]; then
    echo "[ERROR] env-name must be a non-empty simple directory name (got: ${name})" >&2
    exit 1
  fi
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

  begin_marker="# BEGIN managed: tlen config ${base_prefix}"
  end_marker="# END managed: tlen config ${base_prefix}"

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
        print "# shared tlen config server (HTTP)";
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
            print "# shared tlen config server (HTTP)";
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
    --bind)
      BIND="${2:-}"
      shift 2
      ;;
    --port)
      PORT="${2:-}"
      shift 2
      ;;
    --default-venue)
      DEFAULT_VENUE="${2:-}"
      shift 2
      ;;
    --redis-host)
      REDIS_HOST="${2:-}"
      shift 2
      ;;
    --redis-port)
      REDIS_PORT="${2:-}"
      shift 2
      ;;
    --redis-db)
      REDIS_DB="${2:-}"
      shift 2
      ;;
    --redis-password)
      REDIS_PASSWORD="${2:-}"
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
  echo "[ERROR] 需要使用 --env-name 指定部署环境名（例如 tlen_config_shared）" >&2
  usage
  exit 1
fi

ENV_NAME="$(normalize_env_name "$ENV_NAME")"
require_env_name "$ENV_NAME"

if [[ -n "$PORT" && ! "$PORT" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --port must be numeric: $PORT" >&2
  exit 1
fi
if [[ -n "$REDIS_PORT" && ! "$REDIS_PORT" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --redis-port must be numeric: $REDIS_PORT" >&2
  exit 1
fi
if [[ -n "$REDIS_DB" && ! "$REDIS_DB" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --redis-db must be numeric: $REDIS_DB" >&2
  exit 1
fi

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

if [[ -z "$NGINX_PREFIX" ]]; then
  NGINX_PREFIX="/shared/${ENV_NAME}/tlen"
fi

DEST_SCRIPT_DIR="$TARGET_DIR/scripts"
mkdir -p "$DEST_SCRIPT_DIR"
mkdir -p "$TARGET_DIR/config"

FILES=(
  "scripts/tlen_config_server.py"
  "scripts/start_tlen_config_server.sh"
  "scripts/stop_tlen_config_server.sh"
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

cat <<EOF > "$TARGET_DIR/config/tlen_config_server.env"
HOST=${BIND}
PORT=${PORT}
DEFAULT_VENUE=${DEFAULT_VENUE}
REDIS_HOST=${REDIS_HOST}
REDIS_PORT=${REDIS_PORT}
REDIS_DB=${REDIS_DB}
REDIS_PASSWORD=${REDIS_PASSWORD}
EOF

upsert_main_nginx_mapping
if [[ "${APPLY_NGINX}" == "1" ]]; then
  echo "[INFO] Applying nginx config (PORT=${NGINX_PORT}, MAPPING_FILE=${NGINX_MAPPING_FILE})"
  (
    cd "$ROOT_DIR"
    PORT="$NGINX_PORT" MAPPING_FILE="$NGINX_MAPPING_FILE" ./scripts/setup_nginx_4191.sh
  )
fi

echo "[INFO] 已部署 tlen_config_server 脚本到 $DEST_SCRIPT_DIR"
echo "[INFO] 默认端口: ${PORT}"
echo "[INFO] 默认 venue: ${DEFAULT_VENUE}"
echo "[INFO] Redis: ${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB}"
echo "[INFO] 启动: cd $TARGET_DIR && ./scripts/start_tlen_config_server.sh"
echo "[INFO] 停止: cd $TARGET_DIR && ./scripts/stop_tlen_config_server.sh"
