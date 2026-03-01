#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="manual_signal"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_fr_manual_signal.sh --env-name <exchange>_fr_<suffix>
                                      [--exchange <binance|okex|bybit|bitget|gate>]
                                      [--port <default>]
                                      [--nginx-prefix /fr/<env-name>/manual_signal]
                                      [--nginx-port 4191]
                                      [--nginx-mapping-file $HOME/nginx_locations.txt]
                                      [--apply-nginx]
                                      [--scripts-only|--bin-only]

说明:
  - 构建并复制 manual_signal 到目标目录（不自动启动）。
  - 默认同步启动/停止脚本，并写入 nginx 映射（managed block）。
  - FR 目标目录:  $HOME/<exchange>_fr_<suffix>/
  - env-name 必须匹配 <exchange>_fr_<suffix>（suffix 必填，例如 binance_fr_hf01）。
  - exchange 可省略，会从 --env-name 推断（如 okex_fr_hf02 -> okex）。
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

default_port_for_exchange() {
  case "$1" in
    okex) echo "8911" ;;
    gate) echo "8921" ;;
    binance) echo "8931" ;;
    bybit) echo "8941" ;;
    bitget) echo "8951" ;;
    *) echo "8911" ;;
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
  local upstream="http://127.0.0.1:${PORT}/"

  begin_marker="# BEGIN managed: fr manual_signal ${base_prefix}"
  end_marker="# END managed: fr manual_signal ${base_prefix}"

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
      -v upstream="$upstream" '
    BEGIN { in_block = 0; replaced = 0 }
    $0 == begin { in_block = 1; replaced = 1; next }
    in_block && $0 == end {
        in_block = 0;
        print begin;
        print "# fr manual_signal (HTTP)";
        print prefix " " upstream;
        print prefix "/ " upstream;
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
            print "# fr manual_signal (HTTP)";
            print prefix " " upstream;
            print prefix "/ " upstream;
            print end;
        }
    }
  ' "$main_file" >"$tmp"
  mv "$tmp" "$main_file"
}

EXCHANGE=""
ENV_NAME=""
PORT=""
DO_BUILD=1
DO_SCRIPTS=1
ONLY_MODE=""

NGINX_PREFIX=""
NGINX_PORT="4191"
NGINX_MAPPING_FILE=""
APPLY_NGINX="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-}"
      if [[ -z "$EXCHANGE" ]]; then
        echo "[ERROR] --exchange 需要一个值"
        exit 1
      fi
      shift 2
      ;;
    --env-name)
      ENV_NAME="${2:-}"
      if [[ -z "$ENV_NAME" ]]; then
        echo "[ERROR] --env-name 需要一个值"
        exit 1
      fi
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
      NGINX_PORT="${2:-4191}"
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
    --scripts-only)
      if [[ -n "$ONLY_MODE" ]]; then
        echo "[ERROR] --scripts-only 与 --bin-only 互斥"
        exit 1
      fi
      ONLY_MODE="scripts"
      DO_BUILD=0
      DO_SCRIPTS=1
      shift
      ;;
    --bin-only)
      if [[ -n "$ONLY_MODE" ]]; then
        echo "[ERROR] --scripts-only 与 --bin-only 互斥"
        exit 1
      fi
      ONLY_MODE="bin"
      DO_BUILD=1
      DO_SCRIPTS=0
      shift
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

normalize_env_name() {
  echo "$1" | tr 'A-Z' 'a-z'
}

normalize_exchange() {
  local ex="${1,,}"
  if [[ "$ex" == "okx" ]]; then
    ex="okex"
  fi
  echo "$ex"
}

infer_exchange_from_env_name() {
  local name="${1,,}"
  if [[ "$name" =~ ^([a-z0-9]+)[-_]fr([_-].*)?$ ]]; then
    echo "${BASH_REMATCH[1]}"
  fi
}

require_fr_env_name() {
  local exchange="$1"
  local name="$2"
  if [[ ! "$name" =~ ^${exchange}_fr_[a-z0-9][a-z0-9_-]*$ ]]; then
    echo "[ERROR] env-name must match ${exchange}_fr_<suffix> (got: ${name})" >&2
    exit 1
  fi
}

if [[ -z "$ENV_NAME" ]]; then
  echo "[ERROR] 需要使用 --env-name 指定部署环境名（例如 binance_fr_hf01）" >&2
  usage
  exit 1
fi
ENV_NAME="$(normalize_env_name "$ENV_NAME")"

if [[ -z "$EXCHANGE" ]]; then
  EXCHANGE="$(infer_exchange_from_env_name "$ENV_NAME")"
fi
EXCHANGE="$(normalize_exchange "$EXCHANGE")"
case "$EXCHANGE" in
  binance|okex|bybit|bitget|gate)
    ;;
  *)
    echo "[ERROR] 无法从 --env-name 推断 exchange，或 --exchange 无效: $EXCHANGE (支持: binance/okex/bybit/bitget/gate)" >&2
    usage
    exit 1
    ;;
esac

require_fr_env_name "$EXCHANGE" "$ENV_NAME"

if [[ -z "$PORT" ]]; then
  PORT="$(default_port_for_exchange "$EXCHANGE")"
fi
if [[ ! "$PORT" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --port 必须是数字: $PORT"
  exit 1
fi

if [[ -z "$NGINX_PREFIX" ]]; then
  NGINX_PREFIX="/fr/${ENV_NAME}/manual_signal"
fi
if [[ -z "$NGINX_MAPPING_FILE" ]]; then
  NGINX_MAPPING_FILE="$HOME/nginx_locations.txt"
fi

TARGET_DIR="$HOME/${ENV_NAME}"

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] 构建 $BIN_NAME (release)"
  cargo build --release --bin "$BIN_NAME"
fi

mkdir -p "$TARGET_DIR"
if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
  cp "$BIN_PATH" "$TARGET_DIR/"
  chmod +x "$TARGET_DIR/$BIN_NAME"
fi

if [[ "$DO_SCRIPTS" -eq 1 ]]; then
  mkdir -p "$TARGET_DIR/scripts"
  EXTRA_FILES=(
    "scripts/start_fr_manual_signal.sh"
    "scripts/stop_fr_manual_signal.sh"
    "scripts/setup_nginx_4191.sh"
  )
  for file in "${EXTRA_FILES[@]}"; do
    SRC="$ROOT_DIR/$file"
    if [[ -f "$SRC" ]]; then
      rsync -a "$SRC" "$TARGET_DIR/$(dirname "$file")/"
      chmod +x "$TARGET_DIR/$file" 2>/dev/null || true
    else
      echo "[WARN] missing: $SRC"
    fi
  done

  upsert_main_nginx_mapping
  if [[ "${APPLY_NGINX}" == "1" ]]; then
    echo "[INFO] Applying nginx config (PORT=${NGINX_PORT}, MAPPING_FILE=${NGINX_MAPPING_FILE})"
    (
      cd "$ROOT_DIR"
      PORT="${NGINX_PORT}" MAPPING_FILE="${NGINX_MAPPING_FILE}" scripts/setup_nginx_4191.sh
    )
  fi
fi

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 启动: cd $TARGET_DIR && ./scripts/start_fr_manual_signal.sh"
if [[ "$DO_SCRIPTS" -eq 1 ]]; then
  echo "[INFO] nginx mapping updated: ${NGINX_MAPPING_FILE}"
  echo "[INFO] To apply nginx (port ${NGINX_PORT}):"
  echo "       cd ${ROOT_DIR} && PORT=${NGINX_PORT} MAPPING_FILE=${NGINX_MAPPING_FILE} scripts/setup_nginx_4191.sh"
fi
