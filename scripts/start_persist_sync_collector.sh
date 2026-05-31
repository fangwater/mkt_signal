#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/env.sh" || -x "${SCRIPT_DIR}/persist_sync_collector" ]]; then
  BASE_DIR="${SCRIPT_DIR}"
else
  BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
fi

if [[ -f "${BASE_DIR}/env.sh" ]]; then
  # shellcheck disable=SC1091
  source "${BASE_DIR}/env.sh"
fi

CONFIG_PATH="${1:-${PERSIST_SYNC_COLLECTOR_CONFIG:-${PERSIST_CONFIG:-${BASE_DIR}/sync.toml}}}"
if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "[ERROR] config not found: ${CONFIG_PATH}" >&2
  exit 1
fi
CONFIG_ABS="$(cd "$(dirname "$CONFIG_PATH")" && pwd)/$(basename "$CONFIG_PATH")"

find_bin() {
  local bin_name="$1"
  local candidates=(
    "${BASE_DIR}/${bin_name}"
    "${SCRIPT_DIR}/${bin_name}"
    "${BASE_DIR}/target/release/${bin_name}"
    "${SCRIPT_DIR}/../target/release/${bin_name}"
  )
  local cand
  for cand in "${candidates[@]}"; do
    if [[ -x "$cand" ]]; then
      echo "$cand"
      return 0
    fi
  done
  return 1
}

SYNC_BIN="$(find_bin persist_sync_collector || true)"
if [[ -z "$SYNC_BIN" ]]; then
  echo "[ERROR] persist_sync_collector binary not found. Build/deploy first." >&2
  exit 1
fi

READ_BIN="$(find_bin persist_read_server || true)"

ENV_NAME="$(basename "${BASE_DIR}")"
NAMESPACE="${PM2_NAMESPACE:-${ENV_NAME}}"
SYNC_PROC_NAME="${PERSIST_SYNC_PM2_NAME:-${PM2_NAME:-${ENV_NAME}_persist_sync_collector}}"
READ_PROC_NAME="${PERSIST_READ_PM2_NAME:-${ENV_NAME}_persist_read_server}"
RUST_LOG="${RUST_LOG:-info}"
ENABLE_READ_SERVER="${PERSIST_READ_SERVER_ENABLE:-auto}"

config_has_read_server() {
  grep -Eq '^\[read_server\]' "$CONFIG_ABS"
}

config_read_server_disabled() {
  awk '
    /^\[read_server\]/ { in_read=1; next }
    /^\[/ { in_read=0 }
    in_read && $0 ~ /^[[:space:]]*enabled[[:space:]]*=[[:space:]]*false([[:space:]]*(#.*)?)?$/ { found=1 }
    END { exit found ? 0 : 1 }
  ' "$CONFIG_ABS"
}

read_server_enabled() {
  case "$ENABLE_READ_SERVER" in
    1|true|TRUE|yes|YES|on|ON)
      return 0
      ;;
    0|false|FALSE|no|NO|off|OFF)
      return 1
      ;;
    auto|AUTO|"")
      config_has_read_server || return 1
      config_read_server_disabled && return 1
      return 0
      ;;
    *)
      echo "[ERROR] invalid PERSIST_READ_SERVER_ENABLE=${ENABLE_READ_SERVER}; use auto/true/false" >&2
      exit 1
      ;;
  esac
}

start_pm2() {
  local proc_name="$1"
  local bin_path="$2"
  shift 2

  echo "[INFO] Restarting ${proc_name} (namespace=${NAMESPACE})"
  npx pm2 delete "$proc_name" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
  (
    cd "$BASE_DIR"
    RUST_LOG="${RUST_LOG}" npx pm2 start "$bin_path"       --name "$proc_name"       --namespace "$NAMESPACE"       --       "$@"
  )
}

echo "[INFO] Config: ${CONFIG_ABS}"
start_pm2 "$SYNC_PROC_NAME" "$SYNC_BIN" run-all --config "$CONFIG_ABS"

if read_server_enabled; then
  if [[ -z "$READ_BIN" ]]; then
    echo "[ERROR] read_server is enabled but persist_read_server binary not found. Build/deploy first." >&2
    exit 1
  fi
  start_pm2 "$READ_PROC_NAME" "$READ_BIN" --config "$CONFIG_ABS"
else
  echo "[INFO] read_server not enabled for this config; skip persist_read_server"
fi

echo ""
echo "[INFO] Started persist processes"
echo "Namespace: ${NAMESPACE}"
echo "Collector: ${SYNC_PROC_NAME}"
if read_server_enabled; then
  echo "Read:      ${READ_PROC_NAME}"
fi
echo "Logs: npx pm2 logs --namespace ${NAMESPACE}"
echo "Status: npx pm2 status --namespace ${NAMESPACE}"
