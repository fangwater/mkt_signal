#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/env.sh" || -x "${SCRIPT_DIR}/persist_read_server" ]]; then
  BASE_DIR="${SCRIPT_DIR}"
else
  BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
fi

if [[ -f "${BASE_DIR}/env.sh" ]]; then
  # shellcheck disable=SC1091
  source "${BASE_DIR}/env.sh"
fi

default_config_path() {
  local candidates=(
    "${BASE_DIR}/persist.toml"
    "${BASE_DIR}/config/persist.toml"
  )
  local cand
  for cand in "${candidates[@]}"; do
    if [[ -f "$cand" ]]; then
      echo "$cand"
      return 0
    fi
  done
  echo "${BASE_DIR}/persist.toml"
}

DEFAULT_CONFIG="$(default_config_path)"
CONFIG_PATH="${1:-${PERSIST_READ_SERVER_CONFIG:-${PERSIST_CONFIG:-${DEFAULT_CONFIG}}}}"
if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "[ERROR] config not found: ${CONFIG_PATH}" >&2
  exit 1
fi
CONFIG_ABS="$(cd "$(dirname "$CONFIG_PATH")" && pwd)/$(basename "$CONFIG_PATH")"

BIN_CANDIDATES=(
  "${BASE_DIR}/persist_read_server"
  "${SCRIPT_DIR}/persist_read_server"
  "${BASE_DIR}/target/release/persist_read_server"
  "${SCRIPT_DIR}/../target/release/persist_read_server"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] persist_read_server binary not found. Build/deploy first." >&2
  exit 1
fi

ENV_NAME="$(basename "${BASE_DIR}")"
NAMESPACE="${PM2_NAMESPACE:-${ENV_NAME}}"
PROC_NAME="${PERSIST_READ_PM2_NAME:-${ENV_NAME}_persist_read_server}"
RUST_LOG="${RUST_LOG:-info}"

if [[ -n "${PM2_BIN:-}" ]]; then
  PM2_CMD=( $PM2_BIN )
elif command -v pm2 >/dev/null 2>&1; then
  PM2_CMD=( pm2 )
else
  PM2_CMD=( npx pm2 )
fi

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE}, config=${CONFIG_ABS})"
"${PM2_CMD[@]}" delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

(
  cd "$BASE_DIR"
  RUST_LOG="${RUST_LOG}" "${PM2_CMD[@]}" start "$BIN_PATH" \
    --name "$PROC_NAME" \
    --namespace "$NAMESPACE" \
    -- \
    --config "$CONFIG_ABS"
)

echo ""
echo "[INFO] Started persist_read_server"
echo "Namespace: ${NAMESPACE}"
echo "Process: ${PROC_NAME}"
echo "Logs: ${PM2_CMD[*]} logs --namespace ${NAMESPACE} ${PROC_NAME}"
echo "Status: ${PM2_CMD[*]} status --namespace ${NAMESPACE}"
