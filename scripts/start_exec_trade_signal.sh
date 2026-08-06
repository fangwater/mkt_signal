#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

[[ $# -eq 0 ]] || { echo "[ERROR] start_exec_trade_signal.sh takes no arguments" >&2; exit 1; }
[[ -n "${IPC_NAMESPACE:-}" ]] || { echo "[ERROR] IPC_NAMESPACE is required" >&2; exit 1; }

BIN_PATH=""
for candidate in "${BASE_DIR}/trade_signal" "${BASE_DIR}/target/release/trade_signal"; do
  if [[ -x "$candidate" ]]; then
    BIN_PATH="$candidate"
    break
  fi
done
[[ -n "$BIN_PATH" ]] || { echo "[ERROR] trade_signal binary not found" >&2; exit 1; }

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null 2>&1 || { echo "[ERROR] pmdaemon not found: ${PMDAEMON_BIN}" >&2; exit 1; }

dir_tag="$(basename "$BASE_DIR" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/_/g')"
PROC_NAME="${PMDAEMON_NAME:-exec_ts_${dir_tag}}"
CORE="${TRADE_SIGNAL_CORE:-}"
ARGS=()
if [[ -n "$CORE" ]]; then
  [[ "$CORE" =~ ^[0-9]+$ ]] || { echo "[ERROR] TRADE_SIGNAL_CORE must be an integer" >&2; exit 1; }
  ARGS+=(--core "$CORE")
fi

json_args=""
for value in "${ARGS[@]}"; do
  escaped="$(printf '%s' "$value" | sed 's/\\/\\\\/g; s/"/\\"/g')"
  [[ -n "$json_args" ]] && json_args+=","
  json_args+="\"${escaped}\""
done

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file"' EXIT
cat >"$cfg_file" <<JSON
{"apps":[{"name":"${PROC_NAME}","script":"${BIN_PATH}","args":[${json_args}],"cwd":"${BASE_DIR}","env":{"RUST_LOG":"${RUST_LOG:-info}","IPC_NAMESPACE":"${IPC_NAMESPACE}"}}]}
JSON

PMDAEMON_NAME="$PROC_NAME" "${SCRIPT_DIR}/stop_exec_trade_signal.sh"
echo "[INFO] Starting ${PROC_NAME}; Exec branch and venue will be inferred from cwd=${BASE_DIR}"
"$PMDAEMON_BIN" --config "$cfg_file" start --name "$PROC_NAME"
echo "[INFO] Logs: ${PMDAEMON_BIN} logs ${PROC_NAME} --follow"
