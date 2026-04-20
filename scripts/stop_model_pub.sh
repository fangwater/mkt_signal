#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROCESS_MATCH_LIB="${SCRIPT_DIR}/process_match_lib.sh"

if [[ -f "$PROCESS_MATCH_LIB" ]]; then
  # shellcheck disable=SC1090
  source "$PROCESS_MATCH_LIB"
fi

usage() {
  cat <<'USAGE'
Usage:
  stop_model_pub.sh

Behavior:
  - 必须在 model_pub 部署目录下执行（例如 ~/model_pub/binance_futures_direction_model）
  - model_name 由当前目录名自动推断
  - 默认对应的 warming 目录约定为 ./history_ylabel
  - 停止 pmdaemon 进程名: model_pub_<model_name>
  - 可用 PMDAEMON_BIN 覆盖二进制名（默认 pmdaemon）

Examples:
  cd ~/model_pub/binance_futures_direction_model
  ./scripts/stop_model_pub.sh
USAGE
}

if [[ $# -gt 0 ]]; then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 不支持参数: $*" >&2
      usage >&2
      exit 1
      ;;
  esac
fi

MODEL_NAME="$(basename "${BASE_DIR}")"
if [[ -z "$MODEL_NAME" ]]; then
  echo "[ERROR] 无法从目录名推断 model_name: ${BASE_DIR}" >&2
  exit 1
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

name="model_pub_${MODEL_NAME}"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

find_running_pids() {
  safe_find_running_pids "model_pub" "$MODEL_NAME"
}

echo "[INFO] Stopping ${name}"
if "${PMDAEMON[@]}" delete "$name" >/dev/null 2>&1; then
  echo "[INFO] Stopped ${name}"
else
  echo "[WARN] ${name} not found"
fi

mapfile -t leaked_pids < <(find_running_pids || true)
if [[ ${#leaked_pids[@]} -gt 0 ]]; then
  echo "[WARN] Found leaked process after pmdaemon delete: ${leaked_pids[*]}"
  echo "[INFO] Sending SIGTERM to leaked process(es)"
  kill "${leaked_pids[@]}" >/dev/null 2>&1 || true

  deadline=$((SECONDS + KILL_WAIT_SECS))
  while [[ $SECONDS -lt $deadline ]]; do
    mapfile -t leaked_pids < <(find_running_pids || true)
    if [[ ${#leaked_pids[@]} -eq 0 ]]; then
      break
    fi
    sleep 1
  done

  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[WARN] SIGTERM timeout, sending SIGKILL: ${leaked_pids[*]}"
    kill -9 "${leaked_pids[@]}" >/dev/null 2>&1 || true
    sleep 1
    mapfile -t leaked_pids < <(find_running_pids || true)
  fi

  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[ERROR] Failed to kill leaked process(es): ${leaked_pids[*]}" >&2
    exit 1
  fi

  echo "[INFO] Leaked process cleanup done"
fi

echo ""
echo "[INFO] Stopped: ${name}"
echo "Model: ${MODEL_NAME}"
echo "Status: ${PMDAEMON[*]} list"
