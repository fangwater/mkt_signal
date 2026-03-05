#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  stop_model_score_rolling.sh [--model-name <name>]

Behavior:
  - Run in deploy dir (for example: ~/model_score_rolling/<model_name>)
  - If --model-name is omitted, infer from deploy dir name
  - Stop pmdaemon process name: model_score_rolling_<model_name>
USAGE
}

MODEL_NAME=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --model-name)
      MODEL_NAME="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$MODEL_NAME" ]]; then
  MODEL_NAME="$(basename "$BASE_DIR")"
fi
if [[ -z "$MODEL_NAME" ]]; then
  echo "[ERROR] cannot infer model_name from base dir: $BASE_DIR" >&2
  exit 1
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

PROC_NAME="model_score_rolling_${MODEL_NAME}"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

find_running_pids() {
  local pids=()
  while IFS= read -r pid; do
    if [[ -n "$pid" && "$pid" != "$$" && "$pid" != "$PPID" ]]; then
      pids+=("$pid")
    fi
  done < <(
    ps -eo pid=,args= | awk -v model="$MODEL_NAME" -v base_dir="$BASE_DIR" '
      index($0, "model_score_rolling") > 0 &&
      index($0, model) > 0 &&
      index($0, base_dir) > 0 &&
      index($0, "stop_model_score_rolling.sh") == 0 &&
      index($0, "awk -v model") == 0 {
        print $1
      }
    '
  )
  if [[ ${#pids[@]} -gt 0 ]]; then
    printf '%s\n' "${pids[@]}"
  fi
}

echo "[INFO] stopping ${PROC_NAME}"
if "${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1; then
  echo "[INFO] stopped ${PROC_NAME}"
else
  echo "[WARN] ${PROC_NAME} not found"
fi

mapfile -t leaked_pids < <(find_running_pids || true)
if [[ ${#leaked_pids[@]} -gt 0 ]]; then
  echo "[WARN] found leaked process after pmdaemon delete: ${leaked_pids[*]}"
  echo "[INFO] sending SIGTERM to leaked process(es)"
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
    echo "[ERROR] failed to kill leaked process(es): ${leaked_pids[*]}" >&2
    exit 1
  fi

  echo "[INFO] leaked process cleanup done"
fi

echo "[INFO] stopped: ${PROC_NAME}"
echo "[INFO] model: ${MODEL_NAME}"
echo "Status: ${PMDAEMON[*]} list"
