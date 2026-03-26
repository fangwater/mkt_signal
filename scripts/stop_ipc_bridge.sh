#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  stop_ipc_bridge.sh [--cfg <path>]

Behavior:
  - Run inside a bridge deploy directory, e.g. ~/bridge_jp
  - Default process name: bridge_<env>
  - Default config path: ./config/ipc_bridge.yaml
  - Deletes the pmdaemon process and force-cleans leaked bridge processes
USAGE
}

dir_name="$(basename "${BASE_DIR}")"
dir_lc="$(echo "${dir_name}" | tr 'A-Z' 'a-z')"

infer_env_from_dir() {
  local name="${1,,}"
  if [[ "$name" =~ ^bridge[_-]([a-z0-9][a-z0-9_-]*)$ ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

ENV_TAG=""
if ! ENV_TAG="$(infer_env_from_dir "$dir_lc")"; then
  echo "[ERROR] not a bridge env dir: ${dir_name} (expect bridge_jp / bridge_hk)" >&2
  exit 1
fi

CFG_PATH="${BASE_DIR}/config/ipc_bridge.yaml"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cfg)
      CFG_PATH="${2:-}"
      if [[ -z "$CFG_PATH" ]]; then
        echo "[ERROR] --cfg requires a value" >&2
        exit 1
      fi
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

if [[ "$CFG_PATH" != /* ]]; then
  CFG_PATH="${BASE_DIR}/${CFG_PATH#./}"
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

PROC_NAME="bridge_${ENV_TAG}"
LEGACY_PROC_NAME="ipc_bridge_${dir_lc}"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

find_running_pids() {
  local pids=()
  while IFS= read -r pid; do
    if [[ -n "$pid" && "$pid" != "$$" && "$pid" != "$PPID" ]]; then
      pids+=("$pid")
    fi
  done < <(
    ps -eo pid=,args= | awk -v cfg_arg="--cfg ${CFG_PATH}" -v base_dir="$BASE_DIR" '
      index($0, "ipc_bridge") > 0 &&
      index($0, cfg_arg) > 0 &&
      index($0, base_dir) > 0 &&
      index($0, "awk -v ") == 0 &&
      index($0, "start_ipc_bridge.sh") == 0 &&
      index($0, "stop_ipc_bridge.sh") == 0 {
        print $1
      }
    '
  )

  if [[ ${#pids[@]} -gt 0 ]]; then
    printf '%s\n' "${pids[@]}"
  fi
}

echo "[INFO] Stopping ${PROC_NAME}"
stopped=false
if "${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1; then
  stopped=true
fi
if [[ "$LEGACY_PROC_NAME" != "$PROC_NAME" ]] && "${PMDAEMON[@]}" delete "$LEGACY_PROC_NAME" >/dev/null 2>&1; then
  stopped=true
fi
if [[ "$stopped" == true ]]; then
  echo "[INFO] Stopped ${PROC_NAME}"
else
  echo "[WARN] ${PROC_NAME} not found"
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

echo "Status: ${PMDAEMON[*]} list"
