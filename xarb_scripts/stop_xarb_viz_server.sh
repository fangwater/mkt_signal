#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

usage() {
  cat <<'EOF'
用法: xarb_scripts/stop_xarb_viz_server.sh

说明:
  - 会基于部署目录名推断 open/hedge exchange（目录名需形如 <open>-<hedge>-xarb-...）
  - 停止 pmdaemon 进程：xarb_viz_<open>_<hedge>_<env>
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"

OPEN_EXCHANGE=""
HEDGE_EXCHANGE=""
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]xarb([_-].*)?$ ]]; then
  OPEN_EXCHANGE="${BASH_REMATCH[1]}"
  HEDGE_EXCHANGE="${BASH_REMATCH[2]}"
fi

if [[ "$OPEN_EXCHANGE" == "okx" ]]; then
  OPEN_EXCHANGE="okex"
fi
if [[ "$HEDGE_EXCHANGE" == "okx" ]]; then
  HEDGE_EXCHANGE="okex"
fi
ENV_TAG="xarb"
if [[ "$dir_lc" =~ ^[a-z0-9]+[-_][a-z0-9]+[-_]xarb[-_]([a-z0-9][a-z0-9_-]*)$ ]]; then
  ENV_TAG="${BASH_REMATCH[1]//-/_}"
fi

if [[ -z "$OPEN_EXCHANGE" || -z "$HEDGE_EXCHANGE" ]]; then
  echo "[ERROR] 无法从目录名推断 open/hedge (dir=$dir_name)，期望 <open>-<hedge>-xarb-..."
  exit 1
fi

PROC_NAME="xarb_viz_${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}_${ENV_TAG}"

if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

find_running_pids() {
  local pids=()
  while IFS= read -r pid; do
    if [[ -n "$pid" && "$pid" != "$$" && "$pid" != "$PPID" ]]; then
      pids+=("$pid")
    fi
  done < <(
    ps -eo pid=,args= | awk -v base_dir="$BASE_DIR" '
      index($0, "viz_server") > 0 &&
      index($0, base_dir) > 0 &&
      index($0, "awk -v base_dir=") == 0 &&
      index($0, "stop_xarb_viz_server.sh") == 0 {
        print $1
      }
    '
  )

  if [[ ${#pids[@]} -gt 0 ]]; then
    printf '%s\n' "${pids[@]}"
  fi
}

cleanup_leaked() {
  local pattern="${BASE_DIR}.*viz_server"

  mapfile -t leaked_pids < <(find_running_pids || true)
  if [[ ${#leaked_pids[@]} -eq 0 ]]; then
    return 0
  fi

  echo "[WARN] Found leaked viz_server process after pmdaemon delete: pids=${leaked_pids[*]}"
  echo "[INFO] Sending SIGTERM via pkill: pattern=${pattern}"
  pkill -TERM -f "$pattern" >/dev/null 2>&1 || true

  local deadline=$((SECONDS + KILL_WAIT_SECS))
  while [[ $SECONDS -lt $deadline ]]; do
    mapfile -t leaked_pids < <(find_running_pids || true)
    if [[ ${#leaked_pids[@]} -eq 0 ]]; then
      break
    fi
    sleep 1
  done

  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[WARN] SIGTERM timeout, sending SIGKILL via pkill: pids=${leaked_pids[*]}"
    pkill -KILL -f "$pattern" >/dev/null 2>&1 || true
    sleep 1
    mapfile -t leaked_pids < <(find_running_pids || true)
  fi

  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[ERROR] Failed to kill leaked viz_server process(es): pids=${leaked_pids[*]}" >&2
    return 1
  fi

  echo "[INFO] Leaked process cleanup done"
  return 0
}

echo "[INFO] Deleting ${PROC_NAME}"
if "${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1; then
  echo "[INFO] Deleted ${PROC_NAME}"
else
  echo "[WARN] ${PROC_NAME} not found"
fi

cleanup_leaked

echo ""
echo "[INFO] To view remaining processes: ${PMDAEMON[*]} list"
