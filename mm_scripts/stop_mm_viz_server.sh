#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

usage() {
  cat <<'EOF'
Usage: mm_scripts/stop_mm_viz_server.sh [--exchange <binance|okex|gate|bybit|bitget>]

Notes:
  - Exchange is inferred from the directory name (<exchange>_mm_<env>), unless --exchange is set.
  - Stops pmdaemon process: viz_server_<dir>
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

EXCHANGE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    *)
      echo "[ERROR] Unknown arg: $1"
      usage
      exit 1
      ;;
  esac
done

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"
dir_tag="$(echo "${dir_lc}" | sed 's/[^a-z0-9_-]/_/g')"
if [[ -z "$EXCHANGE" ]]; then
  if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]mm([_-].*)?$ ]]; then
    EXCHANGE="${BASH_REMATCH[1]}"
  fi
fi

EXCHANGE="${EXCHANGE,,}"
if [[ "$EXCHANGE" == "okx" ]]; then
  EXCHANGE="okex"
fi
if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] Failed to infer exchange; please pass --exchange"
  exit 1
fi

PROC_NAME="${PMDAEMON_NAME:-viz_server_${dir_tag}}"

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
      index($0, "stop_mm_viz_server.sh") == 0 {
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
