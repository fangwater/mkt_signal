#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")

ensure_pmdaemon() {
  if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
    echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
    echo "[HINT] install with: cargo install pmdaemon" >&2
    exit 1
  fi
}

usage() {
  cat <<'USAGE'
用法: xarb_scripts/stop_xarb_pre_trade.sh

说明:
  - 会基于部署目录名推断 open/hedge exchange（目录名需形如 <open>-<hedge>-xarb-...）
  - 停止 pmdaemon 进程：xarb_pt_<open>_<hedge>
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

normalize_venue() {
  echo "${1,,}"
}

ensure_xarb_venue() {
  local v
  v="$(normalize_venue "$1")"
  if [[ -z "$v" || ! "$v" =~ ^[a-z0-9]+-(margin|futures|spot|swap|perp|perpetual)$ ]]; then
    echo "[ERROR] 非法 xarb venue: $1" >&2
    exit 1
  fi
  echo "$v"
}

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
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

if [[ -z "$OPEN_EXCHANGE" || -z "$HEDGE_EXCHANGE" ]]; then
  echo "[ERROR] 无法从目录名推断 open/hedge (dir=$dir_name)，期望 <open>-<hedge>-xarb-..."
  exit 1
fi

ensure_pmdaemon

OPEN_VENUE="${OPEN_VENUE:-}"
HEDGE_VENUE="${HEDGE_VENUE:-}"
if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  if [[ "$OPEN_EXCHANGE" == "$HEDGE_EXCHANGE" ]]; then
    OPEN_VENUE="${OPEN_VENUE:-${OPEN_EXCHANGE}-margin}"
    HEDGE_VENUE="${HEDGE_VENUE:-${HEDGE_EXCHANGE}-futures}"
  else
    OPEN_VENUE="${OPEN_VENUE:-${OPEN_EXCHANGE}-futures}"
    HEDGE_VENUE="${HEDGE_VENUE:-${HEDGE_EXCHANGE}-futures}"
  fi
fi

OPEN_VENUE="$(ensure_xarb_venue "$OPEN_VENUE")"
HEDGE_VENUE="$(ensure_xarb_venue "$HEDGE_VENUE")"

PROC_NAME="${PMDAEMON_NAME:-${PM2_NAME:-xarb_pt_${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}}}"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

find_running_pids() {
  local open_arg="--open-venue ${OPEN_VENUE}"
  local hedge_arg="--hedge-venue ${HEDGE_VENUE}"
  local pids=()
  while IFS= read -r pid; do
    if [[ -n "$pid" ]]; then
      pids+=("$pid")
    fi
  done < <(
    ps -eo pid=,args= | awk -v base_dir="$BASE_DIR" -v open_arg="$open_arg" -v hedge_arg="$hedge_arg" '
      index($0, "pre_trade") > 0 && index($0, base_dir) > 0 && index($0, open_arg) > 0 && index($0, hedge_arg) > 0 {
        print $1
      }
    '
  )

  if [[ ${#pids[@]} -gt 0 ]]; then
    printf '%s\n' "${pids[@]}"
  fi
}

echo "[INFO] Stopping $PROC_NAME"
if "${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1; then
  echo "[INFO] Stopped $PROC_NAME"
else
  echo "[WARN] Process not found: $PROC_NAME"
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

echo "[INFO] Status: ${PMDAEMON[*]} list"
