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
用法: xarb_scripts/stop_xarb_monitors.sh [open|hedge|all]

说明:
  - 会基于部署目录名推断 open/hedge exchange（目录名需形如 <open>-<hedge>-xarb-...）
  - 默认 all：跨所停止两个 pmdaemon 进程；同所只停止一个
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

MODE="${1:-all}"
case "$MODE" in
  open|hedge|all) ;;
  *)
    echo "[ERROR] 未知参数: $MODE"
    usage
    exit 1
    ;;
esac

ensure_pmdaemon

normalize_exchange() {
  local ex="${1,,}"
  if [[ "$ex" == "okx" ]]; then
    ex="okex"
  fi
  echo "$ex"
}

infer_env_tag_from_dir() {
  local name="${1,,}"
  if [[ "$name" =~ ^[a-z0-9]+[-_][a-z0-9]+[-_]xarb[-_]([a-z0-9][a-z0-9_-]*)[-_](open|hedge)$ ]]; then
    echo "${BASH_REMATCH[1]//-/_}"
    return
  fi
  if [[ "$name" =~ ^[a-z0-9]+[-_][a-z0-9]+[-_]xarb[-_]([a-z0-9][a-z0-9_-]*)$ ]]; then
    echo "${BASH_REMATCH[1]//-/_}"
    return
  fi
  echo "xarb"
}

infer_side_from_dir() {
  local name="${1,,}"
  if [[ "$name" =~ ^[a-z0-9]+[-_][a-z0-9]+[-_]xarb[-_][a-z0-9][a-z0-9_-]*[-_](open|hedge)$ ]]; then
    echo "${BASH_REMATCH[1]}"
    return
  fi
  echo ""
}

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"

OPEN_EXCHANGE=""
HEDGE_EXCHANGE=""
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]xarb([_-].*)?$ ]]; then
  OPEN_EXCHANGE="$(normalize_exchange "${BASH_REMATCH[1]}")"
  HEDGE_EXCHANGE="$(normalize_exchange "${BASH_REMATCH[2]}")"
fi

if [[ -z "$OPEN_EXCHANGE" || -z "$HEDGE_EXCHANGE" ]]; then
  echo "[ERROR] 无法从目录名推断 open/hedge (dir=$dir_name)，期望 <open>-<hedge>-xarb-..."
  exit 1
fi

ENV_TAG="$(infer_env_tag_from_dir "$dir_name")"
SIDE_TAG="$(infer_side_from_dir "$dir_name")"

KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

proc_base_name() {
  if [[ "$OPEN_EXCHANGE" == "$HEDGE_EXCHANGE" ]]; then
    echo "xarb_am_${OPEN_EXCHANGE}_${ENV_TAG}"
  else
    echo "xarb_am_${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}_${ENV_TAG}"
  fi
}

proc_name_for_side() {
  local side="$1"
  local base
  base="$(proc_base_name)"
  if [[ "$OPEN_EXCHANGE" == "$HEDGE_EXCHANGE" ]]; then
    echo "$base"
  else
    echo "${base}_${side}"
  fi
}

find_running_pids() {
  local exchange="$1"
  local pids=()
  while IFS= read -r pid; do
    if [[ -n "$pid" ]]; then
      pids+=("$pid")
    fi
  done < <(
    ps -eo pid=,args= | awk -v exchange="$exchange" -v base_dir="$BASE_DIR" '
      index($0, "account_monitor") > 0 && index($0, exchange) > 0 && index($0, base_dir) > 0 {
        print $1
      }
    '
  )

  if [[ ${#pids[@]} -gt 0 ]]; then
    printf '%s\n' "${pids[@]}"
  fi
}

cleanup_leaked() {
  local exchange="$1"

  mapfile -t leaked_pids < <(find_running_pids "$exchange" || true)
  if [[ ${#leaked_pids[@]} -eq 0 ]]; then
    return 0
  fi

  echo "[WARN] Found leaked account_monitor process(es): ${leaked_pids[*]}"
  echo "[INFO] Sending SIGTERM"
  kill "${leaked_pids[@]}" >/dev/null 2>&1 || true

  deadline=$((SECONDS + KILL_WAIT_SECS))
  while [[ $SECONDS -lt $deadline ]]; do
    mapfile -t leaked_pids < <(find_running_pids "$exchange" || true)
    if [[ ${#leaked_pids[@]} -eq 0 ]]; then
      break
    fi
    sleep 1
  done

  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[WARN] SIGTERM timeout, sending SIGKILL: ${leaked_pids[*]}"
    kill -9 "${leaked_pids[@]}" >/dev/null 2>&1 || true
    sleep 1
    mapfile -t leaked_pids < <(find_running_pids "$exchange" || true)
  fi

  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[ERROR] Failed to kill leaked process(es): ${leaked_pids[*]}" >&2
    return 1
  fi

  return 0
}

stop_one() {
  local side="$1"
  local exchange="$2"
  local proc_name
  proc_name="$(proc_name_for_side "$side")"

  echo "[INFO] Stopping $proc_name"
  if "${PMDAEMON[@]}" delete "$proc_name" >/dev/null 2>&1; then
    echo "[INFO] Stopped $proc_name"
  else
    echo "[WARN] Process not found: $proc_name"
  fi

  cleanup_leaked "$exchange"
}

case "$MODE" in
  open)
    stop_one "open" "$OPEN_EXCHANGE"
    ;;
  hedge)
    stop_one "hedge" "$HEDGE_EXCHANGE"
    ;;
  all)
    if [[ "$SIDE_TAG" == "hedge" ]]; then
      stop_one "hedge" "$HEDGE_EXCHANGE"
    elif [[ "$SIDE_TAG" == "open" ]]; then
      stop_one "open" "$OPEN_EXCHANGE"
    else
      stop_one "open" "$OPEN_EXCHANGE"
    fi
    if [[ -z "$SIDE_TAG" && "$HEDGE_EXCHANGE" != "$OPEN_EXCHANGE" ]]; then
      stop_one "hedge" "$HEDGE_EXCHANGE"
    fi
    ;;
esac

echo "[INFO] Status: ${PMDAEMON[*]} list"
