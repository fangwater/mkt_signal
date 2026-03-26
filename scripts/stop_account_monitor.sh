#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
MM_NAME_LIB="${SCRIPT_DIR}/mm_process_name.sh"

if [[ -f "$MM_NAME_LIB" ]]; then
  # shellcheck disable=SC1090
  source "$MM_NAME_LIB"
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"
dir_tag="$(echo "${dir_lc}" | sed 's/[^a-z0-9_-]/_/g')"
MODE=""
EXCHANGE=""
ENV_TAG=""
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]fr([_-](.+))?$ ]]; then
  MODE="fr"
  EXCHANGE="${BASH_REMATCH[1]}"
  ENV_TAG="$(echo "${BASH_REMATCH[3]:-fr}" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//')"
elif type mm_parse_deploy_dir >/dev/null 2>&1 && read -r EXCHANGE ENV_TAG < <(mm_parse_deploy_dir "$dir_lc"); then
  MODE="mm"
else
  echo "[ERROR] 无法从部署目录名推断 account_monitor 环境: ${dir_name}" >&2
  echo "[ERROR] 期望如 okex_fr_trade / binance_mm_alpha" >&2
  exit 1
fi

if type mm_normalize_exchange >/dev/null 2>&1; then
  EXCHANGE="$(mm_normalize_exchange "$EXCHANGE")"
fi
if [[ -z "$ENV_TAG" ]]; then
  ENV_TAG="$MODE"
fi

if [[ "$MODE" == "mm" ]]; then
  DEFAULT_PROC_NAME="mm_am_${EXCHANGE}_${ENV_TAG}"
else
  short_exchange() {
    if type mm_short_exchange >/dev/null 2>&1; then
      mm_short_exchange "$1"
      return
    fi
    case "${1,,}" in
      binance) echo "bn" ;;
      okex) echo "ok" ;;
      bybit) echo "bb" ;;
      bitget) echo "bg" ;;
      gate) echo "gt" ;;
      *)
        echo "${1,,}" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2
        ;;
    esac
  }
  DEFAULT_PROC_NAME="am_$(short_exchange "$EXCHANGE")_${ENV_TAG}"
fi

PROC_NAME="${PMDAEMON_NAME:-$DEFAULT_PROC_NAME}"
LEGACY_PROC_NAME="account_monitor_${dir_tag}"
BUGGY_MM_PROC_NAME=""
if [[ "$MODE" == "mm" ]]; then
  if type mm_short_exchange >/dev/null 2>&1; then
    BUGGY_MM_PROC_NAME="am_$(mm_short_exchange "$EXCHANGE")_fr"
  else
    BUGGY_MM_PROC_NAME="am_$(printf '%s' "${EXCHANGE,,}" | cut -c1-2)_fr"
  fi
fi
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

find_running_pids() {
  local pids=()
  while IFS= read -r pid; do
    if [[ -n "$pid" && "$pid" != "$$" && "$pid" != "$PPID" ]]; then
      pids+=("$pid")
    fi
  done < <(
    ps -eo pid=,args= | awk -v base_dir="$BASE_DIR" '
      index($0, "account_monitor") > 0 &&
      index($0, base_dir) > 0 &&
      index($0, "awk -v ") == 0 {
        print $1
      }
    '
  )

  if [[ ${#pids[@]} -gt 0 ]]; then
    printf '%s\n' "${pids[@]}"
  fi
}

echo "[INFO] 停止 ${PROC_NAME}"
deleted=false
if "${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1; then
  deleted=true
fi
if [[ "$LEGACY_PROC_NAME" != "$PROC_NAME" ]] && "${PMDAEMON[@]}" delete "$LEGACY_PROC_NAME" >/dev/null 2>&1; then
  deleted=true
fi
if [[ -n "$BUGGY_MM_PROC_NAME" && "$BUGGY_MM_PROC_NAME" != "$PROC_NAME" ]] && "${PMDAEMON[@]}" delete "$BUGGY_MM_PROC_NAME" >/dev/null 2>&1; then
  deleted=true
fi
if [[ "$deleted" == true ]]; then
  echo "[INFO] 已停止 ${PROC_NAME}"
else
  echo "[WARN] 未找到进程 ${PROC_NAME}"
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
