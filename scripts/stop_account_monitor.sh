#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

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
case "$dir_name" in
  okex_fr_*|*okex*|*OKEX*) EXCHANGE="okex" ;;
  binance_fr_*|*binance*|*BINANCE*) EXCHANGE="binance" ;;
  gate_fr_*|*gate*|*GATE*) EXCHANGE="gate" ;;
  bitget_fr_*|*bitget*|*BITGET*) EXCHANGE="bitget" ;;
  *)
    echo "[ERROR] 无法从部署目录名推断 exchange: ${dir_name} (期望 okex_fr_* / binance_fr_* / gate_fr_* / bitget_fr_*)" >&2
    exit 1
    ;;
esac

short_exchange() {
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

env_tag="fr"
if [[ "$dir_lc" =~ ^[a-z0-9]+[-_]fr[-_](.+)$ ]]; then
  env_tag="$(echo "${BASH_REMATCH[1]}" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//')"
fi
if [[ -z "$env_tag" ]]; then
  env_tag="fr"
fi

PROC_NAME="${PMDAEMON_NAME:-am_$(short_exchange "$EXCHANGE")_${env_tag}}"
LEGACY_PROC_NAME="account_monitor_${dir_tag}"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

find_running_pids() {
  local pids=()
  while IFS= read -r pid; do
    if [[ -n "$pid" ]]; then
      pids+=("$pid")
    fi
  done < <(
    ps -eo pid=,args= | awk -v base_dir="$BASE_DIR" '
      index($0, "account_monitor") > 0 && index($0, base_dir) > 0 {
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
