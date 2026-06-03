#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")

ensure_pmdaemon() {
  if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
    echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2; exit 1
  fi
}

ensure_pmdaemon

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"
EXCHANGE=""
ENV_TAG="intra"
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]intra[-_]([a-z0-9][a-z0-9_-]*)$ ]]; then
  EXCHANGE="${BASH_REMATCH[1]}"
  ENV_TAG="${BASH_REMATCH[2]//-/_}"
elif [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]intra$ ]]; then
  EXCHANGE="${BASH_REMATCH[1]}"
fi
if [[ "$EXCHANGE" == "okx" ]]; then EXCHANGE="okex"; fi
if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 无法从目录名推断 exchange (dir=$dir_name)"; exit 1
fi

PROC_NAME="intra_viz_${EXCHANGE}_${ENV_TAG}"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

# pmdaemon delete 只会清自己的注册条目，不保证 OS 进程被信号；leak 的 viz_server
# 还会霸占着监听端口，重启时新进程 bind 报 `Address already in use`。
find_running_pids() {
  local pids=()
  local expected_bin="${BASE_DIR}/viz_server"
  local expected_real=""
  if [[ -e "$expected_bin" ]]; then
    expected_real="$(readlink -f "$expected_bin" 2>/dev/null || true)"
  fi
  while IFS= read -r pid; do
    if [[ -z "$pid" || "$pid" == "$$" || "$pid" == "${BASHPID:-}" ]]; then
      continue
    fi
    local exe_link="" exe_path="" cmdline=""
    exe_link="$(readlink "/proc/${pid}/exe" 2>/dev/null || true)"
    exe_path="$(readlink -f "/proc/${pid}/exe" 2>/dev/null || true)"
    cmdline="$(tr '\0' ' ' <"/proc/${pid}/cmdline" 2>/dev/null || true)"
    if [[ -n "$expected_real" && "$exe_path" == "$expected_real" ]]; then
      pids+=("$pid")
    elif [[ "$exe_link" == "${BASE_DIR}/viz_server" || "$exe_link" == "${BASE_DIR}/viz_server (deleted)" ]]; then
      pids+=("$pid")
    elif [[ "$exe_path" == "${SCRIPT_DIR}/viz_server" || "$exe_path" == "${BASE_DIR}/target/release/viz_server" ]]; then
      pids+=("$pid")
    elif [[ "$cmdline" == "${BASE_DIR}/viz_server"* || "$cmdline" == "${SCRIPT_DIR}/viz_server"* || "$cmdline" == "${BASE_DIR}/target/release/viz_server"* ]]; then
      pids+=("$pid")
    fi
  done < <(
    ps -eo pid=,comm= | awk '$2 == "viz_server" { print $1 }'
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
