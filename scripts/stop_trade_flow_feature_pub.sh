#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENUE_DIR_REGEX='^[a-z0-9]+-(futures|margin)$'

usage() {
  cat <<'USAGE'
Usage:
  stop_trade_flow_feature_pub.sh

Behavior:
  - 必须在单个 venue 部署目录下执行（例如 ~/trade_flow_feature/binance-futures）。
  - venue 由当前目录名自动推断。
  - 删除 pmdaemon 进程名: tff_<ex>_<market>（兼容旧名 trade_flow_feature_pub_<venue>）
  - 可用 PMDAEMON_BIN 覆盖二进制名（默认 pmdaemon）

Examples:
  cd ~/trade_flow_feature/binance-futures
  ./scripts/stop_trade_flow_feature_pub.sh
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

short_market() {
  case "${1,,}" in
    futures) echo "fu" ;;
    margin) echo "mg" ;;
    *)
      echo "${1,,}" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2
      ;;
  esac
}

venue_short_tag() {
  local venue="${1,,}"
  if [[ "$venue" =~ ^([a-z0-9]+)-([a-z0-9]+)$ ]]; then
    echo "$(short_exchange "${BASH_REMATCH[1]}")_$(short_market "${BASH_REMATCH[2]}")"
    return 0
  fi
  echo "$venue" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//'
}

venue="$(basename "${BASE_DIR}" | tr '[:upper:]' '[:lower:]')"
if [[ ! "$venue" =~ $VENUE_DIR_REGEX ]]; then
  echo "[ERROR] 当前目录无法推断 venue: ${BASE_DIR}" >&2
  echo "[ERROR] 期望目录名形如 <exchange>-<market>，例如 binance-futures" >&2
  exit 1
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

name="tff_$(venue_short_tag "$venue")"
legacy_name="trade_flow_feature_pub_${venue}"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

find_running_pids() {
  local venue_arg="--venue ${venue}"
  local pids=()
  while IFS= read -r pid; do
    if [[ -n "$pid" && "$pid" != "$$" && "$pid" != "$PPID" ]]; then
      pids+=("$pid")
    fi
  done < <(
    ps -eo pid=,args= | awk -v venue_arg="$venue_arg" -v base_dir="$BASE_DIR" '
      index($0, "trade_flow_feature_pub") > 0 &&
      index($0, venue_arg) > 0 &&
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

echo "[INFO] Stopping ${name}"
stopped=false
if "${PMDAEMON[@]}" delete "$name" >/dev/null 2>&1; then
  stopped=true
fi
if [[ "$legacy_name" != "$name" ]] && "${PMDAEMON[@]}" delete "$legacy_name" >/dev/null 2>&1; then
  stopped=true
fi
if [[ "$stopped" == true ]]; then
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
echo "Venue: ${venue}"
echo "Status: ${PMDAEMON[*]} list"
