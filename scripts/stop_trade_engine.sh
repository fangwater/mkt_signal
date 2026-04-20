#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROCESS_MATCH_LIB="${SCRIPT_DIR}/process_match_lib.sh"

if [[ -f "$PROCESS_MATCH_LIB" ]]; then
  # shellcheck disable=SC1090
  source "$PROCESS_MATCH_LIB"
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

dir_name="$(basename "${BASE_DIR}")"
dir_lc="$(echo "${dir_name}" | tr 'A-Z' 'a-z')"
dir_tag="$(echo "${dir_lc}" | sed 's/[^a-z0-9_-]/_/g')"

infer_ns_and_suffix() {
  local name="$1"

  if [[ "$name" =~ ^([a-z0-9]+)[-_]fr([_-].*)?$ ]]; then
    echo "fr ${BASH_REMATCH[1]}"
    return 0
  fi

  for env_suffix in "_trade" "_test"; do
    if [[ "$name" == *"$env_suffix" ]]; then
      local base="${name%$env_suffix}"
      base="${base%_}"
      local ns="${base##*_}"
      local prefix="${base%_*}"
      if [[ -n "$ns" && -n "$prefix" ]]; then
        echo "${ns} ${prefix}"
        return 0
      fi
    fi
  done

  for env_suffix in "-trade" "-test"; do
    if [[ "$name" == *"$env_suffix" ]]; then
      local base="${name%$env_suffix}"
      base="${base%-}"
      local ns="${base##*-}"
      local prefix="${base%-*}"
      if [[ -n "$ns" && -n "$prefix" ]]; then
        echo "${ns} ${prefix}"
        return 0
      fi
    fi
  done

  return 1
}

NS=""
SUFFIX=""
if read -r NS SUFFIX < <(infer_ns_and_suffix "$dir_lc"); then
  :
fi

CLI_EXCHANGE="${1:-}"
EXCHANGE=""
ENV_TAG=""
LEGACY_PROC_NAME=""

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

if [[ "$NS" == "fr" ]]; then
  EXCHANGE="$SUFFIX"
  if [[ -n "$CLI_EXCHANGE" && "$CLI_EXCHANGE" != "$EXCHANGE" ]]; then
    echo "[ERROR] exchange mismatch: dir exchange=${EXCHANGE} arg exchange=${CLI_EXCHANGE}" >&2
    exit 1
  fi
  if [[ "$dir_lc" =~ ^[a-z0-9]+[-_]fr[-_](.+)$ ]]; then
    ENV_TAG="$(echo "${BASH_REMATCH[1]}" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//')"
  fi
  if [[ -z "$ENV_TAG" ]]; then
    ENV_TAG="fr"
  fi
  LEGACY_PROC_NAME="trade_engine_${dir_tag}"
elif [[ -n "$CLI_EXCHANGE" ]]; then
  EXCHANGE="$CLI_EXCHANGE"
else
  echo "[ERROR] missing exchange; use a dir like '<exchange>_fr_<suffix>' or pass exchange arg" >&2
  exit 1
fi

if [[ "$NS" == "fr" ]]; then
  PROC_NAME="${PMDAEMON_NAME:-te_$(short_exchange "$EXCHANGE")_${ENV_TAG}}"
else
  PROC_NAME="${PMDAEMON_NAME:-${PM2_NAME:-trade_engine_${dir_tag}}}"
fi
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

find_running_pids() {
  local exchange_arg="--exchange ${EXCHANGE}"
  safe_find_running_pids "trade_engine" "$BASE_DIR" "$exchange_arg"
}

echo "[INFO] Stopping ${PROC_NAME}"
deleted=false
if "${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1; then
  deleted=true
fi
if [[ -n "$LEGACY_PROC_NAME" && "$LEGACY_PROC_NAME" != "$PROC_NAME" ]] && "${PMDAEMON[@]}" delete "$LEGACY_PROC_NAME" >/dev/null 2>&1; then
  deleted=true
fi
if [[ "$deleted" == true ]]; then
  echo "[INFO] Stopped ${PROC_NAME}"
else
  echo "[WARN] Process not found: ${PROC_NAME}"
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
