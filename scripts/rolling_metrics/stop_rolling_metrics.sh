#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PROCESS_MATCH_LIB="${SCRIPT_DIR}/../process_match_lib.sh"

if [[ -f "$PROCESS_MATCH_LIB" ]]; then
  # shellcheck disable=SC1090
  source "$PROCESS_MATCH_LIB"
fi

usage() {
  cat <<'USAGE'
Usage:
  stop_rolling_metrics.sh [--open-venue <venue> --hedge-venue <venue>]

Examples:
  ./scripts/rolling_metrics/stop_rolling_metrics.sh --open-venue binance-margin --hedge-venue binance-futures
  ./scripts/rolling_metrics/stop_rolling_metrics.sh
USAGE
}

infer_venues_from_dir() {
  local dir_name="${1,,}"

  if [[ "$dir_name" =~ ^([a-z0-9]+-(margin|futures))[-_]([a-z0-9]+-(margin|futures))$ ]]; then
    echo "${BASH_REMATCH[1]},${BASH_REMATCH[3]}"
    return 0
  fi

  return 1
}

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
  local raw_venue="${1,,}"
  if [[ "$raw_venue" =~ ^([a-z0-9]+)-([a-z0-9]+)$ ]]; then
    echo "$(short_exchange "${BASH_REMATCH[1]}")_$(short_market "${BASH_REMATCH[2]}")"
    return 0
  fi
  echo "$raw_venue" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//'
}

legacy_token() {
  echo "${1,,}" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//'
}

validate_venue() {
  local v="${1,,}"
  if [[ ! "$v" =~ ^[a-z0-9]+-(margin|futures)$ ]]; then
    echo "[ERROR] invalid venue: $1 (expect <exchange>-<margin|futures>)" >&2
    exit 1
  fi
}

OPEN_VENUE=""
HEDGE_VENUE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --open-venue)
      OPEN_VENUE="${2:-}"
      shift 2
      ;;
    --hedge-venue)
      HEDGE_VENUE="${2:-}"
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

if [[ -n "$OPEN_VENUE" || -n "$HEDGE_VENUE" ]]; then
  if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
    echo "[ERROR] --open-venue and --hedge-venue must be provided together" >&2
    exit 1
  fi
else
  dir_name="$(basename "$BASE_DIR")"
  if inferred="$(infer_venues_from_dir "$dir_name")" && [[ -n "$inferred" ]]; then
    OPEN_VENUE="${inferred%%,*}"
    HEDGE_VENUE="${inferred##*,}"
    echo "[INFO] Inferred from dir '$dir_name': open=$OPEN_VENUE hedge=$HEDGE_VENUE"
  else
    echo "[ERROR] cannot infer open/hedge venues from dir '$dir_name'" >&2
    echo "[HINT] use --open-venue <venue> --hedge-venue <venue>" >&2
    exit 1
  fi
fi

OPEN_VENUE="${OPEN_VENUE,,}"
HEDGE_VENUE="${HEDGE_VENUE,,}"
validate_venue "$OPEN_VENUE"
validate_venue "$HEDGE_VENUE"

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

PROC_NAME="rm_$(venue_short_tag "$OPEN_VENUE")_$(venue_short_tag "$HEDGE_VENUE")"
LEGACY_PROC_NAME="rolling_metrics_$(legacy_token "$OPEN_VENUE")_$(legacy_token "$HEDGE_VENUE")"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

find_running_pids() {
  local open_arg="--open-venue ${OPEN_VENUE}"
  local hedge_arg="--hedge-venue ${HEDGE_VENUE}"
  safe_find_running_pids "rolling_metrics" "$BASE_DIR" "$open_arg" "$hedge_arg"
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
