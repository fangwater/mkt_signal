#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  stop_rolling_metrics.sh [--open-venue <venue> --hedge-venue <venue>]

Examples:
  ./scripts/stop_rolling_metrics.sh --open-venue binance-margin --hedge-venue binance-futures
  ./scripts/stop_rolling_metrics.sh
USAGE
}

infer_venues_from_dir() {
  local dir_name="${1,,}"

  if [[ "$dir_name" =~ ^([a-z0-9]+-(margin|futures|spot|swap|perp|perpetual))[-_]([a-z0-9]+-(margin|futures|spot|swap|perp|perpetual))$ ]]; then
    echo "${BASH_REMATCH[1]},${BASH_REMATCH[3]}"
    return 0
  fi

  return 1
}

sanitize_token() {
  echo "${1,,}" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//'
}

validate_venue() {
  local v="${1,,}"
  if [[ ! "$v" =~ ^[a-z0-9]+-(margin|futures|spot|swap|perp|perpetual)$ ]]; then
    echo "[ERROR] invalid venue: $1 (expect <exchange>-<margin|futures|spot|swap>)" >&2
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

PROC_NAME="rolling_metrics_$(sanitize_token "$OPEN_VENUE")_$(sanitize_token "$HEDGE_VENUE")"

echo "[INFO] Stopping ${PROC_NAME}"
if "${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1; then
  echo "[INFO] Stopped ${PROC_NAME}"
else
  echo "[WARN] ${PROC_NAME} not found"
fi

echo "Status: ${PMDAEMON[*]} list"
