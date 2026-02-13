#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  stop_dat_pbs.sh (--exchange <exchange> | <exchange> | <venue...>)

Examples:
  ./scripts/stop_dat_pbs.sh --exchange binance
  ./scripts/stop_dat_pbs.sh okex
  ./scripts/stop_dat_pbs.sh binance-futures
  ./scripts/stop_dat_pbs.sh binance-futures binance-margin

Notes:
  - Exchange expands to default venues:
      okex    -> okex-futures okex-margin
      binance -> binance-futures binance-margin
      bybit   -> bybit-futures bybit-margin
      bitget  -> bitget-futures bitget-margin
      gate    -> gate-futures gate-margin
  - Stops systemd user services:
      dat_pbs@<venue>.service
USAGE
}

KNOWN_EXCHANGES=("okex" "binance" "bybit" "bitget" "gate")

is_known_exchange() {
  local v="${1,,}"
  for e in "${KNOWN_EXCHANGES[@]}"; do
    if [[ "$v" == "$e" ]]; then
      return 0
    fi
  done
  return 1
}

default_venues_for_exchange() {
  local exchange="${1,,}"
  case "$exchange" in
    okex) echo "okex-futures okex-margin" ;;
    binance) echo "binance-futures binance-margin" ;;
    bybit) echo "bybit-futures bybit-margin" ;;
    bitget) echo "bitget-futures bitget-margin" ;;
    gate) echo "gate-futures gate-margin" ;;
    *)
      echo ""
      return 1
      ;;
  esac
}

SYSTEMCTL=(systemctl --user)

EXCHANGE=""
VENUES=()
POSITIONAL=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace)
      # Backward compatibility: namespace is no longer used for dat_pbs.
      if [[ -z "${2:-}" ]]; then
        echo "[ERROR] --namespace 需要一个值" >&2
        usage >&2
        exit 1
      fi
      echo "[WARN] --namespace is ignored for dat_pbs (using unit dat_pbs@<venue>.service)"
      shift 2
      ;;
    --exchange)
      EXCHANGE="${2:-}"
      if [[ -z "$EXCHANGE" ]]; then
        echo "[ERROR] --exchange 需要一个值" >&2
        usage >&2
        exit 1
      fi
      shift 2
      ;;
    --venue)
      v="${2:-}"
      if [[ -z "$v" ]]; then
        echo "[ERROR] --venue 需要一个值" >&2
        usage >&2
        exit 1
      fi
      VENUES+=("$v")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      POSITIONAL+=("$1")
      shift
      ;;
  esac
done

if [[ -z "$EXCHANGE" && ${#POSITIONAL[@]} -gt 0 ]]; then
  if is_known_exchange "${POSITIONAL[0]}"; then
    EXCHANGE="${POSITIONAL[0]}"
    POSITIONAL=("${POSITIONAL[@]:1}")
  fi
fi

if [[ ${#VENUES[@]} -eq 0 && ${#POSITIONAL[@]} -gt 0 ]]; then
  VENUES=("${POSITIONAL[@]}")
fi

if [[ -z "$EXCHANGE" && ${#VENUES[@]} -eq 0 ]]; then
  dir_name="$(basename "${BASE_DIR}")"
  if [[ "$dir_name" =~ okex|OKEX ]]; then EXCHANGE="okex"; fi
  if [[ "$dir_name" =~ binance|BINANCE ]]; then EXCHANGE="binance"; fi
  if [[ "$dir_name" =~ bybit|BYBIT ]]; then EXCHANGE="bybit"; fi
  if [[ "$dir_name" =~ bitget|BITGET ]]; then EXCHANGE="bitget"; fi
  if [[ "$dir_name" =~ gate|GATE ]]; then EXCHANGE="gate"; fi
fi

if [[ ${#VENUES[@]} -eq 0 && -n "$EXCHANGE" ]]; then
  read -r -a VENUES <<<"$(default_venues_for_exchange "$EXCHANGE")"
fi

if [[ ${#VENUES[@]} -eq 0 ]]; then
  echo "[ERROR] No exchange/venues provided and could not infer from deploy directory name" >&2
  usage >&2
  exit 1
fi

if ! command -v systemctl >/dev/null 2>&1; then
  echo "[ERROR] systemctl not found" >&2
  exit 1
fi

if ! "${SYSTEMCTL[@]}" show-environment >/dev/null 2>&1; then
  echo "[ERROR] systemd --user is not available for current session" >&2
  echo "[HINT] For server usage, run once: sudo loginctl enable-linger $(whoami)" >&2
  exit 1
fi

UNIT_TEMPLATE="dat_pbs@.service"

unit_name_for_venue() {
  local venue="$1"
  if command -v systemd-escape >/dev/null 2>&1; then
    systemd-escape --template "$UNIT_TEMPLATE" "$venue"
  else
    echo "${UNIT_TEMPLATE/@.service/@${venue}.service}"
  fi
}

stop_one() {
  local venue="$1"
  local unit_name
  unit_name="$(unit_name_for_venue "$venue")"

  echo "[INFO] Stopping ${unit_name}"
  if "${SYSTEMCTL[@]}" stop "$unit_name" >/dev/null 2>&1; then
    "${SYSTEMCTL[@]}" reset-failed "$unit_name" >/dev/null 2>&1 || true
    echo "[INFO] Stopped ${unit_name}"
  else
    echo "[WARN] ${unit_name} not found or already stopped"
  fi
}

for venue in "${VENUES[@]}"; do
  stop_one "$venue"
  sleep 1
done

echo ""
echo "[INFO] Stopped venues: ${VENUES[*]}"
echo "To view remaining services: systemctl --user status dat_pbs@<venue>.service"
