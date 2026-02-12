#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  stop_trade_flow_feature_pub.sh [--namespace <ns>] (--exchange <exchange> | <exchange> | <venue...>)

Examples:
  ./scripts/stop_trade_flow_feature_pub.sh --exchange binance
  ./scripts/stop_trade_flow_feature_pub.sh okex
  ./scripts/stop_trade_flow_feature_pub.sh binance-futures
  ./scripts/stop_trade_flow_feature_pub.sh binance-futures binance-margin

Notes:
  - Exchange expands to default venues:
      okex    -> okex-futures okex-margin
      binance -> binance-futures binance-margin
      bybit   -> bybit-futures bybit-margin
      bitget  -> bitget-futures bitget-margin
      gate    -> gate-futures gate-margin
  - Namespace defaults to $PM2_NAMESPACE or the deploy directory name.
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

# PM2 binary: prefer pm2, fallback to npx pm2
PM2=(pm2)
if ! command -v pm2 >/dev/null 2>&1; then
  PM2=(npx pm2)
fi

# Optional PM2 namespace; defaults to deploy dir name unless PM2_NAMESPACE is set
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

# Args parsing
EXCHANGE=""
VENUES=()
POSITIONAL=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace)
      NAMESPACE="${2:-}"
      if [[ -z "$NAMESPACE" ]]; then
        echo "[ERROR] --namespace 需要一个值" >&2
        usage >&2
        exit 1
      fi
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

# Infer exchange from deploy directory name for backward compatibility
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

stop_one() {
  local venue="$1"
  local name="trade_flow_feature_pub_${venue}"

  echo "[INFO] Deleting ${name} (namespace: ${NAMESPACE})"
  if "${PM2[@]}" delete "$name" --namespace "$NAMESPACE"; then
    echo "[INFO] Deleted ${name}"
  else
    echo "[WARN] ${name} not found in namespace ${NAMESPACE}"
  fi
}

for venue in "${VENUES[@]}"; do
  stop_one "$venue"
  sleep 1
done

echo ""
echo "[INFO] Stopped venues: ${VENUES[*]}"
echo "Namespace: ${NAMESPACE}"
echo "To view remaining processes: ${PM2[*]} status --namespace ${NAMESPACE}"
