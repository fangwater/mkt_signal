#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ "${1:-}" =~ ^(-h|--help)$ ]]; then
  cat <<'USAGE'
Usage:
  stop_spread_bbo_zmq_pub.sh

Stops only the spread_bbo_zmq_pub process inferred from the deploy directory.
USAGE
  exit 0
fi
if [[ $# -gt 0 ]]; then
  echo "[ERROR] unsupported argument(s): $*" >&2
  exit 1
fi

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck source=/dev/null
  source "$ENV_FILE"
fi

venue="$(basename "$BASE_DIR" | tr '[:upper:]' '[:lower:]')"
if [[ ! "$venue" =~ ^([a-z0-9]+)-(futures|margin|both)$ ]]; then
  echo "[ERROR] cannot infer venue from deploy directory: ${BASE_DIR}" >&2
  exit 1
fi
exchange="${BASH_REMATCH[1]}"
market="${BASH_REMATCH[2]}"

case "$exchange" in
  binance) exchange_tag="bn" ;;
  okex) exchange_tag="ok" ;;
  bybit) exchange_tag="bb" ;;
  bitget) exchange_tag="bg" ;;
  gate) exchange_tag="gt" ;;
  *) exchange_tag="${exchange:0:2}" ;;
esac
case "$market" in
  futures) market_tag="fu" ;;
  margin) market_tag="mg" ;;
  both) market_tag="bo" ;;
esac

PROC_NAME="${SPREAD_BBO_ZMQ_PM2_NAME:-sbbzp_${exchange_tag}_${market_tag}}"
NAMESPACE="${PM2_NAMESPACE:-spread_bbo_zmq_pub}"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"
BIN_PATH="${BASE_DIR}/spread_bbo_zmq_pub"

if command -v pm2 >/dev/null 2>&1; then
  PM2=(pm2)
elif command -v npx >/dev/null 2>&1; then
  PM2=(npx pm2)
else
  echo "[ERROR] neither pm2 nor npx is available" >&2
  exit 1
fi

find_running_pids() {
  ps -eo pid=,args= | awk -v bin_path="$BIN_PATH" -v venue_arg="--venue ${venue}" '
    index($0, bin_path) > 0 &&
    index($0, venue_arg) > 0 &&
    index($0, "awk -v ") == 0 &&
    index($0, "start_spread_bbo_zmq_pub.sh") == 0 &&
    index($0, "stop_spread_bbo_zmq_pub.sh") == 0 {
      print $1
    }
  '
}

echo "[INFO] Stopping ${PROC_NAME} (namespace=${NAMESPACE})"
"${PM2[@]}" delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
if command -v pmdaemon >/dev/null 2>&1; then
  pmdaemon delete "$PROC_NAME" >/dev/null 2>&1 || true
fi

mapfile -t pids < <(find_running_pids || true)
if [[ ${#pids[@]} -gt 0 ]]; then
  kill "${pids[@]}" >/dev/null 2>&1 || true
  deadline=$((SECONDS + KILL_WAIT_SECS))
  while [[ $SECONDS -lt $deadline ]]; do
    mapfile -t pids < <(find_running_pids || true)
    [[ ${#pids[@]} -eq 0 ]] && break
    sleep 1
  done
  if [[ ${#pids[@]} -gt 0 ]]; then
    kill -9 "${pids[@]}" >/dev/null 2>&1 || true
  fi
fi

mapfile -t pids < <(find_running_pids || true)
if [[ ${#pids[@]} -gt 0 ]]; then
  echo "[ERROR] failed to stop process(es): ${pids[*]}" >&2
  exit 1
fi
echo "[INFO] Stopped ${PROC_NAME}"
