#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  start_spread_bbo_zmq_pub.sh

Behavior:
  - Run from a deployed <venue> directory, for example
    ~/spread_bbo_zmq_pub/binance-futures/scripts/start_spread_bbo_zmq_pub.sh.
  - Sources ../env.sh for bind IP, core, port, HWM, and PM2 namespace.
  - Replaces only the matching spread_bbo_zmq_pub PM2/pmdaemon process.
USAGE
}

if [[ "${1:-}" =~ ^(-h|--help)$ ]]; then
  usage
  exit 0
fi
if [[ $# -gt 0 ]]; then
  echo "[ERROR] unsupported argument(s): $*" >&2
  usage >&2
  exit 1
fi

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck source=/dev/null
  source "$ENV_FILE"
fi

venue="$(basename "$BASE_DIR" | tr '[:upper:]' '[:lower:]')"
if [[ ! "$venue" =~ ^[a-z0-9]+-(futures|margin|both)$ ]]; then
  echo "[ERROR] cannot infer venue from deploy directory: ${BASE_DIR}" >&2
  exit 1
fi

short_exchange() {
  case "${1,,}" in
    binance) echo "bn" ;;
    okex) echo "ok" ;;
    bybit) echo "bb" ;;
    bitget) echo "bg" ;;
    gate) echo "gt" ;;
    *) echo "${1,,}" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2 ;;
  esac
}

short_market() {
  case "${1,,}" in
    futures) echo "fu" ;;
    margin) echo "mg" ;;
    both) echo "bo" ;;
    *) echo "${1,,}" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2 ;;
  esac
}

IFS=- read -r exchange market <<<"$venue"
PROC_NAME="${SPREAD_BBO_ZMQ_PM2_NAME:-sbbzp_$(short_exchange "$exchange")_$(short_market "$market")}"
NAMESPACE="${PM2_NAMESPACE:-spread_bbo_zmq_pub}"
CORE="${SPREAD_BBO_ZMQ_CORE:-5}"
BIND_IP="${SPREAD_BBO_ZMQ_BIND_IP:-0.0.0.0}"
PORT="${SPREAD_BBO_ZMQ_PORT:-6320}"
SNDHWM="${SPREAD_BBO_ZMQ_SNDHWM:-128}"
SERVICE_ROOT="${SPREAD_BBO_ZMQ_SERVICE_ROOT:-spread_pbs}"
RUST_LOG_VAL="${RUST_LOG:-info}"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

for value_name in CORE PORT SNDHWM KILL_WAIT_SECS; do
  value="${!value_name}"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] ${value_name} must be an integer (got: ${value})" >&2
    exit 1
  fi
done
if [[ -z "$BIND_IP" || -z "$SERVICE_ROOT" ]]; then
  echo "[ERROR] bind IP and service root cannot be empty" >&2
  exit 1
fi

BIN_CANDIDATES=(
  "${BASE_DIR}/spread_bbo_zmq_pub"
  "${BASE_DIR}/target/release/spread_bbo_zmq_pub"
)
BIN_PATH=""
for candidate in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$candidate" ]]; then
    BIN_PATH="$candidate"
    break
  fi
done
if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] spread_bbo_zmq_pub binary not found under ${BASE_DIR}" >&2
  exit 1
fi
if [[ ! -x /usr/bin/taskset ]]; then
  echo "[ERROR] /usr/bin/taskset is not executable" >&2
  exit 1
fi

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

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE})"
"${PM2[@]}" delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
if command -v pmdaemon >/dev/null 2>&1; then
  pmdaemon delete "$PROC_NAME" >/dev/null 2>&1 || true
fi

mapfile -t leaked_pids < <(find_running_pids || true)
if [[ ${#leaked_pids[@]} -gt 0 ]]; then
  echo "[WARN] Found leaked process(es): ${leaked_pids[*]}; sending SIGTERM"
  kill "${leaked_pids[@]}" >/dev/null 2>&1 || true
  deadline=$((SECONDS + KILL_WAIT_SECS))
  while [[ $SECONDS -lt $deadline ]]; do
    mapfile -t leaked_pids < <(find_running_pids || true)
    [[ ${#leaked_pids[@]} -eq 0 ]] && break
    sleep 1
  done
  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[WARN] SIGTERM timeout; sending SIGKILL: ${leaked_pids[*]}"
    kill -9 "${leaked_pids[@]}" >/dev/null 2>&1 || true
    sleep 1
  fi
fi

mapfile -t leaked_pids < <(find_running_pids || true)
if [[ ${#leaked_pids[@]} -gt 0 ]]; then
  echo "[ERROR] failed to stop existing process(es): ${leaked_pids[*]}" >&2
  exit 1
fi

RUST_LOG="$RUST_LOG_VAL" "${PM2[@]}" start /usr/bin/taskset \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE" \
  --cwd "$BASE_DIR" \
  --interpreter none \
  -- \
  -c "$CORE" \
  "$BIN_PATH" \
  --venue "$venue" \
  --service-root "$SERVICE_ROOT" \
  --bind-ip "$BIND_IP" \
  --port "$PORT" \
  --sndhwm "$SNDHWM" \
  --core "$CORE"

echo "[INFO] Started ${PROC_NAME}: ${BIND_IP}:${PORT}, core=${CORE}"
echo "Logs:   ${PM2[*]} logs --namespace ${NAMESPACE} ${PROC_NAME}"
echo "Status: ${PM2[*]} status --namespace ${NAMESPACE}"
