#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  stop_depth_pub.sh (--exchange <exchange> | <exchange> | <venue...>)

Examples:
  ./scripts/stop_depth_pub.sh --exchange binance
  ./scripts/stop_depth_pub.sh okex
  ./scripts/stop_depth_pub.sh binance-futures
  ./scripts/stop_depth_pub.sh binance-futures binance-margin

Notes:
  - Exchange expands to default venues:
      okex    -> okex-futures okex-margin
      binance -> binance-futures binance-margin
      bybit   -> bybit-futures bybit-margin
      bitget  -> bitget-futures bitget-margin
      gate    -> gate-futures gate-margin
  - Managed by pmdaemon with process names depth_pub_<venue>.
EOF
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

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

# Args parsing
EXCHANGE=""
VENUES=()
POSITIONAL=()

while [[ $# -gt 0 ]]; do
  case "$1" in
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

if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

find_running_pids_for_venue() {
  local venue="$1"
  local pids=()
  while IFS= read -r pid; do
    if [[ -n "$pid" && "$pid" != "$$" && "$pid" != "$PPID" ]]; then
      pids+=("$pid")
    fi
  done < <(
    ps -eo pid=,args= | awk -v venue="$venue" -v base_dir="$BASE_DIR" '
      index($0, "depth_pub") > 0 &&
      index($0, "--venue " venue) > 0 &&
      index($0, base_dir) > 0 &&
      index($0, "awk -v venue=") == 0 &&
      index($0, "stop_depth_pub.sh") == 0 {
        print $1
      }
    '
  )

  if [[ ${#pids[@]} -gt 0 ]]; then
    printf '%s\n' "${pids[@]}"
  fi
}

cleanup_leaked_for_venue() {
  local venue="$1"
  local venue_arg="--venue ${venue}"
  local pattern="${BASE_DIR}.*depth_pub.*--venue[[:space:]]+${venue}"

  mapfile -t leaked_pids < <(find_running_pids_for_venue "$venue" || true)
  if [[ ${#leaked_pids[@]} -eq 0 ]]; then
    return 0
  fi

  echo "[WARN] Found leaked depth_pub process after pmdaemon delete: venue=${venue} pids=${leaked_pids[*]}"
  echo "[INFO] Sending SIGTERM via pkill: pattern=${BASE_DIR} ... depth_pub ... ${venue_arg}"
  pkill -TERM -f "$pattern" >/dev/null 2>&1 || true

  local deadline=$((SECONDS + KILL_WAIT_SECS))
  while [[ $SECONDS -lt $deadline ]]; do
    mapfile -t leaked_pids < <(find_running_pids_for_venue "$venue" || true)
    if [[ ${#leaked_pids[@]} -eq 0 ]]; then
      break
    fi
    sleep 1
  done

  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[WARN] SIGTERM timeout, sending SIGKILL via pkill: venue=${venue} pids=${leaked_pids[*]}"
    pkill -KILL -f "$pattern" >/dev/null 2>&1 || true
    sleep 1
    mapfile -t leaked_pids < <(find_running_pids_for_venue "$venue" || true)
  fi

  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[ERROR] Failed to kill leaked depth_pub process(es): venue=${venue} pids=${leaked_pids[*]}" >&2
    return 1
  fi

  echo "[INFO] Leaked process cleanup done: venue=${venue}"
  return 0
}

stop_one() {
  local venue="$1"
  local name="depth_pub_${venue}"

  echo "[INFO] Stopping ${name}"
  if "${PMDAEMON[@]}" delete "$name" >/dev/null 2>&1; then
    echo "[INFO] Stopped ${name}"
  else
    echo "[WARN] ${name} not found"
  fi

  cleanup_leaked_for_venue "$venue"
}

for venue in "${VENUES[@]}"; do
  stop_one "$venue"
  sleep 1
done

echo ""
echo "[INFO] Stopped venues: ${VENUES[*]}"
echo "To view remaining processes: ${PMDAEMON[*]} list"
