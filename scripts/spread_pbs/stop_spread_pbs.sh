#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENUE_DIR_REGEX='^([a-z0-9]+-(futures|margin|both)|(binance|bitget)-coin-futures|gate-bitget-futures-bbo)$'

if [[ "${1:-}" =~ ^(-h|--help)$ ]]; then
  cat <<'USAGE'
Usage:
  stop_spread_pbs.sh [--role market|bookticker|split]

Behavior:
  - 必须在 venue 部署目录下执行（如 ~/spread_pbs/okex-futures 或 ~/spread_pbs/gate-both）。
  - pmdaemon delete + 兜底 kill 残留进程。
USAGE
  exit 0
fi

selected_role=""
if [[ $# -gt 0 ]]; then
  if [[ $# != 2 || "$1" != "--role" || ! "$2" =~ ^(market|bookticker|split)$ ]]; then
    echo "[ERROR] expected --role market|bookticker|split" >&2
    exit 1
  fi
  [[ "$2" == "split" ]] || selected_role="$2"
fi

short_exchange() {
  case "${1,,}" in
    binance) echo "bn" ;;
    okex)    echo "ok" ;;
    bybit)   echo "bb" ;;
    bitget)  echo "bg" ;;
    gate)    echo "gt" ;;
    *)       echo "${1,,}" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2 ;;
  esac
}
short_market() {
  case "${1,,}" in
    futures) echo "fu" ;;
    margin)  echo "mg" ;;
    both)    echo "bo" ;;
    *)       echo "${1,,}" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2 ;;
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
  exit 1
fi
if [[ -n "$selected_role" && "$venue" != "binance-futures" ]]; then
  echo "[ERROR] --role requires binance-futures" >&2
  exit 1
fi
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")

name="spp_$(venue_short_tag "$venue")"
market_name="$name"
bookticker_name="$name"
if [[ "$venue" == "binance-futures" ]]; then
  market_name="${name}_market"
  bookticker_name="${name}_bookticker"
elif [[ "$venue" == "bybit-both" ]]; then
  market_name="${name}_market"
  bookticker_name="${name}_bookticker"
fi
echo "[INFO] Stopping ${name} role=${selected_role:-all}"
if [[ "$venue" == "binance-futures" || "$venue" == "bybit-both" ]]; then
  if [[ "$selected_role" != "bookticker" ]]; then
    "${PMDAEMON[@]}" delete "$market_name" >/dev/null 2>&1 || true
  fi
  if [[ "$selected_role" != "market" ]]; then
    "${PMDAEMON[@]}" delete "$bookticker_name" >/dev/null 2>&1 || true
  fi
  if [[ -z "$selected_role" ]]; then
    "${PMDAEMON[@]}" delete "$name" >/dev/null 2>&1 || true
  fi
else
  "${PMDAEMON[@]}" delete "$name" >/dev/null 2>&1 || true
fi

# 兜底 kill 进程
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"
find_running_pids() {
  local venue_arg="--venue ${venue}"
  ps -eo pid=,comm=,args= | awk -v venue_arg="$venue_arg" -v selected_role="$selected_role" '
    $2 == "spread_pbs" &&
    index(" " $0 " ", " " venue_arg " ") > 0 &&
    (selected_role == "" || index(" " $0 " ", " --binance-futures-role " selected_role " ") > 0) &&
    index($0, "awk -v ") == 0 &&
    index($0, "start_spread_pbs.sh") == 0 &&
    index($0, "stop_spread_pbs.sh") == 0 {
      print $1
    }
  '
}

mapfile -t pids < <(find_running_pids || true)
if [[ ${#pids[@]} -gt 0 ]]; then
  echo "[INFO] SIGTERM ${pids[*]}"
  kill "${pids[@]}" >/dev/null 2>&1 || true
  deadline=$((SECONDS + KILL_WAIT_SECS))
  while [[ $SECONDS -lt $deadline ]]; do
    mapfile -t pids < <(find_running_pids || true)
    [[ ${#pids[@]} -eq 0 ]] && break
    sleep 1
  done
  if [[ ${#pids[@]} -gt 0 ]]; then
    echo "[WARN] SIGKILL ${pids[*]}"
    kill -9 "${pids[@]}" >/dev/null 2>&1 || true
  fi
fi

if [[ "$venue" == "binance-futures" || "$venue" == "bybit-both" ]]; then
  echo "[INFO] ${name} role=${selected_role:-all} stopped"
else
  echo "[INFO] ${name} stopped"
fi
