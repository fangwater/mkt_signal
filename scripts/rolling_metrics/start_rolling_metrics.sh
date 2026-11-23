#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  start_rolling_metrics.sh [--open-venue <venue> --hedge-venue <venue>] [--warm-up-dir <path>]

Examples:
  ./scripts/rolling_metrics/start_rolling_metrics.sh --open-venue binance-margin --hedge-venue binance-futures
  ./scripts/rolling_metrics/start_rolling_metrics.sh

Notes:
  - When open/hedge are omitted, infer from deploy dir name:
      <open-venue>-<hedge-venue>
    e.g. ~/rolling_metrics/binance-margin-binance-futures
  - Managed by pmdaemon with process name:
      rm_<open_tag>_<hedge_tag>（兼容删除旧名 rolling_metrics_<open_venue>_<hedge_venue>）
  - Optional: put a taskset CPU list (e.g. 37 or 0-5) in rolling_metrics.core.
USAGE
}

infer_venues_from_dir() {
  local dir_name="${1,,}"

  if [[ "$dir_name" =~ ^([a-z0-9]+-(margin|futures)|(binance|bitget)-coin-futures)[-_]([a-z0-9]+-(margin|futures)|(binance|bitget)-coin-futures)$ ]]; then
    echo "${BASH_REMATCH[1]},${BASH_REMATCH[4]}"
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
  if [[ ! "$v" =~ ^[a-z0-9]+-(margin|futures)$ && "$v" != "binance-coin-futures" && "$v" != "bitget-coin-futures" ]]; then
    echo "[ERROR] invalid venue: $1 (expect <exchange>-<margin|futures>)" >&2
    exit 1
  fi
}

OPEN_VENUE=""
HEDGE_VENUE=""
WARM_UP_DIR=""

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
    --warm-up-dir)
      WARM_UP_DIR="$(realpath -e -- "${2:?missing --warm-up-dir path}")"
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

BIN_CANDIDATES=(
  "${BASE_DIR}/rolling_metrics"
  "${BASE_DIR}/target/release/rolling_metrics"
)
if [[ -n "$WARM_UP_DIR" ]]; then
  [[ "$WARM_UP_DIR" != "$BASE_DIR"* && -s "$WARM_UP_DIR/required.json" && ! -e "$WARM_UP_DIR/publish" ]] || {
    echo "[ERROR] use a separate warm-up directory with required.json and no publish marker" >&2
    exit 1
  }
  BIN_CANDIDATES=("$WARM_UP_DIR/rolling_metrics")
fi

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] rolling_metrics binary not found. Build/deploy first." >&2
  exit 1
fi

PROC_NAME="rm_$(venue_short_tag "$OPEN_VENUE")_$(venue_short_tag "$HEDGE_VENUE")"
LEGACY_PROC_NAME="rolling_metrics_$(legacy_token "$OPEN_VENUE")_$(legacy_token "$HEDGE_VENUE")"
json_warmup_args=""
if [[ -n "$WARM_UP_DIR" ]]; then
  PROC_NAME="${PROC_NAME}_warm"
fi
RUST_LOG_VAL="${RUST_LOG:-info,rolling_metrics=info,mkt_signal=info}"

cfg_file="$(mktemp)"
cleanup() { rm -f "$cfg_file" >/dev/null 2>&1 || true; }
trap cleanup EXIT

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

json_name="$(json_escape "$PROC_NAME")"
json_bin="$(json_escape "$BIN_PATH")"
json_base="$(json_escape "$BASE_DIR")"
json_open="$(json_escape "$OPEN_VENUE")"
json_hedge="$(json_escape "$HEDGE_VENUE")"
json_rust_log="$(json_escape "$RUST_LOG_VAL")"
if [[ -n "$WARM_UP_DIR" ]]; then
  json_warmup_args=", \"--warm-up-dir\", \"$(json_escape "$WARM_UP_DIR")\", \"--iceoryx-node\", \"${PROC_NAME}\""
fi

json_affinity_args=""
if [[ -f "${BASE_DIR}/rolling_metrics.core" ]]; then
  cpu_list="$(<"${BASE_DIR}/rolling_metrics.core")"
  if [[ ! "$cpu_list" =~ ^[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*$ ]]; then
    echo "[ERROR] rolling_metrics.core must contain a CPU list such as 37 or 0-5" >&2
    exit 1
  fi
  taskset_bin="$(command -v taskset)"
  "$taskset_bin" -c "$cpu_list" true
  json_affinity_args="\"-c\", \"${cpu_list}\", \"${json_bin}\", "
  json_bin="$(json_escape "$taskset_bin")"
fi

cat >"$cfg_file" <<EOF
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": [${json_affinity_args}"--open-venue", "${json_open}", "--hedge-venue", "${json_hedge}"${json_warmup_args}],
      "cwd": "${json_base}",
      "env": {
        "RUST_LOG": "${json_rust_log}"
      }
    }
  ]
}
EOF

echo "[INFO] Restarting ${PROC_NAME} (open=${OPEN_VENUE} hedge=${HEDGE_VENUE})"
STOP_SCRIPT="${SCRIPT_DIR}/stop_rolling_metrics.sh"
if [[ ! -x "$STOP_SCRIPT" ]]; then
  echo "[ERROR] stop script not found or not executable: $STOP_SCRIPT" >&2
  exit 1
fi
if [[ -z "$WARM_UP_DIR" ]]; then
  "$STOP_SCRIPT" --open-venue "$OPEN_VENUE" --hedge-venue "$HEDGE_VENUE"
fi
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo "[INFO] Started ${PROC_NAME}"
echo "Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "Status: ${PMDAEMON[*]} list"
