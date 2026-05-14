#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

BIN_CANDIDATES=(
  "${SCRIPT_DIR}/trade_engine"
  "${SCRIPT_DIR}/../trade_engine"
  "${SCRIPT_DIR}/target/release/trade_engine"
  "${SCRIPT_DIR}/../target/release/trade_engine"
  "${BASE_DIR}/trade_engine"
  "${BASE_DIR}/target/release/trade_engine"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] trade_engine binary not found. Build first with: cargo build --release --bin trade_engine" >&2
  exit 1
fi

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
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
  PROC_NAME="${PMDAEMON_NAME:-fr_te_$(short_exchange "$EXCHANGE")_${ENV_TAG}}"
else
  PROC_NAME="${PMDAEMON_NAME:-${PM2_NAME:-trade_engine_${dir_tag}}}"
fi
RUST_LOG="${RUST_LOG:-info}"

find_trade_engine_pids() {
  ps -eo pid=,args= | awk -v base_dir="${BASE_DIR}" -v exchange="${EXCHANGE}" '
    index($0, base_dir "/") && index($0, "trade_engine --exchange " exchange) { print $1 }
  '
}

stop_duplicate_trade_engines() {
  mapfile -t stale_pids < <(find_trade_engine_pids)
  if [[ "${#stale_pids[@]}" -eq 0 ]]; then
    return 0
  fi

  echo "[WARN] Found existing trade_engine instance(s) for ${BASE_DIR} exchange=${EXCHANGE}: ${stale_pids[*]}"
  kill "${stale_pids[@]}" >/dev/null 2>&1 || true

  for _ in {1..20}; do
    sleep 0.2
    mapfile -t stale_pids < <(find_trade_engine_pids)
    if [[ "${#stale_pids[@]}" -eq 0 ]]; then
      break
    fi
  done

  if [[ "${#stale_pids[@]}" -gt 0 ]]; then
    echo "[WARN] trade_engine still alive after SIGTERM, sending SIGKILL: ${stale_pids[*]}"
    kill -9 "${stale_pids[@]}" >/dev/null 2>&1 || true
    sleep 0.2
  fi

  mapfile -t stale_pids < <(find_trade_engine_pids)
  if [[ "${#stale_pids[@]}" -gt 0 ]]; then
    echo "[ERROR] Failed to remove duplicate trade_engine instance(s): ${stale_pids[*]}" >&2
    exit 1
  fi
}

verify_single_trade_engine() {
  sleep 0.5
  mapfile -t live_pids < <(find_trade_engine_pids)
  if [[ "${#live_pids[@]}" -ne 1 ]]; then
    echo "[ERROR] Expected exactly one trade_engine for ${BASE_DIR} exchange=${EXCHANGE}, found ${#live_pids[@]}: ${live_pids[*]}" >&2
    exit 1
  fi
}

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

json_name="$(json_escape "$PROC_NAME")"
json_bin="$(json_escape "$BIN_PATH")"
json_base="$(json_escape "$BASE_DIR")"
json_exchange="$(json_escape "$EXCHANGE")"
json_rust_log="$(json_escape "$RUST_LOG")"

cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": ["--exchange", "${json_exchange}"],
      "cwd": "${json_base}",
      "env": {
        "RUST_LOG": "${json_rust_log}"
      }
    }
  ]
}
JSON

echo "[INFO] Restarting ${PROC_NAME}"
if [[ -f "${BASE_DIR}/trade_engine.toml" ]]; then
  echo "[INFO] 本地 IP 配置来自 ${BASE_DIR}/trade_engine.toml"
elif [[ -f "${BASE_DIR}/trade engine.toml" ]]; then
  echo "[INFO] 本地 IP 配置来自 ${BASE_DIR}/trade engine.toml"
else
  echo "[INFO] 本地 IP 配置来自 ${HOME}/config/mkt_cfg.yaml (fallback)"
fi
stop_duplicate_trade_engines
if [[ -n "$LEGACY_PROC_NAME" && "$LEGACY_PROC_NAME" != "$PROC_NAME" ]]; then
  "${PMDAEMON[@]}" delete "$LEGACY_PROC_NAME" >/dev/null 2>&1 || true
fi
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"
verify_single_trade_engine

echo ""
echo "[INFO] Started trade_engine (exchange=${EXCHANGE})"
echo "Process: ${PROC_NAME}"
echo "Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "Status: ${PMDAEMON[*]} list"
