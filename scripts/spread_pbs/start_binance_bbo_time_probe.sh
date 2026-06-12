#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${ROOT_DIR}/target/release/binance_bbo_time_probe"
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
NAME="${BINANCE_BBO_TIME_PROBE_NAME:-bn_bbo_t_probe}"
CORE="${BINANCE_BBO_TIME_PROBE_CORE:-11}"
MODE="${BINANCE_BBO_TIME_PROBE_MODE:-both}"
CFG="${BINANCE_BBO_TIME_PROBE_CFG:-$HOME/spread_pbs/config/mkt_cfg.yaml}"
SYMBOLS="${BINANCE_BBO_TIME_PROBE_SYMBOLS:-}"
RUST_LOG="${RUST_LOG:-info}"

if [[ ! -x "$BIN" ]]; then
  echo "[ERROR] binary not found: $BIN" >&2
  echo "        build with: cargo build --release --bin binance_bbo_time_probe" >&2
  exit 1
fi
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  exit 1
fi

json_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'; }

json_name="$(json_escape "$NAME")"
json_bin="$(json_escape "$BIN")"
json_root="$(json_escape "$ROOT_DIR")"
json_core="$(json_escape "$CORE")"
json_mode="$(json_escape "$MODE")"
json_cfg="$(json_escape "$CFG")"
json_rust_log="$(json_escape "$RUST_LOG")"

args=(
  "--core" "$json_core"
  "--mode" "$json_mode"
  "--config" "$json_cfg"
)
if [[ -n "$SYMBOLS" ]]; then
  json_symbols="$(json_escape "$SYMBOLS")"
  args+=("--symbols" "$json_symbols")
fi

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

{
  cat <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": [
JSON
  for i in "${!args[@]}"; do
    comma=","
    [[ "$i" == "$((${#args[@]} - 1))" ]] && comma=""
    printf '        "%s"%s\n' "${args[$i]}" "$comma"
  done
  cat <<JSON
      ],
      "cwd": "${json_root}",
      "env": {
        "RUST_LOG": "${json_rust_log}"
      }
    }
  ]
}
JSON
} > "$cfg_file"

echo "[INFO] Restarting ${NAME} (core=${CORE}, mode=${MODE}, cfg=${CFG})"
"$PMDAEMON_BIN" delete "$NAME" >/dev/null 2>&1 || true
"$PMDAEMON_BIN" --config "$cfg_file" start --name "$NAME"
echo "[INFO] Logs: $PMDAEMON_BIN logs $NAME --follow"
