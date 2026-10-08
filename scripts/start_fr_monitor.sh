#!/usr/bin/env bash
# Run from the deployed FR monitor environment. Sending requires --execute.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG="${FR_MONITOR_CONFIG:-$BASE_DIR/config/fr_monitor.toml}"
BIN="${FR_MONITOR_BIN:-$BASE_DIR/fr_monitor}"
MODE=--dry-run
while [[ $# -gt 0 ]]; do
  case "$1" in
    --execute) MODE=--execute; shift ;;
    --dry-run) MODE=--dry-run; shift ;;
    --config) CONFIG="${2:?missing config path}"; shift 2 ;;
    *) echo "Usage: $0 [--dry-run|--execute] [--config path]" >&2; exit 2 ;;
  esac
done
[[ -x "$BIN" ]] || { echo "[ERROR] missing release binary: $BIN" >&2; exit 1; }
[[ -f "$CONFIG" ]] || { echo "[ERROR] missing config: $CONFIG" >&2; exit 1; }
PMDAEMON="${PMDAEMON_BIN:-pmdaemon}"
NAME="${FR_MONITOR_PROCESS_NAME:-fr_monitor}"
command -v "$PMDAEMON" >/dev/null
# Stop only this monitor via its environment-local wrapper.
"$SCRIPT_DIR/stop_fr_monitor.sh"
printf -v CMD 'set -a; if [[ -f %q ]]; then source %q; fi; set +a; exec %q --config %q %q' \
  "$BASE_DIR/env.sh" "$BASE_DIR/env.sh" "$BIN" "$CONFIG" "$MODE"
"$PMDAEMON" start /bin/bash --name "$NAME" --cwd "$BASE_DIR" -- -c "$CMD"
