#!/usr/bin/env bash
# A signal keeps its own environment/ledger and reads the shared combination.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
action="${1:-}"; [[ $# == 0 ]] || shift
[[ "$action" == start || "$action" == stop ]] || exit 1
combination=""; execute=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --combination) combination="$2"; shift 2 ;;
    --execute) execute=1; shift ;;
    *) echo 'Usage: cta_signal_process.sh start|stop --combination NAME [--execute]' >&2; exit 1 ;;
  esac
done
[[ "$combination" =~ ^[a-zA-Z0-9_-]{1,96}$ ]] || { echo '[ERROR] a valid combination name is required' >&2; exit 1; }
if [[ "$action" == start ]]; then
  echo '[ERROR] Position tables now run inside cta-prep-binance-futures / cta-factor-spread. Start the environment-local factor worker; no separate signal process is needed.' >&2
  exit 1
fi
echo "[PLAN] env=$BASE_DIR exchange=binance-futures symbol_scope=$combination mutation=$action-signal-process"
[[ "$execute" == 1 ]] || { echo '[DRY-RUN] no process changes'; exit 0; }
[[ ! -f "$BASE_DIR/env.sh" ]] || source "$BASE_DIR/env.sh"
cd "$BASE_DIR"
config="${CTA_COMBINATIONS_DIR:-$BASE_DIR/../cta-prep-binance-futures/config/cta_prep_binance_futures}/$combination.json"
tag="$(basename "$BASE_DIR" | tr -c 'a-zA-Z0-9_-' '_')"
name="cta_signal_$tag"
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null
source "$SCRIPT_DIR/process_match_lib.sh"
if [[ "$action" == start ]]; then
  [[ -x "$BASE_DIR/cta-signal" ]] || { echo '[ERROR] publish cta-signal first' >&2; exit 1; }
  "$BASE_DIR/cta-signal" --config "$config" --check >/dev/null
  for old_binary in cta_perp_signal cta_perp_5m_signal cta_perp_15m_signal; do
    mapfile -t old_pids < <(safe_find_running_pids "$old_binary" || true)
    for old_pid in "${old_pids[@]}"; do
      if [[ "$(readlink -f "/proc/$old_pid/cwd" 2>/dev/null || true)" == "$(readlink -f "$BASE_DIR")" ]]; then
        echo '[ERROR] stop the old signal through this environment’s stop script before switching to cta-signal' >&2
        exit 1
      fi
    done
  done
fi
timeout -k 1s 45s "$PMDAEMON_BIN" delete "$name" >/dev/null 2>&1 || true
mapfile -t leaked < <(safe_find_running_pids cta-signal "$BASE_DIR" "$config" || true)
[[ ${#leaked[@]} == 0 ]] || kill "${leaked[@]}" >/dev/null 2>&1 || true
[[ "$action" != stop ]] || exit 0
cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file"' EXIT
python3 - "$cfg_file" "$name" "$BASE_DIR" "$config" <<'PY_CONFIG'
import json, os, sys
path, name, base, config = sys.argv[1:]
allowed = ('RUST_LOG', 'RUST_BACKTRACE', 'IPC_NAMESPACE', 'CTA_SIGNAL_CORE', 'CTA_MANAGER_API_URL', 'CTA_MANAGER_API_URLS', 'CTA_PUBLISH_TOKEN', 'CTA_PUBLISH_TOKENS', 'CTA_VIRTUAL_ACCOUNT_NAME', 'CTA_FOLLOWER_SOURCE_ID', 'LD_LIBRARY_PATH')
environment = {key: os.environ[key] for key in allowed if key in os.environ}
with open(path, 'w') as output:
    json.dump({'apps': [{'name': name, 'script': base+'/cta-signal', 'args': ['--config',config], 'cwd':base, 'env':environment}]}, output)
PY_CONFIG
"$PMDAEMON_BIN" --config "$cfg_file" start --name "$name"
