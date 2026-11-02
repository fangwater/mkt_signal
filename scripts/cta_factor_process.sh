#!/usr/bin/env bash
# Environment-local lifecycle only; factor workers never publish position targets.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
kind="${1:-}"; action="${2:-}"; shift "$(( $# >= 2 ? 2 : $# ))"
[[ "$kind" == perp || "$kind" == spread ]] || { echo '[ERROR] expected perp or spread' >&2; exit 1; }
[[ "$action" == start || "$action" == stop ]] || { echo '[ERROR] expected start or stop' >&2; exit 1; }
[[ $# == 0 || ( $# == 1 && "$1" == --execute ) ]] || { echo 'Usage: cta_factor_process.sh perp|spread start|stop [--execute]' >&2; exit 1; }
name="cta-factor-$kind"
config="config/cta_factor_$kind.toml"
[[ "$kind" != perp ]] || { name="cta-prep-binance-futures"; config="config/cta_prep_binance_futures.toml"; }
echo "[PLAN] env=$BASE_DIR exchange=merged-factor-markets symbol_scope=$config mutation=$action-factor-process"
[[ "${1:-}" == --execute ]] || { echo '[DRY-RUN] no process changes'; exit 0; }
[[ ! -f "$BASE_DIR/env.sh" ]] || source "$BASE_DIR/env.sh"
cd "$BASE_DIR"
source "$SCRIPT_DIR/process_match_lib.sh"
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null
if [[ "$action" == start ]]; then
  [[ -x "$BASE_DIR/$name" ]] || { echo "[ERROR] missing published artifact $BASE_DIR/$name" >&2; exit 1; }
  "$BASE_DIR/$name" --config "$config" --check >/dev/null
fi
# This wrapper owns cwd, environment and matching real processes, in addition
# to the supervisor entry. Do not replace it with operator pmdaemon commands.
timeout -k 1s 45s "$PMDAEMON_BIN" delete "$name" >/dev/null 2>&1 || true
mapfile -t leaked < <(safe_find_running_pids "$name" "$BASE_DIR" "$config" || true)
[[ ${#leaked[@]} == 0 ]] || kill "${leaked[@]}" >/dev/null 2>&1 || true
if [[ "$action" == stop ]]; then echo "[INFO] stopped $name"; exit 0; fi
cfg_file="$(mktemp)"
trap 'python3 -c "from pathlib import Path; import sys; Path(sys.argv[1]).unlink(missing_ok=True)" "$cfg_file"' EXIT
python3 - "$cfg_file" "$name" "$BASE_DIR" "$config" <<'PY_CONFIG'
import json, os, sys
path, name, base, config = sys.argv[1:]
with open(path, 'w') as output:
    json.dump({'apps': [{'name': name, 'script': base + '/' + name, 'args': ['--config', config], 'cwd': base, 'env': {'RUST_LOG': os.getenv('RUST_LOG', 'info')}}]}, output)
PY_CONFIG
"$PMDAEMON_BIN" --config "$cfg_file" start --name "$name"
echo "[INFO] started $name"
