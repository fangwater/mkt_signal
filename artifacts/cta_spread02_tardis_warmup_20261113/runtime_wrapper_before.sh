#!/usr/bin/env bash
# Environment-local lifecycle for factor calculation and its position tables.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
kind="${1:-}"; action="${2:-}"; shift "$(( $# >= 2 ? 2 : $# ))"
[[ "$kind" == perp || "$kind" == spread ]] || { echo '[ERROR] expected perp or spread' >&2; exit 1; }
[[ "$action" == start || "$action" == stop ]] || { echo '[ERROR] expected start or stop' >&2; exit 1; }
config_service=0; execute=0
for option in "$@"; do
  case "$option" in --config-service) config_service=1 ;; --execute) execute=1 ;; *) echo 'Usage: cta_factor_process.sh perp|spread start|stop [--config-service] [--execute]' >&2; exit 1 ;; esac
done
name="cta-factor-$kind"
config="config/cta_factor_$kind.toml"
[[ "$kind" != perp ]] || { name="cta-prep-binance-futures"; config="config/cta_prep_binance_futures.toml"; }
binary="$name"
if [[ "$config_service" == 1 ]]; then
  name="${name}-config"; binary=cta-factor-config
fi
echo "[PLAN] env=$BASE_DIR process=$name exchange=merged-factor-markets symbol_scope=$config mutation=$action-process"
[[ "$execute" == 1 ]] || { echo '[DRY-RUN] no process changes'; exit 0; }
[[ ! -f "$BASE_DIR/env.sh" ]] || source "$BASE_DIR/env.sh"
process_args=(--config "$config")
if [[ "$config_service" == 1 ]]; then
  default_port=19220; [[ "$kind" != spread ]] || default_port=19221
  process_args+=(--bind 127.0.0.1 --port "${CTA_FACTOR_CONFIG_PORT:-$default_port}")
  [[ "$kind" != spread ]] || process_args+=(--spread)
  if [[ "$kind" == perp ]]; then
    spread_config="${CTA_SPREAD_FACTOR_CONFIG:-$BASE_DIR/../cta-factor-spread/config/cta_factor_spread.toml}"
    if [[ -n "${CTA_SPREAD_FACTOR_CONFIG:-}" || -f "$spread_config" ]]; then
      process_args+=(--spread-config "$spread_config")
    fi
  fi
fi
cd "$BASE_DIR"
source "$SCRIPT_DIR/process_match_lib.sh"
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null
if [[ "$action" == start ]]; then
  [[ -x "$BASE_DIR/$binary" ]] || { echo "[ERROR] missing published artifact $BASE_DIR/$binary" >&2; exit 1; }
  "$BASE_DIR/$binary" "${process_args[@]}" --check >/dev/null
fi
# This wrapper owns cwd, environment and matching real processes, in addition
# to the supervisor entry. Do not replace it with operator pmdaemon commands.
timeout -k 1s 45s "$PMDAEMON_BIN" delete "$name" >/dev/null 2>&1 || true
mapfile -t leaked < <(safe_find_running_pids "$binary" "$BASE_DIR" "$config" || true)
[[ ${#leaked[@]} == 0 ]] || kill "${leaked[@]}" >/dev/null 2>&1 || true
if [[ "$action" == stop ]]; then echo "[INFO] stopped $name"; exit 0; fi
cfg_file="$(mktemp)"
trap 'python3 -c "from pathlib import Path; import sys; Path(sys.argv[1]).unlink(missing_ok=True)" "$cfg_file"' EXIT
python3 - "$cfg_file" "$name" "$BASE_DIR" "$binary" "${process_args[@]}" <<'PY_CONFIG'
import json, os, sys
path, name, base, binary, *arguments = sys.argv[1:]
allowed = ('RUST_LOG', 'RUST_BACKTRACE', 'IPC_NAMESPACE', 'CTA_PREP_BINANCE_FUTURES_CORE', 'CTA_FACTOR_SPREAD_CORE', 'LD_LIBRARY_PATH', 'CTA_MANAGER_API_URL', 'CTA_MANAGER_API_URLS', 'CTA_PUBLISH_TOKEN', 'CTA_PUBLISH_TOKENS')
environment = {key: os.environ[key] for key in allowed if key in os.environ}
environment.setdefault('RUST_LOG', 'info')
with open(path, 'w') as output:
    json.dump({'apps': [{'name': name, 'script': base + '/' + binary, 'args': arguments, 'cwd': base, 'env': environment}]}, output)
PY_CONFIG
"$PMDAEMON_BIN" --config "$cfg_file" start --name "$name"
echo "[INFO] started $name"
