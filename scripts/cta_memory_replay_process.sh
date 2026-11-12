#!/usr/bin/env bash
# Always invoke this environment-local wrapper for PM2 lifecycle operations.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
action="${1:-}"
[[ "$action" == start || "$action" == stop ]] || { echo 'Usage: cta_memory_replay_process.sh start|stop [--execute]' >&2; exit 1; }
execute=0
[[ "${2:-}" != --execute ]] || execute=1
[[ $# -le 2 ]] || exit 1
[[ ! -f "$BASE_DIR/env.sh" ]] || source "$BASE_DIR/env.sh"
tag="$(basename "$BASE_DIR")"
tag="$(printf '%s' "$tag" | tr -c 'a-zA-Z0-9_-' '_')"
name="${CTA_REPLAY_PROCESS_NAME:-$tag}"
echo "[PLAN] env=$BASE_DIR exchange=binance-futures symbols=JP-Prep-combinations process=$name mutation=$action-simulation-process"
[[ "$execute" == 1 ]] || { echo '[DRY-RUN] no process changes'; exit 0; }
cd "$BASE_DIR"
export PM2_HOME="${CTA_REPLAY_PM2_HOME:-$BASE_DIR/run/pm2}"
PM2_BIN="${PM2_BIN:-pm2}"
command -v "$PM2_BIN" >/dev/null
if [[ "$action" == stop ]]; then
  "$PM2_BIN" delete "$name" >/dev/null 2>&1 || true
  echo "[INFO] stopped $name"
  exit 0
fi
[[ -x "$BASE_DIR/cta-memory-replay" ]] || { echo '[ERROR] missing published binary' >&2; exit 1; }
"$BASE_DIR/cta-memory-replay" --bootstrap config/bootstrap.json --check
mkdir -p run logs
python3 - "$BASE_DIR" "$name" <<'PY'
import json,os,sys
from pathlib import Path
base,name=sys.argv[1:]
args=['--bootstrap','config/bootstrap.json','--brokers',os.getenv('CTA_REPLAY_BROKERS','10.61.10.22:9092'),
      '--bind',os.getenv('CTA_REPLAY_BIND','127.0.0.1'),'--port',os.getenv('CTA_REPLAY_PORT','10242'),
      '--history-days',os.getenv('CTA_REPLAY_HISTORY_DAYS','7')]
config={'apps':[{'name':name,'namespace':'cta-replay','script':base+'/cta-memory-replay','args':args,'interpreter':'none',
    'cwd':base,'instances':1,'exec_mode':'fork','autorestart':True,'restart_delay':3000,
    'max_restarts':10,'min_uptime':10000,'kill_timeout':10000,
    'out_file':base+'/logs/replay.out.log','error_file':base+'/logs/replay.error.log',
    'log_date_format':'YYYY-MM-DD HH:mm:ss Z','env':{'RUST_LOG':os.getenv('RUST_LOG','info')}}]}
(Path(base)/'run/ecosystem.json').write_text(json.dumps(config,indent=2)+'\n')
PY
"$PM2_BIN" startOrRestart "$BASE_DIR/run/ecosystem.json" --only "$name" --update-env
"$PM2_BIN" save --force >/dev/null
echo "[INFO] started $name at http://${CTA_REPLAY_BIND:-127.0.0.1}:${CTA_REPLAY_PORT:-10242}/"
