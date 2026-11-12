#!/usr/bin/env bash
# Local deployment; build only in the maintained arbmm worktree.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target_dir="${HOME}/cta-memory-replay"
bind=127.0.0.1; port=10242; execute=0; skip_build=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir) target_dir="$2"; shift 2 ;;
    --bind) bind="$2"; shift 2 ;;
    --port) port="$2"; shift 2 ;;
    --execute) execute=1; shift ;;
    --skip-build) skip_build=1; shift ;;
    *) echo 'Usage: deploy_cta_memory_replay.sh [--dir PATH] [--bind IP] [--port N] [--skip-build] [--execute]' >&2; exit 1 ;;
  esac
done
cd "$ROOT"
[[ "$(git branch --show-current)" == arbmm ]] || { echo '[ERROR] deploy requires arbmm' >&2; exit 1; }
git fetch origin arbmm >/dev/null
[[ "$(git rev-parse HEAD)" == "$(git rev-parse origin/arbmm)" ]] || { echo '[ERROR] synchronize arbmm before deployment' >&2; exit 1; }
target_dir="$(realpath -m "$target_dir")"
[[ "$target_dir" != "$ROOT" && "$target_dir" != / && "$target_dir" != "$HOME" ]] || { echo '[ERROR] choose a separate deployment directory' >&2; exit 1; }
echo "[PLAN] env=$target_dir exchange=binance-futures symbols=JP-Prep-combinations mutation=install-independent-replay-runtime listen=$bind:$port"
[[ "$execute" == 1 ]] || { echo '[DRY-RUN] no files installed; no processes started'; exit 0; }
[[ "$skip_build" == 1 ]] || cargo build --release --bin cta-memory-replay
mkdir -p "$target_dir"/{config,scripts,run,logs,web/cta_memory_replay,web/vendor}
install -m 644 web/cta_memory_replay/index.html web/cta_memory_replay/app.js "$target_dir/web/cta_memory_replay/"
install -m 644 web/vendor/echarts.min.js "$target_dir/web/vendor/"
for file in bootstrap_cta_memory_replay.py cta_memory_replay_process.sh start_cta_memory_replay.sh stop_cta_memory_replay.sh status_cta_memory_replay.sh; do
  install -m 755 "$ROOT/scripts/$file" "$target_dir/scripts/$file"
done
if [[ ! -f "$target_dir/config/bootstrap.json" ]]; then
  python3 "$target_dir/scripts/bootstrap_cta_memory_replay.py" --output "$target_dir/config/bootstrap.json" --execute
fi
if [[ ! -f "$target_dir/env.sh" ]]; then
  python3 - "$target_dir/env.sh" "$bind" "$port" <<'PY'
import shlex,sys
from pathlib import Path
path,bind,port=sys.argv[1:]
values={'CTA_REPLAY_BIND':bind,'CTA_REPLAY_PORT':port,'CTA_REPLAY_BROKERS':'10.61.10.22:9092','CTA_REPLAY_HISTORY_DAYS':'7'}
Path(path).write_text(''.join('export '+k+'='+shlex.quote(v)+'\n' for k,v in values.items()))
PY
fi
artifact="$ROOT/target/release/cta-memory-replay"
temporary="$(mktemp "$target_dir/.cta-memory-replay.XXXXXX")"
trap 'rm -f "$temporary"' EXIT
install -m 755 "$artifact" "$temporary"
expected="$(sha256sum "$artifact" | cut -d' ' -f1)"
actual="$(sha256sum "$temporary" | cut -d' ' -f1)"
[[ "$expected" == "$actual" ]] || { echo '[ERROR] artifact checksum mismatch' >&2; exit 1; }
"$temporary" --bootstrap "$target_dir/config/bootstrap.json" --check
mv -f "$temporary" "$target_dir/cta-memory-replay"
python3 - "$target_dir/run/release.json" "$expected" "$(git rev-parse HEAD)" <<'PY'
import datetime,json,subprocess,sys
from pathlib import Path
path,checksum,commit=sys.argv[1:]
Path(path).write_text(json.dumps({'deployed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'binary_sha256':checksum,'source_commit':commit,
    'dirty_worktree':bool(subprocess.check_output(['git','status','--porcelain']).strip())},indent=2)+'\n')
PY
echo "[INFO] published sha256=$expected; start using $target_dir/scripts/start_cta_memory_replay.sh --execute"
