#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="${FR_MONITOR_BIN:-$BASE_DIR/fr_monitor}"
PMDAEMON="${PMDAEMON_BIN:-pmdaemon}"
NAME="${FR_MONITOR_PROCESS_NAME:-fr_monitor}"
command -v "$PMDAEMON" >/dev/null
# Env-specific name; never touch trading-stack processes.
"$PMDAEMON" stop "$NAME" >/dev/null 2>&1 || true
"$PMDAEMON" delete "$NAME" >/dev/null 2>&1 || true
# Supervisor registration can disappear while its child is still alive. Match
# the exact deployed executable, including an atomically replaced old inode.
python3 - "$BIN" <<'PY'
import os, pathlib, signal, sys, time
target = str(pathlib.Path(sys.argv[1]).resolve())
if pathlib.Path(target).name != 'fr_monitor':
    raise SystemExit('refusing to clean up a non-fr_monitor executable')
def matches():
    result = []
    for proc in pathlib.Path('/proc').glob('[0-9]*'):
        try:
            if os.readlink(proc / 'exe').removesuffix(' (deleted)') == target:
                result.append(int(proc.name))
        except OSError:
            pass
    return result
for pid in matches():
    try: os.kill(pid, signal.SIGTERM)
    except ProcessLookupError: pass
deadline = time.monotonic() + 10
while matches() and time.monotonic() < deadline:
    time.sleep(.1)
for pid in matches():
    try: os.kill(pid, signal.SIGKILL)
    except ProcessLookupError: pass
if matches():
    raise SystemExit('fr_monitor still present after stop; refusing restart')
PY
