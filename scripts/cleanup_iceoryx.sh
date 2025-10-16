#!/usr/bin/env bash
# Clean up stale Iceoryx shared-memory artifacts and stop any running iox-roudi
# instance. Use this when channel type mismatches (IncompatibleTypes) occur.

set -euo pipefail

log() {
  echo "[cleanup_iceoryx] $*"
}

# Stop any running iox-roudi process so new shared-memory layout can be created.
if pgrep -f iox-roudi >/dev/null 2>&1; then
  log "Stopping existing iox-roudi instances..."
  pkill -f iox-roudi || log "pkill iox-roudi returned non-zero (process may have already exited)."
else
  log "No iox-roudi process detected."
fi

# Paths typically used by Iceoryx for shared-memory segments and bookkeeping.
targets=(
  /tmp/roudi
  /tmp/poseidon* 
  /tmp/iceoryx*
  /tmp/iox*
  /dev/shm/iceoryx_* 
  /dev/shm/iox_* 
  /dev/shm/roudi_* 
)

need_sudo=false
for path in "${targets[@]}"; do
  if compgen -G "$path" >/dev/null 2>&1; then
    if [[ ! -w ${path%%[*} ]]; then
      need_sudo=true
      break
    fi
  fi
done

rm_cmd=(rm -rf)
if $need_sudo && command -v sudo >/dev/null 2>&1; then
  rm_cmd=(sudo rm -rf)
fi

for pattern in "${targets[@]}"; do
  if compgen -G "$pattern" >/dev/null 2>&1; then
    log "Removing $pattern"
    "${rm_cmd[@]}" $pattern || log "Failed to remove $pattern"
  fi
done

log "Cleanup complete. Restart iox-roudi before relaunching producers/consumers."
