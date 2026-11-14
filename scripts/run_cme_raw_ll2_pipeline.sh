#!/usr/bin/env bash
# Independent local data job. Config/run directory/binaries are explicit inputs.
set -Eeuo pipefail
if [[ $# -ne 3 && $# -ne 4 ]] || [[ $# -eq 4 && $4 != --wait-for-source ]]; then
    echo 'usage: run_cme_raw_ll2_pipeline.sh CONFIG BIN_DIR RUN_DIR [--wait-for-source]' >&2
    exit 2
fi
raw_config=$(realpath "$1")
raw_bins=$(realpath "$2")
raw_run=$(realpath "$3")
exec 9>"$raw_run/pipeline.lock"
flock -n 9 || { echo 'pipeline already running' >&2; exit 1; }
raw_stage=preflight
raw_status() {
    python3 - "$raw_run/status.json" "$1" "$raw_stage" "$$" <<'PY'
import json,os,sys
from datetime import datetime,timezone
from pathlib import Path
p=Path(sys.argv[1]);tmp=p.with_suffix('.json.partial')
tmp.write_text(json.dumps(dict(status=sys.argv[2],stage=sys.argv[3],pid=int(sys.argv[4]),updated_at=datetime.now(timezone.utc).isoformat()),indent=2)+'\n')
os.replace(tmp,p)
PY
}
trap 'raw_status failed' ERR
trap 'raw_status interrupted; exit 130' INT TERM
printf '%s\n' "$$" >"$raw_run/pid"
sha256sum "$raw_config" "$raw_bins/cme_raw_ll2_shard" "$raw_bins/cme_raw_ll2_replay" >"$raw_run/input_sha256.txt"
# This batch source is an intranet endpoint. Exclude its configured hostname
# from inherited system proxies in this job only, including subsequent phases.
raw_source_host=$(python3 - "$raw_config" <<'PYHOST'
import sys,tomllib
from urllib.parse import urlsplit
with open(sys.argv[1], 'rb') as f:
    host=urlsplit(tomllib.load(f)['remote_root']).hostname
if not host:
    raise ValueError('remote_root requires a hostname')
print(host)
PYHOST
)
export NO_PROXY="${NO_PROXY:-},${no_proxy:-},${raw_source_host}"
export no_proxy="$NO_PROXY"
printf 'raw_http_proxy_bypass host=%s\n' "$raw_source_host"
raw_local_source=$(python3 - "$raw_config" <<'PYLOCAL'
import sys,tomllib
with open(sys.argv[1], 'rb') as f:
    print('yes' if tomllib.load(f).get('data_root') else 'no')
PYLOCAL
)
raw_download_local() {
    if [[ $raw_local_source == yes ]]; then
        raw_stage=downloading_local_gzip
        raw_status running
        python3 "$raw_bins/download_cme_raw_ll2.py" --config "$raw_config" \
            --workers "${RAW_LL2_DOWNLOAD_WORKERS:-8}" --audit "$raw_run/download_audit.json"
    fi
}
if [[ $# -eq 4 ]]; then
    # Pending source uploads can be cleaned independently, but never replayed as a full year.
    # Keep the refresh target inside this run so no shared source inventory is overwritten.
    raw_inventory=$(python3 - "$raw_config" "$raw_run" <<'PYINV'
import sys,tomllib
from pathlib import Path
with open(sys.argv[1], 'rb') as f:
    cfg=tomllib.load(f)
p=Path(cfg['inventory']).resolve()
if p.parent != Path(sys.argv[2]).resolve() or len(cfg['periods']) != 1:
    raise ValueError('--wait-for-source requires one period and a run-local inventory')
print(p)
PYINV
)
    raw_remote_root=$(python3 - "$raw_config" <<'PYROOT'
import sys,tomllib
with open(sys.argv[1], 'rb') as f:
    print(tomllib.load(f)['remote_root'])
PYROOT
)
    while true; do
        raw_stage=refreshing_source_inventory
        raw_status running
        raw_snapshot="$raw_run/inventory-$(date -u +%Y%m%dT%H%M%SZ).json"
        python3 "$raw_bins/audit_cme_ll2_inventory.py" --raw --remote-root "$raw_remote_root" --output "$raw_snapshot"
        cp "$raw_snapshot" "$raw_inventory.partial"
        mv "$raw_inventory.partial" "$raw_inventory"
        raw_ready=$(python3 - "$raw_config" "$raw_inventory" <<'PYREADY'
import sys,tomllib,json
with open(sys.argv[1], 'rb') as f:
    cfg=tomllib.load(f)
inv=json.load(open(sys.argv[2]))
p=next(p for p in inv['packages'] if p['period']==cfg['periods'][0])
ready=p['report_present'] and not p['missing_part_indices_through_last'] and not p['partial_files']
print('yes' if ready else 'no')
PYREADY
)
        raw_download_local
        if [[ $raw_ready == yes ]]; then
            break
        fi
        raw_stage=sharding_available_parts
        raw_status running
        "$raw_bins/cme_raw_ll2_shard" --config "$raw_config" --available-parts
        raw_stage=waiting_for_source_parts
        raw_status running
        sleep "${RAW_LL2_SOURCE_POLL_SECONDS:-600}"
    done
    raw_stage=preflight
else
    raw_download_local
    raw_stage=preflight
fi
raw_status running
"$raw_bins/cme_raw_ll2_replay" --config "$raw_config" --preflight
raw_stage=sharding
raw_status running
"$raw_bins/cme_raw_ll2_shard" --config "$raw_config"
raw_stage=replay
raw_status running
"$raw_bins/cme_raw_ll2_replay" --config "$raw_config" --audit "$raw_run/replay_audit.json"
raw_stage=complete
raw_status done
