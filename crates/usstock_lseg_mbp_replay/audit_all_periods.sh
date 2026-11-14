#!/usr/bin/env bash

set -Eeuo pipefail

readonly PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly SOURCE_ROOT="${USSTOCK_MBP_SOURCE_ROOT:-/mnt/hdd-raid5-72t/liang_torch/usstock_data/raw_data/mbp}"
readonly OUTPUT_ROOT="${USSTOCK_MBP_AUDIT_ROOT:-/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/usstock_mbp_audit}"
readonly TARGET_DIR="${USSTOCK_MBP_TARGET_DIR:-$PROJECT_DIR/target}"
readonly AUDITOR="$TARGET_DIR/release/audit_templates"
readonly LOCK_FILE="$OUTPUT_ROOT/.audit.lock"

log() {
  printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

mkdir -p "$OUTPUT_ROOT"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  log "another template audit holds $LOCK_FILE"
  exit 1
fi

if [[ ! -x "$AUDITOR" ]]; then
  log "missing audit binary $AUDITOR"
  exit 1
fi

mapfile -t inputs < <(find "$SOURCE_ROOT" -mindepth 2 -maxdepth 2 -type f -name merged-Data.csv.gz | sort)
if [[ "${#inputs[@]}" -eq 0 ]]; then
  log "no raw MBP inputs under $SOURCE_ROOT"
  exit 1
fi

for input in "${inputs[@]}"; do
  directory=$(basename -- "$(dirname -- "$input")")
  period=${directory##*_mbp_}
  output="$OUTPUT_ROOT/$period.full.json"
  if [[ -f "$output" ]]; then
    log "skip completed period=$period output=$output"
    continue
  fi
  log "start period=$period input=$input"
  ionice -c 2 -n 7 nice -n 10 "$AUDITOR" \
    --input "$input" \
    --output "$output" \
    --progress-every 1000000
  log "complete period=$period output=$output"
done

log "all raw MBP template audits complete"
