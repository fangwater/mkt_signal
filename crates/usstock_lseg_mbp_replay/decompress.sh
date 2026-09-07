#!/usr/bin/env bash

set -Eeuo pipefail

readonly SOURCE_ROOT="${USSTOCK_MBP_SOURCE_ROOT:-/mnt/hdd-raid5-72t/liang_torch/usstock_data/raw_data/mbp}"
readonly OUTPUT_ROOT="${USSTOCK_MBP_CSV_ROOT:-/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/usstock_mbp_csv}"
readonly LOCK_FILE="$OUTPUT_ROOT/.decompress.lock"

log() {
  printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

publish_gzip() {
  local source_path="$1"
  local output_path="$2"
  local partial_path="${output_path}.partial"

  if [[ -f "$output_path" ]]; then
    log "skip existing output=$output_path bytes=$(stat -c %s "$output_path")"
    return
  fi

  if [[ -e "$partial_path" ]]; then
    log "discarding incomplete output=$partial_path bytes=$(stat -c %s "$partial_path")"
    unlink -- "$partial_path"
  fi

  log "decompressing source=$source_path output=$output_path"
  gzip -dc -- "$source_path" >"$partial_path"
  sync -d "$partial_path"
  mv -- "$partial_path" "$output_path"
  log "published output=$output_path bytes=$(stat -c %s "$output_path")"
}

mkdir -p "$OUTPUT_ROOT"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  log "another decompression process holds $LOCK_FILE"
  exit 1
fi

mapfile -t period_dirs < <(find "$SOURCE_ROOT" -mindepth 1 -maxdepth 1 -type d -name 'shanghai_evolution_equities_mbp_ric_list_0_mbp_*' | sort)
if [[ "${#period_dirs[@]}" -eq 0 ]]; then
  log "no MBP period directories found under $SOURCE_ROOT"
  exit 1
fi

for source_dir in "${period_dirs[@]}"; do
  period_name=$(basename "$source_dir")
  output_dir="$OUTPUT_ROOT/$period_name"
  mkdir -p "$output_dir"

  publish_gzip "$source_dir/merged-Data.csv.gz" "$output_dir/merged-Data.csv"
  publish_gzip "$source_dir/merged-Report.csv.gz" "$output_dir/merged-Report.csv"

  notes_count=0
  while IFS= read -r -d '' notes_path; do
    notes_name=$(basename "$notes_path")
    if [[ ! -f "$output_dir/$notes_name" ]]; then
      cp -- "$notes_path" "$output_dir/$notes_name"
    fi
    notes_count=$((notes_count + 1))
  done < <(find "$source_dir" -mindepth 1 -maxdepth 1 -type f -name '*.notes.txt' -print0)
  if [[ "$notes_count" -ne 1 ]]; then
    log "expected one notes file in $source_dir, found $notes_count"
    exit 1
  fi

  printf 'source_data_bytes=%s\noutput_data_bytes=%s\ncompleted_utc=%s\n' \
    "$(stat -c %s "$source_dir/merged-Data.csv.gz")" \
    "$(stat -c %s "$output_dir/merged-Data.csv")" \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    >"$output_dir/decompress.complete"
done

log "all MBP periods decompressed output_root=$OUTPUT_ROOT"
