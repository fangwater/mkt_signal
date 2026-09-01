#!/usr/bin/env bash
set -euo pipefail

repo_root=/home/fanghaizhou/mkt_signal
binary_root=/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/release
lock_path=/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_replay_all.lock
staging_root=/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_zstd_shards
input_root=/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1min
output_root=/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1min_size_staging
tas_unit=cme-tas-replay-2017-2019.service
min_available_kb=${CME_SIZE_MIN_AVAILABLE_KB:-134217728}

cd "$repo_root"
exec 9>"$lock_path"

# The replay runner owns the same lock while sharding and replaying. A failed
# systemd attempt may release it briefly before RestartSec starts a new one.
while :; do
    flock 9
    active_state=$(systemctl --user show "$tas_unit" --property=ActiveState --value)
    result=$(systemctl --user show "$tas_unit" --property=Result --value)
    status=$(systemctl --user show "$tas_unit" --property=ExecMainStatus --value)
    if [[ "$active_state" == inactive && "$result" == success && "$status" == 0 ]]; then
        break
    fi
    flock -u 9
    if [[ "$active_state" == failed ]]; then
        echo "$tas_unit failed; refusing to start size-bucket fill" >&2
        exit 1
    fi
    echo "$(date -u +%FT%TZ) waiting for $tas_unit active=$active_state result=$result status=$status"
    sleep 60
done

periods=(
    2017-01-01_2018-01-01
    2018-01-01_2019-01-01
    2019-01-01_2020-01-01
)
expected_sources=(5 7 6)
for index in "${!periods[@]}"; do
    period=${periods[$index]}
    period_dir="$staging_root/shanghai_evolution_futures_time_and_sales_ric_list_0_tas_$period"
    manifest="$period_dir/manifest.json"
    building="$period_dir.building"
    [[ ! -e "$building" ]] || {
        echo "incomplete staging remains: $building" >&2
        exit 1
    }
    [[ -f "$manifest" ]] || {
        echo "missing completed manifest: $manifest" >&2
        exit 1
    }
    jq -e \
        --arg period "$period" \
        --argjson sources "${expected_sources[$index]}" \
        '.complete == true
         and .period == $period
         and (.sources | length) == $sources
         and ([.sources[].complete] | all)
         and (.shards | length) > 0' \
        "$manifest" >/dev/null
    manifest_shards=$(jq '.shards | length' "$manifest")
    actual_shards=$(find "$period_dir" -maxdepth 1 -type f -name '*.csv.zst' -printf x | wc -c)
    [[ "$actual_shards" == "$manifest_shards" ]] || {
        echo "shard count mismatch period=$period manifest=$manifest_shards actual=$actual_shards" >&2
        exit 1
    }
    echo "$(date -u +%FT%TZ) manifest verified period=$period sources=${expected_sources[$index]} shards=$actual_shards"
done

while :; do
    available_kb=$(awk '/^MemAvailable:/{print $2}' /proc/meminfo)
    if [[ -n "$available_kb" ]] && (( available_kb >= min_available_kb )); then
        break
    fi
    echo "$(date -u +%FT%TZ) waiting for memory MemAvailable_kB=${available_kb:-unknown} required_kB=$min_available_kb"
    sleep 60
done

size_args=(
    --direct-read-only
    --input-root "$input_root"
    --output-root "$output_root"
    --start 2020-01-01
    --end 2026-06-01
    --products ES,NQ,RTY,YM,GC,CL
    --workers 12
)
if [[ -e "$output_root" ]]; then
    size_args+=(--overwrite)
fi

echo "$(date -u +%FT%TZ) size fill start workers=12 input=$input_root output=$output_root"
"$binary_root/cme_baseline_fill_size_buckets" "${size_args[@]}"
echo "$(date -u +%FT%TZ) size fill done output=$output_root"
