#!/usr/bin/env bash
set -euo pipefail

repo_root=/home/fanghaizhou/mkt_signal
binary_root=/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/release
lock_path=/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_replay_all.lock
staging_root=/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_zstd_shards
min_available_kb=${CME_TAS_MIN_AVAILABLE_KB:-134217728}

cd "$repo_root"
exec 9>"$lock_path"
if ! flock -n 9; then
    echo "another CME TAS replay runner holds $lock_path" >&2
    exit 1
fi

# A killed sharder cannot resume because no complete manifest exists. Preserve
# its staging before a systemd retry starts again from the read-only gzip source.
failed_root="$staging_root/_failed_2017_2019_$(date -u +%Y%m%dT%H%M%SZ)_${BASHPID}"
for period in \
    2017-01-01_2018-01-01 \
    2018-01-01_2019-01-01 \
    2019-01-01_2020-01-01; do
    building="$staging_root/shanghai_evolution_futures_time_and_sales_ric_list_0_tas_${period}.building"
    if [[ -d "$building" ]]; then
        mkdir -p "$failed_root"
        mv "$building" "$failed_root/"
        echo "$(date -u +%FT%TZ) quarantined incomplete staging $building -> $failed_root/"
    fi
done

while :; do
    available_kb=$(awk '/^MemAvailable:/{print $2}' /proc/meminfo)
    if [[ -n "$available_kb" ]] && (( available_kb >= min_available_kb )); then
        break
    fi
    echo "$(date -u +%FT%TZ) waiting for memory MemAvailable_kB=${available_kb:-unknown} required_kB=$min_available_kb"
    sleep 60
done
echo "$(date -u +%FT%TZ) memory gate passed MemAvailable_kB=$available_kb required_kB=$min_available_kb"

shard_configs=(
    config/cme_tas_shard_2017.toml
    config/cme_tas_shard_2018.toml
    config/cme_tas_shard_2019.toml
)
declare -A shard_jobs=()
for config in "${shard_configs[@]}"; do
    echo "$(date -u +%FT%TZ) start $config"
    "$binary_root/cme_tas_shard" --config "$config" &
    shard_jobs[$!]=$config
done

remaining=${#shard_jobs[@]}
while (( remaining > 0 )); do
    finished_pid=
    if wait -n -p finished_pid "${!shard_jobs[@]}"; then
        echo "$(date -u +%FT%TZ) done ${shard_jobs[$finished_pid]}"
        unset 'shard_jobs[$finished_pid]'
        remaining=$((remaining - 1))
        continue
    else
        status=$?
    fi

    failed_config=${shard_jobs[$finished_pid]:-unknown}
    echo "$(date -u +%FT%TZ) failed $failed_config status=$status" >&2
    unset 'shard_jobs[$finished_pid]'
    for pid in "${!shard_jobs[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait || true
    exit "$status"
done

for year in 2017 2018 2019; do
    config="config/cme_tas_replay_all_${year}.toml"
    echo "$(date -u +%FT%TZ) start $config"
    "$binary_root/cme_tas_replay_all" --config "$config"
    echo "$(date -u +%FT%TZ) done $config"
done
