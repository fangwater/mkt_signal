#!/usr/bin/env bash
set -euo pipefail

repo_root=/home/u171/fanghaizhou/mkt_signal
lock_path=/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_replay_all.lock
binary="$repo_root/target/release/cme_tas_replay_all"

cd "$repo_root"
exec 9>"$lock_path"
if ! flock -n 9; then
    echo "another CME TAS replay runner holds $lock_path" >&2
    exit 1
fi

years=("$@")
if [[ ${#years[@]} -eq 0 ]]; then
    years=(2020 2023 2022 2021)
fi

for year in "${years[@]}"; do
    config="config/cme_tas_replay_all_${year}.toml"
    echo "$(date -u +%FT%TZ) start $config"
    "$binary" --config "$config"
    echo "$(date -u +%FT%TZ) done $config"
done
