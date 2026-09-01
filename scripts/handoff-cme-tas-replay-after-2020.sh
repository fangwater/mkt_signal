#!/usr/bin/env bash
set -euo pipefail

main_unit=cme-tas-replay-2020-2023.service
next_unit=cme-tas-replay-2023-2021.service
repo_root=/home/u171/fanghaizhou/mkt_signal
runner="$repo_root/scripts/run-cme-tas-replay-2020-2023.sh"
stdout_log=/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/logs/cme_tas_replay_all_2020_2023.stdout.log
done_marker='done config/cme_tas_replay_all_2020.toml'

while ! grep -Fq "$done_marker" "$stdout_log"; do
    if ! systemctl --user is-active --quiet "$main_unit"; then
        echo "$main_unit stopped before 2020 completed" >&2
        exit 1
    fi
    sleep 1
done

# The old bash loop may have spawned 2021, but opening the large RocksDB takes
# much longer than this one-second watcher interval, so it has not claimed 2021.
systemctl --user stop "$main_unit"

systemd-run --user --unit="$next_unit" --collect \
    --property="WorkingDirectory=$repo_root" \
    --property="StandardOutput=append:$stdout_log" \
    --property="StandardError=append:$stdout_log" \
    "$runner" 2023 2022 2021
