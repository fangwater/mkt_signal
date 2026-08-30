#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/home/u171/fanghaizhou/mkt_signal"
SHARD_ROOT="/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_ll2_zstd_shards"
LOG_ROOT="/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/logs"
TAS_PATTERN="^/home/u171/fanghaizhou/mkt_signal/target/release/cme_tas_replay_all "

while pgrep -f "${TAS_PATTERN}" >/dev/null; do
  sleep 30
done

cd "${REPO_ROOT}"

MANIFEST_COUNT="$(find "${SHARD_ROOT}" -name manifest.json -type f 2>/dev/null | wc -l)"
if [[ "${MANIFEST_COUNT}" -ne 17 ]]; then
  RUST_LOG=info target/release/cme_ll2_shard \
    --config config/cme_ll2_shard.toml \
    >> "${LOG_ROOT}/cme_ll2_shard.log" 2>&1
fi

target/release/cme_ll2_replay \
  --config config/cme_ll2_replay.toml --preflight \
  >> "${LOG_ROOT}/cme_ll2_replay.log" 2>&1

RUST_LOG=info target/release/cme_ll2_replay \
  --config config/cme_ll2_replay.toml \
  >> "${LOG_ROOT}/cme_ll2_replay.log" 2>&1

target/release/cme_ll2_replay \
  --config config/cme_ll2_replay.toml --verify \
  >> "${LOG_ROOT}/cme_ll2_replay.log" 2>&1

find "${SHARD_ROOT}" -depth -delete
