#!/usr/bin/env bash
set -euo pipefail

project_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$project_dir"

config=${1:-raw_rocksdb.toml}
target_dir=${USSTOCK_RAW_TARGET_DIR:-$project_dir/target}
binary=$target_dir/release/usstock_lseg_raw_rocksdb

export RUST_LOG=${RUST_LOG:-info}
export RUST_BACKTRACE=${RUST_BACKTRACE:-1}

cargo build --release --locked --target-dir "$target_dir" --bin usstock_lseg_raw_rocksdb

workspace_manifest=$(cargo locate-project --workspace --message-format plain)
workspace_lock=$(dirname -- "$workspace_manifest")/Cargo.lock
if find Cargo.toml "$workspace_lock" src -type f -newer "$binary" -print -quit | grep -q .; then
    echo "refusing replay: source is newer than $binary" >&2
    exit 1
fi

exec "$binary" --config "$config"
