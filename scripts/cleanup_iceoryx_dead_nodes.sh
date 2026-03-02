#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BIN_NAME="iceoryx_dead_nodes_cleanup"

BIN_CANDIDATES=(
  "${BASE_DIR}/${BIN_NAME}"
  "${BASE_DIR}/target/release/${BIN_NAME}"
  "${BASE_DIR}/target/debug/${BIN_NAME}"
)

for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    exec "$cand" "$@"
  fi
done

if [[ -f "${BASE_DIR}/Cargo.toml" ]] && command -v cargo >/dev/null 2>&1; then
  exec cargo run --manifest-path "${BASE_DIR}/Cargo.toml" --release --bin "${BIN_NAME}" -- "$@"
fi

echo "[ERROR] ${BIN_NAME} not found and cargo is unavailable." >&2
echo "[HINT] Build first: cargo build --release --bin ${BIN_NAME}" >&2
exit 1
