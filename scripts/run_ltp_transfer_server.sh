#!/usr/bin/env bash
set -euo pipefail
umask 077
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CREDENTIALS="${BASE_DIR}/config/credentials.env"
[[ "$(stat -c '%u:%a' "$CREDENTIALS")" == "$(id -u):600" ]]
# shellcheck disable=SC1090
source "$CREDENTIALS"
cd "$BASE_DIR"
export RUST_LOG="${RUST_LOG:-info}"
exec taskset -c 0-5 "${BASE_DIR}/ltp_transfer_server" --config config/ltp_transfer_server.toml "$@"
