#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
HELPER="${BASE_DIR}/scripts/configure_persist_sync_source.sh"

if [[ ! -x "$HELPER" ]]; then
  echo "[ERROR] missing table-driven persist sync helper: $HELPER" >&2
  echo "[ERROR] redeploy/sync scripts/configure_persist_sync_source.sh and config/persist_sync_distribution.toml" >&2
  exit 1
fi

exec "$HELPER" "$@"
