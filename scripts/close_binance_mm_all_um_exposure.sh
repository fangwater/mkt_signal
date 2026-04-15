#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "[WARN] scripts/close_binance_mm_all_um_exposure.sh 已改名，建议改用 scripts/close_mm_all_um_exposure.sh" >&2
exec "$SCRIPT_DIR/close_mm_all_um_exposure.sh" "$@"
