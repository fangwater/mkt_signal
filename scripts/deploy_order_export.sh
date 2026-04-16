#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-/home/${USER}/order_export}"

echo "[INFO] installing order_export into ${INSTALL_ROOT}"
cargo install --path "${BASE_DIR}" --bin order_export --root "${INSTALL_ROOT}" --force --locked

echo "[INFO] binary installed at ${INSTALL_ROOT}/bin/order_export"
echo "[INFO] set once:"
echo "  export ORDER_EXPORT_BASE_DIR=/home/${USER}"
echo "[INFO] example:"
echo "  cd /home/${USER}/binance_mm_alpha"
echo "  ${INSTALL_ROOT}/bin/order_export --date 2026-03-25"
echo "  ${INSTALL_ROOT}/bin/order_export --start 2026-03-25T01:02:03Z --end 2026-03-25T02:03:04Z"
echo "[INFO] default output dir:"
echo "  ./20260325"
echo "  ./20260325T010203.000000Z__20260325T020304.000000Z"
