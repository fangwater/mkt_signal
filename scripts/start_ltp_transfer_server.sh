#!/usr/bin/env bash
set -euo pipefail
umask 077
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CREDENTIALS="${BASE_DIR}/config/credentials.env"
if [[ ! -f "$CREDENTIALS" || "$(stat -c '%u:%a' "$CREDENTIALS")" != "$(id -u):600" ]]; then
  echo "[ERROR] config/credentials.env must belong to this user with mode 600" >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$CREDENTIALS"
if [[ -z "${LTP_TRANSFER_API_KEY:-}" || -z "${LTP_TRANSFER_API_SECRET:-}" ]]; then
  echo "[ERROR] dedicated transfer credentials are required" >&2
  exit 1
fi
if [[ ! -x "${BASE_DIR}/ltp_transfer_server" ]]; then
  echo "[ERROR] publish a locally built release binary first" >&2
  exit 1
fi
if ss -ltn | awk '{print $4}' | grep -Eq '(^|:)8788$'; then
  echo "[ERROR] port 8788 is in use; stop the environment before starting" >&2
  exit 1
fi
cd "$BASE_DIR"
mkdir -p logs data/transfers
# Credentials are loaded inside the child, so PM2's process registry never stores them.
unset LTP_TRANSFER_API_KEY LTP_TRANSFER_API_SECRET
npx --no-install pm2 --silent start "${BASE_DIR}/scripts/run_ltp_transfer_server.sh" \
  --name ltp-transfer-server --namespace ltp_transfer_server --interpreter none \
  --output "${BASE_DIR}/logs/out.log" --error "${BASE_DIR}/logs/error.log" \
  --time --restart-delay 3000 --kill-timeout 15000
python3 "${BASE_DIR}/scripts/update_ltp_transfer_boot.py"
echo "[INFO] LTP transfer server started on loopback:8788, CPU 0-5"
