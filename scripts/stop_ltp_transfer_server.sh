#!/usr/bin/env bash
set -euo pipefail
umask 077
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE_DIR"
# Preserve incident evidence before PM2 can delete its registration.
BACKUP_DIR="${BASE_DIR}/run/stop-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$BACKUP_DIR"
if [[ -d logs ]]; then cp -a logs "$BACKUP_DIR/"; fi
npx --no-install pm2 delete ltp-transfer-server --namespace ltp_transfer_server >/dev/null 2>&1 || true
for attempt in {1..30}; do
  if ! ss -ltn | awk '{print $4}' | grep -Eq '(^|:)8788$'; then
    python3 "${BASE_DIR}/scripts/update_ltp_transfer_boot.py" --stopped
    echo "[INFO] transfer server stopped; logs preserved in ${BACKUP_DIR}"
    exit 0
  fi
  sleep 0.5
done
echo "[ERROR] listener 8788 remains; investigate before restarting" >&2
exit 1
