#!/usr/bin/env bash
set -euo pipefail

# Configure journald retention policy for this host.
# NOTE:
#   - journald retention is global (host-level), not truly per-process.
#   - Use `journalctl --user -u <unit>` to read a specific service's logs.
#
# Default policy:
#   - Keep logs for at most 3 days
#
# Usage:
#   ./scripts/setup_journald_retention.sh
#   ./scripts/setup_journald_retention.sh --days 3

RETENTION_DAYS="3"
CONFIG_DIR="/etc/systemd/journald.conf.d"
CONFIG_FILE="${CONFIG_DIR}/99-mkt-signal-retention.conf"

usage() {
  cat <<'USAGE'
Usage:
  setup_journald_retention.sh [--days <n>]

Options:
  --days <n>    Retention in days (default: 3)
  -h, --help    Show help

Example:
  ./scripts/setup_journald_retention.sh --days 3
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --days)
      RETENTION_DAYS="${2:-}"
      [[ -n "$RETENTION_DAYS" ]] || { echo "[ERROR] --days requires a value" >&2; exit 1; }
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ ! "$RETENTION_DAYS" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --days must be an integer: $RETENTION_DAYS" >&2
  exit 1
fi

if [[ "$EUID" -ne 0 ]]; then
  if command -v sudo >/dev/null 2>&1; then
    echo "[INFO] Re-running with sudo..."
    exec sudo bash "$0" --days "$RETENTION_DAYS"
  fi
  echo "[ERROR] root privileges are required (try sudo)" >&2
  exit 1
fi

echo "[INFO] Applying journald retention policy"
echo "       retention: ${RETENTION_DAYS} day(s)"

mkdir -p "$CONFIG_DIR"
mkdir -p /var/log/journal

cat >"$CONFIG_FILE" <<EOF
[Journal]
Storage=persistent
Compress=yes
MaxRetentionSec=${RETENTION_DAYS}day
EOF

echo "[INFO] Wrote: $CONFIG_FILE"
systemctl restart systemd-journald

# Apply limits immediately to current logs.
journalctl --rotate
journalctl --vacuum-time="${RETENTION_DAYS}d"

echo "[INFO] Journald policy applied"
journalctl --disk-usage
echo "[INFO] Verify with:"
echo "       grep -n . ${CONFIG_FILE}"
echo "       journalctl --user -u dat_pbs@binance-futures.service -n 200"
