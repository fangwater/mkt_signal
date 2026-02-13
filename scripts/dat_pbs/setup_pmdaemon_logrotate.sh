#!/usr/bin/env bash
set -euo pipefail

# Install/refresh global logrotate policy for pmdaemon logs.
# Default policy:
#   - max file size: 500M
#   - retention: 3 days
#
# Covered paths:
#   - /home/*/.pmdaemon/logs/*.log
#   - /root/.pmdaemon/logs/*.log
#
# Usage:
#   ./scripts/dat_pbs/setup_pmdaemon_logrotate.sh
#   ./scripts/dat_pbs/setup_pmdaemon_logrotate.sh --dry-run
#   ./scripts/dat_pbs/setup_pmdaemon_logrotate.sh --force-rotate

CONFIG_FILE="/etc/logrotate.d/pmdaemon"
MAX_SIZE="500M"
RETENTION_DAYS="3"
DRY_RUN="0"
FORCE_ROTATE="0"

usage() {
  cat <<'USAGE'
Usage:
  setup_pmdaemon_logrotate.sh [--dry-run] [--force-rotate]

Options:
  --dry-run       Validate config only (logrotate -d), do not force rotate
  --force-rotate  Force one rotation immediately after install (logrotate -f)
  -h, --help      Show help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN="1"
      shift
      ;;
    --force-rotate)
      FORCE_ROTATE="1"
      shift
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

if [[ "$EUID" -ne 0 ]]; then
  if command -v sudo >/dev/null 2>&1; then
    echo "[INFO] Re-running with sudo..."
    sudo_args=()
    if [[ "$DRY_RUN" == "1" ]]; then
      sudo_args+=("--dry-run")
    fi
    if [[ "$FORCE_ROTATE" == "1" ]]; then
      sudo_args+=("--force-rotate")
    fi
    exec sudo bash "$0" "${sudo_args[@]}"
  fi
  echo "[ERROR] root privileges are required (try sudo)" >&2
  exit 1
fi

mkdir -p "$(dirname "$CONFIG_FILE")"

cat >"$CONFIG_FILE" <<EOF
/home/*/.pmdaemon/logs/*.log /root/.pmdaemon/logs/*.log {
    daily
    maxsize ${MAX_SIZE}
    rotate ${RETENTION_DAYS}
    maxage ${RETENTION_DAYS}
    missingok
    notifempty
    compress
    delaycompress
    dateext
    copytruncate
}
EOF

echo "[INFO] Wrote ${CONFIG_FILE}"
echo "[INFO] Policy: maxsize=${MAX_SIZE}, retention=${RETENTION_DAYS} day(s)"

if ! command -v logrotate >/dev/null 2>&1; then
  echo "[WARN] logrotate command not found. Install it, then validate with:"
  echo "       sudo logrotate -d ${CONFIG_FILE}"
  exit 0
fi

echo "[INFO] Validating config: logrotate -d ${CONFIG_FILE}"
logrotate -d "${CONFIG_FILE}"

if [[ "${FORCE_ROTATE}" == "1" && "${DRY_RUN}" != "1" ]]; then
  echo "[INFO] Forcing one rotation: logrotate -f ${CONFIG_FILE}"
  logrotate -f "${CONFIG_FILE}"
fi

echo "[INFO] Done"
