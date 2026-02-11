#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="${HOME}/persist_exporter_public"
DO_START=0

usage() {
  cat <<'USAGE'
Usage:
  scripts/deploy_persist_exporter_public_stack.sh [--start]

What it does:
  1) Deploy public persist_auto_exporter
  2) Deploy persist_admin_server (API + web UI)
  3) Optionally start both services with --start

Services (pm2 namespace: persist_exporter):
  - persist_auto_exporter_public
  - persist_admin_server
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start)
      DO_START=1
      shift
      ;;
    *)
      echo "[ERROR] unknown arg: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

echo "[INFO] deploying public persist_auto_exporter..."
bash "${SCRIPT_DIR}/deploy_persist_auto_exporter_public.sh"

echo "[INFO] deploying persist_admin_server..."
bash "${SCRIPT_DIR}/deploy_persist_admin_server.sh"

if [[ "$DO_START" -eq 1 ]]; then
  echo "[INFO] starting public persist services..."
  cd "${TARGET_DIR}"
  ./start_persist_auto_exporter_public.sh
  ./start_persist_admin_server.sh

  echo "[INFO] started. check status with: pm2 status --namespace persist_exporter"
else
  echo "[INFO] deployed. start with:"
  echo "       cd ${TARGET_DIR}"
  echo "       ./start_persist_auto_exporter_public.sh"
  echo "       ./start_persist_admin_server.sh"
fi
