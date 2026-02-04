#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PM2_NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

usage() {
  cat <<'EOF'
Usage: mm_scripts/stop_mm_viz_server.sh [--exchange <binance|okex|gate|bybit|bitget>]

Notes:
  - Exchange is inferred from the directory name (<exchange>_mm_<env>), unless --exchange is set.
  - Stops PM2 process: viz_server_<dir>
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

EXCHANGE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    *)
      echo "[ERROR] Unknown arg: $1"
      usage
      exit 1
      ;;
  esac
done

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"
dir_tag="$(echo "${dir_lc}" | sed 's/[^a-z0-9_-]/_/g')"
if [[ -z "$EXCHANGE" ]]; then
  if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]mm([_-].*)?$ ]]; then
    EXCHANGE="${BASH_REMATCH[1]}"
  fi
fi

EXCHANGE="${EXCHANGE,,}"
if [[ "$EXCHANGE" == "okx" ]]; then
  EXCHANGE="okex"
fi
if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] Failed to infer exchange; please pass --exchange"
  exit 1
fi

PROC_NAME="${PM2_NAME:-viz_server_${dir_tag}}"

echo "[INFO] Deleting ${PROC_NAME} (namespace=${PM2_NAMESPACE})"
if npx pm2 delete "$PROC_NAME" --namespace "$PM2_NAMESPACE"; then
  echo "[INFO] Deleted ${PROC_NAME}"
else
  echo "[WARN] ${PROC_NAME} not found in namespace ${PM2_NAMESPACE}"
fi

echo ""
echo "[INFO] Remaining processes: npx pm2 status --namespace ${PM2_NAMESPACE}"
