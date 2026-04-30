#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"

EXCHANGE=""
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]intra([-_].+)?$ ]]; then
  EXCHANGE="${BASH_REMATCH[1]}"
fi
if [[ "$EXCHANGE" == "okx" ]]; then
  EXCHANGE="okex"
fi
if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] not an intra env dir: ${dir_name} (expect <exchange>-intra-<env>)"
  exit 1
fi

PROC_NAME="trade_signal_${EXCHANGE}"

echo "[INFO] Deleting ${PROC_NAME} (namespace=${NAMESPACE})"
if npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE"; then
  echo "[INFO] Deleted ${PROC_NAME}"
else
  echo "[WARN] ${PROC_NAME} not found in namespace ${NAMESPACE}"
fi

echo ""
echo "[INFO] Remaining processes: npx pm2 status --namespace ${NAMESPACE}"
