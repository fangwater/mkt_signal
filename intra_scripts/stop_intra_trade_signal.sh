#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"

EXCHANGE=""
ENV_TAG=""
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]intra[-_]([a-z0-9][a-z0-9_-]*)$ ]]; then
  EXCHANGE="${BASH_REMATCH[1]}"
  ENV_TAG="${BASH_REMATCH[2]}"
fi
if [[ "$EXCHANGE" == "okx" ]]; then
  EXCHANGE="okex"
fi
if [[ -z "$EXCHANGE" || -z "$ENV_TAG" ]]; then
  echo "[ERROR] not an intra env dir: ${dir_name} (expect <exchange>-intra-<env>)"
  exit 1
fi
ENV_TAG="$(printf '%s' "$ENV_TAG" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//')"

PROC_NAME="intra_${EXCHANGE}_${ENV_TAG}_trade_signal"
LEGACY_PROC_NAME="trade_signal_${EXCHANGE}"

echo "[INFO] Deleting ${PROC_NAME} (namespace=${NAMESPACE})"
deleted=0
if npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "[INFO] Deleted ${PROC_NAME}"
  deleted=1
fi
if npx pm2 delete "$LEGACY_PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "[INFO] Deleted legacy ${LEGACY_PROC_NAME}"
  deleted=1
fi
if [[ $deleted -eq 0 ]]; then
  echo "[WARN] neither ${PROC_NAME} nor ${LEGACY_PROC_NAME} found in namespace ${NAMESPACE}"
fi

echo ""
echo "[INFO] Remaining processes: npx pm2 status --namespace ${NAMESPACE}"
