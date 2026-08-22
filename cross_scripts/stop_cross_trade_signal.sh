#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"

OPEN_EX=""
HEDGE_EX=""
ENV_TAG=""
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross[-_]([a-z0-9][a-z0-9_-]*)$ ]]; then
  OPEN_EX="${BASH_REMATCH[1]}"
  HEDGE_EX="${BASH_REMATCH[2]}"
  ENV_TAG="${BASH_REMATCH[3]}"
fi
if [[ "$OPEN_EX" == "okx" ]]; then
  OPEN_EX="okex"
fi
if [[ "$HEDGE_EX" == "okx" ]]; then
  HEDGE_EX="okex"
fi
if [[ -z "$OPEN_EX" || -z "$HEDGE_EX" || -z "$ENV_TAG" ]]; then
  echo "[ERROR] not an cross env dir: ${dir_name} (expect <open>-<hedge>-cross-<env>)"
  exit 1
fi
ENV_TAG="$(printf '%s' "$ENV_TAG" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//')"
if [[ -z "$ENV_TAG" ]]; then
  echo "[ERROR] invalid cross env tag in dir: ${dir_name}"
  exit 1
fi

if [[ "$OPEN_EX" == "$HEDGE_EX" ]]; then
  LEGACY_PM2_TAG="${OPEN_EX}_std"
else
  LEGACY_PM2_TAG="${OPEN_EX}_${HEDGE_EX}"
fi
PROC_NAME="cross_${OPEN_EX}_${HEDGE_EX}_${ENV_TAG}_trade_signal"
LEGACY_PROC_NAME="trade_signal_${LEGACY_PM2_TAG}"

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
