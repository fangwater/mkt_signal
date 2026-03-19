#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"
MM_NAME_LIB="${SCRIPT_DIR}/../scripts/mm_process_name.sh"

if [[ -f "$MM_NAME_LIB" ]]; then
  # shellcheck disable=SC1090
  source "$MM_NAME_LIB"
fi

DIR_NAME="${BASE_DIR##*/}"
DIR_TAG="$(echo "${DIR_NAME}" | tr 'A-Z' 'a-z' | sed 's/[^a-z0-9_-]/_/g')"
DEFAULT_PROC_NAME="manual_mm_signal_${DIR_TAG}"
if type mm_default_proc_name >/dev/null 2>&1; then
  if inferred_name="$(mm_default_proc_name ms "$DIR_NAME" 2>/dev/null)" && [[ -n "$inferred_name" ]]; then
    DEFAULT_PROC_NAME="$inferred_name"
  fi
fi
PROC_NAME="${PM2_NAME:-$DEFAULT_PROC_NAME}"
LEGACY_PROC_NAME="manual_mm_signal_${DIR_TAG}"

if npx pm2 describe "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "[INFO] Stopping ${PROC_NAME} (namespace=${NAMESPACE})"
  npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE"
else
  echo "[WARN] PM2 process not found: ${PROC_NAME} (namespace=${NAMESPACE})"
fi

if [[ "$LEGACY_PROC_NAME" != "$PROC_NAME" ]]; then
  npx pm2 delete "$LEGACY_PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
fi
