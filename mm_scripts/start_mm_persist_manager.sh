#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

BIN_CANDIDATES=(
  "${BASE_DIR}/persist_manager"
  "${SCRIPT_DIR}/persist_manager"
  "${BASE_DIR}/target/release/persist_manager"
  "${SCRIPT_DIR}/../target/release/persist_manager"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] persist_manager binary not found. Build first with: cargo build --release --bin persist_manager" >&2
  exit 1
fi

usage() {
  cat <<'USAGE'
Usage:
  mm_scripts/start_mm_persist_manager.sh

Notes:
  - Requires IPC_NAMESPACE to be set (in shell or env.sh).
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

if [[ -z "${IPC_NAMESPACE:-}" ]]; then
  echo "[ERROR] IPC_NAMESPACE is not set." >&2
  echo "[ERROR] export IPC_NAMESPACE=$(basename "$BASE_DIR")  # or source ${ENV_FILE}" >&2
  exit 1
fi

PROC_NAME="${PM2_NAME:-mm_persist_manager_$(echo "${BASE_DIR##*/}" | tr 'A-Z' 'a-z' | sed 's/[^a-z0-9_-]/_/g')}"
RUST_LOG="${RUST_LOG:-info}"

mkdir -p "${BASE_DIR}/data/persist_manager" >/dev/null 2>&1 || true

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE})"
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

(
  cd "$BASE_DIR"
  RUST_LOG="$RUST_LOG" npx pm2 start "$BIN_PATH" \
    --name "$PROC_NAME" \
    --namespace "$NAMESPACE" \
    --
)

echo "[INFO] Started ${PROC_NAME}"
echo "[INFO] Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
