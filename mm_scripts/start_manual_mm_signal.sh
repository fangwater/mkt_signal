#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

BIN_CANDIDATES=(
  "${BASE_DIR}/manual_mm_signal"
  "${SCRIPT_DIR}/manual_mm_signal"
  "${BASE_DIR}/target/release/manual_mm_signal"
  "${SCRIPT_DIR}/../target/release/manual_mm_signal"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] manual_mm_signal binary not found. Build first with: cargo build --release --bin manual_mm_signal" >&2
  exit 1
fi

usage() {
  cat <<'USAGE'
Usage:
  mm_scripts/start_manual_mm_signal.sh [--config <path>]

Notes:
  - Default config: config/manual_mm_signal.yaml in base dir.
USAGE
}

CONFIG_PATH="${BASE_DIR}/config/manual_mm_signal.yaml"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG_PATH="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] Unknown arg: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "[ERROR] config not found: $CONFIG_PATH" >&2
  exit 1
fi

PROC_NAME="${PM2_NAME:-manual_mm_signal_$(echo "${BASE_DIR##*/}" | tr 'A-Z' 'a-z' | sed 's/[^a-z0-9_-]/_/g')}"
RUST_LOG="${RUST_LOG:-info}"

npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE" \
  -- \
  --config "$CONFIG_PATH"

echo "[INFO] Started ${PROC_NAME} (config=${CONFIG_PATH})"
echo "[INFO] Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
