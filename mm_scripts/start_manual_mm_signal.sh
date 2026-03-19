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

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

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
RUST_LOG="${RUST_LOG:-info}"
IPC_NS="${IPC_NAMESPACE:-}"
if [[ -z "$IPC_NS" ]]; then
  IPC_NS="$(basename "${BASE_DIR}")"
  echo "[WARN] IPC_NAMESPACE not set; use default: ${IPC_NS}"
fi

if [[ "$LEGACY_PROC_NAME" != "$PROC_NAME" ]]; then
  npx pm2 delete "$LEGACY_PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
fi
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

IPC_NAMESPACE="$IPC_NS" RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE" \
  -- \
  --config "$CONFIG_PATH"

echo "[INFO] Started ${PROC_NAME} (config=${CONFIG_PATH}, ipc_namespace=${IPC_NS})"
echo "[INFO] Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
