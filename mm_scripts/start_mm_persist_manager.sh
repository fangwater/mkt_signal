#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
MM_NAME_LIB="${SCRIPT_DIR}/../scripts/mm_process_name.sh"

if [[ -f "$MM_NAME_LIB" ]]; then
  # shellcheck disable=SC1090
  source "$MM_NAME_LIB"
fi

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
  - IPC_NAMESPACE is optional; defaults to deploy dir name.
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

IPC_NS="${IPC_NAMESPACE:-}"
if [[ -z "$IPC_NS" ]]; then
  IPC_NS="$(basename "${BASE_DIR}")"
  echo "[WARN] IPC_NAMESPACE not set; use default: ${IPC_NS}"
fi

DIR_NAME="${BASE_DIR##*/}"
DIR_TAG="$(echo "${DIR_NAME}" | tr 'A-Z' 'a-z' | sed 's/[^a-z0-9_-]/_/g')"
DEFAULT_PROC_NAME="mm_persist_manager_${DIR_TAG}"
if type mm_default_proc_name >/dev/null 2>&1; then
  if inferred_name="$(mm_default_proc_name pm "$DIR_NAME" 2>/dev/null)" && [[ -n "$inferred_name" ]]; then
    DEFAULT_PROC_NAME="$inferred_name"
  fi
fi
PROC_NAME="${PMDAEMON_NAME:-$DEFAULT_PROC_NAME}"
LEGACY_PROC_NAME="mm_persist_manager_${DIR_TAG}"
RUST_LOG="${RUST_LOG:-info}"

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")

if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

mkdir -p "${BASE_DIR}/data/persist_manager" >/dev/null 2>&1 || true

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

shell_quote() {
  printf '%q' "$1"
}

CFG_FILE="$(mktemp)"
trap 'rm -f "$CFG_FILE" >/dev/null 2>&1 || true' EXIT

cmd="if [[ -f $(shell_quote "$ENV_FILE") ]]; then source $(shell_quote "$ENV_FILE"); fi; exec $(shell_quote "$BIN_PATH")"

cat >"$CFG_FILE" <<EOF
{
  "apps": [
    {
      "name": "$(json_escape "$PROC_NAME")",
      "script": "$(json_escape "/bin/bash")",
      "args": ["-lc", "$(json_escape "$cmd")"],
      "cwd": "$(json_escape "$BASE_DIR")",
      "env": {
        "RUST_LOG": "$(json_escape "$RUST_LOG")",
        "IPC_NAMESPACE": "$(json_escape "$IPC_NS")"
      }
    }
  ]
}
EOF

echo "[INFO] Restarting ${PROC_NAME} (ipc_namespace=${IPC_NS})"
if [[ "$LEGACY_PROC_NAME" != "$PROC_NAME" ]]; then
  "${PMDAEMON[@]}" delete "$LEGACY_PROC_NAME" >/dev/null 2>&1 || true
fi
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$CFG_FILE" start --name "$PROC_NAME"

echo "[INFO] Started ${PROC_NAME} (ipc_namespace=${IPC_NS})"
echo "Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "Status: ${PMDAEMON[*]} list"
