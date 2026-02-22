#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROC_NAME="persist_admin_server"

BIN_PATH="${BASE_DIR}/persist_admin_server"
CFG_PATH="${BASE_DIR}/config/persist_auto_exporter.toml"
BIND_ADDR="${PERSIST_ADMIN_BIND:-0.0.0.0:10331}"

if [[ ! -x "${BIN_PATH}" ]]; then
  echo "[ERROR] binary not found: ${BIN_PATH}" >&2
  exit 1
fi
if [[ ! -f "${CFG_PATH}" ]]; then
  echo "[ERROR] config not found: ${CFG_PATH}" >&2
  exit 1
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")

if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

RUST_LOG="${RUST_LOG:-info}"

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

CFG_FILE="$(mktemp)"
trap 'rm -f "$CFG_FILE" >/dev/null 2>&1 || true' EXIT

cat >"$CFG_FILE" <<EOF
{
  "apps": [
    {
      "name": "$(json_escape "$PROC_NAME")",
      "script": "$(json_escape "$BIN_PATH")",
      "args": ["--config", "$(json_escape "$CFG_PATH")", "--bind", "$(json_escape "$BIND_ADDR")"],
      "cwd": "$(json_escape "$BASE_DIR")",
      "env": {
        "RUST_LOG": "$(json_escape "$RUST_LOG")"
      }
    }
  ]
}
EOF

echo "[INFO] Restarting ${PROC_NAME}"
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$CFG_FILE" start --name "$PROC_NAME"

echo "[INFO] Started ${PROC_NAME} bind=${BIND_ADDR}"
echo "Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "Status: ${PMDAEMON[*]} list"
