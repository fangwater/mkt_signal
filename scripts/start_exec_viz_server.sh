#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
[[ -f "${BASE_DIR}/env.sh" ]] && source "${BASE_DIR}/env.sh"
CFG_PATH="${VIZ_CFG:-config/exec_viz.toml}"
if [[ "${1:-}" == "--cfg" ]]; then CFG_PATH="${2:-}"; shift 2; fi
[[ $# -eq 0 ]] || { echo "[ERROR] Unknown arguments: $*" >&2; exit 1; }
[[ -f "${BASE_DIR}/${CFG_PATH}" ]] || { echo "[ERROR] config not found: ${BASE_DIR}/${CFG_PATH}" >&2; exit 1; }

BIN_PATH=""
for candidate in "${BASE_DIR}/viz_server" "${BASE_DIR}/target/release/viz_server"; do
  if [[ -x "$candidate" ]]; then BIN_PATH="$candidate"; break; fi
done
[[ -n "$BIN_PATH" ]] || { echo "[ERROR] viz_server binary not found" >&2; exit 1; }
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null 2>&1 || { echo "[ERROR] pmdaemon not found: ${PMDAEMON_BIN}" >&2; exit 1; }
dir_tag="$(basename "$BASE_DIR" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/_/g')"
PROC_NAME="${PMDAEMON_NAME:-exec_vz_${dir_tag}}"
cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file"' EXIT
cat >"$cfg_file" <<JSON
{"apps":[{"name":"${PROC_NAME}","script":"${BIN_PATH}","args":[],"cwd":"${BASE_DIR}","env":{"VIZ_CFG":"${CFG_PATH}","RUST_LOG":"${RUST_LOG:-info}"}}]}
JSON
PMDAEMON_NAME="$PROC_NAME" "${SCRIPT_DIR}/stop_exec_viz_server.sh"
"$PMDAEMON_BIN" --config "$cfg_file" start --name "$PROC_NAME"
echo "[INFO] Started ${PROC_NAME}; logs: ${PMDAEMON_BIN} logs ${PROC_NAME} --follow"
