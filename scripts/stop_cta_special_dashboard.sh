#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck disable=SC1090
source "${SCRIPT_DIR}/process_match_lib.sh"
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null 2>&1 || { echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2; exit 1; }
dir_tag="$(basename "$BASE_DIR" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/_/g')"
PROC_NAME="${PMDAEMON_NAME:-cta_special_dash_${dir_tag}}"
"$PMDAEMON_BIN" delete "$PROC_NAME" >/dev/null 2>&1 || true
leaked=()
for comm in python python3; do
  mapfile -t found < <(safe_find_running_pids "$comm" "$BASE_DIR" "cta_special_dashboard.py" || true)
  leaked+=("${found[@]}")
done
[[ ${#leaked[@]} -eq 0 ]] || kill "${leaked[@]}" >/dev/null 2>&1 || true
echo "[INFO] stopped $PROC_NAME"
