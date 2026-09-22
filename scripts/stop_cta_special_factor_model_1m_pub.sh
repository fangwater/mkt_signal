#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
[[ -f "${BASE_DIR}/env.sh" ]] && source "${BASE_DIR}/env.sh"
case "${CTA_SPECIAL_RUN_FACTOR_PUBLISHER:-1}" in
  0|false|FALSE|False|off|OFF|Off)
    echo "[INFO] CTA special env does not own the shared factor publisher"
    exit 0
    ;;
  1|true|TRUE|True|on|ON|On) ;;
  *)
    echo "[ERROR] invalid CTA_SPECIAL_RUN_FACTOR_PUBLISHER: ${CTA_SPECIAL_RUN_FACTOR_PUBLISHER}" >&2
    exit 1
    ;;
esac
# shellcheck disable=SC1090
source "${SCRIPT_DIR}/process_match_lib.sh"
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null 2>&1 || { echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2; exit 1; }
PROC_NAME="${PMDAEMON_NAME:-cta_special_factor_shared_binance_futures}"
"$PMDAEMON_BIN" delete "$PROC_NAME" >/dev/null 2>&1 || true
mapfile -t leaked < <(safe_find_running_pids cta_special_factor_model_1m_pub "$BASE_DIR" "binance-futures" || true)
[[ ${#leaked[@]} -eq 0 ]] || kill "${leaked[@]}" >/dev/null 2>&1 || true
echo "[INFO] stopped $PROC_NAME"
