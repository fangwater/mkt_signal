#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
[[ -f "${BASE_DIR}/env.sh" ]] && source "${BASE_DIR}/env.sh"
case "${CTA_SPREAD_RUN_FACTOR_PUBLISHER:-1}" in
  0|false|FALSE|False|off|OFF|Off)
    echo "[INFO] CTA spread env does not own the shared factor publisher"
    exit 0
    ;;
  1|true|TRUE|True|on|ON|On) ;;
  *)
    echo "[ERROR] invalid CTA_SPREAD_RUN_FACTOR_PUBLISHER: ${CTA_SPREAD_RUN_FACTOR_PUBLISHER}" >&2
    exit 1
    ;;
esac
# shellcheck disable=SC1090
source "${SCRIPT_DIR}/process_match_lib.sh"
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null 2>&1 || { echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2; exit 1; }
PROC_NAME="${PMDAEMON_NAME:-${CTA_SPREAD_FACTOR_PROCESS_NAME:-cta_spread_factor_shared_binance_futures}}"
KEEP_PID="${CTA_SPREAD_FACTOR_KEEP_PID:-}"
if [[ -n "$KEEP_PID" && ! "$KEEP_PID" =~ ^[1-9][0-9]*$ ]]; then
  echo "[ERROR] CTA_SPREAD_FACTOR_KEEP_PID must be a positive PID" >&2
  exit 1
fi
timeout -k 1s 45s "$PMDAEMON_BIN" delete "$PROC_NAME" >/dev/null 2>&1 || true
mapfile -t leaked < <(safe_find_running_pids cta_spread_factor_model_1m_pub "$BASE_DIR" "binance-futures" || true)
for pid in "${leaked[@]}"; do
  [[ "$pid" == "$KEEP_PID" ]] || kill "$pid" >/dev/null 2>&1 || true
done
echo "[INFO] stopped $PROC_NAME"
