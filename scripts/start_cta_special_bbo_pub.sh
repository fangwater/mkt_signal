#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
[[ -f "${BASE_DIR}/env.sh" ]] && source "${BASE_DIR}/env.sh"

SERVICE_ROOT="${MKT_SPREAD_SERVICE_ROOT:-spread_pbs}"
if [[ "$SERVICE_ROOT" == "spread_pbs" ]]; then
  echo "[INFO] CTA special uses shared spread_pbs BBO service"
  exit 0
fi
[[ "$SERVICE_ROOT" =~ ^[A-Za-z0-9_-]+$ ]] || {
  echo "[ERROR] invalid MKT_SPREAD_SERVICE_ROOT: $SERVICE_ROOT" >&2
  exit 1
}
[[ "${CTA_SPECIAL_BBO_CORE:-}" =~ ^[0-9]+$ ]] || {
  echo "[ERROR] CTA_SPECIAL_BBO_CORE must be set for an isolated BBO service" >&2
  exit 1
}

BIN_PATH="${BASE_DIR}/cta_special_bbo_pub"
[[ -x "$BIN_PATH" ]] || { echo "[ERROR] missing executable: $BIN_PATH" >&2; exit 1; }
[[ -f "${BASE_DIR}/config/mkt_cfg.yaml" ]] || {
  echo "[ERROR] missing ${BASE_DIR}/config/mkt_cfg.yaml" >&2
  exit 1
}
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null 2>&1 || {
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  exit 1
}
# shellcheck disable=SC1090
source "${BASE_DIR}/scripts/intra_release_guard.sh"
intra_release_verify_file "$BASE_DIR" spread_pbs "$BIN_PATH"

dir_tag="$(basename "$BASE_DIR" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/_/g')"
PROC_NAME="${PMDAEMON_NAME:-cta_special_bbo_${dir_tag}}"
cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file"' EXIT
printf '{"apps":[{"name":"%s","script":"%s","args":["--venue","binance-futures","--core","%s","--binance-futures-role","bookticker","--spread-service-root","%s"],"cwd":"%s","env":{"RUST_LOG":"%s"}}]}\n' \
  "$PROC_NAME" "$BIN_PATH" "$CTA_SPECIAL_BBO_CORE" "$SERVICE_ROOT" "$BASE_DIR" "${RUST_LOG:-info}" >"$cfg_file"

PMDAEMON_NAME="$PROC_NAME" "${SCRIPT_DIR}/stop_cta_special_bbo_pub.sh"
"$PMDAEMON_BIN" --config "$cfg_file" start --name "$PROC_NAME"
echo "[INFO] started $PROC_NAME root=$SERVICE_ROOT core=$CTA_SPECIAL_BBO_CORE"
