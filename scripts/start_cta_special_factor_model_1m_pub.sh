#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
[[ -f "${BASE_DIR}/env.sh" ]] && source "${BASE_DIR}/env.sh"
BIN_PATH="${BASE_DIR}/cta_special_factor_model_1m_pub"
CONFIG_PATH="${BASE_DIR}/config/cta_special_factor_model_1m_pub.toml"
[[ -x "$BIN_PATH" ]] || { echo "[ERROR] missing executable: $BIN_PATH" >&2; exit 1; }
[[ -f "$CONFIG_PATH" ]] || { echo "[ERROR] missing config: $CONFIG_PATH" >&2; exit 1; }
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null 2>&1 || { echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2; exit 1; }
dir_tag="$(basename "$BASE_DIR" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/_/g')"
PROC_NAME="${PMDAEMON_NAME:-cta_special_factor_${dir_tag}}"
core_args=""
if [[ -n "${CTA_SPECIAL_FACTOR_MODEL_1M_CORE:-}" ]]; then
  [[ "$CTA_SPECIAL_FACTOR_MODEL_1M_CORE" =~ ^[0-9]+$ ]] || { echo "[ERROR] CTA_SPECIAL_FACTOR_MODEL_1M_CORE must be an integer" >&2; exit 1; }
  core_args=",\"--core\",\"${CTA_SPECIAL_FACTOR_MODEL_1M_CORE}\""
fi
cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file"' EXIT
printf '{"apps":[{"name":"%s","script":"%s","args":["--venue","binance-futures","--config","config/cta_special_factor_model_1m_pub.toml"%s],"cwd":"%s","env":{"RUST_LOG":"%s"}}]}\n' \
  "$PROC_NAME" "$BIN_PATH" "$core_args" "$BASE_DIR" "${RUST_LOG:-info}" >"$cfg_file"
PMDAEMON_NAME="$PROC_NAME" "${SCRIPT_DIR}/stop_cta_special_factor_model_1m_pub.sh"
"$PMDAEMON_BIN" --config "$cfg_file" start --name "$PROC_NAME"
echo "[INFO] started $PROC_NAME"
