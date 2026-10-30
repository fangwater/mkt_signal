#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
[[ -f "$BASE_DIR/env.sh" ]] && source "$BASE_DIR/env.sh"
BIN_PATH="$BASE_DIR/exec_volatility_pub"
[[ -x "$BIN_PATH" && -f "$BASE_DIR/config/exec_volatility_pub.toml" ]] || { echo '[ERROR] missing volatility binary/config' >&2; exit 1; }
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PROC_NAME="${PMDAEMON_NAME:-exec_volatility_pub_binance}"
args=''
if [[ -n "${EXEC_VOLATILITY_CORE:-}" ]]; then
  [[ "$EXEC_VOLATILITY_CORE" =~ ^[0-9]+$ ]] || { echo '[ERROR] invalid volatility core' >&2; exit 1; }
  args=",\"--core\",\"$EXEC_VOLATILITY_CORE\""
fi
case "${EXEC_VOLATILITY_WAIT_FOR_PUBLISHERS:-0}" in
  0) ;;
  1) args="$args,\"--wait-for-publishers\"" ;;
  *) echo '[ERROR] EXEC_VOLATILITY_WAIT_FOR_PUBLISHERS must be 0 or 1' >&2; exit 1 ;;
esac
cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file"' EXIT
printf '{"apps":[{"name":"%s","script":"%s","args":["--config","config/exec_volatility_pub.toml"%s],"cwd":"%s","env":{"RUST_LOG":"%s"}}]}\n' \
  "$PROC_NAME" "$BIN_PATH" "$args" "$BASE_DIR" "${RUST_LOG:-info}" > "$cfg_file"
PMDAEMON_NAME="$PROC_NAME" "$SCRIPT_DIR/stop_exec_volatility_pub.sh"
"$PMDAEMON_BIN" --config "$cfg_file" start --name "$PROC_NAME"
echo "[INFO] started $PROC_NAME"
