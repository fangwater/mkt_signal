#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=scripts/deploy_intra_lib.sh
source "$ROOT_DIR/scripts/deploy_intra_lib.sh"

usage() {
  cat <<'EOF'
用法: scripts/deploy_intra_monitors.sh --exchange <binance> [--env-suffix intra-trade] [--env-name binance-intra-trade] [--jobs <n>] [--cargo-target-dir <path>]

说明:
  - 同所期现：只构建并部署单个 account_monitor（对应当前 exchange）
  - 输出到 $HOME/<exchange>-<env_suffix>/：
      account_monitor_<exchange>
      intra_scripts/start_intra_monitors.sh
      intra_scripts/stop_intra_monitors.sh

示例:
  scripts/deploy_intra_monitors.sh --exchange binance
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage; exit 0
fi

ENV_SUFFIX="intra-trade"
ENV_NAME=""
EXCHANGE=""
CARGO_TARGET_DIR_OVERRIDE=""
BUILD_JOBS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-suffix) ENV_SUFFIX="${2:-intra-trade}"; shift 2 ;;
    --env-name)   ENV_NAME="${2:-}"; shift 2 ;;
    --exchange)   EXCHANGE="${2:-}"; shift 2 ;;
    --jobs)       BUILD_JOBS="${2:-}"; shift 2 ;;
    --cargo-target-dir) CARGO_TARGET_DIR_OVERRIDE="${2:-}"; shift 2 ;;
    *)
      echo "[ERROR] 未知参数: $1"; usage; exit 1 ;;
  esac
done

if [[ -n "$ENV_NAME" && -z "$EXCHANGE" ]]; then
  if [[ "${ENV_NAME,,}" =~ ^([a-z0-9]+)[-_]intra[-_][a-z0-9][a-z0-9_-]*$ ]]; then
    EXCHANGE="${BASH_REMATCH[1]}"
  fi
fi

if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 需要 --exchange，或使用 --env-name <exchange>-intra-<tag>"
  usage; exit 1
fi

EXCHANGE="$(intra_ensure_exchange "$EXCHANGE")"

if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${EXCHANGE}-${ENV_SUFFIX}"
fi
TARGET_DIR="$HOME/${ENV_NAME}"
mkdir -p "$TARGET_DIR/intra_scripts"

CARGO_TARGET_DIR_EFFECTIVE="$(intra_effective_cargo_target_dir "$ROOT_DIR" "$CARGO_TARGET_DIR_OVERRIDE")"

case "$EXCHANGE" in
  okex)    BIN_NAME="okex_account_monitor"    ;;
  binance) BIN_NAME="binance_account_monitor" ;;
  gate)    BIN_NAME="gate_account_monitor"    ;;
  bybit)   BIN_NAME="bybit_account_monitor"   ;;
  bitget)  BIN_NAME="bitget_account_monitor"  ;;
  *)
    echo "[ERROR] 未支持的 exchange: $EXCHANGE"; exit 1 ;;
esac
OUT_NAME="account_monitor_${EXCHANGE}"

echo "[INFO] 构建 $BIN_NAME (release)"
(
  cd "$ROOT_DIR"
  CARGO_TARGET_DIR="$CARGO_TARGET_DIR_EFFECTIVE" \
    cargo build --release --bin "$BIN_NAME" ${BUILD_JOBS:+--jobs "$BUILD_JOBS"}
)

echo "[INFO] 部署 $BIN_NAME -> $TARGET_DIR/$OUT_NAME"
BIN_PATH="$(intra_bin_path_release "$CARGO_TARGET_DIR_EFFECTIVE" "$BIN_NAME")"
if ! intra_atomic_install "$BIN_PATH" "$TARGET_DIR/$OUT_NAME"; then
  exit 2
fi

SCRIPTS_TO_SYNC=(
  "scripts/process_match_lib.sh"
  "intra_scripts/intra_monitor_process_lib.sh"
  "intra_scripts/start_intra_monitors.sh"
  "intra_scripts/stop_intra_monitors.sh"
)
for file in "${SCRIPTS_TO_SYNC[@]}"; do
  SRC="$ROOT_DIR/$file"
  if [[ -f "$SRC" ]]; then
    rsync -a "$SRC" "$TARGET_DIR/$(dirname "$file")/"
    chmod +x "$TARGET_DIR/$file" 2>/dev/null || true
  fi
done

echo "[INFO] intra monitors 部署完成: $TARGET_DIR"
echo "[INFO] 启动: cd $TARGET_DIR && source ./env.sh && ./intra_scripts/start_intra_monitors.sh"
