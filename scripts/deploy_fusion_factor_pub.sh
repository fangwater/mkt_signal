#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="fusion_factor_pub"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"
KNOWN_EXCHANGES=("okex" "binance" "bybit" "bitget" "gate")

is_known_exchange() {
  local v="${1,,}"
  for e in "${KNOWN_EXCHANGES[@]}"; do
    if [[ "$v" == "$e" ]]; then
      return 0
    fi
  done
  return 1
}

default_venues_for_exchange() {
  local exchange="${1,,}"
  case "$exchange" in
    okex) echo "okex-futures okex-margin" ;;
    binance) echo "binance-futures binance-margin" ;;
    bybit) echo "bybit-futures bybit-margin" ;;
    bitget) echo "bitget-futures bitget-margin" ;;
    gate) echo "gate-futures gate-margin" ;;
    *)
      echo ""
      return 1
      ;;
  esac
}

usage() {
  cat <<'USAGE'
Usage:
  deploy_fusion_factor_pub.sh --exchange <exchange>

Defaults:
  固定部署根目录 -> $HOME/fusion_factor
  目录结构 -> $HOME/fusion_factor/<venue>/

Examples:
  bash scripts/deploy_fusion_factor_pub.sh --exchange binance
  bash scripts/deploy_fusion_factor_pub.sh --exchange okex

Notes:
  - Exchange expands to default venues:
      okex    -> okex-futures okex-margin
      binance -> binance-futures binance-margin
      bybit   -> bybit-futures bybit-margin
      bitget  -> bitget-futures bitget-margin
      gate    -> gate-futures gate-margin
  - 每个 venue 会生成独立二进制:
      fusion_factor_pub_<venue>
    并额外创建兼容软链:
      fusion_factor_pub -> fusion_factor_pub_<venue>
USAGE
}

TARGET_ROOT="$HOME/fusion_factor"
EXCHANGE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-}"
      if [[ -z "$EXCHANGE" ]]; then
        echo "[ERROR] --exchange 需要一个值" >&2
        usage >&2
        exit 1
      fi
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 未知参数: $1（仅支持 --exchange）" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 必须提供 --exchange" >&2
  usage >&2
  exit 1
fi

if ! is_known_exchange "$EXCHANGE"; then
  echo "[ERROR] 不支持的 exchange: $EXCHANGE" >&2
  usage >&2
  exit 1
fi

read -r -a VENUES <<<"$(default_venues_for_exchange "$EXCHANGE")"

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
SCRIPTS_TO_SYNC=(
  "start_fusion_factor_pub.sh"
  "stop_fusion_factor_pub.sh"
)

for venue in "${VENUES[@]}"; do
  TARGET_DIR="${TARGET_ROOT%/}/${venue}"
  VENUE_BIN_NAME="${BIN_NAME}_${venue}"
  echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
  mkdir -p "$TARGET_DIR"
  cp "$BIN_PATH" "$TARGET_DIR/$VENUE_BIN_NAME"
  chmod +x "$TARGET_DIR/$VENUE_BIN_NAME"
  ln -sfn "$VENUE_BIN_NAME" "$TARGET_DIR/$BIN_NAME"

  mkdir -p "$TARGET_DIR/scripts"
  for script in "${SCRIPTS_TO_SYNC[@]}"; do
    if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
      rsync -a "$SCRIPT_DIR_SRC/$script" "$TARGET_DIR/scripts/"
      chmod +x "$TARGET_DIR/scripts/$script"
    fi
  done

  mkdir -p "$TARGET_DIR/config"
  if [[ -f "$ROOT_DIR/config/fusion_factor_pub.toml" ]]; then
    rsync -a "$ROOT_DIR/config/fusion_factor_pub.toml" "$TARGET_DIR/config/"
  fi
done

echo "[INFO] $BIN_NAME 部署完成"
echo "[INFO] base_dir: $TARGET_ROOT"
echo "[INFO] venues: ${VENUES[*]}"
echo "[INFO] 启动示例: cd ${TARGET_ROOT%/}/${VENUES[0]} && ./scripts/start_fusion_factor_pub.sh"
