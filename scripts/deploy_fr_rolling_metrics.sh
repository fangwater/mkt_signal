#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="rolling_metrics"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'EOF'
用法: scripts/deploy_fr_rolling_metrics.sh [--exchange <binance|okex|bybit|bitget|gate>] [--env-suffix fr_trade]

说明:
  - 构建 rolling_metrics 并拷贝到 $HOME/<exchange>_<env_suffix>/ 目录（默认 env_suffix=fr_trade）。
  - 如未指定 --exchange，将尝试从 env-suffix 或当前目录名推断 exchange。
  - 同步辅助脚本: scripts/print_fr_rolling_metrics_thresholds.py, scripts/print_fr_rolling_metrics_params.py,
    scripts/sync_fr_rolling_metrics_params.py, scripts/start_fr_rolling_metrics.sh, scripts/stop_fr_rolling_metrics.sh, docs/rolling_metrics.md
  - 不自动启动，部署后可在目标目录执行 start/stop 脚本：
      ./scripts/start_fr_rolling_metrics.sh   # 基于目录名自动推断 open/hedge
      ./scripts/stop_fr_rolling_metrics.sh    # 停止对应 PM2 进程

示例:
  scripts/deploy_fr_rolling_metrics.sh --exchange binance
  scripts/deploy_fr_rolling_metrics.sh --env-suffix okex_fr_trade   # env_suffix 含前缀时可省略 --exchange
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_SUFFIX="fr_trade"
EXCHANGE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-suffix)
      ENV_SUFFIX="${2:-fr_trade}"
      shift 2
      ;;
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

# 若未显式指定 exchange，则尝试从 env_suffix 或当前目录推断（下划线前的前缀）
if [[ -z "$EXCHANGE" ]]; then
  candidate="$ENV_SUFFIX"
  if [[ "$candidate" != *_* ]]; then
    candidate="$(basename "$PWD")"
  fi
  if [[ "$candidate" == *_* ]]; then
    EXCHANGE="${candidate%%_*}"
  fi
fi

if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 无法推断 exchange，请使用 --exchange 指定或在 env_suffix/当前目录名中带上 <exchange>_<...>"
  usage
  exit 1
fi

# 规范化为小写
EXCHANGE="$(echo "$EXCHANGE" | tr 'A-Z' 'a-z')"

TARGET_DIR="$HOME/${EXCHANGE}_${ENV_SUFFIX}"

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"

EXTRA_FILES=(
  "scripts/print_fr_rolling_metrics_thresholds.py"
  "scripts/print_fr_rolling_metrics_params.py"
  "scripts/sync_fr_rolling_metrics_params.py"
  "scripts/start_fr_rolling_metrics.sh"
  "scripts/stop_fr_rolling_metrics.sh"
  "docs/rolling_metrics.md"
)

for file in "${EXTRA_FILES[@]}"; do
  SRC_PATH="$ROOT_DIR/$file"
  if [[ -f "$SRC_PATH" ]]; then
    DEST_DIR="$TARGET_DIR/$(dirname "$file")"
    mkdir -p "$DEST_DIR"
    rsync -a "$SRC_PATH" "$DEST_DIR/"
    [[ -x "$SRC_PATH" ]] && chmod +x "$DEST_DIR/$(basename "$file")"
  fi
done

# 最后再覆盖二进制，避免 Text file busy
BIN_TMP="$TARGET_DIR/${BIN_NAME}.new"
cp "$BIN_PATH" "$BIN_TMP"
chmod +x "$BIN_TMP"
mv -f "$BIN_TMP" "$TARGET_DIR/$BIN_NAME"

echo "[INFO] 部署完成: $TARGET_DIR"
echo "[INFO] 手动启动: cd $TARGET_DIR && ./scripts/start_fr_rolling_metrics.sh"
echo "[INFO] 停止: cd $TARGET_DIR && ./scripts/stop_fr_rolling_metrics.sh"
