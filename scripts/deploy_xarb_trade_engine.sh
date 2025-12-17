#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="trade_engine"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'EOF'
用法: scripts/deploy_xarb_trade_engine.sh [--open-venue <okex-futures>] [--hedge-venue <binance-futures>] [--env-suffix xarb-trade] [--env-name okex-binance-xarb-trade]

说明:
  - 构建 trade_engine 并拷贝到 $HOME/<open>-<hedge>-<env_suffix>/ 目录（默认 env_suffix=xarb-trade）。
  - 跨所套利目录名约定: <open>-<hedge>-xarb-trade（例如 okex-binance-xarb-trade）。
  - xarb 固定 futures 资产类型：open/hedge 两侧都会被设置为 <exchange>-futures。
  - 同步辅助脚本:
      xarb_scripts/start_xarb_trade_engine.sh
      xarb_scripts/stop_xarb_trade_engine.sh
  - 不自动启动，部署后可在目标目录执行 start/stop 脚本：
      ./xarb_scripts/start_xarb_trade_engine.sh
      ./xarb_scripts/stop_xarb_trade_engine.sh

提示:
  - 建议先生成并配置 env.sh（用于凭证等）：scripts/deploy_setup_env_xarb.sh --open-venue ... --hedge-venue ...

示例:
  scripts/deploy_xarb_trade_engine.sh --open-venue okex-futures --hedge-venue binance-futures
  scripts/deploy_xarb_trade_engine.sh --env-name okex-binance-xarb-trade
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_SUFFIX="xarb-trade"
ENV_NAME=""
OPEN_VENUE=""
HEDGE_VENUE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-suffix)
      ENV_SUFFIX="${2:-xarb-trade}"
      shift 2
      ;;
    --env-name)
      ENV_NAME="${2:-}"
      shift 2
      ;;
    --open-venue)
      OPEN_VENUE="${2:-}"
      shift 2
      ;;
    --hedge-venue)
      HEDGE_VENUE="${2:-}"
      shift 2
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

infer_pair_from_name() {
  local name="${1,,}"
  local open_ex=""
  local hedge_ex=""

  if [[ "$name" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]xarb([_-].*)?$ ]]; then
    open_ex="${BASH_REMATCH[1]}"
    hedge_ex="${BASH_REMATCH[2]}"
  fi

  if [[ "$open_ex" == "okx" ]]; then
    open_ex="okex"
  fi
  if [[ "$hedge_ex" == "okx" ]]; then
    hedge_ex="okex"
  fi

  if [[ -n "$open_ex" && -n "$hedge_ex" ]]; then
    echo "${open_ex},${hedge_ex}"
  fi
}

normalize_venue() {
  echo "${1,,}"
}

ensure_futures_venue() {
  local v
  v="$(normalize_venue "$1")"
  if [[ -z "$v" || "$v" != *-futures ]]; then
    echo "[ERROR] xarb 只支持 futures：venue 必须以 -futures 结尾: $1"
    exit 1
  fi
  echo "$v"
}

if [[ -n "$ENV_NAME" && ( -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ) ]]; then
  if inferred="$(infer_pair_from_name "$ENV_NAME")" && [[ -n "$inferred" ]]; then
    OPEN_VENUE="${OPEN_VENUE:-${inferred%%,*}-futures}"
    HEDGE_VENUE="${HEDGE_VENUE:-${inferred##*,}-futures}"
  fi
fi

if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  echo "[ERROR] 需要 --open-venue 与 --hedge-venue，或使用 --env-name <open>-<hedge>-xarb-... 以自动推断"
  usage
  exit 1
fi

OPEN_VENUE="$(ensure_futures_venue "$OPEN_VENUE")"
HEDGE_VENUE="$(ensure_futures_venue "$HEDGE_VENUE")"

if [[ "$OPEN_VENUE" == "$HEDGE_VENUE" ]]; then
  echo "[ERROR] xarb 需要跨所：open=$OPEN_VENUE hedge=$HEDGE_VENUE"
  exit 1
fi

OPEN_EXCHANGE="${OPEN_VENUE%%-*}"
HEDGE_EXCHANGE="${HEDGE_VENUE%%-*}"
if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${OPEN_EXCHANGE}-${HEDGE_EXCHANGE}-${ENV_SUFFIX}"
fi

TARGET_DIR="$HOME/${ENV_NAME}"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"

EXTRA_FILES=(
  "xarb_scripts/start_xarb_trade_engine.sh"
  "xarb_scripts/stop_xarb_trade_engine.sh"
)

echo "[INFO] 同步 xarb_scripts 到 $TARGET_DIR（先更新脚本，避免二进制 busy 影响脚本更新）"
for file in "${EXTRA_FILES[@]}"; do
  SRC_PATH="$ROOT_DIR/$file"
  if [[ -f "$SRC_PATH" ]]; then
    DEST_DIR="$TARGET_DIR/$(dirname "$file")"
    mkdir -p "$DEST_DIR"
    rsync -a "$SRC_PATH" "$DEST_DIR/"
    chmod +x "$DEST_DIR/$(basename "$file")" 2>/dev/null || true
  fi
done

echo "[INFO] 构建 $BIN_NAME (release)"
(
  cd "$ROOT_DIR"
  cargo build --release --bin "$BIN_NAME"
)

# 最后再覆盖二进制，避免 Text file busy（失败时脚本已更新）
BIN_TMP="$TARGET_DIR/${BIN_NAME}.new"
cp "$BIN_PATH" "$BIN_TMP"
chmod +x "$BIN_TMP"
move_ok="0"
for _ in 1 2 3 4 5; do
  if mv -f "$BIN_TMP" "$TARGET_DIR/$BIN_NAME" 2>/dev/null; then
    move_ok="1"
    break
  fi
  sleep 0.2
done
if [[ "$move_ok" != "1" ]]; then
  echo "[WARN] 二进制更新失败（可能 Text file busy），但脚本已同步完成：$TARGET_DIR/xarb_scripts"
  echo "[WARN] 请稍后重试二进制更新或先停止进程后再部署：$TARGET_DIR/$BIN_NAME"
  exit 2
fi

echo "[INFO] 部署完成: $TARGET_DIR"
echo "[INFO] venues: open=${OPEN_VENUE} hedge=${HEDGE_VENUE}"
echo "[INFO] 建议先配置 env: scripts/deploy_setup_env_xarb.sh --env-name ${ENV_NAME} --open-venue ${OPEN_VENUE} --hedge-venue ${HEDGE_VENUE}"
echo "[INFO] 手动启动: cd $TARGET_DIR && ./xarb_scripts/start_xarb_trade_engine.sh"
echo "[INFO] 停止: cd $TARGET_DIR && ./xarb_scripts/stop_xarb_trade_engine.sh"

