#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=lib/fr_remote_deploy.sh
source "$ROOT_DIR/scripts/lib/fr_remote_deploy.sh"

BIN_NAME="trade_flow_feature_pub"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"
DEPLOY_ROOT_NAME="trade_flow_feature"

KNOWN_EXCHANGES=("okex" "binance" "bybit" "bitget" "gate")
# 与 deploy_mm_{binance,gate,bitget}.sh 对齐：这三所走远端，其余本地。
REMOTE_EXCHANGES=("binance" "gate" "bitget")

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

is_remote_exchange() {
  local v="${1,,}"
  for e in "${REMOTE_EXCHANGES[@]}"; do
    if [[ "$v" == "$e" ]]; then
      return 0
    fi
  done
  return 1
}

usage() {
  cat <<USAGE
Usage:
  deploy_trade_flow_feature_pub.sh --exchange <exchange> [options]

Options:
  --bin-only         仅替换二进制（跳过 scripts/config）
  --runtime-only     替换二进制 + scripts（跳过 config）
  -h, --help         显示帮助

Defaults:
  本地落点 (okex/bybit) -> \$HOME/trade_flow_feature/<venue>/
  远端落点 (binance/gate/bitget) -> \${FR_DEPLOY_HOST}:\${FR_REMOTE_HOME}/trade_flow_feature/<venue>/

Examples:
  bash scripts/deploy_trade_flow_feature_pub.sh --exchange binance        # 远端
  bash scripts/deploy_trade_flow_feature_pub.sh --exchange okex           # 本地
  bash scripts/deploy_trade_flow_feature_pub.sh --exchange bitget --bin-only
  bash scripts/deploy_trade_flow_feature_pub.sh --exchange gate --runtime-only

Notes:
  - Exchange expands to default venues:
      okex    -> okex-futures okex-margin
      binance -> binance-futures binance-margin
      bybit   -> bybit-futures bybit-margin
      bitget  -> bitget-futures bitget-margin
      gate    -> gate-futures gate-margin
  - 远端模式下 cargo build 仍在本机完成，再 rsync 到远端。
  - --bin-only / --runtime-only 互斥；远端模式下分别走 fr_remote_sync_binaries
    （只同步顶层二进制）和 fr_remote_sync_path（同步整个 venue 目录）。
USAGE
}

EXCHANGE=""
BIN_MODE="0"
RUNTIME_ONLY="0"

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
    --bin-only)
      if [[ "$RUNTIME_ONLY" == "1" ]]; then
        echo "[ERROR] --bin-only 与 --runtime-only 互斥" >&2
        exit 1
      fi
      BIN_MODE="1"
      shift
      ;;
    --runtime-only)
      if [[ "$BIN_MODE" == "1" ]]; then
        echo "[ERROR] --bin-only 与 --runtime-only 互斥" >&2
        exit 1
      fi
      RUNTIME_ONLY="1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 未知参数: $1（仅支持 --exchange / --bin-only / --runtime-only）" >&2
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

if is_remote_exchange "$EXCHANGE"; then
  DEPLOY_TARGET="remote"
else
  DEPLOY_TARGET="local"
fi

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
SCRIPTS_TO_DEPLOY=(
  "start_trade_flow_feature_pub.sh"
  "stop_trade_flow_feature_pub.sh"
  "process_match_lib.sh"
  "print_trade_flow_thresholds.py"
)

write_venue_layout() {
  local stage_dir="$1"
  mkdir -p "$stage_dir"
  cp "$BIN_PATH" "$stage_dir/"
  chmod +x "$stage_dir/$BIN_NAME"

  if [[ "$BIN_MODE" == "1" ]]; then
    return 0
  fi

  mkdir -p "$stage_dir/scripts"
  for script in "${SCRIPTS_TO_DEPLOY[@]}"; do
    if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
      rsync -a "$SCRIPT_DIR_SRC/$script" "$stage_dir/scripts/"
      chmod +x "$stage_dir/scripts/$script"
    fi
  done

  if [[ "$RUNTIME_ONLY" == "1" ]]; then
    return 0
  fi

  mkdir -p "$stage_dir/config"
  if [[ -f "$ROOT_DIR/config/trade_flow_feature_pub.yaml" ]]; then
    rsync -a "$ROOT_DIR/config/trade_flow_feature_pub.yaml" "$stage_dir/config/"
  fi
}

if [[ "$DEPLOY_TARGET" == "remote" ]]; then
  fr_remote_init_ssh "$ROOT_DIR"
  for venue in "${VENUES[@]}"; do
    REMOTE_REL="${DEPLOY_ROOT_NAME}/${venue}"
    LOCAL_STAGE="$HOME/$REMOTE_REL"
    echo "[INFO] [remote] staging $BIN_NAME -> $LOCAL_STAGE"
    write_venue_layout "$LOCAL_STAGE"
    if [[ "$BIN_MODE" == "1" ]]; then
      fr_remote_sync_binaries "$REMOTE_REL"
    else
      fr_remote_sync_path "$REMOTE_REL"
    fi
  done
else
  TARGET_ROOT="$HOME/$DEPLOY_ROOT_NAME"
  for venue in "${VENUES[@]}"; do
    TARGET_DIR="${TARGET_ROOT%/}/${venue}"
    echo "[INFO] [local] 部署 $BIN_NAME -> $TARGET_DIR"
    write_venue_layout "$TARGET_DIR"
  done
fi

echo "[INFO] $BIN_NAME 部署完成"
echo "[INFO] venues : ${VENUES[*]}"
if [[ "$DEPLOY_TARGET" == "remote" ]]; then
  echo "[INFO] target : ${FR_DEPLOY_HOST}:${FR_REMOTE_HOME}/${DEPLOY_ROOT_NAME}"
else
  echo "[INFO] target : $HOME/$DEPLOY_ROOT_NAME"
  echo "[INFO] 启动示例: cd $HOME/$DEPLOY_ROOT_NAME/${VENUES[0]} && ./scripts/start_trade_flow_feature_pub.sh"
fi
