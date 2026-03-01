#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="depth_pub"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"
KNOWN_EXCHANGES=("okex" "binance" "bybit" "bitget" "gate")
KNOWN_VENUES=(
  "okex-futures" "okex-margin"
  "binance-futures" "binance-margin"
  "bybit-futures" "bybit-margin"
  "bitget-futures" "bitget-margin"
  "gate-futures" "gate-margin"
)

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

is_known_venue() {
  local v="${1,,}"
  for venue in "${KNOWN_VENUES[@]}"; do
    if [[ "$v" == "$venue" ]]; then
      return 0
    fi
  done
  return 1
}

usage() {
  cat <<'USAGE'
Usage:
  deploy_depth_pub.sh (--exchange <exchange> | --venue <venue>...) [--root <path>]

Defaults:
  固定部署根目录 -> $HOME/depth_pub
  目录结构 -> $HOME/depth_pub/<venue>/

Examples:
  bash scripts/deploy_depth_pub.sh --exchange binance
  bash scripts/deploy_depth_pub.sh --exchange okex
  bash scripts/deploy_depth_pub.sh --venue gate-futures
  bash scripts/deploy_depth_pub.sh --venue binance-futures --venue binance-margin
  bash scripts/deploy_depth_pub.sh --exchange gate --root "$HOME/depth_pub"

Notes:
  - Exchange expands to default venues:
      okex    -> okex-futures okex-margin
      binance -> binance-futures binance-margin
      bybit   -> bybit-futures bybit-margin
      bitget  -> bitget-futures bitget-margin
      gate    -> gate-futures gate-margin
  - start/stop 在单个 venue 目录中执行（按目录名推断 venue）。
USAGE
}

# 参数解析
TARGET_ROOT="$HOME/depth_pub"
EXCHANGE=""
VENUES_FROM_ARG=()
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
    --venue)
      venue="${2:-}"
      if [[ -z "$venue" ]]; then
        echo "[ERROR] --venue 需要一个值" >&2
        usage >&2
        exit 1
      fi
      VENUES_FROM_ARG+=("${venue,,}")
      shift 2
      ;;
    --root|--dir)
      TARGET_ROOT="${2:-}"
      if [[ -z "$TARGET_ROOT" ]]; then
        echo "[ERROR] --root/--dir 需要一个路径" >&2
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
      echo "[ERROR] 未知参数: $1（仅支持 --exchange / --venue / --root）" >&2
      usage >&2
      exit 1
      ;;
  esac
done

VENUES=()
if [[ ${#VENUES_FROM_ARG[@]} -gt 0 ]]; then
  for venue in "${VENUES_FROM_ARG[@]}"; do
    if ! is_known_venue "$venue"; then
      echo "[ERROR] 不支持的 venue: $venue" >&2
      usage >&2
      exit 1
    fi
    VENUES+=("$venue")
  done
else
  if [[ -z "$EXCHANGE" ]]; then
    echo "[ERROR] 必须提供 --exchange 或 --venue" >&2
    usage >&2
    exit 1
  fi

  if ! is_known_exchange "$EXCHANGE"; then
    echo "[ERROR] 不支持的 exchange: $EXCHANGE" >&2
    usage >&2
    exit 1
  fi

  read -r -a VENUES <<<"$(default_venues_for_exchange "$EXCHANGE")"
fi

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
SCRIPTS_TO_DEPLOY=(
  "start_depth_pub.sh"
  "stop_depth_pub.sh"
)

for venue in "${VENUES[@]}"; do
  TARGET_DIR="${TARGET_ROOT%/}/${venue}"
  echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
  mkdir -p "$TARGET_DIR"

  # 直接覆盖二进制：若文件被运行中进程占用，会触发 Text file busy 并中断部署。
  cp "$BIN_PATH" "$TARGET_DIR/"
  chmod +x "$TARGET_DIR/$BIN_NAME"

  mkdir -p "$TARGET_DIR/scripts"
  for script in "${SCRIPTS_TO_DEPLOY[@]}"; do
    if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
      rsync -a "$SCRIPT_DIR_SRC/$script" "$TARGET_DIR/scripts/"
      chmod +x "$TARGET_DIR/scripts/$script"
    fi
  done

  # depth_pub 使用相对配置路径 config/depth_cfg.yaml，需下发到每个 venue 目录。
  mkdir -p "$TARGET_DIR/config"
  if [[ -f "$ROOT_DIR/config/depth_cfg.yaml" ]]; then
    rsync -a "$ROOT_DIR/config/depth_cfg.yaml" "$TARGET_DIR/config/"
  fi
  if [[ -f "$ROOT_DIR/config/iceoryx2.toml" ]]; then
    rsync -a "$ROOT_DIR/config/iceoryx2.toml" "$TARGET_DIR/config/"
  fi
done

echo "[INFO] $BIN_NAME 部署完成"
echo "[INFO] root_dir: ${TARGET_ROOT%/}"
echo "[INFO] venues: ${VENUES[*]}"
echo "[INFO] 启动示例: cd ${TARGET_ROOT%/}/${VENUES[0]} && ./scripts/start_depth_pub.sh"
