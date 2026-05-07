#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN_NAME="spread_pbs"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

# 5 个 CEX × 2 market = 10 venue，core 0–9 字母序固定映射
KNOWN_VENUES=(
  "binance-margin"
  "binance-futures"
  "bitget-margin"
  "bitget-futures"
  "bybit-margin"
  "bybit-futures"
  "gate-margin"
  "gate-futures"
  "okex-margin"
  "okex-futures"
)
KNOWN_EXCHANGES=("binance" "bitget" "bybit" "gate" "okex")

is_known_exchange() {
  local v="${1,,}"
  for e in "${KNOWN_EXCHANGES[@]}"; do
    [[ "$v" == "$e" ]] && return 0
  done
  return 1
}

default_venues_for_exchange() {
  case "${1,,}" in
    binance) echo "binance-margin binance-futures" ;;
    bitget)  echo "bitget-margin bitget-futures"  ;;
    bybit)   echo "bybit-margin bybit-futures"    ;;
    gate)    echo "gate-margin gate-futures"      ;;
    okex)    echo "okex-margin okex-futures"      ;;
    *) echo ""; return 1 ;;
  esac
}

is_known_venue() {
  local v="${1,,}"
  for venue in "${KNOWN_VENUES[@]}"; do
    [[ "$v" == "$venue" ]] && return 0
  done
  return 1
}

usage() {
  cat <<'USAGE'
Usage:
  deploy_spread_pbs.sh (--exchange <exchange> | --venue <venue>...) [--root <path>]
  deploy_spread_pbs.sh --all          # 一次铺所有 5 CEX × 2 market

Defaults:
  固定部署根目录 -> $HOME/spread_pbs
  目录结构       -> $HOME/spread_pbs/<venue>/

Notes:
  - 当前仅 OKex 的解析路径打通（okex-margin / okex-futures）。
    其他 venue 启动会立即 bail（"only supports okex venues for now"），
    但目录、脚本、core 映射都会一并铺好，等其他 venue parser 接入后即可启动。
  - core 映射写死在 start_spread_pbs.sh 里，按字母序：
      binance-margin=0  binance-futures=1
      bitget-margin=2   bitget-futures=3
      bybit-margin=4    bybit-futures=5
      gate-margin=6     gate-futures=7
      okex-margin=8     okex-futures=9
USAGE
}

TARGET_ROOT="$HOME/spread_pbs"
EXCHANGE=""
VENUES_FROM_ARG=()
DEPLOY_ALL=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-}"
      [[ -z "$EXCHANGE" ]] && { echo "[ERROR] --exchange 需要一个值" >&2; usage >&2; exit 1; }
      shift 2
      ;;
    --venue)
      v="${2:-}"
      [[ -z "$v" ]] && { echo "[ERROR] --venue 需要一个值" >&2; usage >&2; exit 1; }
      VENUES_FROM_ARG+=("${v,,}")
      shift 2
      ;;
    --root|--dir)
      TARGET_ROOT="${2:-}"
      [[ -z "$TARGET_ROOT" ]] && { echo "[ERROR] --root 需要一个路径" >&2; usage >&2; exit 1; }
      shift 2
      ;;
    --all)
      DEPLOY_ALL=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 未知参数: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

VENUES=()
if [[ $DEPLOY_ALL -eq 1 ]]; then
  VENUES=("${KNOWN_VENUES[@]}")
elif [[ ${#VENUES_FROM_ARG[@]} -gt 0 ]]; then
  for v in "${VENUES_FROM_ARG[@]}"; do
    is_known_venue "$v" || { echo "[ERROR] 不支持的 venue: $v" >&2; exit 1; }
    VENUES+=("$v")
  done
else
  [[ -z "$EXCHANGE" ]] && { echo "[ERROR] 必须提供 --exchange / --venue / --all" >&2; usage >&2; exit 1; }
  is_known_exchange "$EXCHANGE" || { echo "[ERROR] 不支持的 exchange: $EXCHANGE" >&2; exit 1; }
  read -r -a VENUES <<<"$(default_venues_for_exchange "$EXCHANGE")"
fi

echo "[INFO] 构建 $BIN_NAME (release)"
( cd "$ROOT_DIR" && cargo build --release --bin "$BIN_NAME" )

SCRIPT_DIR_SRC="$ROOT_DIR/scripts/spread_pbs"
SCRIPTS_TO_DEPLOY=(
  "start_spread_pbs.sh"
  "stop_spread_pbs.sh"
)

for venue in "${VENUES[@]}"; do
  TARGET_DIR="${TARGET_ROOT%/}/${venue}"
  echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
  mkdir -p "$TARGET_DIR/scripts"

  # Linux 下若 binary 仍有进程持有，cp 直接覆盖会触发 ETXTBSY；
  # 先 unlink 旧路径（原 inode 仍被进程持有不影响），再 cp 新文件占用同一路径。
  rm -f "$TARGET_DIR/$BIN_NAME"
  cp "$BIN_PATH" "$TARGET_DIR/"
  chmod +x "$TARGET_DIR/$BIN_NAME"

  for script in "${SCRIPTS_TO_DEPLOY[@]}"; do
    src="$SCRIPT_DIR_SRC/$script"
    if [[ -f "$src" ]]; then
      rsync -a "$src" "$TARGET_DIR/scripts/"
      chmod +x "$TARGET_DIR/scripts/$script"
    fi
  done
done

# 共享配置（mkt_cfg.yaml + iceoryx2.toml）部署到根目录
mkdir -p "${TARGET_ROOT%/}/config"
if [[ -f "$ROOT_DIR/config/mkt_cfg.yaml" ]]; then
  rsync -a "$ROOT_DIR/config/mkt_cfg.yaml" "${TARGET_ROOT%/}/config/"
fi
if [[ -f "$ROOT_DIR/config/iceoryx2.toml" ]]; then
  rsync -a "$ROOT_DIR/config/iceoryx2.toml" "${TARGET_ROOT%/}/config/"
fi

echo "[INFO] $BIN_NAME 部署完成"
echo "[INFO] root_dir: ${TARGET_ROOT%/}"
echo "[INFO] venues:   ${VENUES[*]}"
echo "[INFO] config:   ${TARGET_ROOT%/}/config/mkt_cfg.yaml"
echo "[INFO] 启动示例: cd ${TARGET_ROOT%/}/${VENUES[0]} && ./scripts/start_spread_pbs.sh"
