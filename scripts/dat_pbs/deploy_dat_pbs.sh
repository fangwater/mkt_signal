#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN_NAME="dat_pbs"
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
  deploy_dat_pbs.sh --exchange <exchange> [--root <path>]

Defaults:
  固定部署根目录 -> $HOME/dat_pbs
  目录结构 -> $HOME/dat_pbs/<venue>/

Examples:
  bash scripts/dat_pbs/deploy_dat_pbs.sh --exchange binance
  bash scripts/dat_pbs/deploy_dat_pbs.sh --exchange okex
  bash scripts/dat_pbs/deploy_dat_pbs.sh --exchange gate --root "$HOME/dat_pbs"

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

TARGET_ROOT="$HOME/dat_pbs"
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
      echo "[ERROR] 未知参数: $1（仅支持 --exchange / --root）" >&2
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

SCRIPT_DIR_SRC="$ROOT_DIR/scripts/dat_pbs"
SCRIPTS_TO_DEPLOY=(
  "start_dat_pbs.sh"
  "stop_dat_pbs.sh"
  "setup_pmdaemon_logrotate.sh"
)

for venue in "${VENUES[@]}"; do
  TARGET_DIR="${TARGET_ROOT%/}/${venue}"
  echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
  mkdir -p "$TARGET_DIR"

  # 原子替换，避免 Text file busy。
  tmp_bin="$(mktemp "$TARGET_DIR/${BIN_NAME}.new.XXXXXX")"
  cp "$BIN_PATH" "$tmp_bin"
  chmod +x "$tmp_bin"
  mv -f "$tmp_bin" "$TARGET_DIR/$BIN_NAME"

  mkdir -p "$TARGET_DIR/scripts"
  for script in "${SCRIPTS_TO_DEPLOY[@]}"; do
    if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
      rsync -a "$SCRIPT_DIR_SRC/$script" "$TARGET_DIR/scripts/"
      chmod +x "$TARGET_DIR/scripts/$script"
    fi
  done
done

# dat_pbs 运行时固定读取 $HOME/dat_pbs/config/mkt_cfg.yaml，故始终同步到根目录配置。
mkdir -p "${TARGET_ROOT%/}/config"
if [[ -f "$ROOT_DIR/config/mkt_cfg.yaml" ]]; then
  rsync -a "$ROOT_DIR/config/mkt_cfg.yaml" "${TARGET_ROOT%/}/config/"
fi
if [[ -f "$ROOT_DIR/config/iceoryx2.toml" ]]; then
  rsync -a "$ROOT_DIR/config/iceoryx2.toml" "${TARGET_ROOT%/}/config/"
fi

echo "[INFO] $BIN_NAME 部署完成"
echo "[INFO] root_dir: ${TARGET_ROOT%/}"
echo "[INFO] venues: ${VENUES[*]}"
echo "[INFO] config: ${TARGET_ROOT%/}/config/mkt_cfg.yaml"
echo "[INFO] 启动示例: cd ${TARGET_ROOT%/}/${VENUES[0]} && ./scripts/start_dat_pbs.sh"
