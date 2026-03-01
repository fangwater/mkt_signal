#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="trade_engine"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_fr_trade_engine.sh --env-name <exchange>_fr_<suffix>
                                    [--exchange <binance|okex|bybit|bitget|gate>]

说明:
  - 统一通过 --env-name 指定 FR 环境（suffix 必填）。
  - exchange 可省略，会从 --env-name 推断（例如 binance_fr_hf01 -> binance）。
  - 目标目录: $HOME/<env-name>/
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

EXCHANGE=""
ENV_NAME=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-}"
      if [[ -z "$EXCHANGE" ]]; then
        echo "[ERROR] --exchange 需要一个值"
        exit 1
      fi
      shift 2
      ;;
    --env-name)
      ENV_NAME="${2:-}"
      if [[ -z "$ENV_NAME" ]]; then
        echo "[ERROR] --env-name 需要一个值"
        exit 1
      fi
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

normalize_exchange() {
  local ex="${1,,}"
  if [[ "$ex" == "okx" ]]; then
    ex="okex"
  fi
  echo "$ex"
}

normalize_env_name() {
  echo "$1" | tr 'A-Z' 'a-z'
}

infer_exchange_from_env_name() {
  local name="${1,,}"
  if [[ "$name" =~ ^([a-z0-9]+)[-_]fr([_-].*)?$ ]]; then
    echo "${BASH_REMATCH[1]}"
  fi
}

require_fr_env_name() {
  local exchange="$1"
  local name="$2"
  if [[ ! "$name" =~ ^${exchange}_fr_[a-z0-9][a-z0-9_-]*$ ]]; then
    echo "[ERROR] env-name must match ${exchange}_fr_<suffix> (got: ${name})" >&2
    exit 1
  fi
}

if [[ -z "$ENV_NAME" ]]; then
  echo "[ERROR] 需要使用 --env-name 指定部署环境名（例如 binance_fr_hf01）" >&2
  usage
  exit 1
fi

ENV_NAME="$(normalize_env_name "$ENV_NAME")"

if [[ -z "$EXCHANGE" ]]; then
  EXCHANGE="$(infer_exchange_from_env_name "$ENV_NAME")"
fi
EXCHANGE="$(normalize_exchange "$EXCHANGE")"
case "$EXCHANGE" in
  binance|okex|bybit|bitget|gate)
    ;;
  *)
    echo "[ERROR] 无法从 --env-name 推断 exchange，或 --exchange 无效: $EXCHANGE" >&2
    usage
    exit 1
    ;;
esac

require_fr_env_name "$EXCHANGE" "$ENV_NAME"

TARGET_DIR="$HOME/${ENV_NAME}"

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"
mkdir -p "$TARGET_DIR/config"
cp "$BIN_PATH" "$TARGET_DIR/"
chmod +x "$TARGET_DIR/$BIN_NAME"

# 同步启动/停止脚本
SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
SCRIPTS_TO_SYNC=("start_trade_engine.sh" "stop_trade_engine.sh")
mkdir -p "$TARGET_DIR/scripts"
for script in "${SCRIPTS_TO_SYNC[@]}"; do
  if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
    rsync -a "$SCRIPT_DIR_SRC/$script" "$TARGET_DIR/scripts/"
    chmod +x "$TARGET_DIR/scripts/$script"
  fi
done

# trade_engine 读取 /home/<user>/config/mkt_cfg.yaml 的本地 IP；无需部署额外配置文件

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 手动启动: cd $TARGET_DIR && ./scripts/start_trade_engine.sh"
