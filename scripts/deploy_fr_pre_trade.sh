#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="pre_trade"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_fr_pre_trade.sh --env-name <exchange>_fr_<suffix>
                                 [--exchange <binance|okex|gate>]
                                 [--scripts-only|--bin-only]

说明:
  - 默认构建并复制二进制 pre_trade 到目标目录（不自动启动）。
  - --scripts-only: 仅同步脚本
  - --bin-only: 仅构建并同步二进制
  - FR 目标目录:  $HOME/<exchange>_fr_<suffix>/
  - env-name 必须匹配 <exchange>_fr_<suffix>（suffix 必填，例如 binance_fr_hf01）。
  - exchange 可省略，会从 --env-name 推断（如 okex_fr_hf02 -> okex）。
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

# 参数解析
EXCHANGE=""
ENV_NAME=""
DO_BUILD=1
DO_SCRIPTS=1
ONLY_MODE=""
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
    --scripts-only)
      if [[ -n "$ONLY_MODE" ]]; then
        echo "[ERROR] --scripts-only 与 --bin-only 互斥"
        exit 1
      fi
      ONLY_MODE="scripts"
      DO_BUILD=0
      DO_SCRIPTS=1
      shift
      ;;
    --bin-only)
      if [[ -n "$ONLY_MODE" ]]; then
        echo "[ERROR] --scripts-only 与 --bin-only 互斥"
        exit 1
      fi
      ONLY_MODE="bin"
      DO_BUILD=1
      DO_SCRIPTS=0
      shift
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

normalize_env_name() {
  echo "$1" | tr 'A-Z' 'a-z'
}

normalize_exchange() {
  local ex="${1,,}"
  if [[ "$ex" == "okx" ]]; then
    ex="okex"
  fi
  echo "$ex"
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
  binance|okex|gate)
    ;;
  *)
    echo "[ERROR] 无法从 --env-name 推断 exchange，或 --exchange 无效: $EXCHANGE (支持: binance/okex/gate)" >&2
    usage
    exit 1
    ;;
esac

require_fr_env_name "$EXCHANGE" "$ENV_NAME"

TARGET_DIR="$HOME/${ENV_NAME}"

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] 构建 $BIN_NAME (release)"
  cargo build --release --bin "$BIN_NAME"
fi

mkdir -p "$TARGET_DIR"
if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
  cp "$BIN_PATH" "$TARGET_DIR/"
  chmod +x "$TARGET_DIR/$BIN_NAME"
fi

SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
SCRIPTS_TO_SYNC=(
  "sync_fr_risk_params.py"
  "print_fr_risk_params.py"
  "start_fr_pre_trade.sh"
  "stop_fr_pre_trade.sh"
  "process_match_lib.sh"
)
if [[ "$DO_SCRIPTS" -eq 1 ]]; then
  mkdir -p "$TARGET_DIR/scripts"
  for script in "${SCRIPTS_TO_SYNC[@]}"; do
    if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
      rsync -a "$SCRIPT_DIR_SRC/$script" "$TARGET_DIR/scripts/"
      chmod +x "$TARGET_DIR/scripts/$script"
    fi
  done
fi

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 风控参数同步: cd $TARGET_DIR && ./scripts/sync_fr_risk_params.py  # 会按目录推断 open/hedge"
