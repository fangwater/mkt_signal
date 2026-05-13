#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="trade_signal"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_fr_signal.sh --env-name <exchange>_fr_<suffix>
                              [--exchange <binance|okex|bybit|bitget|gate>]
                              [--scripts-only|--bin-only]

说明:
  - 统一通过 --env-name 指定 FR 环境（suffix 必填）。
  - exchange 可省略，会从 --env-name 推断。
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
    -h|--help)
      usage
      exit 0
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

# 同步启动/停止脚本及相关工具
SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
SCRIPTS_TO_SYNC=(
  "start_trade_signal.sh"
  "stop_trade_signal.sh"
  "start_fr_signal.sh"
  "stop_fr_signal.sh"
  "flatten_fr_futures_exposure.py"
  "flatten_binance_pm.py"
  "cancel_binance_pm_orders.py"
  "flatten_okex_pm.py"
  "cancel_okex_pm_orders.py"
  "flatten_gate_pm.py"
  "cancel_gate_pm_orders.py"
  "flatten_bybit_pm.py"
  "cancel_bybit_pm_orders.py"
  "flatten_bitget_pm.py"
  "cancel_bitget_pm_orders.py"
  "sync_funding_rate_thresholds.py"
  "print_funding_rate_thresholds.py"
  "sync_fr_strategy_params.py"
  "print_fr_strategy_params.py"
  "sync_fr_symbol_lists.py"
  "print_fr_symbol_lists.py"
  "sync_fr_spread_thresholds.py"
  "print_spread_thresholds.py"
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
echo "[INFO] 手动启动: cd $TARGET_DIR && ./scripts/start_trade_signal.sh"
