#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="trade_signal"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_mm_signal.sh --env-name <exchange>_mm_<suffix>
                              [--exchange <binance|okex|bybit|bitget|gate>]
                              [--scripts-only|--bin-only]

说明:
  - 统一通过 --env-name 指定 MM 环境（suffix 可为 alpha/beta/trade/...）。
  - exchange 可省略，会从 --env-name 推断。
  - 仅部署，不自动启动。
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

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
        echo "[ERROR] --exchange 需要一个值" >&2
        exit 1
      fi
      shift 2
      ;;
    --env-name)
      ENV_NAME="${2:-}"
      if [[ -z "$ENV_NAME" ]]; then
        echo "[ERROR] --env-name 需要一个值" >&2
        exit 1
      fi
      shift 2
      ;;
    --scripts-only)
      if [[ -n "$ONLY_MODE" ]]; then
        echo "[ERROR] --scripts-only 与 --bin-only 互斥" >&2
        exit 1
      fi
      ONLY_MODE="scripts"
      DO_BUILD=0
      DO_SCRIPTS=1
      shift
      ;;
    --bin-only)
      if [[ -n "$ONLY_MODE" ]]; then
        echo "[ERROR] --scripts-only 与 --bin-only 互斥" >&2
        exit 1
      fi
      ONLY_MODE="bin"
      DO_BUILD=1
      DO_SCRIPTS=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 未知参数: $1" >&2
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
  if [[ "$name" =~ ^([a-z0-9]+)[-_]mm([_-].*)?$ ]]; then
    echo "${BASH_REMATCH[1]}"
  fi
}

require_mm_env_name() {
  local exchange="$1"
  local name="$2"
  if [[ ! "$name" =~ ^${exchange}_mm(_[a-z0-9][a-z0-9_-]*)?$ ]]; then
    echo "[ERROR] env-name must match ${exchange}_mm_<suffix> (got: ${name})" >&2
    exit 1
  fi
}

if [[ -z "$ENV_NAME" ]]; then
  echo "[ERROR] 需要使用 --env-name 指定部署环境名（例如 binance_mm_beta）" >&2
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

require_mm_env_name "$EXCHANGE" "$ENV_NAME"

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
  "start_trade_signal.sh"
  "stop_trade_signal.sh"
  "mm_process_name.sh"
  "sync_mm_strategy_params.py"
  "print_mm_strategy_params.py"
  "sync_mm_amount_u.py"
  "print_mm_amount_u.py"
  "sync_mm_symbol_list.py"
  "print_mm_symbol_list.py"
)
if [[ "$DO_SCRIPTS" -eq 1 ]]; then
  mkdir -p "$TARGET_DIR/scripts"
  for script in "${SCRIPTS_TO_SYNC[@]}"; do
    if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
      rsync -a "$SCRIPT_DIR_SRC/$script" "$TARGET_DIR/scripts/"
      chmod +x "$TARGET_DIR/scripts/$script" 2>/dev/null || true
    fi
  done
fi

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 手动启动: cd $TARGET_DIR && ./scripts/start_trade_signal.sh"
