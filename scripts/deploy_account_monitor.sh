#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'USAGE_EOF'
Usage:
  deploy_account_monitor.sh --env-name <exchange>_fr_<suffix>
                            [--exchange okex|binance|gate|bitget]
                            [--scripts-only|--bin-only]

Examples:
  bash scripts/deploy_account_monitor.sh --env-name okex_fr_hf01
  bash scripts/deploy_account_monitor.sh --env-name binance_fr_hf02
  bash scripts/deploy_account_monitor.sh --env-name gate_fr_hf01 --exchange gate

Notes:
  - This script builds the existing per-exchange binaries:
      okex   -> okex_account_monitor
      binance-> binance_account_monitor
      gate   -> gate_account_monitor
      bitget -> bitget_account_monitor
  - Deploy dir:
      $HOME/<exchange>_fr_<suffix> (e.g. $HOME/okex_fr_hf01, $HOME/binance_fr_hf02)
  - exchange can be omitted and inferred from --env-name.
USAGE_EOF
}

EXCHANGE=""
ENV_NAME=""
DO_BUILD=1
DO_SCRIPTS=1
ONLY_MODE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    --env-name)
      ENV_NAME="${2:-}"
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
      echo "[ERROR] Unknown argument: $1" >&2
      usage >&2
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
  echo "[ERROR] --env-name is required (e.g. okex_fr_hf01)" >&2
  usage >&2
  exit 1
fi
ENV_NAME="$(normalize_env_name "$ENV_NAME")"

if [[ -z "$EXCHANGE" ]]; then
  EXCHANGE="$(infer_exchange_from_env_name "$ENV_NAME")"
fi
EXCHANGE="$(normalize_exchange "$EXCHANGE")"
case "$EXCHANGE" in
  okex|binance|gate|bitget) ;;
  *)
    echo "[ERROR] --exchange must be one of: okex, binance, gate, bitget" >&2
    usage >&2
    exit 1
    ;;
esac

require_fr_env_name "$EXCHANGE" "$ENV_NAME"

TARGET_DIR="$HOME/${ENV_NAME}"

case "$EXCHANGE" in
  okex) BIN_NAME="okex_account_monitor" ;;
  binance) BIN_NAME="binance_account_monitor" ;;
  gate) BIN_NAME="gate_account_monitor" ;;
  bitget) BIN_NAME="bitget_account_monitor" ;;
esac

BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] Build $BIN_NAME (release)"
  cargo build --release --bin "$BIN_NAME"
fi

mkdir -p "$TARGET_DIR"
if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] Deploy $BIN_NAME -> $TARGET_DIR"
  cp "$BIN_PATH" "$TARGET_DIR/account_monitor"
  chmod +x "$TARGET_DIR/account_monitor"
fi

# 同步启动/停止脚本到 scripts/
SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
SCRIPTS_TO_SYNC=("start_account_monitor.sh" "stop_account_monitor.sh" "process_match_lib.sh")
if [[ "$DO_SCRIPTS" -eq 1 ]]; then
  mkdir -p "$TARGET_DIR/scripts"
  for script in "${SCRIPTS_TO_SYNC[@]}"; do
    if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
      rsync -a "$SCRIPT_DIR_SRC/$script" "$TARGET_DIR/scripts/"
      chmod +x "$TARGET_DIR/scripts/$script"
    fi
  done
fi

echo "[INFO] Done: $TARGET_DIR/account_monitor"
echo "[INFO] Start: cd $TARGET_DIR && ./scripts/start_account_monitor.sh"
