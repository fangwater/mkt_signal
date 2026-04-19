#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  scripts/deploy_mm_account_monitor.sh --exchange <okex|binance|gate|bybit|bitget> --env-suffix <suffix>
                                      [--env-name <exchange>_mm_<suffix>] [--scripts-only|--bin-only]

Notes:
  - Default target dir: $HOME/<exchange>_mm_<suffix>/
  - --env-suffix is required (e.g. beta -> <exchange>_mm_beta)
  - Deploy binary name in target dir: account_monitor
  - --scripts-only: sync scripts only
  - --bin-only: build/copy binary only
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

EXCHANGE="binance"
ENV_NAME=""
ENV_SUFFIX=""
DO_BUILD=1
DO_SCRIPTS=1
ONLY_MODE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-}"
      if [[ -z "$EXCHANGE" ]]; then
        echo "[ERROR] --exchange requires a value" >&2
        exit 1
      fi
      shift 2
      ;;
    --env-name)
      ENV_NAME="${2:-}"
      if [[ -z "$ENV_NAME" ]]; then
        echo "[ERROR] --env-name requires a value" >&2
        exit 1
      fi
      shift 2
      ;;
    --env-suffix)
      ENV_SUFFIX="${2:-}"
      if [[ -z "$ENV_SUFFIX" ]]; then
        echo "[ERROR] --env-suffix requires a value" >&2
        exit 1
      fi
      shift 2
      ;;
    --scripts-only)
      if [[ -n "$ONLY_MODE" ]]; then
        echo "[ERROR] --scripts-only conflicts with --bin-only" >&2
        exit 1
      fi
      ONLY_MODE="scripts"
      DO_BUILD=0
      DO_SCRIPTS=1
      shift
      ;;
    --bin-only)
      if [[ -n "$ONLY_MODE" ]]; then
        echo "[ERROR] --scripts-only conflicts with --bin-only" >&2
        exit 1
      fi
      ONLY_MODE="bin"
      DO_BUILD=1
      DO_SCRIPTS=0
      shift
      ;;
    *)
      echo "[ERROR] Unknown arg: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$ENV_SUFFIX" ]]; then
  echo "[ERROR] --env-suffix is required" >&2
  usage >&2
  exit 1
fi

EXCHANGE="$(echo "$EXCHANGE" | tr 'A-Z' 'a-z')"
case "$EXCHANGE" in
  okex|binance|gate|bybit|bitget)
    ;;
  *)
    echo "[ERROR] unsupported exchange: $EXCHANGE" >&2
    exit 1
    ;;
esac

normalize_env_name() {
  echo "$1" | tr 'A-Z' 'a-z'
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
  ENV_NAME="${EXCHANGE}_mm_${ENV_SUFFIX}"
fi
ENV_NAME="$(normalize_env_name "$ENV_NAME")"
require_mm_env_name "$EXCHANGE" "$ENV_NAME"

TARGET_DIR="$HOME/${ENV_NAME}"

case "$EXCHANGE" in
  okex) BIN_NAME="okex_account_monitor" ;;
  binance) BIN_NAME="binance_account_monitor" ;;
  gate) BIN_NAME="gate_account_monitor" ;;
  bybit) BIN_NAME="bybit_account_monitor" ;;
  bitget) BIN_NAME="bitget_account_monitor" ;;
esac

BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] build $BIN_NAME (release)"
  cargo build --release --bin "$BIN_NAME"
fi

mkdir -p "$TARGET_DIR"
if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] deploy $BIN_NAME -> $TARGET_DIR/account_monitor"
  cp "$BIN_PATH" "$TARGET_DIR/account_monitor"
  chmod +x "$TARGET_DIR/account_monitor"
fi

if [[ "$DO_SCRIPTS" -eq 1 ]]; then
  mkdir -p "$TARGET_DIR/scripts"
  for script in start_account_monitor.sh stop_account_monitor.sh mm_process_name.sh; do
    if [[ -f "$ROOT_DIR/scripts/$script" ]]; then
      rsync -a "$ROOT_DIR/scripts/$script" "$TARGET_DIR/scripts/"
      chmod +x "$TARGET_DIR/scripts/$script"
    fi
  done
fi

echo "[INFO] account monitor deployed to $TARGET_DIR"
echo "[INFO] Start: cd $TARGET_DIR && ./scripts/start_account_monitor.sh"
