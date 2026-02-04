#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="trade_engine"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'USAGE'
Usage:
  scripts/deploy_mm_trade_engine.sh [trade|test] --exchange <binance|okex|gate|bybit|bitget> [--env-suffix <suffix>] [--env-name <exchange>_mm_<suffix>] [--scripts-only|--bin-only]

Notes:
  - Default target dir: $HOME/<exchange>_mm_<trade|test>/
  - --env-suffix overrides the default suffix (e.g. beta -> <exchange>_mm_beta)
  - --scripts-only: sync scripts only
  - --bin-only: build/copy binary only
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_TYPE="trade"
EXCHANGE="binance"
ENV_NAME=""
ENV_SUFFIX=""
DO_BUILD=1
DO_SCRIPTS=1
ONLY_MODE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    trade|test)
      ENV_TYPE="$1"
      shift
      ;;
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

EXCHANGE="$(echo "$EXCHANGE" | tr 'A-Z' 'a-z')"
case "$EXCHANGE" in
  binance|okex|gate|bybit|bitget)
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
  if [[ -n "$ENV_SUFFIX" ]]; then
    ENV_NAME="${EXCHANGE}_mm_${ENV_SUFFIX}"
  else
    ENV_NAME="${EXCHANGE}_mm_${ENV_TYPE}"
  fi
fi
ENV_NAME="$(normalize_env_name "$ENV_NAME")"
require_mm_env_name "$EXCHANGE" "$ENV_NAME"

TARGET_DIR="$HOME/${ENV_NAME}"

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] build $BIN_NAME (release)"
  cargo build --release --bin "$BIN_NAME"
fi

mkdir -p "$TARGET_DIR"
if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] deploy $BIN_NAME to $TARGET_DIR"
  cp "$BIN_PATH" "$TARGET_DIR/"
  chmod +x "$TARGET_DIR/$BIN_NAME"
fi

if [[ "$DO_SCRIPTS" -eq 1 ]]; then
  mkdir -p "$TARGET_DIR/mm_scripts"
  for script in start_mm_trade_engine.sh stop_mm_trade_engine.sh; do
    if [[ -f "$ROOT_DIR/mm_scripts/$script" ]]; then
      rsync -a "$ROOT_DIR/mm_scripts/$script" "$TARGET_DIR/mm_scripts/"
      chmod +x "$TARGET_DIR/mm_scripts/$script"
    fi
  done
fi

echo "[INFO] $BIN_NAME deployed to $TARGET_DIR"
