#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="persist_manager"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'EOF'
Usage:
  scripts/deploy_fr_persist_manager.sh [trade|test] --exchange <binance|okex|bybit|bitget|gate> [--scripts-only|--bin-only]

Notes:
  - Builds and copies persist_manager to $HOME/<exchange>_fr_<trade|test>/ (does not auto-start).
  - --scripts-only: sync scripts only
  - --bin-only: build and sync binary only
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_TYPE="trade"
EXCHANGE="binance"
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
    --scripts-only)
      if [[ -n "$ONLY_MODE" ]]; then
        echo "[ERROR] --scripts-only and --bin-only are mutually exclusive" >&2
        exit 1
      fi
      ONLY_MODE="scripts"
      DO_BUILD=0
      DO_SCRIPTS=1
      shift
      ;;
    --bin-only)
      if [[ -n "$ONLY_MODE" ]]; then
        echo "[ERROR] --scripts-only and --bin-only are mutually exclusive" >&2
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
if [[ "$EXCHANGE" == "okx" ]]; then
  EXCHANGE="okex"
fi

case "$EXCHANGE" in
  binance|okex|bybit|bitget|gate)
    ;;
  *)
    echo "[ERROR] Unsupported exchange: $EXCHANGE (allowed: binance/okex/bybit/bitget/gate)" >&2
    exit 1
    ;;
esac

TARGET_DIR="$HOME/${EXCHANGE}_fr_${ENV_TYPE}"

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] Building $BIN_NAME (release)"
  (cd "$ROOT_DIR" && cargo build --release --bin "$BIN_NAME")
fi

mkdir -p "$TARGET_DIR"
mkdir -p "$TARGET_DIR/data/persist_manager" >/dev/null 2>&1 || true

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] Deploying $BIN_NAME to $TARGET_DIR"
  cp "$BIN_PATH" "$TARGET_DIR/"
  chmod +x "$TARGET_DIR/$BIN_NAME"
fi

SCRIPTS_TO_SYNC=(
  "start_fr_persist_manager.sh"
  "stop_fr_persist_manager.sh"
)

if [[ "$DO_SCRIPTS" -eq 1 ]]; then
  mkdir -p "$TARGET_DIR/scripts"
  for script in "${SCRIPTS_TO_SYNC[@]}"; do
    if [[ -f "$ROOT_DIR/scripts/$script" ]]; then
      rsync -a "$ROOT_DIR/scripts/$script" "$TARGET_DIR/scripts/"
      chmod +x "$TARGET_DIR/scripts/$script"
    fi
  done
fi

echo "[INFO] $BIN_NAME deployed to $TARGET_DIR"
echo "[INFO] Start: cd $TARGET_DIR && ./scripts/start_fr_persist_manager.sh --port <PORT>"
echo "[INFO] Stop:  cd $TARGET_DIR && ./scripts/stop_fr_persist_manager.sh"
