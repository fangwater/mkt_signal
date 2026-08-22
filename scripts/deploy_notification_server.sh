#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="notification_server"
BIN_PATH="${ROOT_DIR}/target/release/${BIN_NAME}"
TARGET_DIR="${HOME}/notification_server"
DO_BUILD=1
DO_SCRIPTS=1

usage() {
  cat <<'USAGE'
Usage:
  deploy_notification_server.sh [--target-dir <path>] [--scripts-only|--bin-only]

Defaults:
  target directory: $HOME/notification_server

The deploy preserves an existing config/notification_server.env file.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target-dir)
      TARGET_DIR="${2:-}"
      if [[ -z "$TARGET_DIR" ]]; then
        echo "[ERROR] --target-dir requires a value" >&2
        exit 1
      fi
      shift 2
      ;;
    --scripts-only)
      DO_BUILD=0
      DO_SCRIPTS=1
      shift
      ;;
    --bin-only)
      DO_BUILD=1
      DO_SCRIPTS=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] building ${BIN_NAME} (release)"
  cargo build --release -p notification_server
fi

mkdir -p "$TARGET_DIR"
if [[ "$DO_BUILD" -eq 1 ]]; then
  TEMP_BIN_PATH="${TARGET_DIR}/.${BIN_NAME}.new"
  cp "$BIN_PATH" "$TEMP_BIN_PATH"
  chmod +x "$TEMP_BIN_PATH"
  mv -f "$TEMP_BIN_PATH" "$TARGET_DIR/$BIN_NAME"
fi

if [[ "$DO_SCRIPTS" -eq 1 ]]; then
  mkdir -p "$TARGET_DIR/scripts" "$TARGET_DIR/config"
  for script in start_notification_server.sh stop_notification_server.sh deploy_notification_server.sh; do
    cp "$ROOT_DIR/scripts/$script" "$TARGET_DIR/scripts/$script"
    chmod +x "$TARGET_DIR/scripts/$script"
  done
  if [[ ! -f "$TARGET_DIR/config/notification_server.env" ]]; then
    cp "$ROOT_DIR/config/notification_server.env.example" "$TARGET_DIR/config/notification_server.env"
    chmod 600 "$TARGET_DIR/config/notification_server.env"
    echo "[WARN] created empty credential config: $TARGET_DIR/config/notification_server.env"
  else
    echo "[INFO] preserving existing credential config"
  fi
fi

echo "[INFO] notification_server deployed to $TARGET_DIR"
echo "[INFO] start: cd $TARGET_DIR && ./scripts/start_notification_server.sh"
echo "[INFO] stop:  cd $TARGET_DIR && ./scripts/stop_notification_server.sh"
