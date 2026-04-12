#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="ipc_bridge"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'USAGE'
Usage:
  scripts/deploy_public_bridge.sh --env <jp|hk> [--target-dir <path>] [--scripts-only|--bin-only]

Behavior:
  - Deploys the standalone public bridge only.
  - Default target dir:
      jp -> $HOME/bridge_jp_public
      hk -> $HOME/bridge_hk_public
  - Default config template:
      jp -> config/ipc_bridge_public_jp.yaml
      hk -> config/ipc_bridge_public_hk.yaml
  - Public bridge carries only global streams:
      - bridge/<venue> market data
      - model_output/binance-futures-mm-xgb-test
    It does not require IPC_NAMESPACE.

Examples:
  bash scripts/deploy_public_bridge.sh --env jp
  bash scripts/deploy_public_bridge.sh --env hk
  bash scripts/deploy_public_bridge.sh --env jp --target-dir "$HOME/bridge_jp_public_v2"
USAGE
}

ENV_TAG=""
TARGET_DIR=""
DO_BUILD=1
DO_SCRIPTS=1
ONLY_MODE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)
      ENV_TAG="${2:-}"
      if [[ -z "$ENV_TAG" ]]; then
        echo "[ERROR] --env requires a value" >&2
        exit 1
      fi
      shift 2
      ;;
    --target-dir)
      TARGET_DIR="${2:-}"
      if [[ -z "$TARGET_DIR" ]]; then
        echo "[ERROR] --target-dir requires a value" >&2
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
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown arg: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$ENV_TAG" ]]; then
  echo "[ERROR] --env is required" >&2
  usage >&2
  exit 1
fi

ENV_TAG="$(echo "$ENV_TAG" | tr 'A-Z' 'a-z')"
case "$ENV_TAG" in
  jp|hk)
    ;;
  *)
    echo "[ERROR] unsupported env: $ENV_TAG (expect jp or hk)" >&2
    exit 1
    ;;
esac

if [[ -z "$TARGET_DIR" ]]; then
  TARGET_DIR="$HOME/bridge_${ENV_TAG}_public"
fi

CFG_SRC="$ROOT_DIR/config/ipc_bridge_public_${ENV_TAG}.yaml"
if [[ ! -f "$CFG_SRC" ]]; then
  echo "[ERROR] public bridge config not found: $CFG_SRC" >&2
  exit 1
fi

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
  mkdir -p "$TARGET_DIR/scripts"
  for script in start_ipc_bridge.sh stop_ipc_bridge.sh deploy_public_bridge.sh; do
    rsync -a "$ROOT_DIR/scripts/$script" "$TARGET_DIR/scripts/"
    chmod +x "$TARGET_DIR/scripts/$script"
  done

  mkdir -p "$TARGET_DIR/config"
  rsync -a "$CFG_SRC" "$TARGET_DIR/config/ipc_bridge.yaml"
  rsync -a "$CFG_SRC" "$TARGET_DIR/config/$(basename "$CFG_SRC")"

  if [[ -f "$ROOT_DIR/config/iceoryx2.toml" ]]; then
    rsync -a "$ROOT_DIR/config/iceoryx2.toml" "$TARGET_DIR/config/"
  fi
fi

echo "[INFO] public bridge deployed to $TARGET_DIR"
echo "[INFO] config: $TARGET_DIR/config/ipc_bridge.yaml"
echo "[INFO] start: cd $TARGET_DIR && ./scripts/start_ipc_bridge.sh"
echo "[INFO] stop:  cd $TARGET_DIR && ./scripts/stop_ipc_bridge.sh"
