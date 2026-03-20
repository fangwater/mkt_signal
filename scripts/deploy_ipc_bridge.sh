#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="ipc_bridge"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'USAGE'
Usage:
  scripts/deploy_ipc_bridge.sh --env <jp|hk> [--env-name bridge_<env>] [--scripts-only|--bin-only]

Notes:
  - Default target dir: $HOME/bridge_<env>/
  - Default process name at runtime: bridge_<env>
  - This script deploys only. It does not start the process.
USAGE
}

ENV_TAG=""
ENV_NAME=""
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
    --env-name)
      ENV_NAME="${2:-}"
      if [[ -z "$ENV_NAME" ]]; then
        echo "[ERROR] --env-name requires a value" >&2
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

if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="bridge_${ENV_TAG}"
fi
ENV_NAME="$(echo "$ENV_NAME" | tr 'A-Z' 'a-z')"
if [[ "$ENV_NAME" != "bridge_${ENV_TAG}" ]]; then
  echo "[ERROR] env-name must match bridge_${ENV_TAG} (got: ${ENV_NAME})" >&2
  exit 1
fi

TARGET_DIR="$HOME/${ENV_NAME}"
CFG_SRC="$ROOT_DIR/config/ipc_bridge_${ENV_TAG}.yaml"

if [[ ! -f "$CFG_SRC" ]]; then
  echo "[ERROR] bridge config template not found: $CFG_SRC" >&2
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
  for script in deploy_ipc_bridge.sh start_ipc_bridge.sh stop_ipc_bridge.sh; do
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

echo "[INFO] $BIN_NAME deployed to $TARGET_DIR"
echo "[INFO] config: $TARGET_DIR/config/ipc_bridge.yaml"
echo "[INFO] start: cd $TARGET_DIR && ./scripts/start_ipc_bridge.sh"
