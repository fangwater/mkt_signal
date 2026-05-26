#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="latency_stable_monitor"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'USAGE'
Usage:
  scripts/deploy_latency_stable_monitor.sh --env <sg|hk> [--target-dir <path>] [--scripts-only|--bin-only]

Behavior:
  - Deploys one latency_stable_monitor process config for the target env.
  - Currently supported env: sg, hk
  - Default target dir:
      sg -> $HOME/latency_stable_monitor_sg
      hk -> $HOME/latency_stable_monitor_hk
  - Default config template:
      sg -> config/latency_stable_monitor_sg.yaml
      hk -> config/latency_stable_monitor_hk.yaml

Examples:
  bash scripts/deploy_latency_stable_monitor.sh --env sg
  bash scripts/deploy_latency_stable_monitor.sh --env hk
  bash scripts/deploy_latency_stable_monitor.sh --env hk --target-dir "$HOME/latency_stable_monitor_hk"
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
  sg|hk)
    ;;
  *)
    echo "[ERROR] unsupported env: $ENV_TAG (supported: sg, hk)" >&2
    exit 1
    ;;
esac

if [[ -z "$TARGET_DIR" ]]; then
  TARGET_DIR="$HOME/latency_stable_monitor_${ENV_TAG}"
fi

CFG_SRC="$ROOT_DIR/config/latency_stable_monitor_${ENV_TAG}.yaml"
if [[ ! -f "$CFG_SRC" ]]; then
  echo "[ERROR] latency monitor config not found: $CFG_SRC" >&2
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
  for script in start_latency_stable_monitor.sh stop_latency_stable_monitor.sh deploy_latency_stable_monitor.sh process_match_lib.sh; do
    cp "$ROOT_DIR/scripts/$script" "$TARGET_DIR/scripts/"
    chmod +x "$TARGET_DIR/scripts/$script"
  done

  mkdir -p "$TARGET_DIR/config"
  cp "$CFG_SRC" "$TARGET_DIR/config/latency_stable_monitor.yaml"
  cp "$CFG_SRC" "$TARGET_DIR/config/$(basename "$CFG_SRC")"

  if [[ -f "$ROOT_DIR/config/iceoryx2.toml" ]]; then
    cp "$ROOT_DIR/config/iceoryx2.toml" "$TARGET_DIR/config/"
  fi
fi

echo "[INFO] latency stable monitor deployed to $TARGET_DIR"
echo "[INFO] config: $TARGET_DIR/config/latency_stable_monitor.yaml"
echo "[INFO] start: cd $TARGET_DIR && ./scripts/start_latency_stable_monitor.sh"
echo "[INFO] stop:  cd $TARGET_DIR && ./scripts/stop_latency_stable_monitor.sh"
