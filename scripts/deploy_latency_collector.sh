#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="latency_collector"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

# SG low-latency box defaults (override via env).
DEPLOY_HOST="${LATENCY_COLLECTOR_HOST:-ubuntu@47.131.162.78}"
DEPLOY_KEY="${LATENCY_COLLECTOR_KEY:-$ROOT_DIR/aws-sg.pem}"
REMOTE_HOME="${LATENCY_COLLECTOR_REMOTE_HOME:-/home/ubuntu}"
REMOTE_DIR="${LATENCY_COLLECTOR_REMOTE_DIR:-$REMOTE_HOME/latency_collector}"

usage() {
  cat <<USAGE
Usage:
  deploy_latency_collector.sh [--skip-build] [--host <user@ip>] [--key <pem>]

Defaults:
  host    $DEPLOY_HOST
  key     $DEPLOY_KEY
  remote  $REMOTE_DIR

Steps:
  1) cargo build --release --bin $BIN_NAME (unless --skip-build)
  2) rsync $BIN_NAME + scripts/start_latency_collector.sh + scripts/stop_latency_collector.sh
     to <host>:$REMOTE_DIR/

Start on remote:
  ssh -i <key> <host> 'cd $REMOTE_DIR && ./scripts/start_latency_collector.sh'
USAGE
}

DO_BUILD=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build) DO_BUILD=0; shift;;
    --host) DEPLOY_HOST="${2:-}"; shift 2;;
    --host=*) DEPLOY_HOST="${1#--host=}"; shift;;
    --key)  DEPLOY_KEY="${2:-}"; shift 2;;
    --key=*) DEPLOY_KEY="${1#--key=}"; shift;;
    -h|--help) usage; exit 0;;
    *) echo "[ERROR] unsupported argument: $1" >&2; usage >&2; exit 1;;
  esac
done

if [[ ! -f "$DEPLOY_KEY" ]]; then
  echo "[ERROR] ssh key not found: $DEPLOY_KEY" >&2; exit 1
fi
chmod 400 "$DEPLOY_KEY" 2>/dev/null || true

SSH_OPTS="-i $DEPLOY_KEY -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15"

# shellcheck disable=SC2086
if ! ssh $SSH_OPTS "$DEPLOY_HOST" 'echo ok' >/dev/null 2>&1; then
  echo "[ERROR] ssh probe to $DEPLOY_HOST failed" >&2; exit 1
fi

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] build $BIN_NAME (release)"
  ( cd "$ROOT_DIR" && cargo build --release --bin "$BIN_NAME" )
fi

if [[ ! -x "$BIN_PATH" ]]; then
  echo "[ERROR] $BIN_PATH missing or not executable" >&2; exit 1
fi

# shellcheck disable=SC2086
ssh $SSH_OPTS "$DEPLOY_HOST" "mkdir -p '$REMOTE_DIR/scripts' '$REMOTE_DIR/data'"

echo "[INFO] rsync $BIN_NAME -> $DEPLOY_HOST:$REMOTE_DIR/"
# Linux 下若 binary 仍有进程持有，cp/rsync 直接覆盖会触发 ETXTBSY；先 unlink 旧路径。
# shellcheck disable=SC2086
ssh $SSH_OPTS "$DEPLOY_HOST" "rm -f '$REMOTE_DIR/$BIN_NAME'"
# shellcheck disable=SC2086
rsync -a --human-readable --info=stats1 \
  -e "ssh $SSH_OPTS" \
  "$BIN_PATH" "$DEPLOY_HOST:$REMOTE_DIR/$BIN_NAME"

echo "[INFO] rsync scripts -> $DEPLOY_HOST:$REMOTE_DIR/scripts/"
# shellcheck disable=SC2086
rsync -a \
  -e "ssh $SSH_OPTS" \
  "$ROOT_DIR/scripts/start_latency_collector.sh" \
  "$ROOT_DIR/scripts/stop_latency_collector.sh" \
  "$DEPLOY_HOST:$REMOTE_DIR/scripts/"

# shellcheck disable=SC2086
ssh $SSH_OPTS "$DEPLOY_HOST" "chmod +x '$REMOTE_DIR/$BIN_NAME' '$REMOTE_DIR/scripts/'*.sh"

echo "[INFO] deployed."
echo "[INFO] start:  ssh -i $DEPLOY_KEY $DEPLOY_HOST 'cd $REMOTE_DIR && ./scripts/start_latency_collector.sh'"
echo "[INFO] stop:   ssh -i $DEPLOY_KEY $DEPLOY_HOST 'cd $REMOTE_DIR && ./scripts/stop_latency_collector.sh'"
echo "[INFO] logs:   ssh -i $DEPLOY_KEY $DEPLOY_HOST 'npx pm2 logs --namespace latency_collector'"
