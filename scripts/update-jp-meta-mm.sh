#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSH_HOST="${MM_UPDATE_HOST:-jp-meta-elvpn}"
ENV_NAME="${MM_UPDATE_ENV:-}"
EXCHANGE="${MM_UPDATE_EXCHANGE:-}"

usage() {
  cat <<'USAGE'
Usage: scripts/update-jp-meta-mm.sh --env-name <name> [options]

Options:
  --host <ssh-host>    SSH config host (default: jp-meta-elvpn)
  --env-name <name>   Binance/OKX MM environment (required)
  --exchange <name>   Exchange (binance, okex, or okx; inferred from env-name)
  -h, --help          Show this help

Live update order:
  1. Build every required release binary locally, including persist_manager
  2. Stop the remote stack and cancel/verify open orders
  3. Publish the already-built files
  4. Start and health-check the remote stack

If the local build fails, no remote process or order is changed.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      SSH_HOST="${2:-}"
      shift 2
      ;;
    --env-name)
      ENV_NAME="${2:-}"
      shift 2
      ;;
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! "$ENV_NAME" =~ ^(binance|okex)_mm_[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "[ERROR] env-name must match binance_mm_<suffix> or okex_mm_<suffix>: $ENV_NAME" >&2
  exit 2
fi
INFERRED_EXCHANGE="${BASH_REMATCH[1]}"
EXCHANGE="${EXCHANGE,,}"
if [[ "$EXCHANGE" == "okx" ]]; then
  EXCHANGE="okex"
fi
if [[ -z "$EXCHANGE" ]]; then
  EXCHANGE="$INFERRED_EXCHANGE"
fi
case "$EXCHANGE" in
  binance|okex) ;;
  *)
    echo "[ERROR] exchange must be binance, okex, or okx: $EXCHANGE" >&2
    exit 2
    ;;
esac
if [[ "$EXCHANGE" != "$INFERRED_EXCHANGE" ]]; then
  echo "[ERROR] exchange/env-name mismatch: exchange=$EXCHANGE env-name=$ENV_NAME" >&2
  exit 2
fi
if [[ -z "$SSH_HOST" || "$SSH_HOST" == -* ]]; then
  echo "[ERROR] invalid SSH host: $SSH_HOST" >&2
  exit 2
fi

echo "[PHASE] build local release binaries"
"$ROOT_DIR/scripts/build-jp-meta-binaries.sh" --exchange "$EXCHANGE"

echo "[PHASE] stop remote MM stack"
"$ROOT_DIR/scripts/stop-jp-meta-mm.sh" \
  --host "$SSH_HOST" \
  --env-name "$ENV_NAME"

echo "[PHASE] publish prebuilt MM files"
"$ROOT_DIR/scripts/publish-jp-meta-mm.sh" \
  --host "$SSH_HOST" \
  --env-name "$ENV_NAME" \
  --exchange "$EXCHANGE" \
  --skip-build

echo "[PHASE] start remote MM stack"
"$ROOT_DIR/scripts/start-jp-meta-mm.sh" \
  --host "$SSH_HOST" \
  --env-name "$ENV_NAME"

echo "[INFO] MM update complete: host=$SSH_HOST exchange=$EXCHANGE env=$ENV_NAME persist_manager=included"
