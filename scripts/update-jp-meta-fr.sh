#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSH_HOST="${FR_UPDATE_HOST:-jp-meta-elvpn}"
ENV_NAME="${FR_UPDATE_ENV:-binance_fr_arb03}"
EXCHANGE="${FR_UPDATE_EXCHANGE:-}"

usage() {
  cat <<'USAGE'
Usage: scripts/update-jp-meta-fr.sh [options]

Options:
  --host <ssh-host>    SSH config host (default: jp-meta-elvpn)
  --env-name <name>   Binance/Gate/Bitget FR environment (default: binance_fr_arb03)
  --exchange <name>   Exchange (binance, gate, or bitget; inferred from env-name)
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

if [[ ! "$ENV_NAME" =~ ^(binance|gate|bitget)_fr_[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "[ERROR] env-name must match binance_fr_<suffix>, gate_fr_<suffix>, or bitget_fr_<suffix>: $ENV_NAME" >&2
  exit 2
fi
INFERRED_EXCHANGE="${BASH_REMATCH[1]}"
EXCHANGE="${EXCHANGE,,}"
if [[ -z "$EXCHANGE" ]]; then
  EXCHANGE="$INFERRED_EXCHANGE"
fi
case "$EXCHANGE" in
  binance|gate|bitget) ;;
  *)
    echo "[ERROR] exchange must be binance, gate, or bitget: $EXCHANGE" >&2
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

echo "[PHASE] stop remote FR stack"
"$ROOT_DIR/scripts/stop-jp-meta-fr.sh" \
  --host "$SSH_HOST" \
  --env-name "$ENV_NAME"

echo "[PHASE] publish prebuilt FR files"
"$ROOT_DIR/scripts/publish-jp-meta-fr.sh" \
  --host "$SSH_HOST" \
  --env-name "$ENV_NAME" \
  --exchange "$EXCHANGE" \
  --skip-build

echo "[PHASE] start remote FR stack"
"$ROOT_DIR/scripts/start-jp-meta-fr.sh" \
  --host "$SSH_HOST" \
  --env-name "$ENV_NAME"

echo "[INFO] FR update complete: host=$SSH_HOST exchange=$EXCHANGE env=$ENV_NAME persist_manager=included"
