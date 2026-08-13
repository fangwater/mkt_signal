#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSH_HOST="${SG_INTRA_UPDATE_HOST:-ubuntu@47.131.162.78}"
SSH_KEY="${SG_INTRA_UPDATE_KEY:-$ROOT_DIR/aws-sg.pem}"
ENV_NAME=""
ALL_ENVS=0

usage() {
  cat <<'USAGE'
Usage: scripts/update-sg-intra.sh (--env-name <name> | --all) [options]

Options:
  --host <ssh-host>    SSH target (default: ubuntu@47.131.162.78)
  --key <identity>     SSH private key (default: ./aws-sg.pem)
  --env-name <name>   Exactly bybit-intra-arb01 or bybit-intra-arb02
  --all                Update arb01, then arb02
  -h, --help           Show this help

Live update order:
  1. Build every required release binary locally, including persist_manager
  2. For each selected environment: stop engine, cancel/verify orders, stop stack
  3. Publish the already-built files with SHA-256 verification
  4. Start and health-check the base stack; keep trade_signal stopped

With --all the binaries are built once and the environments are updated
sequentially. If the local build fails, no SSH connection or live action occurs.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) SSH_HOST="${2:-}"; shift 2 ;;
    --key) SSH_KEY="${2:-}"; shift 2 ;;
    --env-name) ENV_NAME="${2:-}"; shift 2 ;;
    --all) ALL_ENVS=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "[ERROR] unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ "$ALL_ENVS" -eq 1 && -n "$ENV_NAME" ]]; then
  echo "[ERROR] use either --env-name or --all, not both" >&2
  exit 2
fi
if [[ "$ALL_ENVS" -eq 0 && -z "$ENV_NAME" ]]; then
  echo "[ERROR] one of --env-name or --all is required" >&2
  usage >&2
  exit 2
fi
if [[ -n "$ENV_NAME" ]]; then
  case "$ENV_NAME" in
    bybit-intra-arb01|bybit-intra-arb02) ;;
    *) echo "[ERROR] unsupported SG Intra environment: $ENV_NAME" >&2; exit 2 ;;
  esac
fi
if [[ -z "$SSH_HOST" || "$SSH_HOST" == -* ]]; then
  echo "[ERROR] invalid SSH host: $SSH_HOST" >&2
  exit 2
fi
if [[ ! -r "$SSH_KEY" ]]; then
  echo "[ERROR] SSH identity is not readable: $SSH_KEY" >&2
  exit 2
fi
SSH_KEY="$(readlink -f -- "$SSH_KEY")"

if [[ "$ALL_ENVS" -eq 1 ]]; then
  ENV_NAMES=(bybit-intra-arb01 bybit-intra-arb02)
else
  ENV_NAMES=("$ENV_NAME")
fi

echo "[PHASE] build local release binaries"
"$ROOT_DIR/scripts/build-sg-intra-binaries.sh"

for env_name in "${ENV_NAMES[@]}"; do
  echo
  echo "[ENV] update begins: host=$SSH_HOST env=$env_name"

  echo "[PHASE] stop remote Intra stack and cancel/verify orders"
  "$ROOT_DIR/scripts/stop-sg-intra.sh" \
    --host "$SSH_HOST" \
    --key "$SSH_KEY" \
    --env-name "$env_name"

  echo "[PHASE] publish prebuilt Intra files"
  "$ROOT_DIR/scripts/publish-sg-intra.sh" \
    --host "$SSH_HOST" \
    --key "$SSH_KEY" \
    --env-name "$env_name" \
    --skip-build

  echo "[PHASE] start remote Intra stack"
  "$ROOT_DIR/scripts/start-sg-intra.sh" \
    --host "$SSH_HOST" \
    --key "$SSH_KEY" \
    --env-name "$env_name"

  echo "[ENV] update complete: host=$SSH_HOST env=$env_name trade_signal_started=false persist_manager=included"
done

echo
echo "[INFO] SG Bybit Intra update complete; environments=${#ENV_NAMES[@]}"
