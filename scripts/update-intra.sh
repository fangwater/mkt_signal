#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/intra_orchestration_lib.sh
source "$ROOT_DIR/scripts/intra_orchestration_lib.sh"

SSH_KEY=""
ENV_NAME=""
ALL_ENVS=0

usage() {
  cat <<'USAGE'
Usage: scripts/update-intra.sh (--env-name <name> | --all) [options]

Options:
  --key <identity>     Optional Bybit/SG SSH identity override
  --env-name <name>   One supported Intra environment
  --all                Update every supported Intra environment
  -h, --help           Show this help

Supported environments:
  bybit-intra-arb01, bybit-intra-arb02 -> SG
  okex-intra-arb01, binance-intra-arb01 -> jp-meta-elvpn

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
  intra_configure_env "$ENV_NAME"
fi
if [[ -n "$SSH_KEY" ]]; then
  if [[ ! -r "$SSH_KEY" ]]; then
    echo "[ERROR] SSH identity is not readable: $SSH_KEY" >&2
    exit 2
  fi
  SSH_KEY="$(readlink -f -- "$SSH_KEY")"
fi
if [[ -n "$ENV_NAME" ]]; then
  intra_validate_explicit_key "$SSH_KEY"
fi

if [[ "$ALL_ENVS" -eq 1 ]]; then
  ENV_NAMES=("${INTRA_ORCHESTRATION_ENVS[@]}")
else
  ENV_NAMES=("$ENV_NAME")
fi

echo "[PHASE] build local release binaries"
"$ROOT_DIR/scripts/build-intra-binaries.sh"

for env_name in "${ENV_NAMES[@]}"; do
  intra_configure_env "$env_name"
  child_key_args=()
  if [[ -n "$SSH_KEY" && "$env_name" == bybit-* ]]; then
    child_key_args=(--key "$SSH_KEY")
  fi
  echo
  echo "[ENV] update begins: host=$INTRA_SSH_HOST exchange=$INTRA_EXCHANGE env=$env_name"

  echo "[PHASE] stop remote Intra stack and cancel/verify orders"
  "$ROOT_DIR/scripts/stop-intra.sh" \
    "${child_key_args[@]}" \
    --env-name "$env_name"

  echo "[PHASE] publish prebuilt Intra files"
  "$ROOT_DIR/scripts/publish-intra.sh" \
    "${child_key_args[@]}" \
    --env-name "$env_name" \
    --skip-build

  echo "[PHASE] start remote Intra stack"
  "$ROOT_DIR/scripts/start-intra.sh" \
    "${child_key_args[@]}" \
    --env-name "$env_name"

  echo "[ENV] update complete: host=$INTRA_SSH_HOST exchange=$INTRA_EXCHANGE env=$env_name trade_signal_started=false persist_manager=included"
done

echo
echo "[INFO] Intra update complete; environments=${#ENV_NAMES[@]}"
