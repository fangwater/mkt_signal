#!/usr/bin/env bash

_INTRA_ORCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib/ssh_remote_bash.sh
source "$_INTRA_ORCH_DIR/lib/ssh_remote_bash.sh"

# Shared target metadata for the remote Intra orchestration entrypoints.
INTRA_ORCHESTRATION_ENVS=(
  bybit-intra-arb01
  bybit-intra-arb02
  okex-intra-arb01
  binance-intra-arb01
)

intra_supported_envs_csv() {
  local IFS=","
  printf '%s\n' "${INTRA_ORCHESTRATION_ENVS[*]}"
}

intra_configure_env() {
  INTRA_ENV_NAME="${1:-}"
  case "$INTRA_ENV_NAME" in
    bybit-intra-arb01)
      INTRA_EXCHANGE="bybit"
      INTRA_SSH_HOST="ubuntu@47.131.162.78"
      INTRA_CONFIG_PORT="19191"
      INTRA_VIZ_PORT="10174"
      ;;
    bybit-intra-arb02)
      INTRA_EXCHANGE="bybit"
      INTRA_SSH_HOST="ubuntu@47.131.162.78"
      INTRA_CONFIG_PORT="19192"
      INTRA_VIZ_PORT="10175"
      ;;
    okex-intra-arb01)
      INTRA_EXCHANGE="okex"
      INTRA_SSH_HOST="jp-meta-elvpn"
      INTRA_CONFIG_PORT="19181"
      INTRA_VIZ_PORT="10171"
      ;;
    binance-intra-arb01)
      INTRA_EXCHANGE="binance"
      INTRA_SSH_HOST="jp-meta-elvpn"
      INTRA_CONFIG_PORT="19171"
      INTRA_VIZ_PORT="10180"
      ;;
    *)
      echo "[ERROR] unsupported Intra environment: ${INTRA_ENV_NAME:-<empty>}" >&2
      echo "[ERROR] supported environments: $(intra_supported_envs_csv)" >&2
      return 2
      ;;
  esac

  INTRA_ACCOUNT_MONITOR_BIN="${INTRA_EXCHANGE}_account_monitor"
  INTRA_ACCOUNT_MONITOR_DEST="account_monitor_${INTRA_EXCHANGE}"
}

intra_configure_transport() {
  local root_dir="$1"
  local explicit_key="${2:-}"
  local ssh_key="$explicit_key"

  if [[ -n "$ssh_key" && "$INTRA_EXCHANGE" != "bybit" ]]; then
    echo "[ERROR] --key is only valid for Bybit/SG; $INTRA_ENV_NAME uses jp-meta-elvpn SSH config" >&2
    return 2
  fi
  if [[ -z "$ssh_key" && "$INTRA_EXCHANGE" == "bybit" ]]; then
    ssh_key="$root_dir/aws-sg.pem"
  fi
  if [[ -n "$ssh_key" ]]; then
    if [[ ! -r "$ssh_key" ]]; then
      echo "[ERROR] SSH identity is not readable: $ssh_key" >&2
      return 2
    fi
    ssh_key="$(readlink -f -- "$ssh_key")"
  fi

  INTRA_SSH_KEY="$ssh_key"
  INTRA_SSH=(ssh -o BatchMode=yes -o ConnectTimeout=15)
  INTRA_SCP=(scp -o BatchMode=yes -o ConnectTimeout=15)
  if [[ -n "$INTRA_SSH_KEY" ]]; then
    INTRA_SSH+=(-i "$INTRA_SSH_KEY")
    INTRA_SCP+=(-i "$INTRA_SSH_KEY")
  fi
}

intra_validate_explicit_key() {
  local explicit_key="${1:-}"
  if [[ -n "$explicit_key" && "$INTRA_EXCHANGE" != "bybit" ]]; then
    echo "[ERROR] --key is only valid for Bybit/SG; $INTRA_ENV_NAME uses jp-meta-elvpn SSH config" >&2
    return 2
  fi
}

# Intra wrapper around ssh_remote_bash. Requires intra_configure_transport.
#
# Usage:
#   intra_remote_bash arg1 arg2 <<'EOF'
#     echo "$1"
#   EOF
intra_remote_bash() {
  if [[ ${#INTRA_SSH[@]} -eq 0 || -z "${INTRA_SSH_HOST:-}" ]]; then
    echo "[ERROR] intra_remote_bash requires intra_configure_transport first" >&2
    return 2
  fi
  ssh_remote_bash INTRA_SSH "$INTRA_SSH_HOST" "$@"
}
