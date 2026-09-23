#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <binance_exec_trade01..04|binance_fr_arb02..04> <binary>" >&2
  exit 2
fi

ENV_NAME="$1"
BIN_NAME="$2"
case "$ENV_NAME:$BIN_NAME" in
  binance_exec_trade0[1-4]:exec-pre-trade|binance_fr_arb0[2-4]:pre_trade|binance_fr_arb0[2-4]:trade_signal|binance_fr_arb0[2-4]:fr_signal_dashboard) ;;
  *) echo "[ERROR] unsupported environment/binary pair" >&2; exit 2 ;;
esac

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HOST="jp-meta-elvpn"
REMOTE_DIR="/home/ubuntu/$ENV_NAME"
LOCAL_BIN="$REPO_DIR/target/release/$BIN_NAME"
REMOTE_NEXT="$REMOTE_DIR/$BIN_NAME.next.proxy"
REMOTE_BACKUP="$REMOTE_DIR/$BIN_NAME.rollback.proxy"

[[ -x "$LOCAL_BIN" ]] || { echo "[ERROR] missing release binary: $LOCAL_BIN" >&2; exit 1; }
ssh "$HOST" "test -f '$REMOTE_DIR/env.sh' && test -x '$REMOTE_DIR/$BIN_NAME' && test ! -e '$REMOTE_NEXT' && test ! -e '$REMOTE_BACKUP'"
scp "$LOCAL_BIN" "$HOST:$REMOTE_NEXT"
scp "$REPO_DIR/scripts/binance_futures_ipc_proxy/enable-env.patch" \
  "$HOST:/home/ubuntu/binance_futures_ipc_proxy/enable-env.patch"
LOCAL_SHA="$(sha256sum "$LOCAL_BIN" | cut -d' ' -f1)"
REMOTE_SHA="$(ssh "$HOST" "sha256sum '$REMOTE_NEXT'" | cut -d' ' -f1)"
[[ "$LOCAL_SHA" = "$REMOTE_SHA" ]] || { echo "[ERROR] checksum mismatch" >&2; exit 1; }

ssh "$HOST" "cd '$REMOTE_DIR' && cp -p -- '$BIN_NAME' '$BIN_NAME.rollback.proxy' && mv -- '$BIN_NAME.next.proxy' '$BIN_NAME' && if ! grep -q '^export BINANCE_FUTURES_IPC_PROXY=1$' env.sh; then patch --batch -p0 < /home/ubuntu/binance_futures_ipc_proxy/enable-env.patch; fi && grep -q '^export BINANCE_FUTURES_IPC_PROXY=1$' env.sh"
echo "[INFO] deployed $ENV_NAME/$BIN_NAME sha256=$LOCAL_SHA"
