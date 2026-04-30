#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'EOF'
用法:
  scripts/close_binance_intra_all_std_orders.sh --env-name binance-intra-trade [--execute] [--symbol BTCUSDT]
  scripts/close_binance_intra_all_std_orders.sh binance-intra-trade --execute

说明:
  - 同所期现 Binance STANDARD 账户的全撤单脚本
  - 默认 source $HOME/<env-name>/env.sh
  - 默认 dry-run，仅打印现货 + 期货撤单计划；加 --execute 后才真实撤单
  - 现货挂单：REST /api/v3/openOrders + DELETE /api/v3/openOrders
  - 期货挂单：复用 scripts/binance_cancel_all_std_um_ws_orders.py
  - 其余参数（如 --symbol / --timeout）会同时透传给两边脚本
EOF
}

ENV_NAME="${ENV_NAME:-}"
ENV_DIR=""
PYTHON_BIN="${PYTHON_BIN:-python3}"
PASS_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-name)
      ENV_NAME="${2:-}"
      [[ -n "$ENV_NAME" ]] || { echo "[ERROR] --env-name 需要一个值" >&2; exit 1; }
      shift 2 ;;
    --env-dir)
      ENV_DIR="${2:-}"
      [[ -n "$ENV_DIR" ]] || { echo "[ERROR] --env-dir 需要一个值" >&2; exit 1; }
      shift 2 ;;
    --python)
      PYTHON_BIN="${2:-}"
      [[ -n "$PYTHON_BIN" ]] || { echo "[ERROR] --python 需要一个值" >&2; exit 1; }
      shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      if [[ -z "$ENV_NAME" && "$1" != --* ]]; then
        ENV_NAME="$1"
      else
        PASS_ARGS+=("$1")
      fi
      shift ;;
  esac
done

if [[ -z "$ENV_NAME" && -z "$ENV_DIR" ]]; then
  echo "[ERROR] 需要提供 --env-name（例如 binance-intra-trade）或 --env-dir" >&2
  usage >&2; exit 1
fi

if [[ -z "$ENV_DIR" ]]; then
  ENV_DIR="$HOME/$ENV_NAME"
fi

ENV_FILE="${ENV_DIR}/env.sh"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "[ERROR] env 文件不存在: $ENV_FILE" >&2; exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

echo "[INFO] env_dir=$ENV_DIR"
echo "[INFO] account_mode=${BINANCE_ACCOUNT_MODE:-<unset>}"

if [[ "${BINANCE_ACCOUNT_MODE:-}" != "STANDARD" ]]; then
  echo "[ERROR] 该脚本仅用于 Binance STANDARD 账户模式，当前 BINANCE_ACCOUNT_MODE=${BINANCE_ACCOUNT_MODE:-<unset>}" >&2
  exit 1
fi

echo "[INFO] === Spot open orders ==="
"$PYTHON_BIN" "$SCRIPT_DIR/binance_cancel_all_std_spot_orders.py" "${PASS_ARGS[@]}"

echo ""
echo "[INFO] === UM futures open orders ==="
exec "$PYTHON_BIN" "$SCRIPT_DIR/binance_cancel_all_std_um_ws_orders.py" "${PASS_ARGS[@]}"
