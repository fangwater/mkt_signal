#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'EOF'
用法:
  scripts/close_intra_all_orders.sh --env-name binance-intra-trade [--execute] [--symbol BTCUSDT]
  scripts/close_intra_all_orders.sh --env-name okex-intra-trade    [--execute] [--symbol BTCUSDT]
  scripts/close_intra_all_orders.sh --env-name bybit-intra-trade   [--execute] [--symbol BTCUSDT]
  scripts/close_intra_all_orders.sh binance-intra-trade --execute

说明:
  - 同所期现 intra 环境的全撤单脚本（同时撤现货 + 期货）
  - 默认 source $HOME/<env-name>/env.sh，按 env-name 自动调用 binance/okex/bybit 的撤单脚本
  - 默认 dry-run，仅打印撤单计划；加 --execute 后才真实撤单
  - 现货撤单：
      Binance STANDARD : binance_cancel_all_std_spot_orders.py (REST /api/v3/openOrders)
      OKEx margin      : okx_cancel_all_margin_orders.py
      Bybit margin/spot: bybit_cancel_all_spot_orders.py (category=spot)
  - 期货撤单：
      Binance STANDARD : binance_cancel_all_std_um_ws_orders.py
      OKEx swap        : okx_swap_open_orders.py --cancel --real
      Bybit linear     : bybit_cancel_all_um_orders.py
  - 其余参数（如 --symbol / --timeout）会透传给底层 python 脚本
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

if [[ -z "$ENV_NAME" ]]; then
  cwd_name="$(basename "$(pwd)")"
  if [[ "$cwd_name" =~ ^(binance|okex|bybit)-intra-[a-z0-9][a-z0-9_-]*$ ]]; then
    ENV_NAME="$cwd_name"
  fi
fi

if [[ -z "$ENV_NAME" && -z "$ENV_DIR" ]]; then
  echo "[ERROR] 需要提供 --env-name（例如 binance-intra-trade / okex-intra-trade / bybit-intra-trade）或 --env-dir" >&2
  usage >&2; exit 1
fi

if [[ -z "$ENV_DIR" ]]; then
  ENV_DIR="$HOME/$ENV_NAME"
fi

ENV_FILE="${ENV_DIR}/env.sh"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "[ERROR] env 文件不存在: $ENV_FILE" >&2; exit 1
fi

if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="$(basename "$ENV_DIR")"
fi

if [[ ! "$ENV_NAME" =~ ^([a-z0-9]+)-intra-([a-z0-9][a-z0-9_-]*)$ ]]; then
  echo "[ERROR] env-name 必须匹配 <exchange>-intra-<suffix>，当前: $ENV_NAME" >&2
  exit 1
fi
EXCHANGE="${BASH_REMATCH[1]}"
SUFFIX="${BASH_REMATCH[2]}"

# shellcheck disable=SC1090
source "$ENV_FILE"

echo "[INFO] env_dir=$ENV_DIR"
echo "[INFO] exchange=$EXCHANGE suffix=$SUFFIX"
echo "[INFO] account_mode=${BINANCE_ACCOUNT_MODE:-<unset>}"

normalize_okx_inst_id() {
  local raw upper cleaned
  raw="${1:-}"
  upper="$(echo "$raw" | tr '[:lower:]' '[:upper:]')"
  if [[ "$upper" =~ ^[A-Z0-9]+-USDT-SWAP$ ]]; then
    echo "$upper"; return 0
  fi
  cleaned="$(echo "$upper" | sed 's/@.*$//' | sed 's/[^A-Z0-9]//g')"
  if [[ "$cleaned" =~ ^[A-Z0-9]+USDT$ ]]; then
    echo "${cleaned%USDT}-USDT-SWAP"; return 0
  fi
  echo "[ERROR] 无法识别 OKX SWAP symbol: $raw" >&2
  exit 1
}

normalize_okx_margin_id() {
  local raw upper cleaned
  raw="${1:-}"
  upper="$(echo "$raw" | tr '[:lower:]' '[:upper:]')"
  upper="${upper%-SWAP}"
  if [[ "$upper" =~ ^[A-Z0-9]+-USDT$ ]]; then
    echo "$upper"; return 0
  fi
  cleaned="$(echo "$upper" | sed 's/@.*$//' | sed 's/[^A-Z0-9]//g')"
  if [[ "$cleaned" =~ ^[A-Z0-9]+USDT$ ]]; then
    echo "${cleaned%USDT}-USDT"; return 0
  fi
  echo "[ERROR] 无法识别 OKX margin symbol: $raw" >&2
  exit 1
}

run() {
  echo "[RUN] $*"
  "$@"
}

case "$EXCHANGE" in
  binance)
    if [[ "${BINANCE_ACCOUNT_MODE:-}" != "STANDARD" ]]; then
      echo "[ERROR] Binance intra 仅支持 STANDARD 账户模式，当前 BINANCE_ACCOUNT_MODE=${BINANCE_ACCOUNT_MODE:-<unset>}" >&2
      exit 1
    fi
    echo "[INFO] === Spot open orders ==="
    run "$PYTHON_BIN" "$SCRIPT_DIR/binance_cancel_all_std_spot_orders.py" "${PASS_ARGS[@]}"
    echo ""
    echo "[INFO] === UM futures open orders ==="
    exec "$PYTHON_BIN" "$SCRIPT_DIR/binance_cancel_all_std_um_ws_orders.py" "${PASS_ARGS[@]}"
    ;;
  okex)
    OKX_SPOT_ARGS=(--real)
    OKX_SWAP_ARGS=(--real)
    idx=0
    while [[ $idx -lt ${#PASS_ARGS[@]} ]]; do
      arg="${PASS_ARGS[$idx]}"
      case "$arg" in
        --execute)
          OKX_SPOT_ARGS+=(--execute)
          OKX_SWAP_ARGS+=(--cancel)
          ;;
        --symbol)
          idx=$((idx + 1))
          if [[ $idx -ge ${#PASS_ARGS[@]} ]]; then
            echo "[ERROR] --symbol 需要一个值" >&2; exit 1
          fi
          OKX_SPOT_ARGS+=(--inst-id "$(normalize_okx_margin_id "${PASS_ARGS[$idx]}")")
          OKX_SWAP_ARGS+=(--inst-id "$(normalize_okx_inst_id "${PASS_ARGS[$idx]}")")
          ;;
        --symbol=*)
          OKX_SPOT_ARGS+=(--inst-id "$(normalize_okx_margin_id "${arg#--symbol=}")")
          OKX_SWAP_ARGS+=(--inst-id "$(normalize_okx_inst_id "${arg#--symbol=}")")
          ;;
        *)
          OKX_SPOT_ARGS+=("$arg")
          OKX_SWAP_ARGS+=("$arg")
          ;;
      esac
      idx=$((idx + 1))
    done
    echo "[INFO] === OKX MARGIN open orders ==="
    run "$PYTHON_BIN" "$SCRIPT_DIR/okx_cancel_all_margin_orders.py" "${OKX_SPOT_ARGS[@]}"
    echo ""
    echo "[INFO] === OKX SWAP open orders ==="
    exec "$PYTHON_BIN" "$SCRIPT_DIR/okx_swap_open_orders.py" "${OKX_SWAP_ARGS[@]}"
    ;;
  bybit)
    echo "[INFO] === Bybit SPOT open orders ==="
    run "$PYTHON_BIN" "$SCRIPT_DIR/bybit_cancel_all_spot_orders.py" "${PASS_ARGS[@]}"
    echo ""
    echo "[INFO] === Bybit linear futures open orders ==="
    exec "$PYTHON_BIN" "$SCRIPT_DIR/bybit_cancel_all_um_orders.py" "${PASS_ARGS[@]}"
    ;;
  *)
    echo "[ERROR] 不支持的交易所: $EXCHANGE (env_name=$ENV_NAME)" >&2
    exit 1 ;;
esac
