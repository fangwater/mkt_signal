#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
  cat <<'EOF'
用法:
  scripts/close_intra_full_exit.sh --env-name okex-intra-arb01 [--execute] [--symbol BTC]
  scripts/close_intra_full_exit.sh --env-name bybit-intra-arb01 [--execute] [--mode align-futures-to-spot]
  scripts/close_intra_full_exit.sh okex-intra-arb01 --execute --skip-assets BNB,SOL
  scripts/close_intra_full_exit.sh okex-intra-arb01 --execute --min-net-usdt 5

说明:
  - 同所期现 intra 环境的「彻底清仓退场」脚本：撤所有挂单 → 现货 margin 平到 0 → SWAP 合约平到 0
  - 与 close_intra_all_orders.sh / close_intra_futures_exposure.sh 区别：
      * close_intra_all_orders.sh        只撤单，不动头寸
      * close_intra_futures_exposure.sh  只用合约把 net_qty 打到 0，保留现货底仓
      * close_intra_full_exit.sh         双腿都打到 0，账户回到无敞口
  - 默认 source $HOME/<env-name>/env.sh
  - 默认 dry-run，仅打印动作计划；加 --execute 才真正撤单 + 下单
  - 数据来源：本机 dashboard /intra/<exchange>-intra-<suffix>/snapshot 的 pre_trade_exposure
  - 透传给底层 python 的常用参数：
      --execute / --symbol BTC / --skip-assets BNB,SOL / --min-net-usdt 5
      --skip-cancel / --simulate / --td-mode cross|isolated / --timeout N

支持的 exchange:
  - okex   ✓
  - gate   ✓
  - bitget ✓
  - bybit  ✓
  - binance ✗ (尚未实现 — 现货侧批量市价清仓需要新写)
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
  if [[ "$cwd_name" =~ ^(binance|okex|bybit|gate|bitget)-intra-[a-z0-9][a-z0-9_-]*$ ]]; then
    ENV_NAME="$cwd_name"
  fi
fi

if [[ -z "$ENV_NAME" && -z "$ENV_DIR" ]]; then
  echo "[ERROR] 需要提供 --env-name（例如 okex-intra-arb01）或 --env-dir" >&2
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

case "$EXCHANGE" in
  okex)
    exec "$PYTHON_BIN" "$ROOT_DIR/intra_scripts/full_exit_intra_okx.py" --suffix "$SUFFIX" "${PASS_ARGS[@]}"
    ;;
  gate)
    exec "$PYTHON_BIN" "$ROOT_DIR/intra_scripts/full_exit_intra_gate.py" --suffix "$SUFFIX" "${PASS_ARGS[@]}"
    ;;
  bitget)
    exec "$PYTHON_BIN" "$ROOT_DIR/intra_scripts/full_exit_intra_bitget.py" --suffix "$SUFFIX" "${PASS_ARGS[@]}"
    ;;
  bybit)
    exec "$PYTHON_BIN" "$ROOT_DIR/intra_scripts/full_exit_intra_bybit.py" --suffix "$SUFFIX" "${PASS_ARGS[@]}"
    ;;
  binance)
    echo "[ERROR] full-exit 尚未为 $EXCHANGE 实现 (env_name=$ENV_NAME)" >&2
    echo "       现有可用脚本：close_intra_all_orders.sh + close_intra_futures_exposure.sh" >&2
    exit 2 ;;
  *)
    echo "[ERROR] 不支持的交易所: $EXCHANGE (env_name=$ENV_NAME)" >&2
    exit 1 ;;
esac
