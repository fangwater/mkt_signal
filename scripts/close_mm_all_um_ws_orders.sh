#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'EOF'
用法:
  scripts/close_mm_all_um_ws_orders.sh --env-name binance_mm_alpha [--execute] [--symbol BTCUSDT]
  scripts/close_mm_all_um_ws_orders.sh binance_mm_alpha --execute
  scripts/close_mm_all_um_ws_orders.sh --env-name okex_mm_alpha [--execute] [--symbol BTCUSDT]
  scripts/close_mm_all_um_ws_orders.sh --env-name bybit_mm_alpha [--execute] [--symbol BTCUSDT]
  scripts/close_mm_all_um_ws_orders.sh --env-name bitget_mm_alpha [--execute] [--symbol BTCUSDT]
  scripts/close_mm_all_um_ws_orders.sh --env-name gate_mm_alpha [--execute] [--symbol BTCUSDT]

说明:
  - 默认 source $HOME/<env-name>/env.sh，再按 env-name 自动调用 Binance/OKX/Bybit/Bitget/Gate 的撤单脚本
  - 本地 source IP 优先读取 <env-dir>/trade_engine.toml 的 local_ips[0]，否则回退到 $HOME/dat_pbs/config/mkt_cfg.yaml
  - 默认 dry-run，仅打印当前挂单撤单计划；加 --execute 后才真实撤单
  - 其余参数会透传给 Python 脚本
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
      if [[ -z "$ENV_NAME" ]]; then
        echo "[ERROR] --env-name 需要一个值" >&2
        exit 1
      fi
      shift 2
      ;;
    --env-dir)
      ENV_DIR="${2:-}"
      if [[ -z "$ENV_DIR" ]]; then
        echo "[ERROR] --env-dir 需要一个值" >&2
        exit 1
      fi
      shift 2
      ;;
    --python)
      PYTHON_BIN="${2:-}"
      if [[ -z "$PYTHON_BIN" ]]; then
        echo "[ERROR] --python 需要一个值" >&2
        exit 1
      fi
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [[ -z "$ENV_NAME" && "$1" != --* ]]; then
        ENV_NAME="$1"
      else
        PASS_ARGS+=("$1")
      fi
      shift
      ;;
  esac
done

if [[ -z "$ENV_NAME" ]]; then
  cwd_name="$(basename "$(pwd)")"
  if [[ "$cwd_name" =~ ^(binance|okex|bybit|bitget|gate)_mm_[a-z0-9][a-z0-9_-]*$ ]]; then
    ENV_NAME="$cwd_name"
  fi
fi

if [[ -z "$ENV_NAME" && -z "$ENV_DIR" ]]; then
  echo "[ERROR] 需要提供 --env-name（例如 binance_mm_alpha / okex_mm_alpha / bybit_mm_alpha / bitget_mm_alpha / gate_mm_alpha）或 --env-dir" >&2
  usage >&2
  exit 1
fi

if [[ -z "$ENV_DIR" ]]; then
  ENV_DIR="$HOME/$ENV_NAME"
fi

ENV_FILE="${ENV_DIR}/env.sh"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "[ERROR] env 文件不存在: $ENV_FILE" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

detect_exchange() {
  local env_tag
  env_tag="${ENV_NAME:-$(basename "$ENV_DIR")}"
  env_tag="${env_tag,,}"
  case "$env_tag" in
    okex_mm_*|*okex*|*okx*)
      echo "okex"
      return 0
      ;;
    binance_mm_*|*binance*)
      echo "binance"
      return 0
      ;;
    bybit_mm_*|*bybit*)
      echo "bybit"
      return 0
      ;;
    bitget_mm_*|*bitget*)
      echo "bitget"
      return 0
      ;;
    gate_mm_*|*gate*)
      echo "gate"
      return 0
      ;;
  esac
  if [[ -n "${OKX_API_KEY:-}" || -n "${OKX_PASSPHRASE:-}" ]]; then
    echo "okex"
    return 0
  fi
  if [[ -n "${BINANCE_API_KEY:-}" || -n "${BINANCE_ACCOUNT_MODE:-}" ]]; then
    echo "binance"
    return 0
  fi
  if [[ -n "${BYBIT_API_KEY:-}" || -n "${BYBIT_API_SECRET:-}" ]]; then
    echo "bybit"
    return 0
  fi
  if [[ -n "${BITGET_API_KEY:-}" || -n "${BITGET_API_PASSPHRASE:-}" ]]; then
    echo "bitget"
    return 0
  fi
  if [[ -n "${GATE_API_KEY:-}" || -n "${GATE_API_SECRET:-}" ]]; then
    echo "gate"
    return 0
  fi
  return 1
}

normalize_okx_inst_id() {
  local raw upper cleaned
  raw="${1:-}"
  upper="$(echo "$raw" | tr '[:lower:]' '[:upper:]')"
  if [[ "$upper" =~ ^[A-Z0-9]+-USDT-SWAP$ ]]; then
    echo "$upper"
    return 0
  fi
  cleaned="$(echo "$upper" | sed 's/@.*$//' | sed 's/[^A-Z0-9]//g')"
  if [[ "$cleaned" =~ ^[A-Z0-9]+USDT$ ]]; then
    echo "${cleaned%USDT}-USDT-SWAP"
    return 0
  fi
  echo "[ERROR] 无法识别 OKX SWAP symbol: $raw" >&2
  exit 1
}

EXCHANGE="$(detect_exchange || true)"
if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 无法从 env-name/env.sh 推断交易所: ENV_NAME=${ENV_NAME:-<unset>} ENV_DIR=${ENV_DIR}" >&2
  exit 1
fi

echo "[INFO] env_dir=$ENV_DIR"
echo "[INFO] exchange=$EXCHANGE"
echo "[INFO] account_mode=${BINANCE_ACCOUNT_MODE:-<unset>}"
echo "[INFO] local_ip_cfg=${ENV_DIR}/trade_engine.toml"

if [[ "$EXCHANGE" == "binance" ]]; then
  if [[ "${BINANCE_ACCOUNT_MODE:-}" != "STANDARD" ]]; then
    echo "[ERROR] Binance 分支仅用于 STANDARD 账户模式，当前 BINANCE_ACCOUNT_MODE=${BINANCE_ACCOUNT_MODE:-<unset>}" >&2
    exit 1
  fi
  exec "$PYTHON_BIN" "$SCRIPT_DIR/binance_cancel_all_std_um_ws_orders.py" --env-dir "$ENV_DIR" "${PASS_ARGS[@]}"
fi

OKX_ARGS=()
idx=0
while [[ $idx -lt ${#PASS_ARGS[@]} ]]; do
  arg="${PASS_ARGS[$idx]}"
  case "$arg" in
    --execute)
      OKX_ARGS+=(--cancel)
      ;;
    --symbol)
      idx=$((idx + 1))
      if [[ $idx -ge ${#PASS_ARGS[@]} ]]; then
        echo "[ERROR] --symbol 需要一个值" >&2
        exit 1
      fi
      OKX_ARGS+=(--inst-id "$(normalize_okx_inst_id "${PASS_ARGS[$idx]}")")
      ;;
    --symbol=*)
      OKX_ARGS+=(--inst-id "$(normalize_okx_inst_id "${arg#--symbol=}")")
      ;;
    *)
      OKX_ARGS+=("$arg")
      ;;
  esac
  idx=$((idx + 1))
done

case "$EXCHANGE" in
  okex)
    exec "$PYTHON_BIN" "$SCRIPT_DIR/okx_swap_open_orders.py" "${OKX_ARGS[@]}"
    ;;
  bybit)
    exec "$PYTHON_BIN" "$SCRIPT_DIR/bybit_cancel_all_um_orders.py" "${PASS_ARGS[@]}"
    ;;
  bitget)
    exec "$PYTHON_BIN" "$SCRIPT_DIR/bitget_cancel_all_um_orders.py" "${PASS_ARGS[@]}"
    ;;
  gate)
    exec "$PYTHON_BIN" "$SCRIPT_DIR/gate_cancel_all_um_orders.py" "${PASS_ARGS[@]}"
    ;;
  *)
    echo "[ERROR] 不支持的交易所: $EXCHANGE" >&2
    exit 1
    ;;
esac
