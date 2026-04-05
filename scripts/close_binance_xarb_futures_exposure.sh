#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
用法:
  scripts/close_binance_xarb_futures_exposure.sh --env-name binance-binance-xarb-trade01 [--execute] [--min-net-usdt 10]
  scripts/close_binance_xarb_futures_exposure.sh binance-binance-xarb-trade01 --execute

说明:
  - 默认 source $HOME/<env-name>/env.sh
  - 默认 dry-run，仅打印 futures 平敞口计划；加 --execute 后才真实下单
  - 只通过 Binance UM futures 平 pre_trade_exposure，不碰现货
  - 默认跳过 BNB；可通过 --skip-assets 覆盖
  - suffix 会从 <open>-<hedge>-xarb-<suffix> 自动提取
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
      shift 2
      ;;
    --env-dir)
      ENV_DIR="${2:-}"
      [[ -n "$ENV_DIR" ]] || { echo "[ERROR] --env-dir 需要一个值" >&2; exit 1; }
      shift 2
      ;;
    --python)
      PYTHON_BIN="${2:-}"
      [[ -n "$PYTHON_BIN" ]] || { echo "[ERROR] --python 需要一个值" >&2; exit 1; }
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

if [[ -z "$ENV_NAME" && -z "$ENV_DIR" ]]; then
  echo "[ERROR] 需要提供 --env-name（例如 binance-binance-xarb-trade01）或 --env-dir" >&2
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

if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="$(basename "$ENV_DIR")"
fi

if [[ ! "$ENV_NAME" =~ ^[a-z0-9]+-[a-z0-9]+-xarb-([a-z0-9][a-z0-9_-]*)$ ]]; then
  echo "[ERROR] env-name 必须匹配 <open>-<hedge>-xarb-<suffix>，当前: $ENV_NAME" >&2
  exit 1
fi
SUFFIX="${BASH_REMATCH[1]}"

# shellcheck disable=SC1090
source "$ENV_FILE"

echo "[INFO] env_dir=$ENV_DIR"
echo "[INFO] suffix=$SUFFIX"
echo "[INFO] account_mode=${BINANCE_ACCOUNT_MODE:-<unset>}"

if [[ "${BINANCE_ACCOUNT_MODE:-}" != "STANDARD" ]]; then
  echo "[ERROR] 该脚本仅用于 Binance STANDARD 账户模式，当前 BINANCE_ACCOUNT_MODE=${BINANCE_ACCOUNT_MODE:-<unset>}" >&2
  exit 1
fi

HAS_SKIP_ASSETS="0"
for arg in "${PASS_ARGS[@]}"; do
  if [[ "$arg" == "--skip-assets" || "$arg" == --skip-assets=* ]]; then
    HAS_SKIP_ASSETS="1"
    break
  fi
done

ARGS=(--suffix "$SUFFIX")
if [[ "$HAS_SKIP_ASSETS" != "1" ]]; then
  ARGS+=(--skip-assets "BNB")
fi
ARGS+=("${PASS_ARGS[@]}")

exec "$PYTHON_BIN" "$ROOT_DIR/xarb_scripts/flatten_xarb_futures_exposure.py" "${ARGS[@]}"
