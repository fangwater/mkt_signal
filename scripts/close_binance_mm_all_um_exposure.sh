#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'EOF'
用法:
  scripts/close_binance_mm_all_um_exposure.sh --env-name binance_mm_alpha [--execute] [--symbols BTCUSDT,ETHUSDT]
  scripts/close_binance_mm_all_um_exposure.sh binance_mm_alpha --execute

说明:
  - 默认 source $HOME/<env-name>/env.sh，再调用 scripts/flatten_binance_std_um.py
  - 本地 source IP 优先读取 <env-dir>/trade_engine.toml 的 local_ips[0]，否则回退到 $HOME/dat_pbs/config/mkt_cfg.yaml
  - 默认 dry-run，仅打印当前 UM 持仓平仓计划；加 --execute 后才真实下市价平仓单
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
  if [[ "$cwd_name" =~ ^binance_mm_[a-z0-9][a-z0-9_-]*$ ]]; then
    ENV_NAME="$cwd_name"
  fi
fi

if [[ -z "$ENV_NAME" && -z "$ENV_DIR" ]]; then
  echo "[ERROR] 需要提供 --env-name（例如 binance_mm_alpha）或 --env-dir" >&2
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

echo "[INFO] env_dir=$ENV_DIR"
echo "[INFO] account_mode=${BINANCE_ACCOUNT_MODE:-<unset>}"
echo "[INFO] local_ip_cfg=${ENV_DIR}/trade_engine.toml"

if [[ "${BINANCE_ACCOUNT_MODE:-}" != "STANDARD" ]]; then
  echo "[ERROR] 该脚本仅用于 Binance STANDARD 账户模式，当前 BINANCE_ACCOUNT_MODE=${BINANCE_ACCOUNT_MODE:-<unset>}" >&2
  exit 1
fi

exec "$PYTHON_BIN" "$SCRIPT_DIR/flatten_binance_std_um.py" --env-dir "$ENV_DIR" "${PASS_ARGS[@]}"
