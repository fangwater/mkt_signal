#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

BIN_CANDIDATES=(
  "${BASE_DIR}/pre_trade"
  "${SCRIPT_DIR}/pre_trade"
  "${BASE_DIR}/target/release/pre_trade"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] pre_trade binary not found. Build/deploy first."
  exit 1
fi

usage() {
  cat <<'EOF'
用法: xarb_scripts/start_xarb_pre_trade.sh [--resample-suffix <suffix>] [--open-venue <okex-futures>] [--hedge-venue <binance-futures>]

说明:
  - 默认从 env.sh 读取 OPEN_VENUE/HEDGE_VENUE/IPC_NAMESPACE；若未提供则尝试从目录名推断 open/hedge，并默认 futures-only。
  - 将以 PM2 启动 1 个进程：
      xarb_pt_<open>_<hedge> -> pre_trade --open-venue ... --hedge-venue ...
  - 建议先生成并配置 env.sh（包含 IPC_NAMESPACE/凭证等）：
      scripts/deploy_setup_env_xarb.sh --env-name <open>-<hedge>-xarb-... --open-venue ... --hedge-venue ...

示例:
  ./xarb_scripts/start_xarb_pre_trade.sh
  ./xarb_scripts/start_xarb_pre_trade.sh --resample-suffix okex_binance_xarb
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

RESAMPLE_SUFFIX=""
CLI_OPEN_VENUE=""
CLI_HEDGE_VENUE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --resample-suffix)
      RESAMPLE_SUFFIX="${2:-}"
      shift 2
      ;;
    --open-venue)
      CLI_OPEN_VENUE="${2:-}"
      shift 2
      ;;
    --hedge-venue)
      CLI_HEDGE_VENUE="${2:-}"
      shift 2
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
else
  echo "[WARN] 未找到 env.sh：${ENV_FILE}"
  echo "[WARN] 若 IPC_NAMESPACE 未设置，pre_trade 会直接 panic；建议先生成并配置 env.sh。"
fi

normalize_venue() {
  echo "${1,,}"
}

ensure_futures_venue() {
  local v
  v="$(normalize_venue "$1")"
  if [[ -z "$v" || "$v" != *-futures ]]; then
    echo "[ERROR] xarb 只支持 futures：venue 必须以 -futures 结尾: $1"
    exit 1
  fi
  echo "$v"
}

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"

infer_pair_from_dir() {
  local name="$1"
  local open_ex=""
  local hedge_ex=""
  if [[ "$name" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]xarb([_-].*)?$ ]]; then
    open_ex="${BASH_REMATCH[1]}"
    hedge_ex="${BASH_REMATCH[2]}"
  fi
  if [[ "$open_ex" == "okx" ]]; then
    open_ex="okex"
  fi
  if [[ "$hedge_ex" == "okx" ]]; then
    hedge_ex="okex"
  fi
  if [[ -n "$open_ex" && -n "$hedge_ex" ]]; then
    echo "${open_ex},${hedge_ex}"
  fi
}

OPEN_VENUE="${CLI_OPEN_VENUE:-${OPEN_VENUE:-}}"
HEDGE_VENUE="${CLI_HEDGE_VENUE:-${HEDGE_VENUE:-}}"

if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  if inferred="$(infer_pair_from_dir "$dir_lc")" && [[ -n "$inferred" ]]; then
    OPEN_VENUE="${OPEN_VENUE:-${inferred%%,*}-futures}"
    HEDGE_VENUE="${HEDGE_VENUE:-${inferred##*,}-futures}"
  fi
fi

if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  echo "[ERROR] 缺少 open/hedge venue，且无法从目录名推断 (dir=$dir_name)"
  usage
  exit 1
fi

OPEN_VENUE="$(ensure_futures_venue "$OPEN_VENUE")"
HEDGE_VENUE="$(ensure_futures_venue "$HEDGE_VENUE")"
if [[ "$OPEN_VENUE" == "$HEDGE_VENUE" ]]; then
  echo "[ERROR] xarb 需要跨所：open=$OPEN_VENUE hedge=$HEDGE_VENUE"
  exit 1
fi

if [[ -z "${IPC_NAMESPACE:-}" ]]; then
  echo "[ERROR] IPC_NAMESPACE 未设置（env.sh 缺失或未 source）。"
  echo "[ERROR] 请先执行：scripts/deploy_setup_env_xarb.sh --env-name ${dir_name} --open-venue ${OPEN_VENUE} --hedge-venue ${HEDGE_VENUE}"
  exit 1
fi

OPEN_EXCHANGE="${OPEN_VENUE%%-*}"
HEDGE_EXCHANGE="${HEDGE_VENUE%%-*}"
PM2_NAME="xarb_pt_${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}"

args=(--open-venue "$OPEN_VENUE" --hedge-venue "$HEDGE_VENUE")
if [[ -n "$RESAMPLE_SUFFIX" ]]; then
  args+=(--resample-suffix "$RESAMPLE_SUFFIX")
fi

echo "[INFO] Restarting $PM2_NAME (open=$OPEN_VENUE hedge=$HEDGE_VENUE namespace=$IPC_NAMESPACE)"
npx pm2 delete "$PM2_NAME" >/dev/null 2>&1 || true

npx pm2 start "$BIN_PATH" \
  --name "$PM2_NAME" \
  -- \
  "${args[@]}"

echo "[INFO] Started $PM2_NAME. Logs: npx pm2 logs $PM2_NAME"
