#!/usr/bin/env bash
# sync_bybit_to_sg.sh
#
# 把 dev 机 $HOME 下 bybit 已 staging 的组件目录推送到 SG。
# 利用 fr_remote_deploy.sh 现有的 fr_remote_sync_path（同样的 exclude:
# env.sh / data / *.rocksdb / logs / pids / __pycache__）。
#
# 典型流程:
#   1) bash scripts/dat_pbs/deploy_dat_pbs.sh --exchange bybit
#      bash scripts/deploy_depth_pub.sh --exchange bybit
#      bash scripts/spread_pbs/deploy_spread_pbs.sh --exchange bybit
#      bash scripts/deploy_trade_flow_feature_pub.sh --exchange bybit
#   2) bash scripts/sync_bybit_to_sg.sh
#
# 可选附加:
#   bash scripts/deploy_intra_bybit.sh arb01
#   bash scripts/sync_bybit_to_sg.sh --include-intra arb01
#
#   bash scripts/deploy_mm_bybit.sh beta
#   bash scripts/sync_bybit_to_sg.sh --include-mm beta

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# SG 默认目标 (EIP 稳定),可以用环境变量覆盖。
export FR_DEPLOY_HOST="${FR_DEPLOY_HOST:-ubuntu@47.131.162.78}"
export FR_DEPLOY_KEY="${FR_DEPLOY_KEY:-$ROOT_DIR/aws-sg.pem}"
export FR_REMOTE_HOME="${FR_REMOTE_HOME:-/home/ubuntu}"

# shellcheck source=lib/fr_remote_deploy.sh
source "$ROOT_DIR/scripts/lib/fr_remote_deploy.sh"

usage() {
  cat <<'USAGE'
Usage:
  bash scripts/sync_bybit_to_sg.sh [--include-intra <suffix>] [--include-mm <suffix>]

默认同步:
  ~/dat_pbs/{bybit-futures,bybit-margin,config}
  ~/depth_pub/{bybit-futures,bybit-margin,config}
  ~/spread_pbs/{bybit-futures,bybit-margin,config}
  ~/trade_flow_feature/{bybit-futures,bybit-margin}

可选:
  --include-intra <suffix>  附加同步 ~/bybit-intra-<suffix>
  --include-mm    <suffix>  附加同步 ~/bybit_mm_<suffix>

环境变量:
  FR_DEPLOY_HOST  默认 ubuntu@47.131.162.78 (SG EIP)
  FR_DEPLOY_KEY   默认 $ROOT_DIR/aws-sg.pem
  FR_REMOTE_HOME  默认 /home/ubuntu

行为:
  - rsync 一致使用 fr_remote_sync_path,exclude env.sh / data / logs (SG 上的
    env.sh 和持久数据不被覆盖)
  - 本地目录不存在时跳过 (尚未 stage)
USAGE
}

INCLUDE_INTRA=""
INCLUDE_MM=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --include-intra) INCLUDE_INTRA="${2:-}"; shift 2 ;;
    --include-mm)    INCLUDE_MM="${2:-}"; shift 2 ;;
    -h|--help)       usage; exit 0 ;;
    *) echo "[ERROR] 未知参数: $1" >&2; usage >&2; exit 1 ;;
  esac
done

fr_remote_init_ssh "$ROOT_DIR"

PATHS=(
  "dat_pbs/bybit-futures"
  "dat_pbs/bybit-margin"
  "dat_pbs/config"
  "depth_pub/bybit-futures"
  "depth_pub/bybit-margin"
  "depth_pub/config"
  "spread_pbs/bybit-futures"
  "spread_pbs/bybit-margin"
  "spread_pbs/config"
  "trade_flow_feature/bybit-futures"
  "trade_flow_feature/bybit-margin"
)
if [[ -d "$HOME/trade_flow_feature/config" ]]; then
  PATHS+=("trade_flow_feature/config")
fi
if [[ -n "$INCLUDE_INTRA" ]]; then
  PATHS+=("bybit-intra-${INCLUDE_INTRA}")
fi
if [[ -n "$INCLUDE_MM" ]]; then
  PATHS+=("bybit_mm_${INCLUDE_MM}")
fi

SYNCED=()
SKIPPED=()
for p in "${PATHS[@]}"; do
  if [[ ! -d "$HOME/$p" ]]; then
    echo "[SKIP] $HOME/$p not present"
    SKIPPED+=("$p")
    continue
  fi
  fr_remote_sync_path "$p"
  SYNCED+=("$p")
done

echo
echo "[INFO] bybit → SG sync complete"
echo "[INFO] target: $FR_DEPLOY_HOST:$FR_REMOTE_HOME"
echo "[INFO] synced (${#SYNCED[@]}):"
for p in "${SYNCED[@]}"; do echo "  + $p"; done
if [[ ${#SKIPPED[@]} -gt 0 ]]; then
  echo "[INFO] skipped (${#SKIPPED[@]} not staged locally):"
  for p in "${SKIPPED[@]}"; do echo "  - $p"; done
fi
