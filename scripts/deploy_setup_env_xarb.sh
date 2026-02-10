#!/usr/bin/env bash
set -euo pipefail

# Optional remote mode (runs on remote host via ssh)
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/deploy_xarb_lib.sh
source "$ROOT_DIR/scripts/deploy_xarb_lib.sh"
xarb_preparse_remote_args "$@"
set -- "${XARB_FORWARD_ARGS[@]}"
if [[ -n "${XARB_REMOTE_HOST}" ]]; then
  xarb_remote_maybe_sync_repo "$ROOT_DIR"
  xarb_remote_exec "scripts/$(basename "${BASH_SOURCE[0]}")" "$@"
  exit $?
fi

# 生成 xarb 环境变量脚本（futures-only，跨所）
#
# 目录约定:
#   $HOME/<open>-<hedge>-xarb-<trade|test>/
#   例如: $HOME/okex-binance-xarb-trade/
#
# 说明:
#   - 只需要提供两个 venue（必须为 *-futures），exchange 会从 venue 前缀推断
#   - IPC_NAMESPACE 会默认按 open/hedge/env_type 生成，避免与 fr 等环境冲突
#   - env.sh 内会按 open/hedge 自动生成所需的凭证变量占位：
#       - okex: OKX_API_KEY / OKX_API_SECRET / OKX_PASSPHRASE（3 个）
#       - binance: BINANCE_API_KEY / BINANCE_API_SECRET（2 个）
#     例如 okex + binance 跨所合计 5 个

usage() {
  cat <<'EOF'
用法: scripts/deploy_setup_env_xarb.sh [trade|test] --open-venue <okex-futures> --hedge-venue <binance-futures> [--env-name okex-binance-xarb-trade] [--namespace <ns>]
      scripts/deploy_setup_env_xarb.sh --remote-host awsjp [--remote-repo <path>] [--remote-sync] [...]

示例:
  scripts/deploy_setup_env_xarb.sh trade --open-venue okex-futures --hedge-venue binance-futures
  scripts/deploy_setup_env_xarb.sh --env-name okex-binance-xarb-trade --open-venue okex-futures --hedge-venue binance-futures

远程模式（可选）:
  --remote-host <ssh_host>        在远端执行该脚本（用于远端生成 env.sh）
  --remote-repo <path>            远端仓库目录（默认 $HOME/crypto_mkt/mkt_signal）
  --remote-sync                   先 rsync 本地仓库到远端（默认关闭）
  --remote-nice <n>               远端执行优先级（默认 10）
  --remote-ionice/--remote-no-ionice  远端使用 ionice 降低 IO 优先级（默认开启）
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_TYPE="trade"
ENV_NAME=""
NAMESPACE=""
OPEN_VENUE=""
HEDGE_VENUE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    trade|test)
      ENV_TYPE="$1"
      shift
      ;;
    --env-name)
      ENV_NAME="${2:-}"
      shift 2
      ;;
    --namespace)
      NAMESPACE="${2:-}"
      shift 2
      ;;
    --open-venue)
      OPEN_VENUE="${2:-}"
      shift 2
      ;;
    --hedge-venue)
      HEDGE_VENUE="${2:-}"
      shift 2
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

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

if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  echo "[ERROR] 需要 --open-venue 与 --hedge-venue"
  usage
  exit 1
fi

OPEN_VENUE="$(ensure_futures_venue "$OPEN_VENUE")"
HEDGE_VENUE="$(ensure_futures_venue "$HEDGE_VENUE")"
if [[ "$OPEN_VENUE" == "$HEDGE_VENUE" ]]; then
  echo "[ERROR] xarb 需要跨所：open=$OPEN_VENUE hedge=$HEDGE_VENUE"
  exit 1
fi

OPEN_EXCHANGE="${OPEN_VENUE%%-*}"
HEDGE_EXCHANGE="${HEDGE_VENUE%%-*}"

emit_creds_block() {
  local ex="$1"
  case "$ex" in
    okex)
      cat <<'EOF'

# OKX credentials (required when any venue exchange is okex)
export OKX_API_KEY="${OKX_API_KEY:-}"
export OKX_API_SECRET="${OKX_API_SECRET:-}"
export OKX_PASSPHRASE="${OKX_PASSPHRASE:-}"
EOF
      ;;
    binance)
      cat <<'EOF'

# Binance credentials (required when any venue exchange is binance)
export BINANCE_API_KEY="${BINANCE_API_KEY:-}"
export BINANCE_API_SECRET="${BINANCE_API_SECRET:-}"
EOF
      ;;
  esac
}

if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${OPEN_EXCHANGE}-${HEDGE_EXCHANGE}-xarb-${ENV_TYPE}"
fi

if [[ -z "$NAMESPACE" ]]; then
  # IPC_NAMESPACE 不能包含 '/'，建议用 '_' 隔离
  NAMESPACE="${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}_xarb_${ENV_TYPE}"
fi

TARGET_DIR="$HOME/${ENV_NAME}"
mkdir -p "$TARGET_DIR"

ENV_FILE="$TARGET_DIR/env.sh"
cat > "$ENV_FILE" << EOF
#!/usr/bin/env bash
# 自动生成的环境配置文件（xarb）
# 环境类型: $ENV_TYPE
# open venue: $OPEN_VENUE
# hedge venue: $HEDGE_VENUE
# 生成时间: $(date '+%Y-%m-%d %H:%M:%S')

# IceOryx 命名空间（非 dat_pbs 通道会被加上该前缀）
export IPC_NAMESPACE='$NAMESPACE'

# xhub 两侧 venue（futures-only）
export OPEN_VENUE='$OPEN_VENUE'
export HEDGE_VENUE='$HEDGE_VENUE'

# 两侧交易所（从 venue 推断）
export OPEN_EXCHANGE='$OPEN_EXCHANGE'
export HEDGE_EXCHANGE='$HEDGE_EXCHANGE'

$(emit_creds_block "$OPEN_EXCHANGE")
$(emit_creds_block "$HEDGE_EXCHANGE")

# RUST_LOG 配置
export RUST_LOG="\${RUST_LOG:-info,funding_rate_signal=info,mkt_signal=info,hyper=warn,hyper_util=warn,h2=warn,reqwest=warn}"
EOF

chmod +x "$ENV_FILE"

echo "[INFO] 环境配置已部署到 $TARGET_DIR"
echo "[INFO] 使用方法: source $ENV_FILE"
echo "[INFO] namespace: $NAMESPACE"
