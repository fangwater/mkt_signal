#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 生成 cross 环境变量脚本
#
# 目录约定:
#   $HOME/<open>-<hedge>-<env_suffix>/
#   例如: $HOME/okex-binance-cross-trade/
#
# 说明:
#   - 跨所合约对：open/hedge 必须都是 -futures/-swap/-perp，且不相同
#   - IPC_NAMESPACE 默认按 open/hedge/env_suffix 生成（<open>_<hedge>_cross_<suffix>）
#   - env.sh 内会按 open/hedge 自动生成所需的凭证变量占位

usage() {
  cat <<'EOF'
用法: scripts/deploy_setup_env_cross.sh --open-venue <okex-futures> --hedge-venue <binance-futures> [--env-suffix cross-trade] [--env-name okex-binance-cross-trade] [--namespace <ns>]

示例:
  scripts/deploy_setup_env_cross.sh --open-venue okex-futures --hedge-venue binance-futures
  scripts/deploy_setup_env_cross.sh --env-name okex-binance-cross-trade --open-venue okex-futures --hedge-venue binance-futures
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_SUFFIX="cross-trade"
ENV_NAME=""
NAMESPACE=""
OPEN_VENUE=""
HEDGE_VENUE=""
ENV_SUFFIX_EXPLICIT="0"

infer_pair_and_meta_from_env_name() {
  local name="${1,,}"
  local open_ex=""
  local hedge_ex=""
  local instance_tag=""
  local site=""

  if [[ "$name" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross[-_]([a-z0-9][a-z0-9_-]*)[-_](open|hedge)$ ]]; then
    open_ex="${BASH_REMATCH[1]}"
    hedge_ex="${BASH_REMATCH[2]}"
    instance_tag="${BASH_REMATCH[3]}"
    site="${BASH_REMATCH[4]}"
  elif [[ "$name" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross[-_]([a-z0-9][a-z0-9_-]*)$ ]]; then
    open_ex="${BASH_REMATCH[1]}"
    hedge_ex="${BASH_REMATCH[2]}"
    instance_tag="${BASH_REMATCH[3]}"
  fi

  if [[ "$open_ex" == "okx" ]]; then
    open_ex="okex"
  fi
  if [[ "$hedge_ex" == "okx" ]]; then
    hedge_ex="okex"
  fi

  if [[ -n "$open_ex" && -n "$hedge_ex" && -n "$instance_tag" ]]; then
    echo "${open_ex},${hedge_ex},${instance_tag},${site}"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-suffix)
      ENV_SUFFIX="${2:-cross-trade}"
      ENV_SUFFIX_EXPLICIT="1"
      shift 2
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

CROSS_SIDE=""
if [[ -n "$ENV_NAME" ]]; then
  if inferred="$(infer_pair_and_meta_from_env_name "$ENV_NAME")" && [[ -n "$inferred" ]]; then
    if [[ -z "$OPEN_VENUE" ]]; then
      OPEN_VENUE="${inferred%%,*}-futures"
    fi
    rest="${inferred#*,}"
    if [[ -z "$HEDGE_VENUE" ]]; then
      HEDGE_VENUE="${rest%%,*}-futures"
    fi
    rest="${rest#*,}"
    if [[ "$ENV_SUFFIX_EXPLICIT" != "1" ]]; then
      ENV_SUFFIX="cross-${rest%%,*}"
    fi
    CROSS_SIDE="${inferred##*,}"
  fi
fi

normalize_venue() {
  echo "${1,,}"
}

ensure_cross_venue() {
  local v
  v="$(normalize_venue "$1")"
  if [[ -z "$v" || ! "$v" =~ ^[a-z0-9]+-(futures|swap|perp|perpetual)$ ]]; then
    echo "[ERROR] 非法 cross venue（必须是 -futures/-swap/-perp/-perpetual）: $1"
    exit 1
  fi
  echo "$v"
}

if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  echo "[ERROR] 需要 --open-venue 与 --hedge-venue"
  usage
  exit 1
fi

OPEN_VENUE="$(ensure_cross_venue "$OPEN_VENUE")"
HEDGE_VENUE="$(ensure_cross_venue "$HEDGE_VENUE")"
if [[ "$OPEN_VENUE" == "$HEDGE_VENUE" ]]; then
  echo "[ERROR] cross open/hedge venue 不能完全相同：open=$OPEN_VENUE hedge=$HEDGE_VENUE"
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

emit_all_creds_blocks() {
  local open_ex="$1"
  local hedge_ex="$2"

  emit_creds_block "$open_ex"
  if [[ "$hedge_ex" != "$open_ex" ]]; then
    emit_creds_block "$hedge_ex"
  fi
}

if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${OPEN_EXCHANGE}-${HEDGE_EXCHANGE}-${ENV_SUFFIX}"
fi

if [[ -z "$NAMESPACE" ]]; then
  ENV_SUFFIX_NS="${ENV_SUFFIX//-/_}"
  NAMESPACE="${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}_${ENV_SUFFIX_NS}"
fi

TARGET_DIR="$HOME/${ENV_NAME}"
mkdir -p "$TARGET_DIR"

ENV_FILE="$TARGET_DIR/env.sh"
cat > "$ENV_FILE" << EOF
#!/usr/bin/env bash
# 自动生成的环境配置文件（cross）
# 环境后缀: $ENV_SUFFIX
# open venue: $OPEN_VENUE
# hedge venue: $HEDGE_VENUE
# 生成时间: $(date '+%Y-%m-%d %H:%M:%S')

# IceOryx 命名空间（非 dat_pbs 通道会被加上该前缀）
export IPC_NAMESPACE='$NAMESPACE'

# cross 两侧 venue
export OPEN_VENUE='$OPEN_VENUE'
export HEDGE_VENUE='$HEDGE_VENUE'

$(emit_all_creds_blocks "$OPEN_EXCHANGE" "$HEDGE_EXCHANGE")

export CROSS_SIDE='${CROSS_SIDE}'

# RUST_LOG 配置
export RUST_LOG="\${RUST_LOG:-info,funding_rate_signal=info,mkt_signal=info,hyper=warn,hyper_util=warn,h2=warn,reqwest=warn}"
EOF

chmod +x "$ENV_FILE"

echo "[INFO] 环境配置已部署到 $TARGET_DIR"
echo "[INFO] 使用方法: source $ENV_FILE"
echo "[INFO] namespace: $NAMESPACE"
