#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/deploy_intra_lib.sh
source "$ROOT_DIR/scripts/deploy_intra_lib.sh"

# 生成 intra 环境变量脚本（同所期货现货套利）
#
# 目录约定:
#   $HOME/<exchange>-<env_suffix>/
#   例如: $HOME/binance-intra-trade/
#
# 说明:
#   - 同所期现：只需要一个 exchange，open=<ex>-margin，hedge=<ex>-futures
#   - IPC_NAMESPACE 默认按 exchange/env_suffix 生成（避免与 fr / xarb / mm 等冲突）

usage() {
  cat <<'EOF'
用法: scripts/deploy_setup_env_intra.sh --exchange <binance> [--env-suffix intra-trade] [--env-name binance-intra-trade] [--namespace <ns>]

示例:
  scripts/deploy_setup_env_intra.sh --exchange binance
  scripts/deploy_setup_env_intra.sh --env-name binance-intra-trade --exchange binance
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_SUFFIX="intra-trade"
ENV_NAME=""
NAMESPACE=""
EXCHANGE=""
ENV_SUFFIX_EXPLICIT="0"

# 从 env_name 推 exchange + env_suffix（intra 唯一格式：<exchange>-intra-<tag>）
infer_meta_from_env_name() {
  local name="${1,,}"
  local ex=""
  local tag=""

  if [[ "$name" =~ ^([a-z0-9]+)[-_]intra[-_]([a-z0-9][a-z0-9_-]*)$ ]]; then
    ex="${BASH_REMATCH[1]}"
    tag="${BASH_REMATCH[2]}"
  fi
  if [[ "$ex" == "okx" ]]; then
    ex="okex"
  fi
  if [[ -n "$ex" && -n "$tag" ]]; then
    echo "${ex},${tag}"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-suffix)
      ENV_SUFFIX="${2:-intra-trade}"
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
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

# 由 env_name 反推 exchange / env_suffix
if [[ -n "$ENV_NAME" ]]; then
  if inferred="$(infer_meta_from_env_name "$ENV_NAME")" && [[ -n "$inferred" ]]; then
    if [[ -z "$EXCHANGE" ]]; then
      EXCHANGE="${inferred%%,*}"
    fi
    if [[ "$ENV_SUFFIX_EXPLICIT" != "1" ]]; then
      ENV_SUFFIX="intra-${inferred##*,}"
    fi
  fi
fi

if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 需要 --exchange，或使用 --env-name <exchange>-intra-<tag> 推断"
  usage
  exit 1
fi

EXCHANGE="$(intra_ensure_exchange "$EXCHANGE")"

read -r OPEN_VENUE HEDGE_VENUE <<<"$(intra_venues_for_exchange "$EXCHANGE")"

if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${EXCHANGE}-${ENV_SUFFIX}"
fi

if [[ -z "$NAMESPACE" ]]; then
  ENV_SUFFIX_NS="${ENV_SUFFIX//-/_}"
  NAMESPACE="${EXCHANGE}_${ENV_SUFFIX_NS}"
fi

emit_creds_block() {
  local ex="$1"
  case "$ex" in
    okex)
      cat <<'EOF'

# OKX credentials
export OKX_API_KEY="${OKX_API_KEY:-}"
export OKX_API_SECRET="${OKX_API_SECRET:-}"
export OKX_PASSPHRASE="${OKX_PASSPHRASE:-}"
EOF
      ;;
    binance)
      cat <<'EOF'

# Binance credentials
export BINANCE_API_KEY="${BINANCE_API_KEY:-}"
export BINANCE_API_SECRET="${BINANCE_API_SECRET:-}"
EOF
      ;;
    gate)
      cat <<'EOF'

# Gate credentials
export GATE_API_KEY="${GATE_API_KEY:-}"
export GATE_API_SECRET="${GATE_API_SECRET:-}"
EOF
      ;;
    bybit)
      cat <<'EOF'

# Bybit credentials
export BYBIT_API_KEY="${BYBIT_API_KEY:-}"
export BYBIT_API_SECRET="${BYBIT_API_SECRET:-}"
EOF
      ;;
    bitget)
      cat <<'EOF'

# Bitget credentials
export BITGET_API_KEY="${BITGET_API_KEY:-}"
export BITGET_API_SECRET="${BITGET_API_SECRET:-}"
export BITGET_API_PASSPHRASE="${BITGET_API_PASSPHRASE:-${BITGET_PASSPHRASE:-}}"
EOF
      ;;
  esac
}

TARGET_DIR="$HOME/${ENV_NAME}"
mkdir -p "$TARGET_DIR"

ENV_FILE="$TARGET_DIR/env.sh"
cat > "$ENV_FILE" << EOF
#!/usr/bin/env bash
# 自动生成的环境配置文件（intra 同所期现）
# 环境后缀: $ENV_SUFFIX
# exchange: $EXCHANGE
# open venue: $OPEN_VENUE
# hedge venue: $HEDGE_VENUE
# 生成时间: $(date '+%Y-%m-%d %H:%M:%S')

# IceOryx 命名空间（非 dat_pbs 通道会被加上该前缀）
export IPC_NAMESPACE='$NAMESPACE'

# 同所 venue
export OPEN_VENUE='$OPEN_VENUE'
export HEDGE_VENUE='$HEDGE_VENUE'
$(emit_creds_block "$EXCHANGE")

# RUST_LOG 配置
export RUST_LOG="\${RUST_LOG:-info,funding_rate_signal=info,mkt_signal=info,hyper=warn,hyper_util=warn,h2=warn,reqwest=warn}"
EOF

chmod +x "$ENV_FILE"

echo "[INFO] 环境配置已部署到 $TARGET_DIR"
echo "[INFO] 使用方法: source $ENV_FILE"
echo "[INFO] namespace: $NAMESPACE"
