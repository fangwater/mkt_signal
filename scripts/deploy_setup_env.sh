#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 默认环境与交易所
ENV_TYPE="trade"
EXCHANGE="binance"
IPC_NAMESPACE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    trade|test)
      ENV_TYPE="$1"
      shift
      ;;
    --exchange)
      EXCHANGE="${2:-}"
      if [[ -z "$EXCHANGE" ]]; then
        echo "[ERROR] --exchange 需要一个值"
        exit 1
      fi
      shift 2
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      echo "用法: $0 [trade|test] [--exchange binance]"
      exit 1
      ;;
  esac
done

TARGET_DIR="$HOME/${EXCHANGE}_fr_${ENV_TYPE}"
IPC_NAMESPACE="${EXCHANGE}_fr_${ENV_TYPE}"

mkdir -p "$TARGET_DIR"

ENV_FILE="$TARGET_DIR/env.sh"
CREDS_BLOCK=""
if [[ "$EXCHANGE" == "gate" ]]; then
  CREDS_BLOCK=$(cat <<'EOF'

# Gate credentials
export GATE_API_KEY="${GATE_API_KEY:-}"
export GATE_API_SECRET="${GATE_API_SECRET:-}"
EOF
)
fi
cat > "$ENV_FILE" << EOF
#!/usr/bin/env bash
# 自动生成的环境配置文件
# 环境类型: $ENV_TYPE
# 交易所: $EXCHANGE
# 生成时间: $(date '+%Y-%m-%d %H:%M:%S')

export IPC_NAMESPACE='$IPC_NAMESPACE'

# RUST_LOG 配置
export RUST_LOG="\${RUST_LOG:-info,funding_rate_signal=info,mkt_signal=info,hyper=warn,hyper_util=warn,h2=warn,reqwest=warn}"
$CREDS_BLOCK
EOF

chmod +x "$ENV_FILE"

echo "[INFO] 环境配置已部署到 $TARGET_DIR"
echo "[INFO] 使用方法: source $ENV_FILE"
