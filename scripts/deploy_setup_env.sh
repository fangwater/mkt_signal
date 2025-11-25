#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 解析参数
ENV_TYPE="${1:-trade}"
case "$ENV_TYPE" in
    trade)
        TARGET_DIR="$HOME/fr_trade"
        IPC_NAMESPACE="fr_trade"
        ;;
    test)
        TARGET_DIR="$HOME/fr_test"
        IPC_NAMESPACE="fr_test"
        ;;
    *)
        echo "[ERROR] 未知环境类型: $ENV_TYPE"
        echo "用法: $0 [trade|test]"
        exit 1
        ;;
esac

CONFIG_FILE="$ROOT_DIR/config/mkt_api_key.toml"

# 读取API密钥
if [[ -f "$CONFIG_FILE" ]]; then
    BINANCE_API_KEY=$(grep -m1 "BINANCE_API_KEY" "$CONFIG_FILE" | cut -d"'" -f2 || true)
    BINANCE_API_SECRET=$(grep -m1 "BINANCE_API_SECRET" "$CONFIG_FILE" | cut -d"'" -f2 || true)
else
    echo "[ERROR] 配置文件 $CONFIG_FILE 不存在"
    exit 1
fi

if [[ -z "${BINANCE_API_KEY:-}" ]] || [[ -z "${BINANCE_API_SECRET:-}" ]]; then
    echo "[ERROR] 无法从 $CONFIG_FILE 读取 API 密钥"
    exit 1
fi

# 确保目标目录存在
mkdir -p "$TARGET_DIR"
mkdir -p "$TARGET_DIR/config"

# 生成env.sh文件
ENV_FILE="$TARGET_DIR/env.sh"
echo "[INFO] 生成环境配置文件: $ENV_FILE"

cat > "$ENV_FILE" << EOF
#!/usr/bin/env bash
# 自动生成的环境配置文件
# 环境类型: $ENV_TYPE
# 生成时间: $(date '+%Y-%m-%d %H:%M:%S')

export BINANCE_API_KEY='$BINANCE_API_KEY'
export BINANCE_API_SECRET='$BINANCE_API_SECRET'
export IPC_NAMESPACE='$IPC_NAMESPACE'

# RUST_LOG 配置
export RUST_LOG="\${RUST_LOG:-info,funding_rate_signal=info,mkt_signal=info,hyper=warn,hyper_util=warn,h2=warn,reqwest=warn}"
EOF

chmod +x "$ENV_FILE"

echo "[INFO] 环境配置已部署到 $TARGET_DIR"
echo "[INFO] 使用方法: source $ENV_FILE"
