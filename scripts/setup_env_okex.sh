#!/usr/bin/env bash
# OKX 环境变量设置脚本
# 直接设置环境变量（请勿提交真实密钥到仓库）

set -euo pipefail

# 设置 OKX API 凭证
export OKX_API_KEY="c445393e-0013-4388-a8c4-26ad158b4b17"
export OKX_API_SECRET="D173555BA3419757E86B1D994BF34117"
export OKX_PASSPHRASE="Ok@lgq02"
export IPC_NAMESPACE="OKEX_TEST"

echo "OKX env set:"
echo "  OKX_API_KEY=${OKX_API_KEY}"
echo "  OKX_API_SECRET=${OKX_API_SECRET:0:4}...${OKX_API_SECRET: -4}"
echo "  OKX_PASSPHRASE=***"
echo "  IPC_NAMESPACE=${IPC_NAMESPACE}"