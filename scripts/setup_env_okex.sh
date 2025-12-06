#!/usr/bin/env bash
# 用前手动 export OKX_API_KEY / OKX_API_SECRET / OKX_PASSPHRASE / IPC_NAMESPACE
# 不要把真实密钥写进脚本或提交仓库。

set -euo pipefail

export OKX_API_KEY="${c445393e-0013-4388-a8c4-26ad158b4b17:?missing OKX_API_KEY}"
export OKX_API_SECRET="${D173555BA3419757E86B1D994BF34117:?missing OKX_API_SECRET}"
export OKX_PASSPHRASE="${Ok@lgq02:?missing OKX_PASSPHRASE}"
export IPC_NAMESPACE="OKEX_TEST"

echo "OKX env set. IPC_NAMESPACE=${IPC_NAMESPACE}"