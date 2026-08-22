#!/usr/bin/env bash
# 用 PM2 启动 remote_ip_diag（独立进程：DNS 发现 + TCP 建连探针打分 → 写 Redis）。
#
# 可选 env：
#   REMOTE_IP_DIAG_CONFIG  config 路径（默认 config/remote_ip_diag.toml）
#   REMOTE_IP_DIAG_CORE    绑核编号（默认不绑；建议 0-5 管家核）
#   RUST_LOG               日志级别（默认 info）
set -euo pipefail

cd "$(dirname "$0")/.."   # repo 根目录

NAME="remote_ip_diag"
BIN="./target/release/remote_ip_diag"
CONFIG="${REMOTE_IP_DIAG_CONFIG:-config/remote_ip_diag.toml}"
CORE="${REMOTE_IP_DIAG_CORE:-}"

if [ ! -f "$CONFIG" ]; then
    echo "config not found: $CONFIG" >&2
    exit 1
fi
if [ ! -x "$BIN" ]; then
    echo "release binary missing, building..."
    cargo build --release --bin remote_ip_diag
fi

ARGS=(--config "$CONFIG")
if [ -n "$CORE" ]; then
    ARGS+=(--core "$CORE")
fi

# 先清掉旧实例，避免重复。
pm2 delete "$NAME" >/dev/null 2>&1 || true

RUST_LOG="${RUST_LOG:-info}" pm2 start "$BIN" --name "$NAME" -- "${ARGS[@]}"
pm2 save >/dev/null 2>&1 || true

echo "started $NAME (config=$CONFIG core=${CORE:-none}); 查看日志: pm2 logs $NAME"
echo "查看 Redis 快照: scripts/remote_ip_diag_print.sh"
