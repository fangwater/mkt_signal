#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-start}"
if [[ $# -gt 0 ]]; then
  shift
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE_DIR="$(cd "$ROOT_DIR/.." && pwd)"
TARGET_DIR="${SOZU_TARGET_DIR:-$WORKSPACE_DIR}"
CONFIG_PATH="${SOZU_CONFIG_PATH:-$TARGET_DIR/config/sozu.toml}"
SOZU_BIN="${SOZU_BIN:-$HOME/.cargo/bin/sozu}"

if [[ ! -x "$SOZU_BIN" ]]; then
  echo "[ERROR] 未找到 sozu 可执行文件：$SOZU_BIN" >&2
  exit 1
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "[ERROR] 未找到 sozu 配置：$CONFIG_PATH" >&2
  exit 1
fi

COMMAND_SOCKET_RAW="$(awk -F '\"' '/command_socket/ {print $2; exit}' "$CONFIG_PATH")"
if [[ -z "$COMMAND_SOCKET_RAW" || "$COMMAND_SOCKET_RAW" != unix:* ]]; then
  echo "[ERROR] 配置中 command_socket 字段非法：$COMMAND_SOCKET_RAW" >&2
  exit 1
fi
COMMAND_SOCKET="$COMMAND_SOCKET_RAW"
SOCKET_PATH="${COMMAND_SOCKET#unix:}"
SOCKET_DIR="$(dirname "$SOCKET_PATH")"

LOG_TARGET="$(awk -F '\"' '/^\s*target/ {print $2; exit}' "$CONFIG_PATH")"
LOG_DIR=""
if [[ -n "${LOG_TARGET:-}" ]]; then
  LOG_DIR="$(dirname "$LOG_TARGET")"
fi

case "$ACTION" in
  start)
    echo "[INFO] 使用配置：$CONFIG_PATH"
    mkdir -p "$SOCKET_DIR"
    if [[ -n "$LOG_DIR" ]]; then
      mkdir -p "$LOG_DIR"
    fi
    echo "[INFO] 启动 sozu ..."
    exec "$SOZU_BIN" start --config "$CONFIG_PATH" "$@"
    ;;
  reload)
    echo "[INFO] 重新加载配置：$CONFIG_PATH"
    exec "$SOZU_BIN" reload --config "$CONFIG_PATH" --file "$CONFIG_PATH"
    ;;
  status)
    echo "[INFO] 查询 sozu 状态"
    exec "$SOZU_BIN" status --config "$CONFIG_PATH"
    ;;
  stop)
    echo "[INFO] 停止 sozu"
    exec "$SOZU_BIN" shutdown --config "$CONFIG_PATH"
    ;;
  *)
    cat <<'EOF'
用法: run_sozu.sh [start|reload|status|stop] [额外参数...]

环境变量：
  SOZU_CONFIG_PATH  指定配置文件路径（默认 $SOZU_TARGET_DIR/config/sozu.toml）
  SOZU_TARGET_DIR   指定 deploy 后的目标根目录（默认仓库根目录）
  SOZU_BIN          指定 sozu 可执行文件（默认 ~/.cargo/bin/sozu）
EOF
    exit 1
    ;;
esac
