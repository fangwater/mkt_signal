#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=lib/fr_remote_deploy.sh
source "$ROOT_DIR/scripts/lib/fr_remote_deploy.sh"

BIN_NAME="depth_pub"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"
DEPLOY_ROOT_NAME="depth_pub"

KNOWN_EXCHANGES=("okex" "binance" "bybit" "bitget" "gate" "hyperliquid")
KNOWN_VENUES=(
  "okex-futures" "okex-margin" "okex-both"
  "binance-futures" "binance-coin-futures" "binance-margin" "binance-both"
  "bybit-futures" "bybit-margin" "bybit-both"
  "bitget-futures" "bitget-coin-futures" "bitget-margin" "bitget-both"
  "gate-futures" "gate-margin" "gate-both"
  "hyperliquid-futures" "hyperliquid-margin" "hyperliquid-both"
)
# 与 deploy_mm_{binance,gate,bitget}.sh 对齐：这三所走远端，其余本地。
REMOTE_EXCHANGES=("binance" "gate" "bitget")

is_known_exchange() {
  local v="${1,,}"
  for e in "${KNOWN_EXCHANGES[@]}"; do
    if [[ "$v" == "$e" ]]; then
      return 0
    fi
  done
  return 1
}

default_venues_for_exchange() {
  local exchange="${1,,}"
  case "$exchange" in
    okex) echo "okex-both" ;;
    binance) echo "binance-both" ;;
    bybit) echo "bybit-both" ;;
    bitget) echo "bitget-both" ;;
    gate) echo "gate-both" ;;
    hyperliquid) echo "hyperliquid-both" ;;
    *)
      echo ""
      return 1
      ;;
  esac
}

is_known_venue() {
  local v="${1,,}"
  for venue in "${KNOWN_VENUES[@]}"; do
    if [[ "$v" == "$venue" ]]; then
      return 0
    fi
  done
  return 1
}

exchange_of_venue() {
  echo "${1%%-*}"
}

is_remote_exchange() {
  local v="${1,,}"
  for e in "${REMOTE_EXCHANGES[@]}"; do
    if [[ "$v" == "$e" ]]; then
      return 0
    fi
  done
  return 1
}

aws_marketdata_depth_core_for_venue() {
  case "${1,,}" in
    bitget-both)  echo 13 ;;
    gate-both)    echo 14 ;;
    binance-both) echo 15 ;;
    *) return 1 ;;
  esac
}

upsert_env_exports_block() {
  local env_file="$1"
  local marker="$2"
  local comment="$3"
  shift 3

  mkdir -p "$(dirname "$env_file")"
  touch "$env_file"

  local tmp
  tmp="$(mktemp)"
  awk -v begin="# BEGIN ${marker}" -v end="# END ${marker}" '
    $0 == begin { skip = 1; next }
    $0 == end { skip = 0; next }
    !skip { print }
  ' "$env_file" > "$tmp"

  {
    cat "$tmp"
    if [[ -s "$tmp" ]]; then
      echo
    fi
    echo "# BEGIN ${marker}"
    [[ -n "$comment" ]] && echo "# ${comment}"
    local line
    for line in "$@"; do
      echo "export ${line}"
    done
    echo "# END ${marker}"
  } > "$env_file"
  rm -f "$tmp"
}

usage() {
  cat <<USAGE
Usage:
  deploy_depth_pub.sh (--exchange <exchange> | --venue <venue>...) [options]

Options:
  --root <path>      （仅本地交易所有效，或配合 --local-only）覆盖部署根目录，默认 \$HOME/depth_pub
  --local-only       强制所有 venue 只部署到本机，不做远端 rsync
  --aws-marketdata-core-layout
                     按 AWS 行情机 CPU13-15 布局写入 DEPTH_PUB_CORE
  --bin-only         仅替换二进制（跳过 scripts/config）
  --runtime-only     替换二进制 + scripts（跳过 config）
  -h, --help         显示帮助

Defaults:
  本地落点 -> \$HOME/depth_pub/<venue>/
  远端落点 (binance/gate/bitget) -> \${FR_DEPLOY_HOST}:\${FR_REMOTE_HOME}/depth_pub/<venue>/
  本地落点 (okex/bybit/hyperliquid) 保持本机部署。

Examples:
  bash scripts/deploy_depth_pub.sh --exchange binance          # 远端
  bash scripts/deploy_depth_pub.sh --exchange okex             # 本地
  bash scripts/deploy_depth_pub.sh --venue gate-futures
  bash scripts/deploy_depth_pub.sh --exchange bitget --bin-only
  bash scripts/deploy_depth_pub.sh --exchange binance --runtime-only

Notes:
  - Exchange expands to default venues:
      okex    -> okex-both
      binance -> binance-both
      bybit   -> bybit-both
      bitget  -> bitget-both
      gate    -> gate-both
      hyperliquid -> hyperliquid-both
  - <exchange>-both 部署目录由 start_depth_pub.sh 展开为 margin+futures。
  - 远端模式下 cargo build 仍在本机完成，再 rsync 到远端。
  - --bin-only / --runtime-only 互斥；远端模式下分别走 fr_remote_sync_binaries
    （只同步顶层二进制）和 fr_remote_sync_path（同步整个 venue 目录）。
  - --aws-marketdata-core-layout 当前映射：
      bitget-both=13 gate-both=14 binance-both=15
USAGE
}

TARGET_ROOT="$HOME/$DEPLOY_ROOT_NAME"
EXCHANGE=""
VENUES_FROM_ARG=()
BIN_MODE="0"
RUNTIME_ONLY="0"
ROOT_OVERRIDE="0"
LOCAL_ONLY="0"
AWS_MARKETDATA_CORE_LAYOUT="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-}"
      if [[ -z "$EXCHANGE" ]]; then
        echo "[ERROR] --exchange 需要一个值" >&2
        usage >&2
        exit 1
      fi
      shift 2
      ;;
    --venue)
      venue="${2:-}"
      if [[ -z "$venue" ]]; then
        echo "[ERROR] --venue 需要一个值" >&2
        usage >&2
        exit 1
      fi
      VENUES_FROM_ARG+=("${venue,,}")
      shift 2
      ;;
    --root|--dir)
      TARGET_ROOT="${2:-}"
      if [[ -z "$TARGET_ROOT" ]]; then
        echo "[ERROR] --root/--dir 需要一个路径" >&2
        usage >&2
        exit 1
      fi
      ROOT_OVERRIDE="1"
      shift 2
      ;;
    --bin-only)
      if [[ "$RUNTIME_ONLY" == "1" ]]; then
        echo "[ERROR] --bin-only 与 --runtime-only 互斥" >&2
        exit 1
      fi
      BIN_MODE="1"
      shift
      ;;
    --runtime-only)
      if [[ "$BIN_MODE" == "1" ]]; then
        echo "[ERROR] --bin-only 与 --runtime-only 互斥" >&2
        exit 1
      fi
      RUNTIME_ONLY="1"
      shift
      ;;
    --local-only)
      LOCAL_ONLY="1"
      shift
      ;;
    --aws-marketdata-core-layout)
      AWS_MARKETDATA_CORE_LAYOUT="1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 未知参数: $1（仅支持 --exchange / --venue / --root / --bin-only / --runtime-only）" >&2
      usage >&2
      exit 1
      ;;
  esac
done

VENUES=()
if [[ ${#VENUES_FROM_ARG[@]} -gt 0 ]]; then
  for venue in "${VENUES_FROM_ARG[@]}"; do
    if ! is_known_venue "$venue"; then
      echo "[ERROR] 不支持的 venue: $venue" >&2
      usage >&2
      exit 1
    fi
    VENUES+=("$venue")
  done
else
  if [[ -z "$EXCHANGE" ]]; then
    echo "[ERROR] 必须提供 --exchange 或 --venue" >&2
    usage >&2
    exit 1
  fi

  if ! is_known_exchange "$EXCHANGE"; then
    echo "[ERROR] 不支持的 exchange: $EXCHANGE" >&2
    usage >&2
    exit 1
  fi

  read -r -a VENUES <<<"$(default_venues_for_exchange "$EXCHANGE")"
fi

# 按本地 / 远端拆分
LOCAL_VENUES=()
REMOTE_VENUES=()
for venue in "${VENUES[@]}"; do
  if [[ "$LOCAL_ONLY" == "0" ]] && is_remote_exchange "$(exchange_of_venue "$venue")"; then
    REMOTE_VENUES+=("$venue")
  else
    LOCAL_VENUES+=("$venue")
  fi
done

if [[ "$ROOT_OVERRIDE" == "1" && ${#REMOTE_VENUES[@]} -gt 0 ]]; then
  echo "[WARN] --root 仅作用于本地 venue；远端 venue 仍落到 \$FR_REMOTE_HOME/$DEPLOY_ROOT_NAME"
fi

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
SCRIPTS_TO_DEPLOY=(
  "start_depth_pub.sh"
  "stop_depth_pub.sh"
  "process_match_lib.sh"
)

write_venue_layout() {
  local stage_dir="$1"
  local venue="$2"
  mkdir -p "$stage_dir"

  # 直接覆盖二进制：若文件被运行中进程占用，会触发 Text file busy 并中断部署。
  cp "$BIN_PATH" "$stage_dir/"
  chmod +x "$stage_dir/$BIN_NAME"

  if [[ "$BIN_MODE" == "1" ]]; then
    return 0
  fi

  mkdir -p "$stage_dir/scripts"
  for script in "${SCRIPTS_TO_DEPLOY[@]}"; do
    if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
      rsync -a "$SCRIPT_DIR_SRC/$script" "$stage_dir/scripts/"
      chmod +x "$stage_dir/scripts/$script"
    fi
  done

  if [[ "$RUNTIME_ONLY" == "1" ]]; then
    return 0
  fi

  # depth_pub 使用相对配置路径 config/depth_cfg.yaml，需下发到每个 venue 目录。
  mkdir -p "$stage_dir/config"
  if [[ -f "$ROOT_DIR/config/depth_cfg.yaml" ]]; then
    rsync -a "$ROOT_DIR/config/depth_cfg.yaml" "$stage_dir/config/"
  fi
  if [[ -f "$ROOT_DIR/config/iceoryx2.toml" ]]; then
    rsync -a "$ROOT_DIR/config/iceoryx2.toml" "$stage_dir/config/"
  fi

  if [[ "$AWS_MARKETDATA_CORE_LAYOUT" == "1" ]] && core_override="$(aws_marketdata_depth_core_for_venue "$venue")"; then
    upsert_env_exports_block \
      "$stage_dir/env.sh" \
      "managed AWS marketdata core layout" \
      "AWS market-data host: pin depth_pub to the second L3 CPU group, CPU13-15." \
      "DEPTH_PUB_CORE='${core_override}'"
    echo "[INFO] AWS marketdata depth_pub core override written: $venue -> core $core_override"
  fi
}

for venue in "${LOCAL_VENUES[@]}"; do
  TARGET_DIR="${TARGET_ROOT%/}/${venue}"
  echo "[INFO] [local] 部署 $BIN_NAME -> $TARGET_DIR"
  write_venue_layout "$TARGET_DIR" "$venue"
done

if [[ ${#REMOTE_VENUES[@]} -gt 0 ]]; then
  fr_remote_init_ssh "$ROOT_DIR"
  for venue in "${REMOTE_VENUES[@]}"; do
    REMOTE_REL="${DEPLOY_ROOT_NAME}/${venue}"
    LOCAL_STAGE="$HOME/$REMOTE_REL"
    echo "[INFO] [remote] staging $BIN_NAME -> $LOCAL_STAGE"
    write_venue_layout "$LOCAL_STAGE" "$venue"
    if [[ "$BIN_MODE" == "1" ]]; then
      fr_remote_sync_binaries "$REMOTE_REL"
    else
      fr_remote_sync_path "$REMOTE_REL"
    fi
  done
fi

echo "[INFO] $BIN_NAME 部署完成"
if [[ ${#LOCAL_VENUES[@]} -gt 0 ]]; then
  echo "[INFO] local venues : ${LOCAL_VENUES[*]}"
  echo "[INFO] local root   : ${TARGET_ROOT%/}"
  echo "[INFO] 启动示例     : cd ${TARGET_ROOT%/}/${LOCAL_VENUES[0]} && ./scripts/start_depth_pub.sh"
fi
if [[ ${#REMOTE_VENUES[@]} -gt 0 ]]; then
  echo "[INFO] remote venues: ${REMOTE_VENUES[*]}"
  echo "[INFO] remote target: ${FR_DEPLOY_HOST}:${FR_REMOTE_HOME}/${DEPLOY_ROOT_NAME}"
fi
