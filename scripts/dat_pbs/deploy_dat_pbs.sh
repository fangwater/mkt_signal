#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN_NAME="dat_pbs"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"
KNOWN_EXCHANGES=("okex" "binance" "bybit" "bitget" "gate" "aster")
KNOWN_VENUES=(
  "okex-futures" "okex-margin"
  "binance-futures" "binance-margin"
  "bybit-futures" "bybit-margin"
  "bitget-futures" "bitget-margin"
  "gate-futures" "gate-margin"
  "aster-futures" "aster-margin"
)

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
    okex) echo "okex-futures okex-margin" ;;
    binance) echo "binance-futures binance-margin" ;;
    bybit) echo "bybit-futures bybit-margin" ;;
    bitget) echo "bitget-futures bitget-margin" ;;
    gate) echo "gate-futures gate-margin" ;;
    aster) echo "aster-futures aster-margin" ;;
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

usage() {
  cat <<'USAGE'
Usage:
  deploy_dat_pbs.sh (--exchange <exchange> | --venue <venue>...) [--root <path>]

Defaults:
  固定部署根目录 -> $HOME/dat_pbs
  目录结构 -> $HOME/dat_pbs/<venue>/

Examples:
  bash scripts/dat_pbs/deploy_dat_pbs.sh --exchange binance
  bash scripts/dat_pbs/deploy_dat_pbs.sh --exchange okex
  bash scripts/dat_pbs/deploy_dat_pbs.sh --venue gate-futures
  bash scripts/dat_pbs/deploy_dat_pbs.sh --venue binance-futures --venue binance-margin
  bash scripts/dat_pbs/deploy_dat_pbs.sh --exchange gate --root "$HOME/dat_pbs"

Notes:
  - Exchange expands to default venues:
      okex    -> okex-futures okex-margin
      binance -> binance-futures binance-margin
      bybit   -> bybit-futures bybit-margin
      bitget  -> bitget-futures bitget-margin
      gate    -> gate-futures gate-margin
      aster   -> aster-futures aster-margin
  - start/stop 在单个 venue 目录中执行（按目录名推断 venue）。
USAGE
}

TARGET_ROOT="$HOME/dat_pbs"
EXCHANGE=""
VENUES_FROM_ARG=()
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
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 未知参数: $1（仅支持 --exchange / --root）" >&2
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

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

SCRIPT_DIR_SRC="$ROOT_DIR/scripts/dat_pbs"
SCRIPTS_TO_DEPLOY=(
  "start_dat_pbs.sh"
  "stop_dat_pbs.sh"
  "setup_pmdaemon_logrotate.sh"
)
HELPER_SCRIPTS_TO_DEPLOY=(
  "$ROOT_DIR/scripts/process_match_lib.sh"
)

for venue in "${VENUES[@]}"; do
  TARGET_DIR="${TARGET_ROOT%/}/${venue}"
  echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
  mkdir -p "$TARGET_DIR"

  # 直接覆盖二进制：若文件被运行中进程占用，会触发 Text file busy 并中断部署。
  cp "$BIN_PATH" "$TARGET_DIR/"
  chmod +x "$TARGET_DIR/$BIN_NAME"

  mkdir -p "$TARGET_DIR/scripts"
  for script in "${SCRIPTS_TO_DEPLOY[@]}"; do
    if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
      rsync -a "$SCRIPT_DIR_SRC/$script" "$TARGET_DIR/scripts/"
      chmod +x "$TARGET_DIR/scripts/$script"
    fi
  done
  for helper in "${HELPER_SCRIPTS_TO_DEPLOY[@]}"; do
    if [[ -f "$helper" ]]; then
      rsync -a "$helper" "$TARGET_DIR/scripts/"
      chmod +x "$TARGET_DIR/scripts/$(basename "$helper")"
    fi
  done
done

# dat_pbs 运行时固定读取 $HOME/dat_pbs/config/mkt_cfg.yaml，故始终同步到根目录配置。
mkdir -p "${TARGET_ROOT%/}/config"
if [[ -f "$ROOT_DIR/config/mkt_cfg.yaml" ]]; then
  rsync -a "$ROOT_DIR/config/mkt_cfg.yaml" "${TARGET_ROOT%/}/config/"
fi
if [[ -f "$ROOT_DIR/config/iceoryx2.toml" ]]; then
  rsync -a "$ROOT_DIR/config/iceoryx2.toml" "${TARGET_ROOT%/}/config/"
fi

# 远端分流：binance/bitget/gate 的 venue 推到 AWS 远端主机
REMOTE_VENUE_REGEX='^(binance|bitget|gate)-(futures|margin)$'
REMOTE_VENUES=()
LOCAL_VENUES=()
for v in "${VENUES[@]}"; do
  if [[ "$v" =~ $REMOTE_VENUE_REGEX ]]; then
    REMOTE_VENUES+=("$v")
  else
    LOCAL_VENUES+=("$v")
  fi
done

if [[ ${#REMOTE_VENUES[@]} -gt 0 && "${TARGET_ROOT%/}" != "$HOME/dat_pbs" ]]; then
  echo "[ERROR] --root override 与远端 rsync 不兼容（fr_remote_sync_path 固定从 \$HOME/dat_pbs/ 读）" >&2
  exit 1
fi

if [[ ${#REMOTE_VENUES[@]} -gt 0 ]]; then
  # shellcheck source=../lib/fr_remote_deploy.sh
  source "$ROOT_DIR/scripts/lib/fr_remote_deploy.sh"
  fr_remote_init_ssh "$ROOT_DIR"
  for v in "${REMOTE_VENUES[@]}"; do
    fr_remote_sync_path "dat_pbs/$v"
  done
  fr_remote_sync_path "dat_pbs/config"
fi

echo "[INFO] $BIN_NAME 部署完成"
echo "[INFO] root_dir: ${TARGET_ROOT%/}"
if [[ ${#LOCAL_VENUES[@]} -gt 0 ]]; then
  echo "[INFO] local venues:"
  for v in "${LOCAL_VENUES[@]}"; do
    echo "  - ${v} -> ${TARGET_ROOT%/}/${v}/"
  done
fi
if [[ ${#REMOTE_VENUES[@]} -gt 0 ]]; then
  echo "[INFO] remote venues (${FR_DEPLOY_HOST}:${FR_REMOTE_HOME}/dat_pbs/):"
  for v in "${REMOTE_VENUES[@]}"; do
    echo "  - ${v}"
  done
fi
echo "[INFO] config: ${TARGET_ROOT%/}/config/mkt_cfg.yaml"
echo "[INFO] 启动:"
if [[ ${#LOCAL_VENUES[@]} -gt 0 ]]; then
  echo "  - 本地: cd ${TARGET_ROOT%/}/<venue> && ./scripts/start_dat_pbs.sh"
fi
if [[ ${#REMOTE_VENUES[@]} -gt 0 ]]; then
  echo "  - 远端: ssh ${FR_DEPLOY_HOST} 'cd ${FR_REMOTE_HOME}/dat_pbs/<venue> && ./scripts/start_dat_pbs.sh'"
fi
