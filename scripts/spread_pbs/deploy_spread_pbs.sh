#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN_NAME="spread_pbs"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

# 5 个 CEX × 2 market = 10 单边 venue；*-both 由一个进程同时启动 margin/futures。
# HK el-cc-okx-srv01 的 OKEX venue 会写入 SPREAD_PBS_CORE 覆盖到 12/14。
KNOWN_VENUES=(
  "binance-margin"
  "binance-futures"
  "binance-coin-futures"
  "binance-both"
  "bitget-margin"
  "bitget-futures"
  "bitget-both"
  "bybit-margin"
  "bybit-futures"
  "bybit-both"
  "gate-margin"
  "gate-futures"
  "gate-both"
  "okex-margin"
  "okex-futures"
  "okex-both"
)
ALL_BOTH_VENUES=(
  "binance-both"
  "bitget-both"
  "bybit-both"
  "gate-both"
  "okex-both"
)
KNOWN_EXCHANGES=("binance" "bitget" "bybit" "gate" "okex")

is_known_exchange() {
  local v="${1,,}"
  for e in "${KNOWN_EXCHANGES[@]}"; do
    [[ "$v" == "$e" ]] && return 0
  done
  return 1
}

both_venue_for_exchange() {
  case "${1,,}" in
    binance) echo "binance-both" ;;
    bitget)  echo "bitget-both"  ;;
    bybit)   echo "bybit-both"   ;;
    gate)    echo "gate-both"    ;;
    okex)    echo "okex-both"    ;;
    *) echo ""; return 1 ;;
  esac
}

is_known_venue() {
  local v="${1,,}"
  for venue in "${KNOWN_VENUES[@]}"; do
    [[ "$v" == "$venue" ]] && return 0
  done
  return 1
}

hk_okex_spread_core_for_venue() {
  case "${1,,}" in
    okex-margin)  echo 12 ;;
    okex-futures) echo 14 ;;
    okex-both)    echo 12 ;;
    *) return 1 ;;
  esac
}

aws_marketdata_spread_core_for_venue() {
  case "${1,,}" in
    binance-margin)  echo 8 ;;
    binance-futures) echo 9 ;;
    binance-coin-futures) echo 10 ;;
    gate-both)       echo 10 ;;
    okex-both)       echo 11 ;;
    bitget-both)     echo 12 ;;
    bybit-both)      echo "bybit-split" ;;
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
  cat <<'USAGE'
Usage:
  deploy_spread_pbs.sh (--exchange <exchange> | --venue <venue>...) [--root <path>] [--local-only] [--aws-marketdata-core-layout]
  deploy_spread_pbs.sh --all          # 每个 exchange 铺 <exchange>-both

Defaults:
  固定部署根目录 -> $HOME/spread_pbs
  目录结构       -> $HOME/spread_pbs/<venue>/

Notes:
  - --exchange <exchange> 自动部署 <exchange>-both。
  - --venue <exchange>-margin / --venue <exchange>-futures 只部署对应单边。
  - <exchange>-both 会在一个 spread_pbs 进程内同时启动该 exchange 的 margin/futures。
    IPC topic 仍按单 venue 分开，例如 spread_pbs/gate-margin/ask_bid_spread 与
    spread_pbs/gate-futures/ask_bid_spread。
  - 默认 core 映射在 start_spread_pbs.sh 里，按字母序：
      binance-margin=0  binance-futures=1  binance-both=0
      bitget-margin=2   bitget-futures=3
      bybit-margin=4    bybit-futures=5
      gate-margin=6     gate-futures=7
      okex-margin=8     okex-futures=9
    <exchange>-both 默认复用该 exchange 的 margin core；env.sh 可用 SPREAD_PBS_CORE 覆盖。
  - HK el-cc-okx-srv01 上，OKEX venue 会在 env.sh 写入覆盖：
      okex-margin SPREAD_PBS_CORE=12
      okex-futures SPREAD_PBS_CORE=14
      okex-both SPREAD_PBS_CORE=12
  - --local-only 强制所有 venue 只部署到本机，不做远端 rsync。
  - --aws-marketdata-core-layout 按 AWS 行情机 CPU8-15 布局写入 env.sh：
      binance-margin=8 binance-futures=9 gate-both=10 okex-both=11 bitget-both=12
      bybit-both 拆成 SPREAD_PBS_BYBIT_MARKET_CORE=8 / SPREAD_PBS_BYBIT_BOOKTICKER_CORE=9
USAGE
}

TARGET_ROOT="$HOME/spread_pbs"
EXCHANGE=""
VENUES_FROM_ARG=()
DEPLOY_ALL=0
LOCAL_ONLY=0
AWS_MARKETDATA_CORE_LAYOUT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-}"
      [[ -z "$EXCHANGE" ]] && { echo "[ERROR] --exchange 需要一个值" >&2; usage >&2; exit 1; }
      shift 2
      ;;
    --venue)
      v="${2:-}"
      [[ -z "$v" ]] && { echo "[ERROR] --venue 需要一个值" >&2; usage >&2; exit 1; }
      VENUES_FROM_ARG+=("${v,,}")
      shift 2
      ;;
    --root|--dir)
      TARGET_ROOT="${2:-}"
      [[ -z "$TARGET_ROOT" ]] && { echo "[ERROR] --root 需要一个路径" >&2; usage >&2; exit 1; }
      shift 2
      ;;
    --all)
      DEPLOY_ALL=1
      shift
      ;;
    --local-only)
      LOCAL_ONLY=1
      shift
      ;;
    --aws-marketdata-core-layout)
      AWS_MARKETDATA_CORE_LAYOUT=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 未知参数: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

VENUES=()
if [[ $DEPLOY_ALL -eq 1 ]]; then
  VENUES=("${ALL_BOTH_VENUES[@]}")
elif [[ ${#VENUES_FROM_ARG[@]} -gt 0 ]]; then
  for v in "${VENUES_FROM_ARG[@]}"; do
    is_known_venue "$v" || { echo "[ERROR] 不支持的 venue: $v" >&2; exit 1; }
    VENUES+=("$v")
  done
else
  [[ -z "$EXCHANGE" ]] && { echo "[ERROR] 必须提供 --exchange / --venue / --all" >&2; usage >&2; exit 1; }
  is_known_exchange "$EXCHANGE" || { echo "[ERROR] 不支持的 exchange: $EXCHANGE" >&2; exit 1; }
  read -r -a VENUES <<<"$(both_venue_for_exchange "$EXCHANGE")"
fi

echo "[INFO] 构建 $BIN_NAME (release)"
( cd "$ROOT_DIR" && cargo build --release --bin "$BIN_NAME" )

SCRIPT_DIR_SRC="$ROOT_DIR/scripts/spread_pbs"
SCRIPTS_TO_DEPLOY=(
  "start_spread_pbs.sh"
  "stop_spread_pbs.sh"
)

for venue in "${VENUES[@]}"; do
  TARGET_DIR="${TARGET_ROOT%/}/${venue}"
  echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
  mkdir -p "$TARGET_DIR/scripts"

  # Linux 下若 binary 仍有进程持有，cp 直接覆盖会触发 ETXTBSY；
  # 先 unlink 旧路径（原 inode 仍被进程持有不影响），再 cp 新文件占用同一路径。
  rm -f "$TARGET_DIR/$BIN_NAME"
  cp "$BIN_PATH" "$TARGET_DIR/"
  chmod +x "$TARGET_DIR/$BIN_NAME"

  for script in "${SCRIPTS_TO_DEPLOY[@]}"; do
    src="$SCRIPT_DIR_SRC/$script"
    if [[ -f "$src" ]]; then
      rsync -a "$src" "$TARGET_DIR/scripts/"
      chmod +x "$TARGET_DIR/scripts/$script"
    fi
  done

  if [[ "$AWS_MARKETDATA_CORE_LAYOUT" == "1" ]] && core_override="$(aws_marketdata_spread_core_for_venue "$venue")"; then
    if [[ "$core_override" == "bybit-split" ]]; then
      upsert_env_exports_block \
        "$TARGET_DIR/env.sh" \
        "managed AWS marketdata core layout" \
        "AWS SG Bybit: split JSON market and bookticker roles across CPU8/9." \
        "SPREAD_PBS_BYBIT_MARKET_CORE='8'" \
        "SPREAD_PBS_BYBIT_BOOKTICKER_CORE='9'"
      echo "[INFO] AWS marketdata split override written: $venue -> market core 8, bookticker core 9"
    else
      upsert_env_exports_block \
        "$TARGET_DIR/env.sh" \
        "managed AWS marketdata core layout" \
        "AWS market-data host: pin spread_pbs to the second L3 CPU group, CPU8-12." \
        "SPREAD_PBS_CORE='${core_override}'"
      echo "[INFO] AWS marketdata spread_pbs core override written: $venue -> core $core_override"
    fi
  elif core_override="$(hk_okex_spread_core_for_venue "$venue")"; then
    upsert_env_exports_block \
      "$TARGET_DIR/env.sh" \
      "managed HK isolated-core layout" \
      "HK el-cc-okx-srv01: OKEX spread_pbs uses isolated physical cores; okex-intra hot cores=4,6,8,10; keep sibling CPUs 5,7,9,11,13,15 unused/offline." \
      "SPREAD_PBS_CORE='${core_override}'"
    echo "[INFO] HK OKEX spread_pbs core override written: $venue -> core $core_override"
  fi
done

# 共享配置（mkt_cfg.yaml + iceoryx2.toml）部署到根目录
mkdir -p "${TARGET_ROOT%/}/config"
if [[ -f "$ROOT_DIR/config/mkt_cfg.yaml" ]]; then
  rsync -a "$ROOT_DIR/config/mkt_cfg.yaml" "${TARGET_ROOT%/}/config/"
fi
if [[ -f "$ROOT_DIR/config/iceoryx2.toml" ]]; then
  rsync -a "$ROOT_DIR/config/iceoryx2.toml" "${TARGET_ROOT%/}/config/"
fi

# 远端分流：binance/bitget/gate 的 venue 推到 AWS 远端主机
REMOTE_VENUE_REGEX='^(binance-(futures|margin|both)|(bitget|gate)-(futures|margin|both))$'
REMOTE_VENUES=()
LOCAL_VENUES=()
for v in "${VENUES[@]}"; do
  if [[ "$LOCAL_ONLY" == "0" && "$v" =~ $REMOTE_VENUE_REGEX ]]; then
    REMOTE_VENUES+=("$v")
  else
    LOCAL_VENUES+=("$v")
  fi
done

if [[ ${#REMOTE_VENUES[@]} -gt 0 && "${TARGET_ROOT%/}" != "$HOME/spread_pbs" ]]; then
  echo "[ERROR] --root override 与远端 rsync 不兼容（fr_remote_sync_path 固定从 \$HOME/spread_pbs/ 读）" >&2
  exit 1
fi

if [[ ${#REMOTE_VENUES[@]} -gt 0 ]]; then
  # shellcheck source=../lib/fr_remote_deploy.sh
  source "$ROOT_DIR/scripts/lib/fr_remote_deploy.sh"
  fr_remote_init_ssh "$ROOT_DIR"
  for v in "${REMOTE_VENUES[@]}"; do
    fr_remote_sync_path "spread_pbs/$v"
  done
  fr_remote_sync_path "spread_pbs/config"
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
  echo "[INFO] remote venues (${FR_DEPLOY_HOST}:${FR_REMOTE_HOME}/spread_pbs/):"
  for v in "${REMOTE_VENUES[@]}"; do
    echo "  - ${v}"
  done
fi
echo "[INFO] config:   ${TARGET_ROOT%/}/config/mkt_cfg.yaml"
echo "[INFO] 启动:"
if [[ ${#LOCAL_VENUES[@]} -gt 0 ]]; then
  echo "  - 本地: cd ${TARGET_ROOT%/}/<venue> && ./scripts/start_spread_pbs.sh"
fi
if [[ ${#REMOTE_VENUES[@]} -gt 0 ]]; then
  echo "  - 远端: ssh ${FR_DEPLOY_HOST} 'cd ${FR_REMOTE_HOME}/spread_pbs/<venue> && ./scripts/start_spread_pbs.sh'"
fi
