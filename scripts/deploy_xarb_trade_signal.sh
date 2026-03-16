#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="trade_signal"
BIN_PATH=""

# shellcheck source=scripts/deploy_xarb_lib.sh
source "$ROOT_DIR/scripts/deploy_xarb_lib.sh"
xarb_preparse_remote_args "$@"
set -- "${XARB_FORWARD_ARGS[@]}"
if [[ -n "${XARB_REMOTE_HOST}" ]]; then
  xarb_remote_maybe_sync_repo "$ROOT_DIR"
  xarb_remote_exec "scripts/$(basename "${BASH_SOURCE[0]}")" "$@"
  exit $?
fi

usage() {
  cat <<'EOF'
用法: scripts/deploy_xarb_trade_signal.sh --open-venue <okex-futures> --hedge-venue <binance-futures> [--env-suffix xarb-trade] [--env-name okex-binance-xarb-trade] [--jobs <n>] [--cargo-target-dir <path>] [--sync-scripts|--bin-only]
      scripts/deploy_xarb_trade_signal.sh --remote-host awsjp [--remote-repo <path>] [--remote-sync] [...]

说明:
  - 构建 trade_signal 并拷贝到 $HOME/<openEx>-<hedgeEx>-<env_suffix>/（默认 env_suffix=xarb-trade，可通过 --env-suffix / --env-name 指定）。
  - xarb 支持显式 open/hedge venue；same exchange 默认解释为 open=margin, hedge=futures。
  - trade_signal 启动时依赖 CWD 目录名推断分支与 Redis 前缀：
      - <exchange>_fr_trade / <exchange>_fr_test
      - <open>-<hedge>-xarb-trade / <open>-<hedge>-xarb-test
  - 默认只更新二进制；如需同步脚本请添加 --sync-scripts（或显式使用 --bin-only）

示例:
  scripts/deploy_xarb_trade_signal.sh --open-venue okex-futures --hedge-venue binance-futures
  scripts/deploy_xarb_trade_signal.sh --env-name okex-binance-xarb-trade --open-venue okex-futures --hedge-venue binance-futures

远程模式（可选）:
  --remote-host <ssh_host>        在远端编译并部署（避免本机编译）
  --remote-repo <path>            远端仓库目录（默认 $HOME/crypto_mkt/mkt_signal）
  --remote-sync                   先 rsync 本地仓库到远端（默认关闭）
  --remote-cargo-target-dir <p>   远端 cargo target 目录（默认 $HOME/.cache/mkt_signal/cargo_target_xarb）
  --remote-nice <n>               远端执行优先级（默认 10）
  --remote-ionice/--remote-no-ionice  远端使用 ionice 降低 IO 优先级（默认开启）
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_SUFFIX="xarb-trade"
ENV_NAME=""
OPEN_VENUE=""
HEDGE_VENUE=""
CARGO_TARGET_DIR_OVERRIDE=""
BUILD_JOBS=""
SYNC_SCRIPTS="0"
BIN_ONLY="0"
if [[ -n "${XARB_REMOTE_RUN:-}" ]]; then
  BUILD_JOBS="1"
fi

normalize_venue() {
  echo "${1,,}"
}

ensure_xarb_venue() {
  local v
  v="$(normalize_venue "$1")"
  if [[ -z "$v" || ! "$v" =~ ^[a-z0-9]+-(margin|futures|spot|swap|perp|perpetual)$ ]]; then
    echo "[ERROR] 非法 xarb venue: $1"
    exit 1
  fi
  echo "$v"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    trade|test)
      echo "[ERROR] 不再支持 trade/test 位置参数，请使用 --env-suffix 或 --env-name"
      usage
      exit 1
      ;;
    --env-suffix)
      ENV_SUFFIX="${2:-xarb-trade}"
      shift 2
      ;;
    --env-name)
      ENV_NAME="${2:-}"
      shift 2
      ;;
    --open-venue)
      OPEN_VENUE="${2:-}"
      shift 2
      ;;
    --hedge-venue)
      HEDGE_VENUE="${2:-}"
      shift 2
      ;;
    --jobs)
      BUILD_JOBS="${2:-}"
      shift 2
      ;;
    --cargo-target-dir)
      CARGO_TARGET_DIR_OVERRIDE="${2:-}"
      shift 2
      ;;
    --sync-scripts)
      if [[ "$BIN_ONLY" == "1" ]]; then
        echo "[ERROR] --sync-scripts 与 --bin-only 互斥"
        exit 1
      fi
      SYNC_SCRIPTS="1"
      shift
      ;;
    --bin-only)
      if [[ "$SYNC_SCRIPTS" == "1" ]]; then
        echo "[ERROR] --sync-scripts 与 --bin-only 互斥"
        exit 1
      fi
      BIN_ONLY="1"
      SYNC_SCRIPTS="0"
      shift
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  echo "[ERROR] 需要 --open-venue 与 --hedge-venue"
  usage
  exit 1
fi

OPEN_VENUE="$(ensure_xarb_venue "$OPEN_VENUE")"
HEDGE_VENUE="$(ensure_xarb_venue "$HEDGE_VENUE")"
if [[ "$OPEN_VENUE" == "$HEDGE_VENUE" ]]; then
  echo "[ERROR] xarb open/hedge venue 不能完全相同：open=$OPEN_VENUE hedge=$HEDGE_VENUE"
  exit 1
fi

OPEN_EXCHANGE="${OPEN_VENUE%%-*}"
HEDGE_EXCHANGE="${HEDGE_VENUE%%-*}"
if [[ "$OPEN_EXCHANGE" == "okx" ]]; then
  OPEN_EXCHANGE="okex"
fi
if [[ "$HEDGE_EXCHANGE" == "okx" ]]; then
  HEDGE_EXCHANGE="okex"
fi

if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${OPEN_EXCHANGE}-${HEDGE_EXCHANGE}-${ENV_SUFFIX}"
fi
TARGET_DIR="$HOME/${ENV_NAME}"

echo "[INFO] 构建 $BIN_NAME (release)"
CARGO_TARGET_DIR_EFFECTIVE="$(xarb_effective_cargo_target_dir "$ROOT_DIR" "$CARGO_TARGET_DIR_OVERRIDE")"
(
  cd "$ROOT_DIR"
  CARGO_TARGET_DIR="$CARGO_TARGET_DIR_EFFECTIVE" \
    cargo build --release --bin "$BIN_NAME" ${BUILD_JOBS:+--jobs "$BUILD_JOBS"}
)
BIN_PATH="$(xarb_bin_path_release "$CARGO_TARGET_DIR_EFFECTIVE" "$BIN_NAME")"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"

if [[ "$SYNC_SCRIPTS" == "1" ]]; then
  SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
  XARB_SCRIPT_DIR_SRC="$ROOT_DIR/xarb_scripts"
  XARB_SCRIPTS_TO_SYNC=(
    "start_xarb_trade_signal.sh"
    "stop_xarb_trade_signal.sh"
  )
  XARB_TOOLS_TO_SYNC=(
    "sync_xarb_symbol_lists.py"
    "print_xarb_symbol_lists.py"
    "set_um_leverage.py"
    "sync_xarb_strategy_params.py"
    "print_xarb_strategy_params.py"
    "sync_xarb_spread_thresholds.py"
    "print_xarb_spread_thresholds.py"
    "sync_xarb_funding_rate_thresholds.py"
    "print_xarb_funding_rate_thresholds.py"
  )
  mkdir -p "$TARGET_DIR/xarb_scripts"

  for script in "${XARB_SCRIPTS_TO_SYNC[@]}"; do
    if [[ -f "$XARB_SCRIPT_DIR_SRC/$script" ]]; then
      rsync -a "$XARB_SCRIPT_DIR_SRC/$script" "$TARGET_DIR/xarb_scripts/"
      chmod +x "$TARGET_DIR/xarb_scripts/$script"
    fi
  done

  for tool in "${XARB_TOOLS_TO_SYNC[@]}"; do
    if [[ -f "$XARB_SCRIPT_DIR_SRC/$tool" ]]; then
      rsync -a "$XARB_SCRIPT_DIR_SRC/$tool" "$TARGET_DIR/xarb_scripts/"
      chmod +x "$TARGET_DIR/xarb_scripts/$tool"
    elif [[ -f "$SCRIPT_DIR_SRC/$tool" ]]; then
      rsync -a "$SCRIPT_DIR_SRC/$tool" "$TARGET_DIR/xarb_scripts/"
      chmod +x "$TARGET_DIR/xarb_scripts/$tool"
    fi
  done
else
  echo "[INFO] 跳过脚本同步（如需同步脚本，请添加 --sync-scripts）"
fi

if ! xarb_atomic_install "$BIN_PATH" "$TARGET_DIR/$BIN_NAME"; then
  exit 2
fi

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 手动启动: cd $TARGET_DIR && ./xarb_scripts/start_xarb_trade_signal.sh"
if [[ "$SYNC_SCRIPTS" == "1" ]]; then
  echo "[INFO] xarb 的 Redis 参数/交易对同步脚本在: $TARGET_DIR/xarb_scripts"
  cat <<EOF
[INFO] 常用命令（在目标目录执行，可自动从 CWD 推断 open/hedge pair）:
  cd "$TARGET_DIR"
  ./xarb_scripts/sync_xarb_symbol_lists.py
  ./xarb_scripts/print_xarb_symbol_lists.py
  ./xarb_scripts/set_um_leverage.py --leverage 6
  ./xarb_scripts/sync_xarb_strategy_params.py
  ./xarb_scripts/print_xarb_strategy_params.py
EOF
else
  echo "[INFO] 未同步脚本（保留目标目录已有 xarb_scripts，需更新请加 --sync-scripts）"
fi
