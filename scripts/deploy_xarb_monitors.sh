#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

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
用法: scripts/deploy_xarb_monitors.sh --open-venue <okex-futures> --hedge-venue <binance-futures> [--env-suffix xarb-trade] [--env-name okex-binance-xarb-trade] [--jobs <n>] [--cargo-target-dir <path>]
      scripts/deploy_xarb_monitors.sh --remote-host awsjp [--remote-repo <path>] [--remote-sync] [...]

说明:
  - 构建并部署 xarb 所需的两侧账户 monitor（二进制为 okex_account_monitor / binance_account_monitor）。
  - 输出到 $HOME/<open>-<hedge>-<env_suffix>/（默认 env_suffix=xarb-trade，可通过 --env-suffix / --env-name 指定）：
      account_monitor_okex
      account_monitor_binance
      xarb_scripts/start_xarb_monitors.sh
      xarb_scripts/stop_xarb_monitors.sh

示例:
  scripts/deploy_xarb_monitors.sh --open-venue okex-futures --hedge-venue binance-futures
  scripts/deploy_xarb_monitors.sh --env-name okex-binance-xarb-trade --open-venue okex-futures --hedge-venue binance-futures

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

normalize_exchange() {
  local ex="${1,,}"
  if [[ "$ex" == "okx" ]]; then
    ex="okex"
  fi
  echo "$ex"
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

OPEN_EXCHANGE="$(normalize_exchange "${OPEN_VENUE%%-*}")"
HEDGE_EXCHANGE="$(normalize_exchange "${HEDGE_VENUE%%-*}")"
if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${OPEN_EXCHANGE}-${HEDGE_EXCHANGE}-${ENV_SUFFIX}"
fi

TARGET_DIR="$HOME/${ENV_NAME}"
mkdir -p "$TARGET_DIR/xarb_scripts"
CARGO_TARGET_DIR_EFFECTIVE="$(xarb_effective_cargo_target_dir "$ROOT_DIR" "$CARGO_TARGET_DIR_OVERRIDE")"

deploy_one() {
  local exchange="$1"
  local bin_name=""
  local out_name=""
  case "$exchange" in
    okex)
      bin_name="okex_account_monitor"
      out_name="account_monitor_okex"
      ;;
    binance)
      bin_name="binance_account_monitor"
      out_name="account_monitor_binance"
      ;;
    *)
      echo "[ERROR] xarb monitors 当前仅支持 okex/binance：got exchange=$exchange"
      exit 1
      ;;
  esac

  echo "[INFO] 构建 $bin_name (release)"
  (
    cd "$ROOT_DIR"
    CARGO_TARGET_DIR="$CARGO_TARGET_DIR_EFFECTIVE" \
      cargo build --release --bin "$bin_name" ${BUILD_JOBS:+--jobs "$BUILD_JOBS"}
  )

  echo "[INFO] 部署 $bin_name -> $TARGET_DIR/$out_name"
  local bin_path
  bin_path="$(xarb_bin_path_release "$CARGO_TARGET_DIR_EFFECTIVE" "$bin_name")"
  if ! xarb_atomic_install "$bin_path" "$TARGET_DIR/$out_name"; then
    exit 2
  fi
}

deploy_one "$OPEN_EXCHANGE"
if [[ "$HEDGE_EXCHANGE" != "$OPEN_EXCHANGE" ]]; then
  deploy_one "$HEDGE_EXCHANGE"
fi

# 同步 xarb monitor start/stop 脚本
SCRIPTS_TO_SYNC=(
  "xarb_scripts/start_xarb_monitors.sh"
  "xarb_scripts/stop_xarb_monitors.sh"
)
for file in "${SCRIPTS_TO_SYNC[@]}"; do
  SRC="$ROOT_DIR/$file"
  if [[ -f "$SRC" ]]; then
    rsync -a "$SRC" "$TARGET_DIR/$(dirname "$file")/"
    chmod +x "$TARGET_DIR/$file" 2>/dev/null || true
  fi
done

echo "[INFO] xarb monitors 部署完成: $TARGET_DIR"
echo "[INFO] 启动: cd $TARGET_DIR && source ./env.sh && ./xarb_scripts/start_xarb_monitors.sh"
