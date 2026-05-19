#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=scripts/deploy_cross_lib.sh
source "$ROOT_DIR/scripts/deploy_cross_lib.sh"

usage() {
  cat <<'EOF'
用法: scripts/deploy_cross_monitors.sh --open-venue <okex-futures> --hedge-venue <binance-futures> [--env-suffix cross-trade] [--env-name okex-binance-cross-trade] [--jobs <n>] [--cargo-target-dir <path>]

说明:
  - 跨所合约对套利专用：open/hedge 必须都是 -futures/-swap/-perp，且不相同。
  - 构建并部署 cross 所需的账户 monitor（二进制为 <exchange>_account_monitor）。
  - 若 env-name 以 `_open` / `_hedge` 结尾，则只部署对应一侧。
  - 输出到 $HOME/<open>-<hedge>-<env_suffix>/（默认 env_suffix=cross-trade）：
      account_monitor_<open_exchange>
      account_monitor_<hedge_exchange>
      cross_scripts/start_cross_monitors.sh
      cross_scripts/stop_cross_monitors.sh

示例:
  scripts/deploy_cross_monitors.sh --open-venue okex-futures --hedge-venue binance-futures
  scripts/deploy_cross_monitors.sh --env-name okex-binance-cross-trade01_hedge
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_SUFFIX="cross-trade"
ENV_NAME=""
OPEN_VENUE=""
HEDGE_VENUE=""
CARGO_TARGET_DIR_OVERRIDE=""
BUILD_JOBS=""
DEPLOY_SIDE=""

infer_pair_and_side_from_name() {
  local name="${1,,}"
  local open_ex=""
  local hedge_ex=""
  local side=""

  if [[ "$name" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross([_-].*)?[-_](open|hedge)$ ]]; then
    open_ex="${BASH_REMATCH[1]}"
    hedge_ex="${BASH_REMATCH[2]}"
    side="${BASH_REMATCH[4]}"
  elif [[ "$name" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross([_-].*)?$ ]]; then
    open_ex="${BASH_REMATCH[1]}"
    hedge_ex="${BASH_REMATCH[2]}"
  fi

  if [[ "$open_ex" == "okx" ]]; then
    open_ex="okex"
  fi
  if [[ "$hedge_ex" == "okx" ]]; then
    hedge_ex="okex"
  fi

  if [[ -n "$open_ex" && -n "$hedge_ex" ]]; then
    echo "${open_ex},${hedge_ex},${side}"
  fi
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
    --env-suffix)
      ENV_SUFFIX="${2:-cross-trade}"
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

if [[ -n "$ENV_NAME" && ( -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" || -z "$DEPLOY_SIDE" ) ]]; then
  if inferred="$(infer_pair_and_side_from_name "$ENV_NAME")" && [[ -n "$inferred" ]]; then
    if [[ -z "$OPEN_VENUE" ]]; then
      OPEN_VENUE="${inferred%%,*}-futures"
    fi
    rest="${inferred#*,}"
    if [[ -z "$HEDGE_VENUE" ]]; then
      HEDGE_VENUE="${rest%%,*}-futures"
    fi
    if [[ -z "$DEPLOY_SIDE" ]]; then
      DEPLOY_SIDE="${inferred##*,}"
    fi
  fi
fi

if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  echo "[ERROR] 需要 --open-venue 与 --hedge-venue，或使用 --env-name <open>-<hedge>-cross-<suffix>[_open|_hedge]"
  usage
  exit 1
fi

PAIR_RESULT="$(cross_ensure_venue_pair "$OPEN_VENUE" "$HEDGE_VENUE")"
if [[ -z "$PAIR_RESULT" ]]; then
  exit 1
fi
read -r OPEN_VENUE HEDGE_VENUE <<<"$PAIR_RESULT"

OPEN_EXCHANGE="$(normalize_exchange "${OPEN_VENUE%%-*}")"
HEDGE_EXCHANGE="$(normalize_exchange "${HEDGE_VENUE%%-*}")"
if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${OPEN_EXCHANGE}-${HEDGE_EXCHANGE}-${ENV_SUFFIX}"
fi

TARGET_DIR="$HOME/${ENV_NAME}"
mkdir -p "$TARGET_DIR/cross_scripts"
CARGO_TARGET_DIR_EFFECTIVE="$(cross_effective_cargo_target_dir "$ROOT_DIR" "$CARGO_TARGET_DIR_OVERRIDE")"

deploy_one() {
  local exchange="$1"
  local bin_name=""
  local out_name=""
  case "$exchange" in
    binance|okex|bybit|bitget|gate)
      bin_name="${exchange}_account_monitor"
      out_name="account_monitor_${exchange}"
      ;;
    *)
      echo "[ERROR] cross monitors 当前仅支持 binance/okex/bybit/bitget/gate：got exchange=$exchange"
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
  bin_path="$(cross_bin_path_release "$CARGO_TARGET_DIR_EFFECTIVE" "$bin_name")"
  if ! cross_atomic_install "$bin_path" "$TARGET_DIR/$out_name"; then
    exit 2
  fi
}

case "$DEPLOY_SIDE" in
  open)
    echo "[INFO] side=open，仅部署 open 侧 monitor: $OPEN_EXCHANGE"
    deploy_one "$OPEN_EXCHANGE"
    ;;
  hedge)
    echo "[INFO] side=hedge，仅部署 hedge 侧 monitor: $HEDGE_EXCHANGE"
    deploy_one "$HEDGE_EXCHANGE"
    ;;
  "")
    deploy_one "$OPEN_EXCHANGE"
    if [[ "$HEDGE_EXCHANGE" != "$OPEN_EXCHANGE" ]]; then
      deploy_one "$HEDGE_EXCHANGE"
    fi
    ;;
  *)
    echo "[ERROR] 非法 side 后缀: $DEPLOY_SIDE（仅支持 _open / _hedge）"
    exit 1
    ;;
esac

SCRIPTS_TO_SYNC=(
  "cross_scripts/start_cross_monitors.sh"
  "cross_scripts/stop_cross_monitors.sh"
)
for file in "${SCRIPTS_TO_SYNC[@]}"; do
  SRC="$ROOT_DIR/$file"
  if [[ -f "$SRC" ]]; then
    rsync -a "$SRC" "$TARGET_DIR/$(dirname "$file")/"
    chmod +x "$TARGET_DIR/$file" 2>/dev/null || true
  fi
done

echo "[INFO] cross monitors 部署完成: $TARGET_DIR"
echo "[INFO] 启动: cd $TARGET_DIR && source ./env.sh && ./cross_scripts/start_cross_monitors.sh"
