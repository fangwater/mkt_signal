#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# 可选：设置 PM2 namespace（默认使用部署目录名，可用环境变量覆盖）
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"
MM_NAME_LIB="${SCRIPT_DIR}/mm_process_name.sh"

if [[ -f "$MM_NAME_LIB" ]]; then
  # shellcheck disable=SC1090
  source "$MM_NAME_LIB"
fi

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

# 候选二进制位置：部署目录优先，其次源码目录
BIN_CANDIDATES=(
  "${SCRIPT_DIR}/trade_signal"
  "${SCRIPT_DIR}/../trade_signal"
  "${SCRIPT_DIR}/target/release/trade_signal"
  "${SCRIPT_DIR}/../target/release/trade_signal"
  "${BASE_DIR}/trade_signal"
  "${BASE_DIR}/target/release/trade_signal"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] trade_signal binary not found. Build first with: cargo build --release --bin trade_signal"
  exit 1
fi

dir_name="$(basename "${BASE_DIR}")"
dir_lc="$(echo "${dir_name}" | tr 'A-Z' 'a-z')"
dir_tag="$(echo "${dir_lc}" | sed 's/[^a-z0-9_-]/_/g')"

infer_ns_and_suffix() {
  local name="$1"

  if [[ "$name" =~ ^([a-z0-9]+)[-_]fr([_-].*)?$ ]]; then
    echo "fr ${BASH_REMATCH[1]}"
    return 0
  fi
  python3 - "$name" <<'PY'
import sys

name = sys.argv[1].strip().lower()

def split_last(value: str):
    idx_dash = value.rfind("-")
    idx_us = value.rfind("_")
    idx = max(idx_dash, idx_us)
    if idx <= 0 or idx >= len(value) - 1:
        return None
    return value[:idx], value[idx + 1:]

first = split_last(name)
if not first:
    raise SystemExit(1)
base, _env_tag = first
second = split_last(base)
if not second:
    raise SystemExit(1)
prefix, ns = second
if not prefix or not ns:
    raise SystemExit(1)
print(ns, prefix)
PY
}

NS=""
SUFFIX=""
if read -r NS SUFFIX < <(infer_ns_and_suffix "$dir_lc"); then
  :
elif type mm_parse_deploy_dir >/dev/null 2>&1 && read -r mm_exchange mm_env_tag < <(mm_parse_deploy_dir "$dir_lc"); then
  NS="mm"
  SUFFIX="${mm_exchange}_${mm_env_tag}"
fi

CLI_EXCHANGE="${1:-}"
PM2_TAG=""
ARGS=()

case "$NS" in
  fr)
    EXCHANGE="$SUFFIX"
    if [[ -n "$CLI_EXCHANGE" && "$CLI_EXCHANGE" != "$EXCHANGE" ]]; then
      echo "[ERROR] exchange mismatch: dir exchange=${EXCHANGE} arg exchange=${CLI_EXCHANGE}"
      exit 1
    fi
    PM2_TAG="$dir_tag"
    ARGS=(--exchange "$EXCHANGE")
    ;;
  xarb)
    open_ex="${SUFFIX%%-*}"
    rest="${SUFFIX#*-}"
    hedge_ex="${rest%%-*}"
    if [[ -z "$open_ex" || -z "$hedge_ex" || "$open_ex" == "$hedge_ex" ]]; then
      echo "[ERROR] invalid xarb dir suffix: ${SUFFIX} (expect like okex-binance)"
      exit 1
    fi
    PM2_TAG="${open_ex}_${hedge_ex}"
    ;;
  mm)
    EXCHANGE="${SUFFIX%%_*}"
    ENV_TAG="${SUFFIX#*_}"
    if [[ -z "$EXCHANGE" || -z "$ENV_TAG" ]]; then
      echo "[ERROR] invalid mm dir suffix: ${SUFFIX} (expect like binance_alpha)"
      exit 1
    fi
    if [[ -n "$CLI_EXCHANGE" && "$CLI_EXCHANGE" != "$EXCHANGE" ]]; then
      echo "[ERROR] exchange mismatch: dir exchange=${EXCHANGE} arg exchange=${CLI_EXCHANGE}"
      exit 1
    fi
    PM2_TAG="${EXCHANGE}_${ENV_TAG}"
    ARGS=(--exchange "$EXCHANGE")
    ;;
  *)
    # Fallback: use directory name as tag; trade_signal will require proper CWD or args.
    PM2_TAG="$(echo "${dir_lc}" | sed 's/[^a-z0-9_-]/_/g')"
    ;;
esac

if [[ "$NS" == "mm" ]]; then
  DEFAULT_PROC_NAME="mm_ts_${PM2_TAG}"
  LEGACY_PROC_NAME="trade_signal_${dir_tag}"
else
  DEFAULT_PROC_NAME="trade_signal_${PM2_TAG}"
  LEGACY_PROC_NAME=""
fi
PROC_NAME="${PM2_NAME:-$DEFAULT_PROC_NAME}"
RUST_LOG="${RUST_LOG:-info}"

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE})"
if [[ -n "$LEGACY_PROC_NAME" ]]; then
  npx pm2 delete "$LEGACY_PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
fi
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE" \
  -- \
  "${ARGS[@]}"

echo ""
echo "[INFO] Started trade_signal (ns=${NS:-unknown} suffix=${SUFFIX:-unknown})"
echo "Namespace: ${NAMESPACE}"
echo "Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
echo "Status: npx pm2 status --namespace ${NAMESPACE}"
