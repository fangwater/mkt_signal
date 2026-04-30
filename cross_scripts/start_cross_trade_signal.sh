#!/usr/bin/env bash
set -euo pipefail

# cross trade_signal 启动脚本：
# - 依赖部署目录名推断 cross pair（目录名需形如 <open>-<hedge>-cross-<env>）
# - 若存在 env.sh，会自动 source（用于 account mode / venue / credentials / RUST_LOG）
# - 使用 PM2 namespace（默认=部署目录名，可用 PM2_NAMESPACE 覆盖）

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
else
  echo "[WARN] 未找到 env.sh：${ENV_FILE}"
fi

BIN_CANDIDATES=(
  "${BASE_DIR}/trade_signal"
  "${SCRIPT_DIR}/trade_signal"
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
  echo "[ERROR] trade_signal binary not found. Deploy/build first."
  exit 1
fi

dir_name="$(basename "${BASE_DIR}")"
dir_lc="$(echo "${dir_name}" | tr 'A-Z' 'a-z')"

infer_ns_and_suffix() {
  local name="$1"
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
fi

PM2_TAG=""
case "$NS" in
  cross)
    open_ex="${SUFFIX%%-*}"
    rest="${SUFFIX#*-}"
    hedge_ex="${rest%%-*}"
    if [[ -z "$open_ex" || -z "$hedge_ex" ]]; then
      echo "[ERROR] invalid cross dir suffix: ${SUFFIX} (expect like okex-binance)"
      exit 1
    fi
    if [[ "$open_ex" == "$hedge_ex" ]]; then
      PM2_TAG="${open_ex}_std"
    else
      PM2_TAG="${open_ex}_${hedge_ex}"
    fi
    ;;
  *)
    echo "[ERROR] not an cross env dir: ${dir_name} (expect like okex-binance-cross-<env>)"
    exit 1
    ;;
esac

PROC_NAME="trade_signal_${PM2_TAG}"
RUST_LOG="${RUST_LOG:-info}"

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE})"
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE" \
  --cwd "$BASE_DIR"

echo ""
echo "[INFO] Started trade_signal (ns=${NS} suffix=${SUFFIX})"
echo "Namespace: ${NAMESPACE}"
echo "Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
echo "Status: npx pm2 status --namespace ${NAMESPACE}"
