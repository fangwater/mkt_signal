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

dir_name="$(basename "${BASE_DIR}")"
dir_lc="$(echo "${dir_name}" | tr 'A-Z' 'a-z')"
dir_tag="$(echo "${dir_lc}" | sed 's/[^a-z0-9_-]/_/g')"

infer_ns_and_suffix() {
  local name="$1"

  if [[ "$name" =~ ^([a-z0-9]+)[-_]fr([_-].*)?$ ]]; then
    echo "fr ${BASH_REMATCH[1]}"
    return 0
  fi

  for env_suffix in "_trade" "_test"; do
    if [[ "$name" == *"$env_suffix" ]]; then
      local base="${name%$env_suffix}"
      base="${base%_}"
      local ns="${base##*_}"
      local prefix="${base%_*}"
      if [[ -n "$ns" && -n "$prefix" ]]; then
        echo "${ns} ${prefix}"
        return 0
      fi
    fi
  done

  for env_suffix in "-trade" "-test"; do
    if [[ "$name" == *"$env_suffix" ]]; then
      local base="${name%$env_suffix}"
      base="${base%-}"
      local ns="${base##*-}"
      local prefix="${base%-*}"
      if [[ -n "$ns" && -n "$prefix" ]]; then
        echo "${ns} ${prefix}"
        return 0
      fi
    fi
  done

  return 1
}

NS=""
SUFFIX=""
if type mm_parse_deploy_dir >/dev/null 2>&1 && read -r mm_exchange mm_env_tag < <(mm_parse_deploy_dir "$dir_lc"); then
  NS="mm"
  SUFFIX="${mm_exchange}_${mm_env_tag}"
elif read -r NS SUFFIX < <(infer_ns_and_suffix "$dir_lc"); then
  :
fi

CLI_EXCHANGE="${1:-}"
PM2_TAG=""
BUGGY_PROC_NAME=""

case "$NS" in
  fr)
    EXCHANGE="$SUFFIX"
    if [[ -n "$CLI_EXCHANGE" && "$CLI_EXCHANGE" != "$EXCHANGE" ]]; then
      echo "[ERROR] exchange mismatch: dir exchange=${EXCHANGE} arg exchange=${CLI_EXCHANGE}"
      exit 1
    fi
    PM2_TAG="$dir_tag"
    ;;
  xarb)
    open_ex="${SUFFIX%%-*}"
    rest="${SUFFIX#*-}"
    hedge_ex="${rest%%-*}"
    if [[ -z "$open_ex" || -z "$hedge_ex" ]]; then
      echo "[ERROR] invalid xarb dir suffix: ${SUFFIX} (expect like okex-binance)"
      exit 1
    fi
    if [[ "$open_ex" == "$hedge_ex" ]]; then
      PM2_TAG="${open_ex}_std"
    else
      PM2_TAG="${open_ex}_${hedge_ex}"
    fi
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
    ;;
  *)
    PM2_TAG="$(echo "${dir_lc}" | sed 's/[^a-z0-9_-]/_/g')"
    ;;
esac

if [[ "$NS" == "mm" ]]; then
  if type mm_trade_signal_proc_name >/dev/null 2>&1; then
    DEFAULT_PROC_NAME="$(mm_trade_signal_proc_name "$EXCHANGE" "$ENV_TAG")"
    BUGGY_PROC_NAME="$(mm_trade_signal_proc_name "$EXCHANGE" "$EXCHANGE")"
  else
    DEFAULT_PROC_NAME="mm_${EXCHANGE}_futures_${ENV_TAG}_trade_signal"
    BUGGY_PROC_NAME="mm_${EXCHANGE}_futures_${EXCHANGE}_trade_signal"
  fi
  LEGACY_PROC_NAME="trade_signal_${dir_tag}"
else
  DEFAULT_PROC_NAME="trade_signal_${PM2_TAG}"
  LEGACY_PROC_NAME=""
fi
PROC_NAME="${PM2_NAME:-$DEFAULT_PROC_NAME}"

echo "[INFO] Deleting ${PROC_NAME} (namespace=${NAMESPACE})"
if [[ -n "$LEGACY_PROC_NAME" ]]; then
  npx pm2 delete "$LEGACY_PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
fi
if [[ -n "$BUGGY_PROC_NAME" && "$BUGGY_PROC_NAME" != "$PROC_NAME" ]]; then
  npx pm2 delete "$BUGGY_PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
fi
if npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE"; then
  echo "[INFO] Deleted ${PROC_NAME}"
else
  echo "[WARN] ${PROC_NAME} not found in namespace ${NAMESPACE}"
fi

echo ""
echo "[INFO] Remaining processes: npx pm2 status --namespace ${NAMESPACE}"
