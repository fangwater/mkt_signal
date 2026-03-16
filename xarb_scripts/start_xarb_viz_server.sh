#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")

BIN_CANDIDATES=(
  "${BASE_DIR}/viz_server"
  "${SCRIPT_DIR}/viz_server"
  "${BASE_DIR}/target/release/viz_server"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] viz_server binary not found. Deploy/build first."
  exit 1
fi

usage() {
  cat <<'EOF'
用法: xarb_scripts/start_xarb_viz_server.sh [--cfg config/viz.toml]

说明:
  - 以 pmdaemon 启动 viz_server，并按配置订阅 pre_trade 的 resample（positions/exposure/risk）并通过 WS 转发。
  - 默认读取: ./config/viz.toml（可用 --cfg 指定，或设置环境变量 VIZ_CFG）。

示例:
  cd $HOME/okex-binance-xarb-trade
  ./xarb_scripts/start_xarb_viz_server.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

CFG_PATH=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --cfg)
      CFG_PATH="${2:-}"
      shift 2
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$CFG_PATH" ]]; then
  CFG_PATH="${VIZ_CFG:-config/viz.toml}"
fi

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"
OPEN_EXCHANGE=""
HEDGE_EXCHANGE=""
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]xarb([_-].*)?$ ]]; then
  OPEN_EXCHANGE="${BASH_REMATCH[1]}"
  HEDGE_EXCHANGE="${BASH_REMATCH[2]}"
fi
if [[ "$OPEN_EXCHANGE" == "okx" ]]; then
  OPEN_EXCHANGE="okex"
fi
if [[ "$HEDGE_EXCHANGE" == "okx" ]]; then
  HEDGE_EXCHANGE="okex"
fi
if [[ -z "$OPEN_EXCHANGE" || -z "$HEDGE_EXCHANGE" ]]; then
  echo "[ERROR] 无法从目录名推断 open/hedge (dir=$dir_name)，期望 <open>-<hedge>-xarb-..."
  exit 1
fi

PROC_NAME="viz_server_xarb_${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}"
RUST_LOG="${RUST_LOG:-info}"

if [[ ! -f "$BASE_DIR/$CFG_PATH" ]]; then
  echo "[ERROR] viz config not found: $BASE_DIR/$CFG_PATH"
  echo "[ERROR] 建议先部署：scripts/deploy_xarb_viz_server.sh ..."
  exit 1
fi

if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

TMP_FILES=()
cleanup_tmp_files() {
  if [[ ${#TMP_FILES[@]} -gt 0 ]]; then
    rm -f "${TMP_FILES[@]}" >/dev/null 2>&1 || true
  fi
}
trap cleanup_tmp_files EXIT

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

cfg_file="$(mktemp)"
TMP_FILES+=("$cfg_file")

json_name="$(json_escape "$PROC_NAME")"
json_bin="$(json_escape "$BIN_PATH")"
json_base="$(json_escape "$BASE_DIR")"
json_cfg="$(json_escape "$CFG_PATH")"
json_rust_log="$(json_escape "$RUST_LOG")"

cat >"$cfg_file" <<CFG
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": [],
      "cwd": "${json_base}",
      "env": {
        "VIZ_CFG": "${json_cfg}",
        "RUST_LOG": "${json_rust_log}"
      }
    }
  ]
}
CFG

echo "[INFO] Restarting ${PROC_NAME} (cfg=${CFG_PATH})"
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo ""
echo "[INFO] Started viz_server"
echo "Config: ${CFG_PATH}"
echo "Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "Status: ${PMDAEMON[*]} list"
