#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_NAME=""
VENUE=""
VIZ_PORT="10041"
DO_BUILD=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-name) ENV_NAME="${2:-}"; shift 2 ;;
    --venue) VENUE="${2:-}"; shift 2 ;;
    --viz-port) VIZ_PORT="${2:-}"; shift 2 ;;
    --scripts-only) DO_BUILD=0; shift ;;
    -h|--help)
      echo "Usage: scripts/deploy_exec.sh --env-name <name> --venue <binance-futures|okex-futures> [--viz-port 10041] [--scripts-only]"
      exit 0
      ;;
    *) echo "[ERROR] Unknown arg: $1" >&2; exit 1 ;;
  esac
done
[[ "$ENV_NAME" =~ ^[A-Za-z0-9][A-Za-z0-9_-]*$ ]] || { echo "[ERROR] invalid --env-name" >&2; exit 1; }
[[ "$VENUE" == "binance-futures" || "$VENUE" == "okex-futures" ]] || { echo "[ERROR] unsupported --venue" >&2; exit 1; }
[[ "$VIZ_PORT" =~ ^[0-9]+$ ]] || { echo "[ERROR] invalid --viz-port" >&2; exit 1; }

DEPLOY_ROOT="${DEPLOY_ROOT:-${HOME}}"
TARGET_DIR="${DEPLOY_ROOT}/${ENV_NAME}"
mkdir -p "${TARGET_DIR}/scripts" "${TARGET_DIR}/config"
if [[ $DO_BUILD -eq 1 ]]; then
  (cd "$ROOT_DIR" && cargo build --release --bin exec-pre-trade)
  (cd "$ROOT_DIR" && cargo build --release -p viz_server --bin viz_server)
  install -m 755 "${ROOT_DIR}/target/release/exec-pre-trade" "${TARGET_DIR}/exec-pre-trade"
  install -m 755 "${ROOT_DIR}/target/release/viz_server" "${TARGET_DIR}/viz_server"
fi

FILES=(
  start_exec_pre_trade.sh stop_exec_pre_trade.sh
  start_exec_viz_server.sh stop_exec_viz_server.sh
  start_exec_config_server.sh stop_exec_config_server.sh exec_config_server.py
  process_match_lib.sh okx_swap_open_orders.py
  binance_cancel_all_std_um_ws_orders.py binance_cancel_all_unified_open_orders.py
  binance_local_ip.py sell_margin_spot.py
  sync_exec_risk_params.py print_exec_risk_params.py
  sync_exec_max_pos_u.py print_exec_max_pos_u.py
)
for file in "${FILES[@]}"; do install -m 755 "${ROOT_DIR}/scripts/${file}" "${TARGET_DIR}/scripts/${file}"; done
if [[ ! -f "${TARGET_DIR}/config/iceoryx2.toml" ]]; then
  install -m 644 "${ROOT_DIR}/config/iceoryx2.toml" "${TARGET_DIR}/config/iceoryx2.toml"
fi
if [[ ! -f "${TARGET_DIR}/config/exec_viz.toml" ]]; then
  cat >"${TARGET_DIR}/config/exec_viz.toml" <<TOML
[[servers]]
[servers.http]
bind = "0.0.0.0"
port = ${VIZ_PORT}
ws_path = "/ws"
[servers.pre_trade]
enabled = false
[servers.exec_pre_trade]
enabled = true
namespace = "${ENV_NAME}"
TOML
fi
if [[ ! -f "${TARGET_DIR}/config/exec_config_server.env" ]]; then
  cat >"${TARGET_DIR}/config/exec_config_server.env" <<ENV
BIND=0.0.0.0
PORT=18161
ENV_NAME=${ENV_NAME}
VENUE=${VENUE}
REDIS_URL=redis://127.0.0.1:6379/0
DASHBOARD_URL=http://127.0.0.1:${VIZ_PORT}/
ENV
fi
echo "[INFO] Deployed Exec files to ${TARGET_DIR}; processes were not started"
