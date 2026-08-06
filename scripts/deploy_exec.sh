#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_NAME=""
VENUE=""
VIZ_PORT=""
CONFIG_PORT=""
DO_BUILD=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-name) ENV_NAME="${2:-}"; shift 2 ;;
    --venue) VENUE="${2:-}"; shift 2 ;;
    --viz-port) VIZ_PORT="${2:-}"; shift 2 ;;
    --config-port) CONFIG_PORT="${2:-}"; shift 2 ;;
    --scripts-only) DO_BUILD=0; shift ;;
    -h|--help)
      echo "Usage: scripts/deploy_exec.sh --env-name <nameNN> --venue <binance-futures|okex-futures> [--viz-port <port>] [--config-port <port>] [--scripts-only]"
      echo "Deploys the complete Exec runtime plus the matching spread_pbs venue; nothing is started."
      echo "Default ports are derived from the trailing instance number: 01 -> 10041/18161, 02 -> 10042/18162."
      exit 0
      ;;
    *) echo "[ERROR] Unknown arg: $1" >&2; exit 1 ;;
  esac
done
[[ "$ENV_NAME" =~ ^[A-Za-z0-9][A-Za-z0-9_-]*$ ]] || { echo "[ERROR] invalid --env-name" >&2; exit 1; }
[[ "$VENUE" == "binance-futures" || "$VENUE" == "okex-futures" ]] || { echo "[ERROR] unsupported --venue" >&2; exit 1; }
if [[ "$ENV_NAME" =~ ([0-9]+)$ ]]; then
  INSTANCE_SUFFIX="${BASH_REMATCH[1]}"
  INSTANCE_INDEX=$((10#$INSTANCE_SUFFIX))
else
  echo "[ERROR] env-name must end with an instance number, e.g. binance_exec_trade01" >&2
  exit 1
fi
((INSTANCE_INDEX >= 1 && INSTANCE_INDEX <= 99)) || { echo "[ERROR] instance number must be 01..99" >&2; exit 1; }
VIZ_PORT="${VIZ_PORT:-$((10040 + INSTANCE_INDEX))}"
CONFIG_PORT="${CONFIG_PORT:-$((18160 + INSTANCE_INDEX))}"
[[ "$VIZ_PORT" =~ ^[0-9]+$ ]] && ((VIZ_PORT >= 1 && VIZ_PORT <= 65535)) || { echo "[ERROR] invalid --viz-port" >&2; exit 1; }
[[ "$CONFIG_PORT" =~ ^[0-9]+$ ]] && ((CONFIG_PORT >= 1 && CONFIG_PORT <= 65535)) || { echo "[ERROR] invalid --config-port" >&2; exit 1; }
[[ "$VIZ_PORT" != "$CONFIG_PORT" ]] || { echo "[ERROR] viz and config ports must differ" >&2; exit 1; }

DEPLOY_ROOT="${DEPLOY_ROOT:-${HOME}}"
TARGET_DIR="${DEPLOY_ROOT}/${ENV_NAME}"
SPREAD_ROOT="${DEPLOY_ROOT}/spread_pbs"
SPREAD_DIR="${SPREAD_ROOT}/${VENUE}"
HOST_CONFIG_DIR="${DEPLOY_ROOT}/config"
case "$VENUE" in
  binance-futures)
    EXCHANGE="binance"
    ACCOUNT_MONITOR_BIN="binance_account_monitor"
    ;;
  okex-futures)
    EXCHANGE="okex"
    ACCOUNT_MONITOR_BIN="okex_account_monitor"
    ;;
esac

mkdir -p \
  "${TARGET_DIR}/scripts" \
  "${TARGET_DIR}/config" \
  "${TARGET_DIR}/data/persist_manager" \
  "${SPREAD_DIR}/scripts" \
  "${SPREAD_ROOT}/config" \
  "${HOST_CONFIG_DIR}"
if [[ $DO_BUILD -eq 1 ]]; then
  (cd "$ROOT_DIR" && cargo build --release -p mkt_signal \
    --bin exec-pre-trade \
    --bin trade_engine \
    --bin "$ACCOUNT_MONITOR_BIN" \
    --bin spread_pbs)
  (cd "$ROOT_DIR" && cargo build --release -p trade_signal --bin trade_signal)
  (cd "$ROOT_DIR" && cargo build --release -p viz_server --bin viz_server)
  (cd "$ROOT_DIR" && cargo build --release -p persist_manager --features runtime --bin persist_manager)
  install -m 755 "${ROOT_DIR}/target/release/exec-pre-trade" "${TARGET_DIR}/exec-pre-trade"
  install -m 755 "${ROOT_DIR}/target/release/trade_signal" "${TARGET_DIR}/trade_signal"
  install -m 755 "${ROOT_DIR}/target/release/trade_engine" "${TARGET_DIR}/trade_engine"
  install -m 755 "${ROOT_DIR}/target/release/${ACCOUNT_MONITOR_BIN}" "${TARGET_DIR}/account_monitor"
  install -m 755 "${ROOT_DIR}/target/release/persist_manager" "${TARGET_DIR}/persist_manager"
  install -m 755 "${ROOT_DIR}/target/release/viz_server" "${TARGET_DIR}/viz_server"
  install -m 755 "${ROOT_DIR}/target/release/spread_pbs" "${SPREAD_DIR}/spread_pbs"
fi

FILES=(
  start_exec_pre_trade.sh stop_exec_pre_trade.sh
  start_exec_trade_signal.sh stop_exec_trade_signal.sh
  start_exec_trade_engine.sh stop_exec_trade_engine.sh
  start_exec_persist_manager.sh stop_exec_persist_manager.sh
  start_exec_viz_server.sh stop_exec_viz_server.sh
  start_exec_config_server.sh stop_exec_config_server.sh exec_config_server.py
  start_account_monitor.sh stop_account_monitor.sh
  start_trade_engine.sh stop_trade_engine.sh
  start_fr_persist_manager.sh stop_fr_persist_manager.sh
  process_match_lib.sh okx_swap_open_orders.py
  binance_cancel_all_std_um_ws_orders.py binance_cancel_all_unified_open_orders.py
  binance_local_ip.py sell_margin_spot.py
  sync_exec_risk_params.py print_exec_risk_params.py
  sync_exec_max_pos_u.py print_exec_max_pos_u.py
)
for file in "${FILES[@]}"; do install -m 755 "${ROOT_DIR}/scripts/${file}" "${TARGET_DIR}/scripts/${file}"; done
for file in start_spread_pbs.sh stop_spread_pbs.sh; do
  install -m 755 "${ROOT_DIR}/scripts/spread_pbs/${file}" "${SPREAD_DIR}/scripts/${file}"
done
if [[ ! -f "${TARGET_DIR}/config/iceoryx2.toml" ]]; then
  install -m 644 "${ROOT_DIR}/config/iceoryx2.toml" "${TARGET_DIR}/config/iceoryx2.toml"
fi
if [[ ! -f "${TARGET_DIR}/config/persist_sync_distribution.toml" ]]; then
  install -m 644 "${ROOT_DIR}/config/persist_sync_distribution.toml" "${TARGET_DIR}/config/persist_sync_distribution.toml"
fi
if [[ ! -f "${SPREAD_ROOT}/config/iceoryx2.toml" ]]; then
  install -m 644 "${ROOT_DIR}/config/iceoryx2.toml" "${SPREAD_ROOT}/config/iceoryx2.toml"
fi
if [[ ! -f "${SPREAD_ROOT}/config/mkt_cfg.yaml" ]]; then
  install -m 644 "${ROOT_DIR}/config/mkt_cfg.yaml" "${SPREAD_ROOT}/config/mkt_cfg.yaml"
fi
if [[ ! -f "${HOST_CONFIG_DIR}/mkt_cfg.yaml" ]]; then
  install -m 644 "${ROOT_DIR}/config/mkt_cfg.yaml" "${HOST_CONFIG_DIR}/mkt_cfg.yaml"
fi
if [[ ! -f "${TARGET_DIR}/env.sh" ]]; then
  cat >"${TARGET_DIR}/env.sh" <<ENV
#!/usr/bin/env bash
export IPC_NAMESPACE="${ENV_NAME}"
export EXEC_VENUE="${VENUE}"
export VENUE="${VENUE}"
export EXEC_INSTANCE="${INSTANCE_SUFFIX}"
export EXEC_VIZ_PORT="${VIZ_PORT}"
export EXEC_CONFIG_PORT="${CONFIG_PORT}"
export PYTHON_BIN="\${PYTHON_BIN:-\${HOME}/.venvs/default/bin/python}"

# Add the ${EXCHANGE} account credentials and account-mode settings before starting.
ENV
  chmod 600 "${TARGET_DIR}/env.sh"
fi
if [[ ! -f "${TARGET_DIR}/config/exec_viz.toml" ]]; then
  cat >"${TARGET_DIR}/config/exec_viz.toml" <<TOML
[[servers]]
[servers.http]
bind = "127.0.0.1"
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
BIND=127.0.0.1
PORT=${CONFIG_PORT}
ENV_NAME=${ENV_NAME}
VENUE=${VENUE}
REDIS_URL=redis://127.0.0.1:6379/0
DASHBOARD_URL=http://127.0.0.1:${VIZ_PORT}/
ENV
fi
echo "[INFO] Deployed complete Exec runtime to ${TARGET_DIR}"
echo "[INFO] Deployed spread_pbs venue to ${SPREAD_DIR}"
echo "[INFO] Ports: viz=${VIZ_PORT} config=${CONFIG_PORT} instance=${INSTANCE_SUFFIX}"
echo "[INFO] Processes were not started"
