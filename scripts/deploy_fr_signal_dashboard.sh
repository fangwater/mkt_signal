#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="fr_signal_dashboard"
BIN_PATH="${ROOT_DIR}/target/release/${BIN_NAME}"

ENV_NAME=""
EXCHANGE=""
VIZ_PORT=""
BIND="0.0.0.0"
WS_PATH="/ws"
DO_BUILD=1
DO_START=1

usage() {
  cat <<'EOF'
Usage:
  scripts/deploy_fr_signal_dashboard.sh --env-name <exchange>_fr_<suffix>
                                        --viz-port <viz_server_port>
                                        [--exchange <binance|okex|gate|bybit|bitget|hyperliquid>]
                                        [--bind 0.0.0.0]
                                        [--ws-path /ws]
                                        [--bin-only|--scripts-only]
                                        [--no-start]

Notes:
  - Per-env deploy under $HOME/<env-name>/, parallel to viz_server.
  - fr_signal_dashboard port = viz-port + 1 (hardcoded convention,
    matched by deploy_fr_viz_server.sh nginx upsert).
  - PM2 namespace = env-name; proc_name = ${env_name}_fr_signal_dashboard.
  - Generates dashboard.env for start_fr_signal_dashboard.sh consumption.
  - --exchange optional; inferred from env-name prefix if omitted.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-name)
      ENV_NAME="${2:-}"
      shift 2
      ;;
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    --viz-port)
      VIZ_PORT="${2:-}"
      shift 2
      ;;
    --bind)
      BIND="${2:-0.0.0.0}"
      shift 2
      ;;
    --ws-path)
      WS_PATH="${2:-/ws}"
      shift 2
      ;;
    --bin-only)
      DO_BUILD=1
      shift
      ;;
    --scripts-only)
      DO_BUILD=0
      shift
      ;;
    --no-start)
      DO_START=0
      shift
      ;;
    *)
      echo "[ERROR] unknown arg: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

normalize_exchange() {
  local ex="${1,,}"
  if [[ "$ex" == "okx" ]]; then
    ex="okex"
  fi
  echo "$ex"
}

infer_exchange_from_env_name() {
  local name="${1,,}"
  if [[ "$name" =~ ^([a-z0-9]+)[-_]fr([_-].*)?$ ]]; then
    echo "${BASH_REMATCH[1]}"
  fi
}

if [[ -z "$ENV_NAME" ]]; then
  echo "[ERROR] --env-name is required (e.g. binance_fr_trade01)" >&2
  usage >&2
  exit 1
fi
ENV_NAME="${ENV_NAME,,}"

if [[ -z "$VIZ_PORT" ]]; then
  echo "[ERROR] --viz-port is required (matches deploy_fr_viz_server.sh --port)" >&2
  usage >&2
  exit 1
fi
if [[ ! "$VIZ_PORT" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --viz-port must be numeric: $VIZ_PORT" >&2
  exit 1
fi

if [[ -z "$EXCHANGE" ]]; then
  EXCHANGE="$(infer_exchange_from_env_name "$ENV_NAME")"
fi
EXCHANGE="$(normalize_exchange "$EXCHANGE")"
case "$EXCHANGE" in
  binance|okex|gate|bybit|bitget|hyperliquid)
    ;;
  *)
    echo "[ERROR] unable to determine exchange (got '${EXCHANGE}'); pass --exchange explicitly" >&2
    exit 1
    ;;
esac

PORT=$((VIZ_PORT + 1))
TARGET_DIR="${HOME}/${ENV_NAME}"
PM2_NS="${ENV_NAME}"
PM2_NAME="${ENV_NAME}_fr_signal_dashboard"

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] building ${BIN_NAME} (release)"
  cargo build --release --bin "${BIN_NAME}"
fi

mkdir -p "${TARGET_DIR}"

if [[ "$DO_BUILD" -eq 1 ]]; then
  cp -f "${BIN_PATH}" "${TARGET_DIR}/${BIN_NAME}"
  chmod +x "${TARGET_DIR}/${BIN_NAME}"
fi

cp -f "${ROOT_DIR}/scripts/start_fr_signal_dashboard.sh" "${TARGET_DIR}/start_fr_signal_dashboard.sh"
cp -f "${ROOT_DIR}/scripts/stop_fr_signal_dashboard.sh" "${TARGET_DIR}/stop_fr_signal_dashboard.sh"
chmod +x "${TARGET_DIR}/start_fr_signal_dashboard.sh" "${TARGET_DIR}/stop_fr_signal_dashboard.sh"

cat > "${TARGET_DIR}/dashboard.env" <<EOF
FR_DASHBOARD_EXCHANGE=${EXCHANGE}
FR_DASHBOARD_BIND=${BIND}
FR_DASHBOARD_PORT=${PORT}
FR_DASHBOARD_WS_PATH=${WS_PATH}
PM2_NAMESPACE=${PM2_NS}
PM2_NAME=${PM2_NAME}
RUST_LOG=info
EOF

echo "[INFO] deployed ${BIN_NAME} to ${TARGET_DIR} (port=${PORT}, viz_port=${VIZ_PORT})"
echo "[INFO] config: ${TARGET_DIR}/dashboard.env"
echo "[INFO] PM2: namespace=${PM2_NS} name=${PM2_NAME}"

if [[ "$DO_START" -eq 1 ]]; then
  (
    cd "${TARGET_DIR}"
    ./start_fr_signal_dashboard.sh
  )
else
  echo "[INFO] start manually: cd ${TARGET_DIR} && ./start_fr_signal_dashboard.sh"
fi
