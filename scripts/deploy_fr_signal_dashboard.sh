#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="fr_signal_dashboard"
BIN_PATH="${ROOT_DIR}/target/release/${BIN_NAME}"

TARGET_DIR="/ubuntu/dashboard"
EXCHANGE=""
BIND="0.0.0.0"
PORT="6305"
WS_PATH="/ws"
DO_BUILD=1
DO_START=1

usage() {
  cat <<'EOF'
Usage:
  scripts/deploy_fr_signal_dashboard.sh --exchange <binance|okex|gate|bybit|bitget|hyperliquid>
                                        [--target /ubuntu/dashboard]
                                        [--bind 0.0.0.0]
                                        [--port 6305]
                                        [--ws-path /ws]
                                        [--bin-only|--scripts-only]
                                        [--no-start]

Notes:
  - Deploys the standalone FR dashboard into /ubuntu/dashboard by default.
  - Generates dashboard.env so PM2 startup does not rely on directory-name inference.
  - Starts the service with PM2 by default on port 6305.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    --target)
      TARGET_DIR="${2:-}"
      shift 2
      ;;
    --bind)
      BIND="${2:-0.0.0.0}"
      shift 2
      ;;
    --port)
      PORT="${2:-6305}"
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

EXCHANGE="$(normalize_exchange "$EXCHANGE")"
case "$EXCHANGE" in
  binance|okex|gate|bybit|bitget|hyperliquid)
    ;;
  *)
    echo "[ERROR] --exchange is required and must be a supported venue family" >&2
    usage >&2
    exit 1
    ;;
esac

if [[ ! "$PORT" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --port must be numeric: $PORT" >&2
  exit 1
fi

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
PM2_NAMESPACE=dashboard
PM2_NAME=fr_signal_dashboard
RUST_LOG=info
EOF

echo "[INFO] deployed ${BIN_NAME} to ${TARGET_DIR}"
echo "[INFO] config: ${TARGET_DIR}/dashboard.env"

if [[ "$DO_START" -eq 1 ]]; then
  (
    cd "${TARGET_DIR}"
    ./start_fr_signal_dashboard.sh
  )
else
  echo "[INFO] start manually: cd ${TARGET_DIR} && ./start_fr_signal_dashboard.sh"
fi
