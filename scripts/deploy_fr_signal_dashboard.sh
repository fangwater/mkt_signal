#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="fr_signal_dashboard"
BIN_PATH="${ROOT_DIR}/target/release/${BIN_NAME}"

ENV_NAME=""
EXCHANGE=""
VIZ_PORT=""
DASH_PORT_ARG=""
BIND="0.0.0.0"
WS_PATH="/ws"
DO_BUILD=1
DO_START=1

usage() {
  cat <<'EOF'
Usage:
  scripts/deploy_fr_signal_dashboard.sh --env-name <exchange>_fr_<suffix>
                                        --viz-port <viz_server_port>
                                        [--dashboard-port <port>]
                                        [--exchange <binance|okex|gate|bybit|bitget|hyperliquid>]
                                        [--bind 0.0.0.0]
                                        [--ws-path /ws]
                                        [--bin-only|--scripts-only]
                                        [--no-start]

Notes:
  - Per-env deploy under $HOME/<env-name>/, parallel to viz_server.
  - --dashboard-port is the source of truth. If omitted, falls back to
    viz_port + 10 for backward compat; new wrappers SHOULD pass it
    explicitly because viz_port ranges across exchanges can overlap
    (e.g. gate viz 20121 + 10 = 20131 collides with binance arb viz).
  - PM2 namespace = env-name; proc_name = ${env_name}_fr_signal_dashboard.
  - Upserts FR_DASHBOARD_* managed block into env.sh (no separate dashboard.env).
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
    --dashboard-port)
      DASH_PORT_ARG="${2:-}"
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

require_fr_env_name() {
  local exchange="$1"
  local name="$2"
  if [[ ! "$name" =~ ^${exchange}_fr_[a-z0-9][a-z0-9_-]*$ ]]; then
    echo "[ERROR] env-name must match ${exchange}_fr_<suffix> (got: ${name})" >&2
    exit 1
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
require_fr_env_name "$EXCHANGE" "$ENV_NAME"

if [[ -n "$DASH_PORT_ARG" ]]; then
  if [[ ! "$DASH_PORT_ARG" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] --dashboard-port must be numeric: $DASH_PORT_ARG" >&2
    exit 1
  fi
  PORT="$DASH_PORT_ARG"
else
  PORT=$((VIZ_PORT + 10))
fi
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

# env.sh upsert happens on the target host (rsync excludes env.sh). Emit a
# snippet here so the wrapper (or an operator running deploy on-host) can
# apply it later. fr_remote_upsert_dashboard_env_sh in fr_remote_deploy.sh
# consumes the FR_DASHBOARD_* values via its own args.
SNIPPET_FILE="${TARGET_DIR}/.fr_dashboard_env.snippet"
{
  echo "# BEGIN managed: fr_signal_dashboard"
  echo "export FR_DASHBOARD_EXCHANGE=\"${EXCHANGE}\""
  echo "export FR_DASHBOARD_BIND=\"${BIND}\""
  echo "export FR_DASHBOARD_PORT=\"${PORT}\""
  echo "export FR_DASHBOARD_WS_PATH=\"${WS_PATH}\""
  echo "# END managed: fr_signal_dashboard"
} > "${SNIPPET_FILE}"

# Drop deprecated dashboard.env (config now lives in env.sh on the host).
rm -f "${TARGET_DIR}/dashboard.env"

# If env.sh exists locally (operator running deploy on the target host),
# apply the snippet directly so the dev/test loop stays self-contained.
ENV_FILE="${TARGET_DIR}/env.sh"
if [[ -f "${ENV_FILE}" ]]; then
  TMP_FILE="$(mktemp)"
  awk -v begin="# BEGIN managed: fr_signal_dashboard" \
      -v end="# END managed: fr_signal_dashboard" \
      -v exchange="${EXCHANGE}" -v bind="${BIND}" -v port="${PORT}" -v ws_path="${WS_PATH}" '
    function emit() {
      print begin
      print "export FR_DASHBOARD_EXCHANGE=\"" exchange "\""
      print "export FR_DASHBOARD_BIND=\"" bind "\""
      print "export FR_DASHBOARD_PORT=\"" port "\""
      print "export FR_DASHBOARD_WS_PATH=\"" ws_path "\""
      print end
    }
    BEGIN { in_block = 0; replaced = 0 }
    $0 == begin { in_block = 1; replaced = 1; next }
    in_block && $0 == end { in_block = 0; emit(); next }
    in_block { next }
    { print }
    END { if (!replaced) { print ""; emit() } }
  ' "${ENV_FILE}" > "${TMP_FILE}"
  mv "${TMP_FILE}" "${ENV_FILE}"
  chmod 600 "${ENV_FILE}"
  echo "[INFO] env.sh: managed block 'fr_signal_dashboard' upserted in ${ENV_FILE}"
else
  echo "[INFO] env.sh missing locally (expected when running deploy via remote-rsync wrapper);"
  echo "       snippet written to ${SNIPPET_FILE}; wrapper must upsert to remote env.sh."
fi

echo "[INFO] deployed ${BIN_NAME} to ${TARGET_DIR} (port=${PORT}, viz_port=${VIZ_PORT})"
echo "[INFO] PM2: namespace=${PM2_NS} name=${PM2_NAME}"

if [[ "$DO_START" -eq 1 ]]; then
  (
    cd "${TARGET_DIR}"
    ./start_fr_signal_dashboard.sh
  )
else
  echo "[INFO] start manually: cd ${TARGET_DIR} && ./start_fr_signal_dashboard.sh"
fi
