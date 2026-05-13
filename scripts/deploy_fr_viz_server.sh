#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="viz_server"

usage() {
  cat <<'EOF'
Usage: scripts/deploy_fr_viz_server.sh --env-name <exchange>_fr_<suffix>
                                      [--exchange <binance|okex|gate|bybit|bitget>]
                                      [--bind 0.0.0.0] --port <port>
                                      [--dashboard-port <port>]
                                      [--ws-path /ws]
                                      [--namespace <IPC_NAMESPACE>]
                                      [--instance-label <label>]
                                      [--jobs <n>] [--cargo-target-dir <path>]
                                      [--nginx-prefix /fr/<env-name>] [--nginx-port 4191]
                                      [--nginx-mapping-file $HOME/nginx_locations.txt]
                                      [--apply-nginx]
                                      [--scripts-only|--bin-only]
                                      [--no-dashboard]

Notes:
  - exchange 可省略，会从 --env-name 推断（格式如 binance_fr_hf01）。
  - Deploys viz_server into $HOME/<env-name>/.
  - Generates config/viz.toml with pre_trade resample only.
  - Copies docs/pre_trade_dashboard.html into www/ and index.html.
  - env-name 必须匹配 <exchange>_fr_<suffix>（suffix 必填，例如 binance_fr_hf01）。
  - --port 必填（不再按 exchange 自动补默认端口）。
  - --dashboard-port 是 fr_signal_dashboard 的监听端口；不传则回退到 viz-port+10
    （仅向后兼容；新的入口脚本应该显式传入，因为跨交易所 viz 段可能重叠）。
  - Default nginx prefix follows deploy dir name: /fr/<env-name> (e.g. /fr/binance_fr_trade).
  - Updates nginx mapping file with static + ws + healthz entries (managed block).
  - Also runs scripts/deploy_fr_signal_dashboard.sh to deploy
    fr_signal_dashboard alongside viz_server. Pass --no-dashboard to skip.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

EXCHANGE=""
ENV_NAME=""
DASH_PORT_ARG=""

BIND="0.0.0.0"
PORT=""
WS_PATH="/ws"

IPC_NS_OVERRIDE=""
INSTANCE_LABEL=""

CARGO_TARGET_DIR_OVERRIDE=""
BUILD_JOBS=""

DO_BUILD=1
DO_SCRIPTS=1
ONLY_MODE=""

NGINX_PREFIX=""
NGINX_PORT="4191"
NGINX_MAPPING_FILE=""
APPLY_NGINX="0"

DEPLOY_DASHBOARD=1

normalize_exchange() {
  local ex="${1,,}"
  if [[ "$ex" == "okx" ]]; then
    ex="okex"
  fi
  echo "$ex"
}

normalize_env_name() {
  echo "$1" | tr 'A-Z' 'a-z'
}

require_fr_env_name() {
  local exchange="$1"
  local name="$2"
  if [[ ! "$name" =~ ^${exchange}_fr_[a-z0-9][a-z0-9_-]*$ ]]; then
    echo "[ERROR] env-name must match ${exchange}_fr_<suffix> (got: ${name})" >&2
    exit 1
  fi
}

infer_exchange_from_env_name() {
  local name="${1,,}"
  if [[ "$name" =~ ^([a-z0-9]+)[-_]fr([_-].*)?$ ]]; then
    echo "${BASH_REMATCH[1]}"
  fi
}

upsert_main_nginx_mapping() {
  local main_file begin_marker end_marker tmp
  if [[ -z "${NGINX_MAPPING_FILE}" ]]; then
    NGINX_MAPPING_FILE="$HOME/nginx_locations.txt"
  fi
  main_file="${NGINX_MAPPING_FILE}"

  if [[ ! -f "$main_file" ]]; then
    if [[ -f "${ROOT_DIR}/config/nginx_locations.txt" ]]; then
      mkdir -p "$(dirname "$main_file")" >/dev/null 2>&1 || true
      cp "${ROOT_DIR}/config/nginx_locations.txt" "$main_file"
      echo "[INFO] Initialized nginx mapping file: $main_file (from ${ROOT_DIR}/config/nginx_locations.txt)"
    else
      echo "[ERROR] Missing nginx mapping file: $main_file" >&2
      echo "[ERROR] Also missing template: ${ROOT_DIR}/config/nginx_locations.txt" >&2
      exit 1
    fi
  fi

  if [[ "${NGINX_PREFIX}" != /* ]]; then
    echo "[ERROR] --nginx-prefix must start with /: ${NGINX_PREFIX}" >&2
    exit 1
  fi

  local base_prefix="${NGINX_PREFIX%/}"
  local static_prefix="${base_prefix}/"
  local ws_path="${WS_PATH}"
  if [[ "$ws_path" != /* ]]; then
    ws_path="/${ws_path}"
  fi
  local ws_location="${base_prefix}${ws_path}"
  local health_location="${base_prefix}/healthz"
  local snapshot_location="${base_prefix}/snapshot"
  local fr_ws_location="${base_prefix}/fr_ws"
  local static_dir="${TARGET_DIR}/www/"
  local fr_port="${DASH_PORT_ARG:-$((PORT + 10))}"

  begin_marker="# BEGIN managed: fr viz ${base_prefix}"
  end_marker="# END managed: fr viz ${base_prefix}"

  if grep -Fqx "$begin_marker" "$main_file" && ! grep -Fqx "$end_marker" "$main_file"; then
    echo "[ERROR] nginx_locations.txt has begin marker but missing end marker:" >&2
    echo "        ${begin_marker}" >&2
    echo "        (please fix the block manually and retry)" >&2
    exit 1
  fi

  tmp="$(mktemp)"
  awk -v begin="$begin_marker" \
      -v end="$end_marker" \
      -v prefix="$base_prefix" \
      -v static_prefix="$static_prefix" \
      -v static_dir="$static_dir" \
      -v ws_location="$ws_location" \
      -v health_location="$health_location" \
      -v snapshot_location="$snapshot_location" \
      -v fr_ws_location="$fr_ws_location" \
      -v port="$PORT" \
      -v fr_port="$fr_port" \
      -v ws_path="$ws_path" '
    BEGIN { in_block = 0; replaced = 0 }
    $0 == begin { in_block = 1; replaced = 1; next }
    in_block && $0 == end {
        in_block = 0;
        print begin;
        print "# fr pre_trade dashboard (static) + viz_server (WS/healthz/snapshot) + fr_signal_dashboard (fr_ws)";
        print static_prefix " static:" static_dir;
        print ws_location " http://127.0.0.1:" port ws_path;
        print health_location " http://127.0.0.1:" port "/healthz";
        print snapshot_location " http://127.0.0.1:" port "/snapshot";
        print fr_ws_location " http://127.0.0.1:" fr_port "/ws";
        print end;
        next
    }
    in_block { next }
    {
        if (substr($0, 1, length(prefix)) == prefix && substr($0, length(prefix) + 1, 1) ~ /[[:space:]]/) {
            next
        }
        if (substr($0, 1, length(prefix) + 1) == (prefix "/") && substr($0, length(prefix) + 2, 1) ~ /[[:space:]]/) {
            next
        }
        print
    }
    END {
        if (!replaced) {
            print "";
            print begin;
            print "# fr pre_trade dashboard (static) + viz_server (WS/healthz/snapshot) + fr_signal_dashboard (fr_ws)";
            print static_prefix " static:" static_dir;
            print ws_location " http://127.0.0.1:" port ws_path;
            print health_location " http://127.0.0.1:" port "/healthz";
            print snapshot_location " http://127.0.0.1:" port "/snapshot";
            print fr_ws_location " http://127.0.0.1:" fr_port "/ws";
            print end;
        }
    }
  ' "$main_file" >"$tmp"
  mv "$tmp" "$main_file"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    --env-name)
      ENV_NAME="${2:-}"
      shift 2
      ;;
    --bind)
      BIND="${2:-0.0.0.0}"
      shift 2
      ;;
    --port)
      PORT="${2:-}"
      shift 2
      ;;
    --dashboard-port)
      DASH_PORT_ARG="${2:-}"
      shift 2
      ;;
    --ws-path)
      WS_PATH="${2:-/ws}"
      shift 2
      ;;
    --namespace)
      IPC_NS_OVERRIDE="${2:-}"
      shift 2
      ;;
    --instance-label)
      INSTANCE_LABEL="${2:-}"
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
    --nginx-prefix)
      NGINX_PREFIX="${2:-}"
      shift 2
      ;;
    --nginx-port)
      NGINX_PORT="${2:-4191}"
      shift 2
      ;;
    --nginx-mapping-file)
      NGINX_MAPPING_FILE="${2:-}"
      shift 2
      ;;
    --apply-nginx)
      APPLY_NGINX="1"
      shift
      ;;
    --scripts-only)
      if [[ -n "$ONLY_MODE" ]]; then
        echo "[ERROR] --scripts-only and --bin-only are mutually exclusive"
        exit 1
      fi
      ONLY_MODE="scripts"
      DO_BUILD=0
      DO_SCRIPTS=1
      shift
      ;;
    --bin-only)
      if [[ -n "$ONLY_MODE" ]]; then
        echo "[ERROR] --scripts-only and --bin-only are mutually exclusive"
        exit 1
      fi
      ONLY_MODE="bin"
      DO_BUILD=1
      DO_SCRIPTS=0
      shift
      ;;
    --no-dashboard)
      DEPLOY_DASHBOARD=0
      shift
      ;;
    *)
      echo "[ERROR] Unknown arg: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$ENV_NAME" ]]; then
  echo "[ERROR] --env-name is required (e.g. binance_fr_hf01)"
  usage
  exit 1
fi
ENV_NAME="$(normalize_env_name "$ENV_NAME")"

if [[ -z "$EXCHANGE" ]]; then
  EXCHANGE="$(infer_exchange_from_env_name "$ENV_NAME")"
fi
EXCHANGE="$(normalize_exchange "$EXCHANGE")"
case "$EXCHANGE" in
  binance|okex|gate|bybit|bitget) ;;
  *)
    echo "[ERROR] Unsupported exchange: $EXCHANGE"
    exit 1
    ;;
esac

require_fr_env_name "$EXCHANGE" "$ENV_NAME"

if [[ -z "$INSTANCE_LABEL" ]]; then
  INSTANCE_LABEL="$EXCHANGE"
fi

if [[ -z "$PORT" ]]; then
  echo "[ERROR] --port is required for fr viz deployment"
  usage
  exit 1
fi
if [[ ! "$PORT" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --port must be a number: $PORT"
  exit 1
fi

if [[ "$WS_PATH" != /* ]]; then
  WS_PATH="/$WS_PATH"
fi

TARGET_DIR="$HOME/${ENV_NAME}"
mkdir -p "$TARGET_DIR"
DEPLOY_DIR_NAME="$(basename "$TARGET_DIR")"

if [[ -z "$NGINX_PREFIX" ]]; then
  NGINX_PREFIX="/fr/${DEPLOY_DIR_NAME}"
fi
if [[ -z "$NGINX_MAPPING_FILE" ]]; then
  NGINX_MAPPING_FILE="$HOME/nginx_locations.txt"
fi

IPC_NAMESPACE=""
ENV_FILE="$TARGET_DIR/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  IPC_NAMESPACE="${IPC_NAMESPACE:-}"
fi
IPC_NAMESPACE="${IPC_NS_OVERRIDE:-${IPC_NAMESPACE:-}}"
if [[ -z "$IPC_NAMESPACE" ]]; then
  IPC_NAMESPACE="${ENV_NAME}"
  echo "[WARN] IPC_NAMESPACE not set (env.sh missing or empty). Using default: ${IPC_NAMESPACE}"
fi

CARGO_TARGET_DIR_EFFECTIVE="$ROOT_DIR/target"
if [[ -n "$CARGO_TARGET_DIR_OVERRIDE" ]]; then
  if [[ "$CARGO_TARGET_DIR_OVERRIDE" == /* ]]; then
    CARGO_TARGET_DIR_EFFECTIVE="$CARGO_TARGET_DIR_OVERRIDE"
  else
    CARGO_TARGET_DIR_EFFECTIVE="$ROOT_DIR/$CARGO_TARGET_DIR_OVERRIDE"
  fi
fi

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] Building $BIN_NAME (release)"
  (
    cd "$ROOT_DIR"
    if [[ -n "$CARGO_TARGET_DIR_OVERRIDE" ]]; then
      CARGO_TARGET_DIR="$CARGO_TARGET_DIR_EFFECTIVE" \
        cargo build --release --bin "$BIN_NAME" ${BUILD_JOBS:+--jobs "$BUILD_JOBS"}
    else
      cargo build --release --bin "$BIN_NAME" ${BUILD_JOBS:+--jobs "$BUILD_JOBS"}
    fi
  )
fi

BIN_PATH="$CARGO_TARGET_DIR_EFFECTIVE/release/$BIN_NAME"

if [[ "$DO_BUILD" -eq 1 ]]; then
  if [[ ! -f "$BIN_PATH" ]]; then
    echo "[ERROR] Binary not found: $BIN_PATH"
    exit 1
  fi
  echo "[INFO] Deploying $BIN_NAME to $TARGET_DIR"
  cp "$BIN_PATH" "$TARGET_DIR/"
  chmod +x "$TARGET_DIR/$BIN_NAME"
fi

emit_server_block() {
  local bind="$1"
  local port="$2"
  local ws_path="$3"

  cat <<EOF
[[servers]]
namespaces = ["$IPC_NAMESPACE"]
[servers.http]
bind = "$bind"
port = $port
ws_path = "$ws_path"
[servers.pre_trade]
enabled = true
EOF

  local exposure_ch="pre_trade_exposure"
  local risk_ch="pre_trade_risk"

  cat <<EOF
[[servers.pre_trade.instances]]
label = "$INSTANCE_LABEL"
exposure_channel = "$exposure_ch"
risk_channel = "$risk_ch"
EOF

}

if [[ "$DO_SCRIPTS" -eq 1 ]]; then
  mkdir -p "$TARGET_DIR/config" "$TARGET_DIR/www" "$TARGET_DIR/scripts"

  echo "[INFO] Writing viz config: $TARGET_DIR/config/viz.toml (namespace=$IPC_NAMESPACE)"
  emit_server_block "$BIND" "$PORT" "$WS_PATH" > "$TARGET_DIR/config/viz.toml"

  if [[ -f "$ROOT_DIR/docs/fr_pre_trade_dashboard.html" ]]; then
    cp "$ROOT_DIR/docs/fr_pre_trade_dashboard.html" "$TARGET_DIR/www/pre_trade_dashboard.html"
    cp "$ROOT_DIR/docs/fr_pre_trade_dashboard.html" "$TARGET_DIR/www/index.html"
    echo "[INFO] Synced dashboard: $TARGET_DIR/www/pre_trade_dashboard.html (FR 二合一)"
  elif [[ -f "$ROOT_DIR/docs/pre_trade_dashboard.html" ]]; then
    cp "$ROOT_DIR/docs/pre_trade_dashboard.html" "$TARGET_DIR/www/pre_trade_dashboard.html"
    cp "$ROOT_DIR/docs/pre_trade_dashboard.html" "$TARGET_DIR/www/index.html"
    echo "[INFO] Synced dashboard: $TARGET_DIR/www/pre_trade_dashboard.html (通用回退)"
  else
    echo "[WARN] Missing dashboard: $ROOT_DIR/docs/fr_pre_trade_dashboard.html"
  fi

  EXTRA_FILES=(
    "scripts/start_fr_viz_server.sh"
    "scripts/stop_fr_viz_server.sh"
    "scripts/process_match_lib.sh"
    "scripts/setup_nginx_4191.sh"
  )
  for file in "${EXTRA_FILES[@]}"; do
    SRC="$ROOT_DIR/$file"
    if [[ -f "$SRC" ]]; then
      rsync -a "$SRC" "$TARGET_DIR/$(dirname "$file")/"
      chmod +x "$TARGET_DIR/$file" 2>/dev/null || true
    else
      echo "[WARN] missing: $SRC"
    fi
  done

  upsert_main_nginx_mapping
  if [[ "${APPLY_NGINX}" == "1" ]]; then
    echo "[INFO] Applying nginx config (PORT=${NGINX_PORT}, MAPPING_FILE=${NGINX_MAPPING_FILE})"
    (
      cd "$ROOT_DIR"
      PORT="${NGINX_PORT}" MAPPING_FILE="${NGINX_MAPPING_FILE}" scripts/setup_nginx_4191.sh
    )
  fi
fi

if [[ "$DEPLOY_DASHBOARD" -eq 1 ]]; then
  DASHBOARD_SCRIPT="$ROOT_DIR/scripts/deploy_fr_signal_dashboard.sh"
  if [[ ! -x "$DASHBOARD_SCRIPT" ]]; then
    echo "[ERROR] companion deploy script not executable: $DASHBOARD_SCRIPT" >&2
    exit 1
  fi
  DASHBOARD_PORT="${DASH_PORT_ARG:-$((PORT + 10))}"
  DASHBOARD_ARGS=(
    --env-name "$ENV_NAME"
    --exchange "$EXCHANGE"
    --viz-port "$PORT"
    --dashboard-port "$DASHBOARD_PORT"
    --bind "$BIND"
    --ws-path "$WS_PATH"
    --no-start
  )
  if [[ "$DO_BUILD" -eq 0 ]]; then
    DASHBOARD_ARGS+=( --scripts-only )
  fi
  echo ""
  echo "[INFO] Deploying fr_signal_dashboard companion (port=${DASHBOARD_PORT})"
  "$DASHBOARD_SCRIPT" "${DASHBOARD_ARGS[@]}"
fi

echo ""
echo "[INFO] viz_server deployed: $TARGET_DIR"
echo "[INFO] Start: cd $TARGET_DIR && ./scripts/start_fr_viz_server.sh"
echo "[INFO] Stop:  cd $TARGET_DIR && ./scripts/stop_fr_viz_server.sh"
if [[ "$DEPLOY_DASHBOARD" -eq 1 ]]; then
  echo "[INFO] fr_signal_dashboard: cd $TARGET_DIR && ./start_fr_signal_dashboard.sh / ./stop_fr_signal_dashboard.sh"
fi

if [[ "$DO_SCRIPTS" -eq 1 ]]; then
  echo "[INFO] nginx mapping updated: ${NGINX_MAPPING_FILE}"
  echo "[INFO] To apply nginx (port ${NGINX_PORT}):"
  echo "       cd ${ROOT_DIR} && PORT=${NGINX_PORT} MAPPING_FILE=${NGINX_MAPPING_FILE} scripts/setup_nginx_4191.sh"
fi
