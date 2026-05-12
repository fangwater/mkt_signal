#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="viz_server"

# shellcheck source=scripts/deploy_cross_lib.sh
source "$ROOT_DIR/scripts/deploy_cross_lib.sh"

usage() {
  cat <<'EOF'
用法: scripts/deploy_cross_viz_server.sh --open-venue <okex-futures> --hedge-venue <binance-futures>
                                        [--env-suffix cross-trade]
                                        [--env-name okex-binance-cross-trade]
                                        [--bind 0.0.0.0] [--ws-path /ws]
                                        [--namespace <IPC_NAMESPACE>]
                                        [--nginx-prefix /cross/<env-name>]
                                        [--nginx-port 4191]
                                        [--nginx-mapping-file $HOME/nginx_locations.txt]
                                        [--apply-nginx]
                                        [--resample-suffix <suffix>] [--instance-label <label>]
                                        [--jobs <n>] [--cargo-target-dir <path>]

说明:
  - 跨所合约对：构建并部署 viz_server 到 $HOME/<open>-<hedge>-<env_suffix>/
  - 在目标目录生成 config/viz.toml（订阅 pre_trade resample exposure/risk 通道）
  - port 由 env-suffix 硬编码映射决定（cross-trade → 10211, cross-arb01..03 → 10251..10253）

启动/停止（在目标目录）:
  source ./env.sh
  ./cross_scripts/start_cross_viz_server.sh
  ./cross_scripts/stop_cross_viz_server.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage; exit 0
fi

ENV_SUFFIX="cross-trade"
ENV_NAME=""
OPEN_VENUE=""
HEDGE_VENUE=""
BIND="0.0.0.0"
WS_PATH="/ws"
IPC_NS_OVERRIDE=""
RESAMPLE_SUFFIX=""
INSTANCE_LABEL="cross"
CARGO_TARGET_DIR_OVERRIDE=""
BUILD_JOBS=""
NGINX_PREFIX=""
NGINX_PORT="4191"
NGINX_MAPPING_FILE=""
APPLY_NGINX="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-suffix) ENV_SUFFIX="${2:-cross-trade}"; shift 2 ;;
    --env-name)   ENV_NAME="${2:-}"; shift 2 ;;
    --open-venue) OPEN_VENUE="${2:-}"; shift 2 ;;
    --hedge-venue) HEDGE_VENUE="${2:-}"; shift 2 ;;
    --bind)       BIND="${2:-0.0.0.0}"; shift 2 ;;
    --port)
      echo "[ERROR] --port 已移除，port 由 env-suffix 硬编码映射决定"
      exit 1 ;;
    --ws-path)    WS_PATH="${2:-/ws}"; shift 2 ;;
    --namespace)  IPC_NS_OVERRIDE="${2:-}"; shift 2 ;;
    --nginx-prefix) NGINX_PREFIX="${2:-}"; shift 2 ;;
    --nginx-port)   NGINX_PORT="${2:-4191}"; shift 2 ;;
    --nginx-mapping-file) NGINX_MAPPING_FILE="${2:-}"; shift 2 ;;
    --apply-nginx) APPLY_NGINX="1"; shift ;;
    --resample-suffix) RESAMPLE_SUFFIX="${2:-}"; shift 2 ;;
    --instance-label)  INSTANCE_LABEL="${2:-cross}"; shift 2 ;;
    --jobs)        BUILD_JOBS="${2:-}"; shift 2 ;;
    --cargo-target-dir) CARGO_TARGET_DIR_OVERRIDE="${2:-}"; shift 2 ;;
    *)
      echo "[ERROR] 未知参数: $1"; usage; exit 1 ;;
  esac
done

resolve_port_by_suffix() {
  case "$1" in
    cross-trade) echo "10211" ;;
    cross-arb01) echo "10251" ;;
    cross-arb02) echo "10252" ;;
    cross-arb03) echo "10253" ;;
    *) echo "[ERROR] 未知的 env-suffix: $1，无法确定 port" >&2; exit 1 ;;
  esac
}
PORT="$(resolve_port_by_suffix "$ENV_SUFFIX")"

if [[ -n "$ENV_NAME" && ( -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ) ]]; then
  if inferred="$(cross_infer_pair_from_name "$ENV_NAME")" && [[ -n "$inferred" ]]; then
    OPEN_VENUE="${OPEN_VENUE:-${inferred%%,*}-futures}"
    HEDGE_VENUE="${HEDGE_VENUE:-${inferred##*,}-futures}"
  fi
fi

if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  echo "[ERROR] 需要 --open-venue 与 --hedge-venue，或使用 --env-name <open>-<hedge>-cross-<tag>"
  usage; exit 1
fi

PAIR_RESULT="$(cross_ensure_venue_pair "$OPEN_VENUE" "$HEDGE_VENUE")"
if [[ -z "$PAIR_RESULT" ]]; then
  exit 1
fi
read -r OPEN_VENUE HEDGE_VENUE <<<"$PAIR_RESULT"

OPEN_EXCHANGE="${OPEN_VENUE%%-*}"
HEDGE_EXCHANGE="${HEDGE_VENUE%%-*}"
if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${OPEN_EXCHANGE}-${HEDGE_EXCHANGE}-${ENV_SUFFIX}"
fi
ENV_NAME="${ENV_NAME,,}"

if [[ "$WS_PATH" != /* ]]; then WS_PATH="/$WS_PATH"; fi

TARGET_DIR="$HOME/${ENV_NAME}"
mkdir -p "$TARGET_DIR"

if [[ -z "$NGINX_PREFIX" ]]; then
  NGINX_PREFIX="/cross/${ENV_NAME}"
fi
if [[ -z "$NGINX_MAPPING_FILE" ]]; then
  NGINX_MAPPING_FILE="$HOME/nginx_locations.txt"
fi

upsert_main_nginx_mapping() {
  local main_file begin_marker end_marker tmp
  main_file="${NGINX_MAPPING_FILE}"
  if [[ ! -f "$main_file" ]]; then
    if [[ -f "${ROOT_DIR}/config/nginx_locations.txt" ]]; then
      mkdir -p "$(dirname "$main_file")" >/dev/null 2>&1 || true
      cp "${ROOT_DIR}/config/nginx_locations.txt" "$main_file"
      echo "[INFO] Initialized nginx mapping file: $main_file"
    else
      echo "[ERROR] Missing nginx mapping file: $main_file" >&2
      exit 1
    fi
  fi
  if [[ "${NGINX_PREFIX}" != /* ]]; then
    echo "[ERROR] --nginx-prefix must start with /: ${NGINX_PREFIX}" >&2; exit 1
  fi
  local base_prefix="${NGINX_PREFIX%/}"
  local static_prefix="${base_prefix}/"
  local ws_location="${base_prefix}${WS_PATH}"
  local health_location="${base_prefix}/healthz"
  local snapshot_location="${base_prefix}/snapshot"
  local static_dir="${TARGET_DIR}/www/"

  begin_marker="# BEGIN managed: cross viz ${base_prefix}"
  end_marker="# END managed: cross viz ${base_prefix}"
  if grep -Fqx "$begin_marker" "$main_file" && ! grep -Fqx "$end_marker" "$main_file"; then
    echo "[ERROR] nginx_locations.txt has begin marker but missing end marker: ${begin_marker}" >&2
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
      -v port="$PORT" \
      -v ws_path="$WS_PATH" '
    BEGIN { in_block = 0; replaced = 0 }
    $0 == begin { in_block = 1; replaced = 1; next }
    in_block && $0 == end {
        in_block = 0;
        print begin;
        print "# cross pre_trade dashboard (static) + viz_server (WS/healthz/snapshot)";
        print static_prefix " static:" static_dir;
        print ws_location " http://127.0.0.1:" port ws_path;
        print health_location " http://127.0.0.1:" port "/healthz";
        print snapshot_location " http://127.0.0.1:" port "/snapshot";
        print end;
        next
    }
    in_block { next }
    {
        if (substr($0, 1, length(prefix)) == prefix && substr($0, length(prefix) + 1, 1) ~ /[[:space:]]/) { next }
        if (substr($0, 1, length(prefix) + 1) == (prefix "/") && substr($0, length(prefix) + 2, 1) ~ /[[:space:]]/) { next }
        print
    }
    END {
        if (!replaced) {
            print "";
            print begin;
            print "# cross pre_trade dashboard (static) + viz_server (WS/healthz/snapshot)";
            print static_prefix " static:" static_dir;
            print ws_location " http://127.0.0.1:" port ws_path;
            print health_location " http://127.0.0.1:" port "/healthz";
            print snapshot_location " http://127.0.0.1:" port "/snapshot";
            print end;
        }
    }
  ' "$main_file" >"$tmp"
  mv "$tmp" "$main_file"
}

IPC_NAMESPACE=""
ENV_FILE="$TARGET_DIR/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  IPC_NAMESPACE="${IPC_NAMESPACE:-}"
fi
IPC_NAMESPACE="${IPC_NS_OVERRIDE:-${IPC_NAMESPACE:-}}"
if [[ -z "$IPC_NAMESPACE" ]]; then
  ENV_SUFFIX_NS="${ENV_SUFFIX//-/_}"
  IPC_NAMESPACE="${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}_${ENV_SUFFIX_NS}"
  echo "[WARN] 未读取到 IPC_NAMESPACE 且未指定 --namespace；使用默认推断: $IPC_NAMESPACE"
  echo "[WARN] 建议先生成 env.sh: scripts/deploy_setup_env_cross.sh --env-name ${ENV_NAME} --open-venue ${OPEN_VENUE} --hedge-venue ${HEDGE_VENUE}"
fi

echo "[INFO] 构建 $BIN_NAME (release)"
CARGO_TARGET_DIR_EFFECTIVE="$(cross_effective_cargo_target_dir "$ROOT_DIR" "$CARGO_TARGET_DIR_OVERRIDE")"
(
  cd "$ROOT_DIR"
  CARGO_TARGET_DIR="$CARGO_TARGET_DIR_EFFECTIVE" \
    cargo build --release --bin "$BIN_NAME" ${BUILD_JOBS:+--jobs "$BUILD_JOBS"}
)
BIN_PATH="$(cross_bin_path_release "$CARGO_TARGET_DIR_EFFECTIVE" "$BIN_NAME")"

mkdir -p "$TARGET_DIR/config" "$TARGET_DIR/cross_scripts"
echo "[INFO] 生成 viz 配置: $TARGET_DIR/config/viz.toml (namespaces=[\"$IPC_NAMESPACE\"] port=${PORT})"

emit_server_block() {
  local exposure_ch="pre_trade_exposure"
  local risk_ch="pre_trade_risk"
  if [[ -n "$RESAMPLE_SUFFIX" ]]; then
    exposure_ch="pre_trade_exposure_${RESAMPLE_SUFFIX}"
    risk_ch="pre_trade_risk_${RESAMPLE_SUFFIX}"
  fi

  cat <<EOF
[[servers]]
namespaces = ["$IPC_NAMESPACE"]
[servers.http]
bind = "$BIND"
port = $PORT
ws_path = "$WS_PATH"
[servers.pre_trade]
enabled = true
[[servers.pre_trade.instances]]
label = "$INSTANCE_LABEL"
exposure_channel = "$exposure_ch"
risk_channel = "$risk_ch"
EOF
}

emit_server_block > "$TARGET_DIR/config/viz.toml"

mkdir -p "$TARGET_DIR/www"
if [[ -f "$ROOT_DIR/docs/cross_pre_trade_dashboard.html" ]]; then
  cp "$ROOT_DIR/docs/cross_pre_trade_dashboard.html" "$TARGET_DIR/www/pre_trade_dashboard.html"
  cp "$ROOT_DIR/docs/cross_pre_trade_dashboard.html" "$TARGET_DIR/www/index.html"
  echo "[INFO] 已同步 cross dashboard: $TARGET_DIR/www/pre_trade_dashboard.html"
elif [[ -f "$ROOT_DIR/docs/pre_trade_dashboard.html" ]]; then
  cp "$ROOT_DIR/docs/pre_trade_dashboard.html" "$TARGET_DIR/www/pre_trade_dashboard.html"
  cp "$ROOT_DIR/docs/pre_trade_dashboard.html" "$TARGET_DIR/www/index.html"
  echo "[WARN] 缺少 cross dashboard，回退到通用版"
fi

EXTRA_FILES=(
  "cross_scripts/start_cross_viz_server.sh"
  "cross_scripts/stop_cross_viz_server.sh"
  "scripts/setup_nginx_4191.sh"
)
for file in "${EXTRA_FILES[@]}"; do
  SRC="$ROOT_DIR/$file"
  if [[ -f "$SRC" ]]; then
    DEST_DIR="$TARGET_DIR/$(dirname "$file")"
    mkdir -p "$DEST_DIR"
    rsync -a "$SRC" "$DEST_DIR/"
    chmod +x "$TARGET_DIR/$file" 2>/dev/null || true
  fi
done

upsert_main_nginx_mapping
if [[ "${APPLY_NGINX}" == "1" ]]; then
  echo "[INFO] Applying nginx config (PORT=${NGINX_PORT}, MAPPING_FILE=${NGINX_MAPPING_FILE})"
  (
    cd "$ROOT_DIR"
    PORT="$NGINX_PORT" MAPPING_FILE="$NGINX_MAPPING_FILE" ./scripts/setup_nginx_4191.sh
  )
fi

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
if ! cross_atomic_install "$BIN_PATH" "$TARGET_DIR/$BIN_NAME"; then
  exit 2
fi

echo "[INFO] 部署完成: $TARGET_DIR"
echo "[INFO] 启动: cd $TARGET_DIR && ./cross_scripts/start_cross_viz_server.sh"
echo "[INFO] 停止: cd $TARGET_DIR && ./cross_scripts/stop_cross_viz_server.sh"
echo "[INFO] dashboard nginx: ${NGINX_PREFIX}/"
echo "[INFO] dashboard(静态): $TARGET_DIR/www/"
