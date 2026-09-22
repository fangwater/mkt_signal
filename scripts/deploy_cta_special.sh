#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/deploy_intra_lib.sh"

usage() {
  cat <<'EOF'
Usage:
  scripts/deploy_cta_special.sh --env-name binance-cta-special-<tag>
                                --factor <tp_vpi_018|baseline_104>
                                --config-port <port> --dashboard-port <port>
                                [--namespace <name>]
                                [--trade-engine-config <path>]
                                [--skip-build]

Deploys a local, disabled-by-default, LTP-only futures CTA environment.
It never starts or stops processes and never overwrites an existing env.sh or
trade_engine.toml. A missing trade_engine.toml is initialized from the supplied
path, defaulting to $HOME/binance-cta-rx01/trade_engine.toml.
EOF
}

ENV_NAME=""
FACTOR=""
CONFIG_PORT=""
DASHBOARD_PORT=""
NAMESPACE=""
TRADE_ENGINE_CONFIG_SOURCE=""
SKIP_BUILD=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-name) ENV_NAME="${2:-}"; shift 2 ;;
    --factor) FACTOR="${2:-}"; shift 2 ;;
    --config-port) CONFIG_PORT="${2:-}"; shift 2 ;;
    --dashboard-port) DASHBOARD_PORT="${2:-}"; shift 2 ;;
    --namespace) NAMESPACE="${2:-}"; shift 2 ;;
    --trade-engine-config) TRADE_ENGINE_CONFIG_SOURCE="${2:-}"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "[ERROR] unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

ENV_NAME="${ENV_NAME,,}"
FACTOR="${FACTOR,,}"
[[ "$ENV_NAME" =~ ^binance-cta-special-[a-z0-9][a-z0-9_-]*$ ]] || {
  echo "[ERROR] --env-name must match binance-cta-special-<tag>" >&2; exit 1;
}
[[ "$FACTOR" == "tp_vpi_018" || "$FACTOR" == "baseline_104" ]] || {
  echo "[ERROR] --factor must be tp_vpi_018 or baseline_104" >&2; exit 1;
}
for value in "$CONFIG_PORT" "$DASHBOARD_PORT"; do
  [[ "$value" =~ ^[0-9]+$ && "$value" -ge 1024 && "$value" -le 65535 ]] || {
    echo "[ERROR] config/dashboard ports must be in 1024..65535" >&2; exit 1;
  }
done
[[ "$CONFIG_PORT" != "$DASHBOARD_PORT" ]] || { echo "[ERROR] ports must differ" >&2; exit 1; }
NAMESPACE="${NAMESPACE:-${ENV_NAME//-/_}}"
TRADE_ENGINE_CONFIG_SOURCE="${TRADE_ENGINE_CONFIG_SOURCE:-${HOME}/binance-cta-rx01/trade_engine.toml}"

cd "$ROOT_DIR"
[[ "$(git branch --show-current)" == "arbmm" ]] || {
  echo "[ERROR] production deploy artifacts must be built from branch arbmm" >&2; exit 1;
}
[[ -z "$(git status --porcelain)" ]] || {
  echo "[ERROR] worktree changes present; deploy from a clean arbmm worktree" >&2; exit 1;
}
if git show-ref --verify --quiet refs/remotes/origin/arbmm; then
  read -r behind ahead < <(git rev-list --left-right --count origin/arbmm...arbmm)
  [[ "$behind" == "0" && "$ahead" == "0" ]] || {
    echo "[ERROR] arbmm must be synchronized with origin/arbmm (behind=$behind ahead=$ahead)" >&2; exit 1;
  }
else
  echo "[ERROR] origin/arbmm is unavailable; cannot verify production source" >&2
  exit 1
fi

TARGET_DIR="${HOME}/${ENV_NAME}"
if [[ ! -f "${TARGET_DIR}/env.sh" ]]; then
  bash scripts/deploy_setup_env_intra.sh \
    --env-name "$ENV_NAME" \
    --exchange binance \
    --exec-backend ltp \
    --namespace "$NAMESPACE"
fi
mkdir -p "$TARGET_DIR/config" "$TARGET_DIR/scripts" "$TARGET_DIR/intra_scripts" \
  "$TARGET_DIR/web/cta_special" "$TARGET_DIR/web/cta_special_config" "$TARGET_DIR/run"

if [[ ! -f "$TARGET_DIR/trade_engine.toml" ]]; then
  [[ -f "$TRADE_ENGINE_CONFIG_SOURCE" ]] || {
    echo "[ERROR] missing trade engine config source: $TRADE_ENGINE_CONFIG_SOURCE" >&2
    exit 1
  }
  install -m 644 "$TRADE_ENGINE_CONFIG_SOURCE" "$TARGET_DIR/trade_engine.toml"
  echo "[INFO] initialized $TARGET_DIR/trade_engine.toml from $TRADE_ENGINE_CONFIG_SOURCE"
fi

intra_upsert_env_exports_block \
  "$TARGET_DIR/env.sh" \
  "managed cta special runtime" \
  "Pure Binance futures CTA special; LTP execution only." \
  "OPEN_VENUE='binance-futures'" \
  "HEDGE_VENUE='binance-futures'" \
  "TRADE_ENGINE_EXEC_BACKEND_MAP='binance=ltp'" \
  "CTA_SPECIAL_CONFIG_PORT='${CONFIG_PORT}'" \
  "CTA_SPECIAL_DASHBOARD_PORT='${DASHBOARD_PORT}'"

TARGET_BUILD_DIR="$(intra_effective_cargo_target_dir "$ROOT_DIR" "")"
if [[ "$SKIP_BUILD" -eq 0 ]]; then
  cargo build --release \
    --bin cta_special_signal \
    --bin cta_special_factor_model_1m_pub \
    --bin pre_trade \
    --bin trade_engine \
    --bin rapidx_account_monitor
  cargo build --release --package persist_manager --features runtime --bin persist_manager
fi

declare -A BIN_SOURCES=(
  [cta_special_signal]="$(intra_bin_path_release "$TARGET_BUILD_DIR" cta_special_signal)"
  [cta_special_factor_model_1m_pub]="$(intra_bin_path_release "$TARGET_BUILD_DIR" cta_special_factor_model_1m_pub)"
  [pre_trade]="$(intra_bin_path_release "$TARGET_BUILD_DIR" pre_trade)"
  [trade_engine]="$(intra_bin_path_release "$TARGET_BUILD_DIR" trade_engine)"
  [account_monitor_binance]="$(intra_bin_path_release "$TARGET_BUILD_DIR" rapidx_account_monitor)"
  [persist_manager]="$(intra_bin_path_release "$TARGET_BUILD_DIR" persist_manager)"
)
for name in "${!BIN_SOURCES[@]}"; do
  [[ -x "${BIN_SOURCES[$name]}" ]] || { echo "[ERROR] missing release binary: ${BIN_SOURCES[$name]}" >&2; exit 1; }
  intra_atomic_install "${BIN_SOURCES[$name]}" "$TARGET_DIR/$name"
done

copy_file() {
  local relative="$1"
  mkdir -p "$TARGET_DIR/$(dirname "$relative")"
  install -m 644 "$ROOT_DIR/$relative" "$TARGET_DIR/$relative"
}
copy_script() {
  local relative="$1"
  mkdir -p "$TARGET_DIR/$(dirname "$relative")"
  install -m 755 "$ROOT_DIR/$relative" "$TARGET_DIR/$relative"
}

for file in \
  scripts/cta_special_config_server.py scripts/cta_special_dashboard.py \
  scripts/start_cta_special.sh scripts/stop_cta_special.sh \
  scripts/start_cta_special_signal.sh scripts/stop_cta_special_signal.sh \
  scripts/start_cta_special_factor_model_1m_pub.sh scripts/stop_cta_special_factor_model_1m_pub.sh \
  scripts/start_cta_special_config_server.sh scripts/stop_cta_special_config_server.sh \
  scripts/start_cta_special_dashboard.sh scripts/stop_cta_special_dashboard.sh \
  scripts/process_match_lib.sh scripts/execution_backend_lib.sh scripts/intra_release_guard.sh \
  intra_scripts/start_intra_monitors.sh intra_scripts/stop_intra_monitors.sh \
  intra_scripts/start_intra_trade_engine.sh intra_scripts/stop_intra_trade_engine.sh \
  intra_scripts/start_intra_pre_trade.sh intra_scripts/stop_intra_pre_trade.sh \
  intra_scripts/start_intra_persist_manager.sh intra_scripts/stop_intra_persist_manager.sh \
  intra_scripts/intra_monitor_process_lib.sh intra_scripts/sync_cta_risk_params.py \
  intra_scripts/sync_intra_risk_params.py intra_scripts/print_intra_risk_params.py; do
  copy_script "$file"
done
for file in web/cta_special/index.html web/cta_special_config/index.html config/iceoryx2.toml; do
  copy_file "$file"
done

created_config=0
if [[ ! -f "$TARGET_DIR/config/cta_special.json" ]]; then
  copy_file config/cta_special.json
  created_config=1
fi
python3 - "$TARGET_DIR/config/cta_special.json" "$FACTOR" "$created_config" <<'PY'
import json
import os
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
factor = sys.argv[2]
created = sys.argv[3] == "1"
config = json.loads(path.read_text())
if created:
    config["enabled"] = False
    config["rule_name"] = factor
elif str(config.get("rule_name", "")).lower() != factor:
    raise SystemExit("existing cta_special.json factor does not match --factor")
entry = config.get("entry") or {}
execution = config.get("execution") or {}
compact = {
    "enabled": bool(config.get("enabled", False)),
    "rule_name": factor,
    "symbols": config.get("symbols", []),
    "entry": {
        "trade_sides": entry.get("trade_sides", "both"),
        "factor_long_quantile": entry.get("factor_long_quantile", 0.9),
        "factor_short_quantile": entry.get("factor_short_quantile", 0.1),
        "cooldown_seconds": entry.get("cooldown_seconds", 0),
        "signal_delay_seconds": entry.get("signal_delay_seconds", 1),
        "nq_change_enabled": bool(entry.get("nq_change_enabled", True)),
        "nq_long_quantile": entry.get("nq_long_quantile", 0.5),
        "nq_short_quantile": entry.get("nq_short_quantile", 0.5),
    },
    "execution": {
        "order_notional_usdt": execution.get("order_notional_usdt", 100.0),
        "open_offsets": execution.get("open_offsets", [0.0, 0.0001, 0.0003, 0.0005]),
        "maker_ttl_seconds": execution.get("maker_ttl_seconds", 120),
        "factor_exit_enabled": bool(execution.get("factor_exit_enabled", True)),
        "factor_exit_quantile_long": execution.get("factor_exit_quantile_long", 0.3),
        "factor_exit_quantile_short": execution.get("factor_exit_quantile_short", 0.7),
        "trailing_stop_enabled": bool(execution.get("trailing_stop_enabled", True)),
        "trailing_stop_trigger_step": execution.get("trailing_stop_trigger_step", 0.02),
        "trailing_stop_move_step": execution.get("trailing_stop_move_step", 0.01),
        "max_holding_seconds": execution.get("max_holding_seconds", 0),
    },
}
temp = path.with_name(f".{path.name}.deploy.tmp")
temp.write_text(json.dumps(compact, indent=2) + "\n")
temp.chmod(path.stat().st_mode & 0o777)
os.replace(temp, path)
PY
python3 "$TARGET_DIR/scripts/cta_special_config_server.py" \
  --config "$TARGET_DIR/config/cta_special.json" --check >/dev/null
[[ -f "$TARGET_DIR/config/cta_special_factor_model_1m_pub.toml" ]] || \
  copy_file config/cta_special_factor_model_1m_pub.toml

manifest="$TARGET_DIR/intra-release.manifest"
{
  for spec in \
    "pre_trade:pre_trade" \
    "trade_engine:trade_engine" \
    "rapidx_account_monitor:account_monitor_binance" \
    "persist_manager:persist_manager" \
    "cta_special_signal:cta_special_signal" \
    "cta_special_factor_model_1m_pub:cta_special_factor_model_1m_pub"; do
    name="${spec%%:*}"
    file="${spec##*:}"
    echo "binary $name $(sha256sum "$TARGET_DIR/$file" | awk '{print $1}')"
  done
} >"${manifest}.tmp"
release_id="$(sha256sum "${manifest}.tmp" | awk '{print $1}')"
{ echo "release_id $release_id"; cat "${manifest}.tmp"; } >"$manifest"
rm -f "${manifest}.tmp"

echo "[INFO] deployed disabled CTA special env: $TARGET_DIR"
echo "[INFO] factor=$FACTOR config_port=$CONFIG_PORT dashboard_port=$DASHBOARD_PORT backend=ltp"
echo "[INFO] initialize the isolated risk key before starting:"
echo "       cd $TARGET_DIR && ./intra_scripts/sync_cta_risk_params.py --env-name $ENV_NAME --open-venue binance-futures --hedge-venue binance-futures"
echo "[INFO] dry-run start: cd $TARGET_DIR && ./scripts/start_cta_special.sh"
