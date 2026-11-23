#!/usr/bin/env bash
set +x
set -euo pipefail
umask 077
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE_DIR"
environment="${BASE_DIR##*/}"
unit="$environment-trader.service"
persist_unit="$environment-persist.service"
# Exec and persist_manager share one iceoryx namespace (also the state IPC).
export IPC_NAMESPACE="$environment"
fifo="$BASE_DIR/run/commands.fifo"
args=(--config "$BASE_DIR/config/trade.toml" --market-data-config "$BASE_DIR/config/md.toml")
run_flags=()

fail() {
    printf '%s\n' "$1" >&2
    exit 1
}

check_fifo() {
    [[ -p "$fifo" && -O "$fifo" && ! -L "$fifo" ]] || fail 'invalid private command FIFO'
}

select_mode() {
    local mode
    mode=$(/usr/bin/python3 - "$BASE_DIR/config/trade.toml" <<'PY'
from pathlib import Path
import sys, tomllib

config = tomllib.loads(Path(sys.argv[1]).read_text())
exec_cfg = config.get("exec")
if exec_cfg is None:
    print("read-only")
else:
    ingress = config.get("position_target_ingress", {})
    if set(ingress) != {"source_id", "manager_plan"}:
        raise SystemExit("managed execution requires a Manager subscription")
    print("manager-target")
PY
)
    case "$mode" in
        read-only) args+=(--read-only) ;;
        manager-target) run_flags+=(--execute) ;;
        *) fail 'invalid CTP execution mode' ;;
    esac
}

[[ $# == 1 ]] || fail "Usage: $0 {install|start|stop|status|state|run|persist}"
case "$1" in
    install)
        [[ "$environment" =~ ^[a-zA-Z0-9_-]+$ && "$BASE_DIR" =~ ^[/a-zA-Z0-9_.-]+$ ]] || fail 'unsupported environment path'
        systemd_dir="$HOME/.config/systemd/user"
        mkdir -p "$systemd_dir" logs run data/stops
        candidate="$(mktemp "$systemd_dir/.ctp-unit.XXXXXX")"
        retry_candidate="$(mktemp "$systemd_dir/.ctp-recovery.XXXXXX")"
        trap 'rm -f -- "$candidate" "$retry_candidate"' EXIT
        cat > "$candidate" <<EOF
[Unit]
Description=$environment read-only CTP Trader and Md
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$BASE_DIR
ExecStart=/usr/bin/bash $BASE_DIR/scripts/ctp_env.sh run
Restart=no
TimeoutStopSec=35
KillMode=control-group
UMask=0077
LimitNOFILE=65536
StandardOutput=append:$BASE_DIR/logs/trader.log
StandardError=append:$BASE_DIR/logs/trader.log

[Install]
WantedBy=default.target
EOF
        if [[ -e "$systemd_dir/$unit" ]]; then
            cmp -s "$candidate" "$systemd_dir/$unit" || fail 'existing CTP unit differs; preserve operator configuration'
        else
            mv -- "$candidate" "$systemd_dir/$unit"
        fi
        persist_candidate="$(mktemp "$systemd_dir/.ctp-persist.XXXXXX")"
        cat > "$persist_candidate" <<UNIT
[Unit]
Description=$environment domestic persist_manager (uniform orders, trade updates, trader log)
After=network-online.target

[Service]
Type=simple
WorkingDirectory=$BASE_DIR
ExecStart=/usr/bin/bash $BASE_DIR/scripts/ctp_env.sh persist
Restart=on-failure
RestartSec=5s
TimeoutStopSec=35
KillMode=control-group
UMask=0077
LimitNOFILE=65536
StandardOutput=append:$BASE_DIR/logs/trader.log
StandardError=append:$BASE_DIR/logs/persist.log

[Install]
WantedBy=default.target
UNIT
        if [[ -e "$systemd_dir/$persist_unit" ]]; then
            cmp -s "$persist_candidate" "$systemd_dir/$persist_unit" || fail 'existing persist unit differs; preserve operator configuration'
            rm -f -- "$persist_candidate"
        else
            mv -- "$persist_candidate" "$systemd_dir/$persist_unit"
        fi
        # Shared with the Yida environment: only cn_futures::restart exit 75 restarts.
        retry_config="$systemd_dir/$unit.d/10-cold-restart.conf"
        cat > "$retry_candidate" <<'EOF'
[Service]
RestartForceExitStatus=75
RestartSec=60s
EOF
        if [[ -e "$retry_config" ]]; then
            cmp -s "$retry_candidate" "$retry_config" || fail 'existing cold-restart policy differs; preserve operator configuration'
        else
            mkdir -p -- "$(dirname "$retry_config")"
            mv -- "$retry_candidate" "$retry_config"
        fi
        systemctl --user daemon-reload
        systemctl --user enable "$persist_unit" "$unit"
        ;;
    start)
        select_mode
        "$BASE_DIR/bin/ctp-trader" "${args[@]}" --check-config
        # persist_manager first: Exec publishes without waiting for anyone.
        systemctl --user start "$persist_unit"
        sleep 2
        systemctl --user is-active --quiet "$persist_unit" || fail 'persist_manager failed to start'
        systemctl --user start "$unit"
        ;;
    stop)
        mkdir -p "$BASE_DIR/data/stops"
        archive="$(mktemp -d "$BASE_DIR/data/stops/$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")"
        if [[ -f logs/trader.log ]]; then
            cp -p -- logs/trader.log "$archive/"
        fi
        # systemd delivers SIGTERM; the Rust runtime drains and closes its writer.
        systemctl --user stop "$unit"
        # After the trader has drained, let persist_manager write the tail.
        sleep 2
        systemctl --user stop "$persist_unit"
        ;;
    status)
        systemctl --user show "$persist_unit" "$unit" --property=Id,ActiveState,SubState,MainPID,ExecMainStatus
        ;;
    state)
        check_fifo
        systemctl --user is-active --quiet "$unit" || fail 'CTP Trader is stopped'
        timeout 2s bash -c 'printf "%s\n" '\''{"op":"state"}'\'' > "$1"' _ "$fifo"
        printf '%s\n' 'read-only state request queued; response remains in private runtime logs'
        ;;
    run)
        select_mode
        source "$BASE_DIR/env.sh"
        mkdir -p run
        [[ -e "$fifo" || -L "$fifo" ]] || mkfifo -m 0600 -- "$fifo"
        check_fifo
        exec "$BASE_DIR/bin/ctp-trader" "${args[@]}" --connect "${run_flags[@]}" <> "$fifo"
        ;;
    persist)
        # Background CPU only; the business CPU belongs to the trader thread.
        export RUST_LOG="${RUST_LOG:-info}"
        exec "$BASE_DIR/bin/persist_manager" --core 3
        ;;
    *) fail "Usage: $0 {install|start|stop|status|state|run|persist}" ;;
esac
