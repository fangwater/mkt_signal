#!/usr/bin/env bash
set +x
set -euo pipefail
umask 077
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE_DIR"
environment="${BASE_DIR##*/}"
unit="$environment-trader.service"
fifo="$BASE_DIR/run/commands.fifo"
args=(--config "$BASE_DIR/config/trade.toml" --market-data-config "$BASE_DIR/config/md.toml" --read-only)

fail() {
    printf '%s\n' "$1" >&2
    exit 1
}

check_fifo() {
    [[ -p "$fifo" && -O "$fifo" && ! -L "$fifo" ]] || fail 'invalid private command FIFO'
}

[[ $# == 1 ]] || fail "Usage: $0 {install|start|stop|status|state|run}"
case "$1" in
    install)
        [[ "$environment" =~ ^[a-zA-Z0-9_-]+$ && "$BASE_DIR" =~ ^[/a-zA-Z0-9_.-]+$ ]] || fail 'unsupported environment path'
        systemd_dir="$HOME/.config/systemd/user"
        mkdir -p "$systemd_dir" logs run data/stops
        candidate="$(mktemp "$systemd_dir/.ctp-unit.XXXXXX")"
        trap 'rm -f -- "$candidate"' EXIT
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
        systemctl --user daemon-reload
        systemctl --user enable "$unit"
        ;;
    start)
        "$BASE_DIR/bin/ctp-trader" "${args[@]}" --check-config
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
        ;;
    status)
        systemctl --user show "$unit" --property=ActiveState,SubState,MainPID,ExecMainStatus
        ;;
    state)
        check_fifo
        systemctl --user is-active --quiet "$unit" || fail 'CTP account observer is stopped'
        timeout 2s bash -c 'printf "%s\n" '\''{"op":"state"}'\'' > "$1"' _ "$fifo"
        printf '%s\n' 'read-only state request queued; response remains in private runtime logs'
        ;;
    run)
        source "$BASE_DIR/env.sh"
        mkdir -p run
        [[ -e "$fifo" || -L "$fifo" ]] || mkfifo -m 0600 -- "$fifo"
        check_fifo
        exec "$BASE_DIR/bin/ctp-trader" "${args[@]}" --connect <> "$fifo"
        ;;
    *) fail "Usage: $0 {install|start|stop|status|state|run}" ;;
esac
