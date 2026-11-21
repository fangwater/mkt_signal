#!/usr/bin/env bash
# Install in the sh_dev configuration-panel environment; manage its HTTP gateway.
set -euo pipefail
action="${1:-status}"
case "$action" in
  status) systemctl is-active nginx; exit ;;
  check) sudo -n nginx -t; exit ;;
  start|reload) ;;
  *) echo 'Usage: cta_cn_futures_public.sh status|check|start|reload [--execute]' >&2; exit 2 ;;
esac
if [[ "${2:-}" != --execute ]]; then
    echo "[DRY-RUN] host=sh_dev env=cta_cn_futures_public exchange=SHFE symbol_scope=SN action=$action-configuration-HTTP"
    exit
fi
sudo -n nginx -t
if [[ "$action" == start ]]; then
    sudo -n systemctl enable --now nginx
    sudo -n systemctl reload nginx
else
    sudo -n systemctl reload nginx
fi
