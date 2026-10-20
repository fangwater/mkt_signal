#!/usr/bin/env bash
# Close direct external access to the nine FR web backends. Nginx uses loopback.
# This changes no exchange egress, trading listener, route, or source-IP setting.
set -euo pipefail
if [[ "${1:-}" != "--execute" && "${1:-}" != "--remove" ]]; then
  echo 'Usage: delist_fr_port_guard.sh --execute | --remove (requires root)'
  exit 2
fi
for firewall in iptables ip6tables; do
  command -v "$firewall" >/dev/null
  if [[ "$1" == "--remove" ]]; then
    while "$firewall" -w -C INPUT -j DELIST_FR_ACCESS 2>/dev/null; do
      "$firewall" -w -D INPUT -j DELIST_FR_ACCESS
    done
    "$firewall" -w -F DELIST_FR_ACCESS 2>/dev/null || true
    "$firewall" -w -X DELIST_FR_ACCESS 2>/dev/null || true
    continue
  fi
  if ! "$firewall" -w -S DELIST_FR_ACCESS >/dev/null 2>&1; then
    "$firewall" -w -N DELIST_FR_ACCESS
    "$firewall" -w -A DELIST_FR_ACCESS -i lo -j RETURN
    "$firewall" -w -A DELIST_FR_ACCESS -p tcp -m multiport --dports 8787,20021,20022,20023,20032,20033,20034,20051,20052,20053,20121,20122,20123,20132,20133 -j REJECT --reject-with tcp-reset
    "$firewall" -w -A DELIST_FR_ACCESS -p tcp -m multiport --dports 20134,20142,20143,20144,20151,20152,20153,20161,20162,20163,20171,20172,20173 -j REJECT --reject-with tcp-reset
    "$firewall" -w -A DELIST_FR_ACCESS -j RETURN
  fi
  if ! "$firewall" -w -C INPUT -j DELIST_FR_ACCESS 2>/dev/null; then
    "$firewall" -w -I INPUT 1 -j DELIST_FR_ACCESS
  fi
done
