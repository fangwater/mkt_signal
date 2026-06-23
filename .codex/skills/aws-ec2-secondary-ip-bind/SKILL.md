---
name: aws-ec2-secondary-ip-bind
description: Bind AWS EC2 public/EIP addresses by discovering their associated ENI private IPv4 addresses and adding those secondary private IPs to the Linux network interface. Use when a user says new AWS public IPs/EIPs were added but need to be bound on an EC2 host, especially Ubuntu/netplan hosts reached by SSH aliases such as jp2.
---

# AWS EC2 Secondary IP Bind

## Overview

AWS public IPv4 addresses and Elastic IPs are associated with private IPv4 addresses on an ENI. Do not add the public IP directly to Linux. Add the associated private IPs to the target interface, typically as `/20` in this VPC, then verify the public-to-private mapping.

Treat this as a live network operation. State the target host, interface, private IPs, public IPs, and whether each command observes or mutates state before making changes.

## Read-Only Discovery

First confirm the target host, interface, existing addresses, routes, and netplan files:

```bash
ssh -o BatchMode=yes -o ConnectTimeout=8 <host> hostname
ssh -o BatchMode=yes -o ConnectTimeout=8 <host> 'ip -br addr; ip route'
ssh -o BatchMode=yes -o ConnectTimeout=8 <host> 'ls -l /etc/netplan && sudo sed -n "1,220p" /etc/netplan/*.yaml'
```

Use IMDSv2 on the target host to discover ENI private/public mappings. The `ipv4-associations` metadata path is keyed by public IPv4 and returns the associated private IPv4:

```bash
ssh -o BatchMode=yes -o ConnectTimeout=8 <host> '
TOKEN=$(curl -sS -X PUT http://169.254.169.254/latest/api/token \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
for mac in $(curl -sS -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/network/interfaces/macs/); do
  echo "MAC $mac"
  echo "interface-id: $(curl -sS -H "X-aws-ec2-metadata-token: $TOKEN" \
    http://169.254.169.254/latest/meta-data/network/interfaces/macs/${mac}interface-id)"
  for pub in $(curl -sS -H "X-aws-ec2-metadata-token: $TOKEN" \
    http://169.254.169.254/latest/meta-data/network/interfaces/macs/${mac}public-ipv4s); do
    priv=$(curl -fsS -H "X-aws-ec2-metadata-token: $TOKEN" \
      "http://169.254.169.254/latest/meta-data/network/interfaces/macs/${mac}ipv4-associations/${pub}" || true)
    printf "%s -> %s\n" "$priv" "$pub"
  done
done'
```

If `aws` CLI is available and configured, it can provide the same mapping, but metadata is usually enough and avoids credential assumptions.

## Binding With Netplan

For Ubuntu/netplan hosts, preserve DHCP for the primary address and add secondary private IPs under `addresses:` on the same interface. Do not alter default routes unless the user asked for multi-ENI source routing.

Before writing:

- Include every secondary private IP that should be persistent.
- Do not include the DHCP primary private IP in `addresses:` unless the existing local pattern already does so.
- Use the subnet prefix from metadata or existing interface output.
- Back up the original netplan file with a timestamp.

Example for `jp2`, interface `ens41`, with secondary private IPs `172.31.35.229/20` through `172.31.35.234/20`:

```yaml
network:
  version: 2
  ethernets:
    ens41:
      match:
        macaddress: "06:52:65:8f:d7:37"
      addresses:
      - "172.31.35.229/20"
      - "172.31.35.230/20"
      - "172.31.35.231/20"
      - "172.31.35.232/20"
      - "172.31.35.233/20"
      - "172.31.35.234/20"
      dhcp4: true
      dhcp6: false
      set-name: "ens41"
```

Write, validate, and apply in separate steps:

```bash
ssh -o BatchMode=yes -o ConnectTimeout=8 <host> '
set -euo pipefail
backup="/etc/netplan/50-cloud-init.yaml.codex-backup-$(date -u +%Y%m%dT%H%M%SZ)"
sudo cp -a /etc/netplan/50-cloud-init.yaml "$backup"
# Write the reviewed YAML here with sudo tee.
sudo chmod 600 /etc/netplan/50-cloud-init.yaml
sudo netplan generate
echo "backup=$backup"
'

ssh -o BatchMode=yes -o ConnectTimeout=8 <host> 'sudo netplan apply'
```

If the host has multiple ENIs, follow the local pattern for route metrics and policy routing. For a single ENI host, secondary private IPs normally do not need extra routes.

## Verification

After `netplan apply`, verify the interface and routes:

```bash
ssh -o BatchMode=yes -o ConnectTimeout=8 <host> 'ip -br addr show dev <iface>; ip route'
```

Confirm the AWS mapping again and report it to the user. If testing outbound source addresses is useful and allowed, use `curl --interface <private-ip> https://checkip.amazonaws.com` for each bound private IP.

Keep the backup path in the final report. To roll back, restore the backup file and run `sudo netplan generate && sudo netplan apply`.
