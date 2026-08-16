---
name: mkt-host-access
description: Use when connecting to, documenting, or probing the crypto market mkt_signal JP/SG hosts; covers SSH aliases, PEM safety, and where core allocation lives.
---

# mkt_signal Host Access

Use this skill when the user asks how to SSH/probe the crypto market
`mkt_signal` hosts.

## Hosts

- JP: SSH alias `jp-meta-elvpn`
- SG: SSH alias `sg`

Do not use the retired public IP `54.64.147.69`. Core pins: `docs/core_allocation.md`.
SSH details: `docs/mkt_signal_host_access.md`.

## SSH Pattern

```bash
ssh -o BatchMode=yes jp-meta-elvpn '<read-only command>'
ssh -o BatchMode=yes sg '<read-only command>'
```

For live-risk hosts, state the target, env/symbol scope, and whether the
command observes or mutates state before running it.

## PEM Rules

- Never print private key contents.
- Never commit PEM files; repo `.gitignore` excludes `*.pem`.
- Before uploading a PEM to a remote host, state source key, destination
  host/path, and why that host needs the key.
- Remote PEMs go under `~/.ssh/` with directory mode `700` and key mode `400`.
- After copying, verify by SSH probe only; do not cat or diff key contents.
