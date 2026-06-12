---
name: mkt-host-access
description: Use when connecting to, documenting, probing, or copying SSH keys for the crypto market mkt_signal SG or JP hosts; covers the correct hostnames, PEM files, deploy defaults, and PEM safety rules.
---

# mkt_signal Host Access

Use this skill when the user asks how Codex connects to the crypto market
`mkt_signal` hosts, asks to SSH/probe SG/JP, or asks to copy a PEM between
these hosts.

## Hosts

- SG: `ubuntu@47.131.162.78`, key `aws-sg.pem`
- JP: `ubuntu@54.64.147.69`, key `aws-jp-srv-1.pem`

JP is the default remote deploy target in `scripts/lib/fr_remote_deploy.sh`.

## SSH Pattern

Use non-interactive SSH for probes and one-off remote commands:

```bash
ssh -i <key.pem> \
  -o BatchMode=yes \
  -o StrictHostKeyChecking=accept-new \
  -o ConnectTimeout=12 \
  <user@host> '<read-only command>'
```

For live-risk hosts, state the target host, symbol/env scope when relevant, and
whether the command observes or mutates state before running it.

## PEM Rules

- Never print private key contents.
- Never commit PEM files; repo `.gitignore` excludes `*.pem`.
- Before uploading a PEM to a remote host, state:
  - source key file,
  - destination host,
  - destination path,
  - why the remote host needs that key.
- Store remote PEMs under `/home/ubuntu/.ssh/` with directory mode `700` and
  key mode `400`.
- After copying, verify by SSH probe only; do not cat or diff key contents.

## Common Tasks

Probe JP:

```bash
ssh -i aws-jp-srv-1.pem -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=12 ubuntu@54.64.147.69 'hostname'
```

Copy SG key to JP so JP can initiate SSH to SG:

```bash
ssh -i aws-jp-srv-1.pem ubuntu@54.64.147.69 'mkdir -p ~/.ssh && chmod 700 ~/.ssh'
scp -i aws-jp-srv-1.pem aws-sg.pem ubuntu@54.64.147.69:/home/ubuntu/.ssh/aws-sg.pem
ssh -i aws-jp-srv-1.pem ubuntu@54.64.147.69 'chmod 400 ~/.ssh/aws-sg.pem'
ssh -i aws-jp-srv-1.pem ubuntu@54.64.147.69 'ssh -i ~/.ssh/aws-sg.pem -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=12 ubuntu@47.131.162.78 hostname'
```

Main reference: `docs/mkt_signal_host_access.md`.
