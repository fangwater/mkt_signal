---
name: mkt-host-access
description: Use when connecting to, documenting, probing, or copying SSH keys for the crypto market mkt_signal SG, JP, or JP2 hosts; covers the correct hostnames, PEM files, deploy defaults, and PEM safety rules.
---

# mkt_signal Host Access

Use this skill when the user asks how Codex connects to the crypto market
`mkt_signal` hosts, asks to SSH/probe SG/JP/JP2, or asks to copy a PEM between
these hosts.

## Hosts

- SG: `ubuntu@47.131.162.78`, key `aws-sg.pem`
- JP: `ubuntu@54.64.147.69`, key `aws-jp-srv-1.pem`
- JP2 HFQ: `ubuntu@52.68.224.23`, key `aws-jp-aws-hfq.pem`

JP is the default remote deploy target in `scripts/lib/fr_remote_deploy.sh`.
JP2 must be selected explicitly with `FR_DEPLOY_HOST=ubuntu@52.68.224.23` and
`FR_DEPLOY_KEY=$PWD/aws-jp-aws-hfq.pem`.

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

Probe JP2:

```bash
ssh -i aws-jp-aws-hfq.pem -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=12 ubuntu@52.68.224.23 'hostname'
```

Copy SG key to JP so JP can initiate SSH to SG:

```bash
ssh -i aws-jp-srv-1.pem ubuntu@54.64.147.69 'mkdir -p ~/.ssh && chmod 700 ~/.ssh'
scp -i aws-jp-srv-1.pem aws-sg.pem ubuntu@54.64.147.69:/home/ubuntu/.ssh/aws-sg.pem
ssh -i aws-jp-srv-1.pem ubuntu@54.64.147.69 'chmod 400 ~/.ssh/aws-sg.pem'
ssh -i aws-jp-srv-1.pem ubuntu@54.64.147.69 'ssh -i ~/.ssh/aws-sg.pem -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=12 ubuntu@47.131.162.78 hostname'
```

## JP2 Nginx Static Dashboard 403

JP2 serves HTTP/WebSocket dashboards on public `4191` from
`/home/ubuntu/nginx_locations.txt`. Static dashboard mappings look like:

```text
/intra/binance-intra-arb01/ static:$HOME/binance-intra-arb01/www/
```

If `http://52.68.224.23:4191/intra/binance-intra-arb01/` returns nginx
`403 Forbidden` while direct `viz_server` access on `127.0.0.1:10180` returns
`404 Not Found`, the 403 is from nginx static file access, not from
`viz_server`.

Check the path:

```bash
namei -l /home/ubuntu/binance-intra-arb01/www/index.html
```

On JP2, `/home/ubuntu` may be mode `750` (`drwxr-x--- ubuntu ubuntu ubuntu`).
The nginx worker user cannot traverse it, even though `www/index.html` exists
and is world-readable. Fix by granting execute-only traversal on the home dir:

```bash
chmod o+x /home/ubuntu
sudo nginx -t && sudo systemctl reload nginx
curl -I http://127.0.0.1:4191/intra/binance-intra-arb01/
```

Expected result is `HTTP/1.1 200 OK`. This grants directory traversal only; it
does not grant world read on `/home/ubuntu`.

Main reference: `docs/mkt_signal_host_access.md`.
