# mkt_signal Host Access

This document records the SSH access pattern used for the crypto market
`mkt_signal` hosts. PEM files are intentionally gitignored and must never be
printed, committed, or copied into docs.

## Hosts

| Site | Host | SSH key in repo root | Notes |
| --- | --- | --- | --- |
| SG | `ubuntu@47.131.162.78` | `aws-sg.pem` | Bybit intra / SG runtime host |
| JP | `ubuntu@54.64.147.69` | `aws-jp-srv-1.pem` | Default FR/intra/MM remote deploy target |

## Local SSH Commands

Use non-interactive SSH when Codex or scripts only need to probe or run one
remote command:

```bash
ssh -i aws-sg.pem \
  -o BatchMode=yes \
  -o StrictHostKeyChecking=accept-new \
  -o ConnectTimeout=12 \
  ubuntu@47.131.162.78 'hostname'
```

```bash
ssh -i aws-jp-srv-1.pem \
  -o BatchMode=yes \
  -o StrictHostKeyChecking=accept-new \
  -o ConnectTimeout=12 \
  ubuntu@54.64.147.69 'hostname'
```

## Deploy Defaults

Most deploy helpers use the old JP host by default:

```bash
FR_DEPLOY_HOST=ubuntu@54.64.147.69
FR_DEPLOY_KEY=$PWD/aws-jp-srv-1.pem
```

Deploy wrappers build and sync files. They should not be treated as process
start commands unless the specific wrapper says so.

## SG Key On JP

If JP needs to initiate SSH into SG, place the SG private key on JP under
`/home/ubuntu/.ssh/aws-sg.pem` with strict permissions:

```bash
ssh -i aws-jp-srv-1.pem ubuntu@54.64.147.69 \
  'mkdir -p ~/.ssh && chmod 700 ~/.ssh'
scp -i aws-jp-srv-1.pem aws-sg.pem \
  ubuntu@54.64.147.69:/home/ubuntu/.ssh/aws-sg.pem
ssh -i aws-jp-srv-1.pem ubuntu@54.64.147.69 \
  'chmod 400 ~/.ssh/aws-sg.pem'
```

Probe SG from JP without printing key material:

```bash
ssh -i aws-jp-srv-1.pem ubuntu@54.64.147.69 \
  'ssh -i ~/.ssh/aws-sg.pem -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=12 ubuntu@47.131.162.78 hostname'
```

Treat copying PEMs to remote machines as a sensitive operation. State the source
key, destination host, destination path, and whether the command only observes
or mutates state before running it.
