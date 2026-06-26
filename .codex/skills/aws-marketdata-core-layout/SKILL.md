---
name: aws-marketdata-core-layout
description: AWS market-data deployment layout for mkt_signal. Use when deploying or documenting the current AWS market-data host core binding for spread_pbs and depth_pub, especially Binance/Gate/OKEx/Bitget market-data publishers and the live Binance depth override.
---

# AWS Marketdata Core Layout

## Scope

Use this skill in `/home/ubuntu/crypto_mkt/mkt_signal` when redeploying or
documenting the current AWS market-data host reached by `ssh jp2`.

The repo checkout that invokes this skill may be on a different host. Always
operate on `ssh jp2` for this layout. As of 2026-06-26, `ssh jp2` resolves to
host `ip-172-31-35-228`; a local shell on `ip-172-31-33-133` is not this
market-data target.

This layout pins market-data processes to dedicated cores where possible.
`lscpu -e=CPU,NODE,SOCKET,CORE,CACHE,ONLINE` on `jp2` currently shows CPUs
0-47 online and a single visible L3 cache id (`...:0`) for all online CPUs.
Treat CPU0-5 as housekeeping/general OS capacity and market-data cores as
explicit per-process bindings.

- 5 `spread_pbs` processes:
  - Binance split into 2 processes: `binance-margin` and `binance-futures`.
  - Gate, OKEx, and Bitget each run as one `*-both` process.
- 2 `depth_pub` processes:
  - Binance runs one `binance-both` depth publisher on CPU14.
  - OKEx runs one `okex-both` depth publisher on CPU13 for model input.
  - Bitget and Gate depth publishers are not running in the current `jp2`
    layout.
- Model-input auxiliary processes also run on housekeeping/general cores:
  - OKEx margin/futures rolling metrics: `rm_ok_mg_ok_fu`.
  - OKEx margin trade-flow feature publisher: `tff_ok_mg`.
  - OKEx futures trade-flow feature publisher: `tff_ok_fu`.

## Core Map

| CPU | Process | Venue dir | Env override | pmdaemon name |
| --- | --- | --- | --- | --- |
| 8 | `spread_pbs` | `~/spread_pbs/binance-margin` | `SPREAD_PBS_CORE=8` | `spp_bn_mg` |
| 9 | `spread_pbs` | `~/spread_pbs/binance-futures` | `SPREAD_PBS_CORE=9` | `spp_bn_fu` |
| 10 | `spread_pbs` | `~/spread_pbs/gate-both` | `SPREAD_PBS_CORE=10` | `spp_gt_bo` |
| 11 | `spread_pbs` | `~/spread_pbs/bitget-both` | `SPREAD_PBS_CORE=11` | `spp_bg_bo` |
| 12 | `spread_pbs` | `~/spread_pbs/okex-both` | `SPREAD_PBS_CORE=12` plus OKX env sourced from `~/okex-intra-arb01/env.sh` | `spp_ok_bo` |
| 13 | `depth_pub` | `~/depth_pub/okex-both` | `DEPTH_PUB_CORE=13` | `dp_ok_both` |
| 14 | `depth_pub` | `~/depth_pub/binance-both` | `DEPTH_PUB_CORE=14` | `dp_bn_both` |

Treat the table as authoritative for `ssh jp2` unless the user explicitly
updates the topology. Do not assume CPU13 is Bitget depth on this host; it is
currently OKEx `depth_pub`.

## Model Input Auxiliaries

OKEx model-input support is deployed under:

| pmdaemon name | Binary | Venue dir | Key config |
| --- | --- | --- | --- |
| `rm_ok_mg_ok_fu` | `rolling_metrics` | `~/rolling_metrics/okex-margin-okex-futures` | Redis params key `rolling_metrics_params_okex-margin_okex-futures`; output key `rolling_metrics_thresholds_okex-margin_okex-futures` |
| `tff_ok_mg` | `trade_flow_feature_pub` | `~/trade_flow_feature/okex-margin` | `config/trade_flow_feature_pub.yaml` uses `depth_channel: "none"` |
| `tff_ok_fu` | `trade_flow_feature_pub` | `~/trade_flow_feature/okex-futures` | `config/trade_flow_feature_pub.yaml` uses `depth_channel: "depth25"` and subscribes `depth_pubs/okex-futures/depth25` |

The OKEx rolling Redis params were copied from
`rolling_metrics_params_bitget-margin_bitget-futures`, but the output hash key
must remain OKEx-specific. Do not copy Bitget's `output_hash_key` verbatim.

The OKEx futures trade-flow feature publisher requires
`okex-futures:amount-thresholds`. On 2026-06-26 this key was initialized from
the OKEx rolling symbol set with 211 fields and the same default futures
threshold shape used by Bitget/Gate futures:
`medium_notional_threshold=1.0`, `large_notional_threshold=1000.0`.

## Deployment Notes

Check worktree state before changing repo scripts:

```bash
git status --short
git diff --stat
```

Deploy the selected venues from the repo checkout:

```bash
ssh jp2 'cd ~/spread_pbs/<venue> && ./scripts/start_spread_pbs.sh'
ssh jp2 'cd ~/depth_pub/binance-both && ./scripts/start_depth_pub.sh'
ssh jp2 'cd ~/trade_flow_feature/okex-futures && ./scripts/start_trade_flow_feature_pub.sh'
```

The live `jp2` host may not have a repo checkout at
`~/crypto_mkt/mkt_signal`; it uses deployed runtime directories under
`~/spread_pbs`, `~/depth_pub`, `~/rolling_metrics`, and
`~/trade_flow_feature`. The spread/depth start scripts read per-venue `env.sh`
files:

- `~/spread_pbs/<venue>/env.sh` with `export SPREAD_PBS_CORE='<cpu>'`
- `~/depth_pub/<venue>/env.sh` with `export DEPTH_PUB_CORE='<cpu>'`

Write or preserve only the relevant core override in each deployed venue. Do
not hard-code credentials in repo files. OKEx `spread_pbs` needs
`OKX_API_KEY`, `OKX_API_SECRET`, and `OKX_PASSPHRASE` for SBE handshake; on
`jp2`, `~/spread_pbs/okex-both/env.sh` sources `~/okex-intra-arb01/env.sh`
and then sets `SPREAD_PBS_CORE=12`.

## Startup Order

Start each process from its deployed venue directory:

```bash
ssh jp2 'cd ~/spread_pbs/binance-margin && ./scripts/start_spread_pbs.sh'
ssh jp2 'cd ~/spread_pbs/binance-futures && ./scripts/start_spread_pbs.sh'
ssh jp2 'cd ~/spread_pbs/gate-both && ./scripts/start_spread_pbs.sh'
ssh jp2 'cd ~/spread_pbs/bitget-both && ./scripts/start_spread_pbs.sh'
ssh jp2 'cd ~/spread_pbs/okex-both && ./scripts/start_spread_pbs.sh'
ssh jp2 'cd ~/depth_pub/okex-both && ./scripts/start_depth_pub.sh'
ssh jp2 'cd ~/depth_pub/binance-both && ./scripts/start_depth_pub.sh'
ssh jp2 'pmdaemon delete rm_ok_mg_ok_fu >/dev/null 2>&1 || true; cd ~/rolling_metrics/okex-margin-okex-futures && source ./env.sh && pmdaemon start -n rm_ok_mg_ok_fu --cwd ~/rolling_metrics/okex-margin-okex-futures -e RUST_LOG=info,rolling_metrics=info,mkt_signal=info ~/rolling_metrics/okex-margin-okex-futures/rolling_metrics -- --open-venue okex-margin --hedge-venue okex-futures'
ssh jp2 'cd ~/trade_flow_feature/okex-margin && source ./env.sh && ./scripts/start_trade_flow_feature_pub.sh'
ssh jp2 'cd ~/trade_flow_feature/okex-futures && source ./env.sh && ./scripts/start_trade_flow_feature_pub.sh'
```

Before starting a `*-both` `spread_pbs`, stop conflicting single-side processes for the same exchange. The start script checks for conflicts, but do not rely on it as the only guard when operating live deployments.

## Verification

After startup, verify the market-data process names exist and are pinned to the
expected live cores:

```bash
ssh jp2 'pmdaemon list | grep -E "spp_|dp_"'
ssh jp2 'ps -eo pid,psr,comm,args | grep -E "spread_pbs|depth_pub" | grep -v grep'
ssh jp2 'pmdaemon list | grep -E "rm_ok_mg_ok_fu|tff_ok_mg|tff_ok_fu"'
ssh jp2 'ps -eo pid,psr,comm,args | grep -E "rolling_metrics|trade_flow_feature_pub" | grep -E "okex|ok_" | grep -v grep'
```

Expected process names:

- `spp_bn_mg`
- `spp_bn_fu`
- `spp_gt_bo`
- `spp_bg_bo`
- `spp_ok_bo`
- `dp_ok_both`
- `dp_bn_both`
- `rm_ok_mg_ok_fu`
- `tff_ok_mg`
- `tff_ok_fu`

If a process is missing or on a different CPU, inspect that venue's `env.sh` first, then restart only that venue.

For OKEx futures trade-flow feature, the startup log must include both:

- `Subscribed to trade channel: dat_pbs/okex-futures/trade`
- `Subscribed to depth channel: depth_pubs/okex-futures/depth25`
