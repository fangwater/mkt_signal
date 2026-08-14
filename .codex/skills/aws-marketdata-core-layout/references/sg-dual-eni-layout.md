# SG Dual-ENI Execution Layout

## Contents

- [Invariant and current status](#invariant-and-current-status)
- [Verified topology](#verified-topology)
- [CPU and IRQ layout](#cpu-and-irq-layout)
- [Required discovery](#required-discovery)
- [Routing](#routing)
- [Application bindings](#application-bindings)
- [Bybit REST source binding](#bybit-rest-source-binding)
- [Cross-host endpoints](#cross-host-endpoints)
- [Startup and verification](#startup-and-verification)
- [Iceoryx2 cleanup](#iceoryx2-cleanup)
- [Rollback](#rollback)

## Invariant and current status

Treat this as a live-trading runbook. Before a mutating command, state the SG host, affected
environment, process scope, and whether orders can be emitted. Never print credentials or complete
exchange whitelist values.

Preserve this traffic split:

- Use the 下单网卡 only for account_monitor, trade_engine, and any script that can place, amend,
  cancel, or flatten orders.
- Use the 行情网卡 for market data, bridge, persist sync, monitoring, and ordinary/default outbound
  traffic.
- Bind Linux private addresses. Public addresses are AWS mappings and must never be assigned
  directly to an interface.

The layout was applied and re-audited on 2026-08-13 UTC. At the final audit:

- Bybit intra01 and MM account monitors and trade engines were online.
- Both environments passed Bybit private authentication through the 下单网卡.
- trade_signal and latency_stable_monitor were intentionally stopped.
- Bybit intra02 and the Bybit-Binance cross trade engine remained stopped.
- Market-data, bridge, and persist paths were online through the 行情网卡.
- irqbalance was inactive and both ENA affinity units were active.

Re-read live state before relying on this snapshot.

## Verified topology

The host has four public/private mappings:

| Role | Interface | Private IPv4 | Public IPv4 | Use |
| --- | --- | --- | --- | --- |
| 下单网卡 primary | enp39s0 | 172.31.7.124 | 47.131.162.78 | account_monitor, trade_engine, order operations, current SSH alias |
| 下单网卡 reserve | enp39s0 | 172.31.7.123 | 18.140.198.179 | reserve; do not use for the current application layout |
| 行情网卡 primary | enp40s0 | 172.31.10.217 | 47.128.92.224 | market data, bridge, persist, default traffic |
| 行情网卡 reserve | enp40s0 | 172.31.10.218 | 175.41.172.253 | reserve; do not use for the current application layout |

The current source-policy layout is:

| Interface | Main-route metric | Policy table | Source rules |
| --- | --- | --- | --- |
| enp40s0 行情网卡 | 100 | 101 | 172.31.10.217/32 and 172.31.10.218/32 |
| enp39s0 下单网卡 | 200 | 10001 | 172.31.7.123/32 and 172.31.7.124/32 |

Keep IPv4 rp_filter in loose mode (2). Each policy table must contain its own connected route and
default route through the gateway on that interface.

The expected public identities are:

- Unbound/default traffic: 47.128.92.224.
- Source 172.31.10.217: 47.128.92.224.
- Source 172.31.7.124: 47.131.162.78.
- Source 172.31.7.123: 18.140.198.179.
- Source 172.31.10.218: 175.41.172.253.

## CPU and IRQ layout

SG has CPUs 0-15, one hardware thread per physical core, and no SMT. CPUs 0-7 are housekeeping.
CPUs 8-15 are isolated with isolcpus/nohz_full/rcu_nocbs and require explicit placement when used.

| CPU | Assignment |
| --- | --- |
| 0-7 | Housekeeping and unbound MM/persist/depth/TFF/auxiliary processes |
| 8 | spread_pbs Bybit market role |
| 9 | spread_pbs Bybit bookTicker role |
| 10 | 下单网卡 enp39s0 IRQs only |
| 11 | bybit-intra-arb01 account_monitor |
| 12 | bybit-intra-arb01 trade_signal, reserved but intentionally stopped |
| 13 | bybit-intra-arb01 pre_trade |
| 14 | bybit-intra-arb01 trade_engine main thread |
| 15 | bybit-intra-arb01 trade_engine IPC thread plus 行情网卡 enp40s0 IRQs |

Keep these environment settings:

~~~text
ACCOUNT_MONITOR_CORE=11
TRADE_SIGNAL_CORE=12
PRE_TRADE_CORE=13
TRADE_ENGINE_CORE=14
TRADE_ENGINE_IPC_CORE=15
PERSIST_MANAGER_CORE=
DEPTH_PUB_CORE=
~~~

Leave the MM account monitor and trade engine unbound; verify Cpus_allowed_list is 0-7. Do not
interpret one ps PSR sample as an affinity setting.

ENA layout:

| Interface | Role | Tx/Rx IRQ CPU | Unit |
| --- | --- | --- | --- |
| enp39s0 | 下单网卡 | 10 | pin-aws-ena-irq@enp39s0.service |
| enp40s0 | 行情网卡 | 15 | pin-aws-ena-irq@enp40s0.service |

Use these tracked defaults:

~~~text
scripts/systemd/pin-aws-ena-irq-enp39s0.default: IRQ_CPUS=10
scripts/systemd/pin-aws-ena-irq-enp40s0.default: IRQ_CPUS=15
~~~

Keep irqbalance inactive. Verify every matching IRQ in both smp_affinity_list and
effective_affinity_list. CPU10 must remain free of user-space affinity. CPU15 intentionally shares
行情网卡 IRQ work with the intra01 TE IPC thread; measure tail latency before changing this choice.
Do not change RPS, XPS, or ENA coalescing as part of the lane migration.

## Required discovery

Connect through the existing sg SSH alias and do not expose env.sh values.

~~~bash
ssh -o BatchMode=yes -o ConnectTimeout=8 sg 'hostname -f; ip -br link; ip -br -4 addr'
ssh -o BatchMode=yes -o ConnectTimeout=8 sg 'ip -4 route; ip -4 rule; ip -4 route show table 101; ip -4 route show table 10001'
ssh -o BatchMode=yes -o ConnectTimeout=8 sg 'lscpu -e=CPU,NODE,CORE,ONLINE; systemctl is-active irqbalance || true'
ssh -o BatchMode=yes -o ConnectTimeout=8 sg 'ss -lntup; ps -eo pid,psr,comm,args'
~~~

Use IMDSv2, following the aws-ec2-secondary-ip-bind skill, to rediscover ENI IDs, private
addresses, subnet, gateway, and public associations. Do not infer mappings from route names or
historical notes.

Before changing routing or IRQs, inventory:

- Netplan files, owners, modes, renderer, metrics, routes, and routing-policy entries.
- ENA queue count, /proc/interrupts rows, configured/effective affinity, RPS/XPS, and coalescing.
- CPU isolation command-line settings and all user processes on CPUs 8-15.
- Runtime directories, pmdaemon processes, systemd units, nginx streams, and bound listeners.
- Deployed and tracked references to all four SG public/private mappings.

## Routing

Keep the 行情网卡 as the ordinary default and use source policy for deterministic return paths.
Follow the current SG netplan schema; do not replace it with a generic template.

Before applying netplan:

1. Stop application processes when the operator has scheduled the network change.
2. Create UTC timestamped root-owned backups of every modified file.
3. Run sudo netplan generate and stop on any warning or error.
4. Keep the current SSH session open and arrange a timed rollback or equivalent recovery path.
5. Apply, then immediately verify a fresh SSH connection and all source routes.

Validate selection:

~~~bash
ip route get 1.1.1.1
ip route get 1.1.1.1 from 172.31.10.217
ip route get 1.1.1.1 from 172.31.10.218
ip route get 1.1.1.1 from 172.31.7.123
ip route get 1.1.1.1 from 172.31.7.124
curl --interface 172.31.10.217 https://checkip.amazonaws.com
curl --interface 172.31.7.124 https://checkip.amazonaws.com
~~~

Require default and 172.31.10.217 to select enp40s0. Require 172.31.7.124 to select enp39s0.

## Application bindings

Configure all four SG trading environments with the 下单网卡 private address:

~~~toml
local_ips = ["172.31.7.124", "172.31.7.124"]
~~~

Audit:

- /home/ubuntu/bybit-intra-arb01
- /home/ubuntu/bybit-intra-arb02
- /home/ubuntu/bybit_mm_alpha
- /home/ubuntu/bybit-binance-cross-arb01

The repeated value intentionally keeps both Bybit connections on one approved public IP. Confirm
each account monitor loads its environment-local trade_engine.toml.

Bind external market-data connections explicitly to the 行情网卡:

~~~yaml
primary_local_ip: "172.31.10.217"
secondary_local_ip: "172.31.10.217"
~~~

Check at least dat_pbs/config/mkt_cfg.yaml and spread_pbs/config/mkt_cfg.yaml in their deployed
directories.

Use these non-order listeners:

| Service | Bind |
| --- | --- |
| SG ipc_bridge receiver | tcp://172.31.10.217:6360 |
| persist-sync nginx | 172.31.10.217:6351-6353 |
| persist-sync source servers | 127.0.0.1:50051-50053 |
| latency monitor, only if separately enabled | tcp://172.31.10.217:6370 |

Keep latency_stable_monitor stopped unless explicitly requested. Its future clients use
47.128.92.224:6370.

depth_pub uses IPC and a local Unix socket. trade_flow_feature_pub uses IPC and loopback Redis.
Neither needs an ENI/IP setting. Leave both unbound for CPU placement.

Port 4191, SSH, viz, and configuration servers are control-plane services and may retain existing
listen behavior. Linux binds do not replace security-group review.

## Bybit REST source binding

Treat successful WebSocket source binding as insufficient proof for trade_engine. All three paths
must use 172.31.7.124:

1. Bybit trading WebSockets.
2. Startup account-mode precheck REST calls.
3. Runtime query-router REST calls.

The startup and query-router clients must be built with
build_bybit_rest_client(self.local_ips.first().copied()). A plain reqwest Client::new follows the
default route through 172.31.10.217 and can return Bybit retCode 10010 even while WebSockets and
account_monitor correctly use the 下单网卡.

Require startup logs to contain:

~~~text
bybit precheck REST source local IP: 172.31.7.124
bybit precheck pass: unifiedMarginStatus=... spotMarginMode=1
Bybit auth successful
~~~

Also require ss output for every external trade_engine socket to show local address 172.31.7.124.
If precheck reports Unmatched IP:

1. Stop that trade engine.
2. Confirm trade_engine.toml still contains 172.31.7.124 twice.
3. Confirm source routing and the bound public identity.
4. Confirm the deployed binary includes the REST source-binding fix.
5. Only then investigate exchange whitelist state.

Never switch account/trade traffic to the 行情网卡 merely because its public IP authenticates.

## Cross-host endpoints

Use the 行情网卡 for SG data-plane endpoints:

- Remote bridge senders connect to 47.128.92.224:6360.
- The SG bridge binds 172.31.10.217:6360.
- Remote persist collectors connect to 47.128.92.224:6351-6353.
- SG nginx binds 172.31.10.217:6351-6353 and proxies to loopback sources.
- Future latency clients connect to 47.128.92.224:6370; do not start them implicitly.

Keep fixed persist ports: intra01 6351, MM alpha 6352, intra02 6353.

Update deployed copies and tracked templates together:

- config/ipc_bridge_local_to_sg_binance_models.yaml
- config/ipc_bridge_public_hk.yaml
- config/ipc_bridge_public_sg.yaml
- config/ipc_bridge_sg_public_binance_models.yaml
- config/persist.toml
- config/persist_sync_distribution.toml
- config/latency_stable_monitor_sg.yaml
- config/latency_csv_capture_sg.toml

Do not blindly replace 47.131.162.78 in SSH/deployment wrappers. That address remains a valid
control-plane entry on the 下单网卡 even though data-plane clients must use 47.128.92.224.

## Startup and verification

For a fresh migration:

1. Discover live topology and compare it with this reference.
2. Back up netplan, nginx, systemd/default files, and runtime configs with one UTC stamp.
3. Prepare and validate repository/template changes before process startup.
4. Apply and verify source-policy routing while preserving recovery access.
5. Install and verify IRQ affinity while applications are stopped.
6. Apply explicit source/listen bindings.
7. Update matching JP-meta/HK senders and collectors.
8. Start market-data, bridge, and persist components; verify actual data flow.
9. Start account_monitor, verify private authentication and socket sources.
10. Start trade_engine, verify REST precheck, WS auth, CPU placement, and socket sources.
11. Keep trade_signal and latency stopped unless explicitly requested.

Start only the requested environments. Do not infer permission to start intra02 or cross.

Final checks:

- Default egress and 172.31.10.217 return 47.128.92.224.
- 172.31.7.124 returns 47.131.162.78.
- All account_monitor and trade_engine external sockets use 172.31.7.124.
- All spread_pbs external sockets use 172.31.10.217.
- Ports 6360 and 6351-6353 listen only on 172.31.10.217.
- Port 6370 is absent while latency_stable_monitor is stopped.
- JP-meta/HK bridge and persist connections terminate at 47.128.92.224.
- intra01 account_monitor is on CPU11, pre_trade on CPU13, TE main on CPU14, TE IPC on CPU15.
- MM account_monitor and TE have Cpus_allowed_list 0-7.
- enp39s0 IRQs are 10/10 configured/effective; enp40s0 IRQs are 15/15.
- irqbalance is inactive and both affinity units are active.
- trade_signal and latency_stable_monitor have zero processes.
- No modified service reconnects repeatedly or remains failed.

Use ss with PIDs to attribute sockets. Do not use an unbound curl result as proof for an explicitly
bound process.

## Iceoryx2 cleanup

A dead publisher can occupy one service slot even when a global cleanup reports no dead nodes.
Operate from a directory containing the deployment iceoryx2.toml.

For each blocked service:

1. Inspect the exact service and record publisher node ID and owner PID.
2. Confirm the PID is dead and the node is not needed by a live subscriber/publisher.
3. Run iceoryx_remove_node_from_service with the service, node ID, and --dry-run.
4. Verify the resolved service/node, then remove only that node.
5. Reinspect before restarting the publisher.

Never delete /tmp/iceoryx2 while live subscribers exist.

## Rollback

Keep rollback granular:

1. Stop only processes started during the change.
2. Restore the affected runtime and remote sender/collector backups.
3. Restore nginx only after nginx -t succeeds.
4. Disable changed IRQ units and restore recorded affinities; never guess a fallback list.
5. Restore timestamped netplan files and run netplan generate before applying.
6. Verify routes, public identities, listeners, and SSH before restoring process startup.
7. Restore the prior trade_engine binary backup if the REST-source build regresses.

Record every backup path and keep backups until the migration is accepted.
