# jp-meta-elvpn IP 绑定

最后更新: 2026-10-08 06:00 UTC。**分配、改写 `trade_engine.toml local_ips`、新开或下线任何用独立 source IP 的环境时，请同步更新本文件。**

绑核登记见 `docs/core_allocation.md`。本文件只记公网/私网 IP 与策略环境的对应关系。

不要把公网 IP 直接加到 Linux 网卡上。验证出口用：

```bash
curl --interface <private-ip> https://checkip.amazonaws.com
```

## 约定

`ens41`（主网卡，metric 100）上的 `172.31.35.228/20`–`.234/20` 对应 7 个公网 IPv4：

- `.228` / `13.115.227.29`：主 IP、默认出口、SSH/管理入口；**套利**固定 IP。
- `.229` / `52.193.90.33` 与 `.230` / `54.238.72.43`：MM 使用（当前 `okex_mm_alpha`；原共用的 `binance_mm_alpha` 已下线退役）。
- `.231` / `52.69.78.134`：资金费率固定 IP。
- `.232` / `54.238.97.67` 与 `.233` / `54.64.165.84`：`gate_fr_arb02`。
- `.234` / `54.64.228.233`：Exec `trade02`、`trade03`、`trade04` 的第二个 source IP 配置。

`ens42`（第二块网卡，metric 200）上的 `172.31.46.90/20`–`.93/20` 已绑定。无 `trade_engine.toml` 引用，但现场行情/`ipc_bridge` 已有 socket。不要在未更新本表前改作交易 source IP。

## 主机

- SSH 别名：`jp-meta-elvpn`
- hostname：`ip-172-31-35-228`
- `ens41` MAC：`06:52:65:8f:d7:37`；DHCP 主地址 `.228`，其余为 secondary
- `ens42` MAC：`06:17:74:aa:b2:f9`

## ens41 映射

| 私网 IP | 公网 IP | 状态 | 当前用途 |
| --- | --- | --- | --- |
| `172.31.35.228` | `13.115.227.29` | 已使用 / 固定 | 套利；SSH/默认出口。`binance-cta-rx01`、`binance-cta-special-rx02`、`binance-cta-special-rx03`、`okex-intra-arb01`；Exec `trade02`–`trade04` `local_ips[0]` |
| `172.31.35.229` | `52.193.90.33` | 已使用 | `okex_mm_alpha` `local_ips[0]`（原 `binance_mm_alpha` 已退役） |
| `172.31.35.230` | `54.238.72.43` | 已使用 | `okex_mm_alpha` `local_ips[1]`（原 `binance_mm_alpha` 已退役） |
| `172.31.35.231` | `52.69.78.134` | 已使用 / 固定 | 资金费率。`binance_fr_arb01`–`04`、`gate_fr_arb01`/`03`、`bitget_fr_arb01`/`02`/`03` |
| `172.31.35.232` | `54.238.97.67` | 已使用 | `gate_fr_arb02` `local_ips[0]` |
| `172.31.35.233` | `54.64.165.84` | 已使用 | `gate_fr_arb02` `local_ips[1]` |
| `172.31.35.234` | `54.64.228.233` | 已配置 | Exec `trade02`、`trade03`、`trade04` `local_ips[1]` |

## ens42 映射

| 私网 IP | 公网 IP | 状态 | 当前用途 |
| --- | --- | --- | --- |
| `172.31.46.90` | `52.69.209.108` | 已使用 | 现场 socket：`ipc_bridge`、各 `spread_pbs`、`spread_bbo_zmq_pub`（未写入 `trade_engine.toml`） |
| `172.31.46.91` | `54.199.82.56` | 已使用 | 少量 `spread_pbs` |
| `172.31.46.92` | `52.192.54.88` | 已使用 | `rclone mount` |
| `172.31.46.93` | `18.181.48.65` | 已使用 / 非交易 | CTA Manager 1 分钟 K 线 REST 缓存；少量 `ipc_bridge` socket |

`ens42` 没有策略 `local_ips` 引用；行情和 IPC 连接已使用这块网卡。
2026-10-05 部署 CTA Manager 分钟 K 线缓存：`[kline].local_ip = "172.31.46.93"`，
`public_ip = "18.181.48.65"`，请求 Binance USDⓈ-M `/fapi/v1/klines?interval=1m`。
通过 IMDSv2 与绑定私网地址的外部请求核验映射；检查全部 24 份现场
`trade_engine.toml` 后确认 `.93` 未用于下单。Manager 排除所有交易私网地址和
`ens41` 的 7 个交易公网地址；`0.0.0.0` 交易绑定按默认路由 `.228` 处理。
`.93` 保留给非交易请求；不要同时用作交易 source IP。此次未修改任何交易配置。

本机 `ip -br addr`（2026-08-16）：

```text
ens41  172.31.35.229/20 172.31.35.230/20 172.31.35.231/20
       172.31.35.232/20 172.31.35.233/20 172.31.35.234/20
       172.31.35.228/20 metric 100
ens42  172.31.46.91/20 172.31.46.92/20 172.31.46.93/20
       172.31.46.90/20 metric 200
```

## 当前 `trade_engine.toml local_ips`

2026-10-04 15:11 UTC 核验并重新发布 `gate_fr_arb03`（`322c482c`，Gate WS 保证金
恢复解锁）：两个交易连接仍绑定 `172.31.35.231`，部署前后 `trade_engine.toml`
校验一致。

2026-10-07 15:38 UTC 按 publish FR 流程重新发布 `binance_fr_arb03`（`d35f67ea`）：
两个交易连接的 `local_ips` 仍为 `172.31.35.231`，trade_engine / account_monitor
启动日志已核验该 source IP；部署前后 `trade_engine.toml` 和 `env.sh` 校验一致。

2026-10-08 06:00 UTC 完成 `gate-intra-arb01`、`bitget-intra-arb01` 退役，
RocksDB 归档并逐文件校验后删除部署目录及 `local_ips` 配置；共享出口 `.228`
仍由其他环境使用。`bitget-gate-cross-arb01` 的 Bitget 凭据已独立写入其 `env.sh`，
与原 `bitget-intra-arb01` 相同，Gate 凭据保持一致；Cross 配置服务已重新加载，
交易栈仍停止，不依赖已删除的目录。

```text
binance-cta-rx01             172.31.35.228                  RapidX/LTP
binance-cta-special-rx02     172.31.35.228                  RapidX/LTP
binance-cta-special-rx03     172.31.35.228                  RapidX/LTP
okex-intra-arb01             172.31.35.228, 172.31.35.228
okex_mm_alpha                172.31.35.229, 172.31.35.230
binance_fr_arb01             172.31.35.231, 172.31.35.231
binance_fr_arb02             172.31.35.231, 172.31.35.231
binance_fr_arb03             172.31.35.231, 172.31.35.231
binance_fr_arb04             172.31.35.231, 172.31.35.231
gate_fr_arb01                172.31.35.231, 172.31.35.231
gate_fr_arb03                172.31.35.231, 172.31.35.231
bitget_fr_arb01              172.31.35.231, 172.31.35.231
bitget_fr_arb02              172.31.35.231, 172.31.35.231
bitget_fr_arb03              172.31.35.231, 172.31.35.231
gate_fr_arb02                172.31.35.232, 172.31.35.233
bitget-gate-cross-arb01      0.0.0.0, 0.0.0.0
binance_exec_trade01         0.0.0.0, 0.0.0.0
binance_exec_trade02         172.31.35.228, 172.31.35.234
binance_exec_trade03         172.31.35.228, 172.31.35.234
binance_exec_trade04         172.31.35.228, 172.31.35.234
```

`0.0.0.0` 表示不绑独立 source IP，走默认出口（`.228`）。

## 已退役 intra RocksDB 归档

JP `~/retired_data/` 中仅保存以下 RocksDB 数据归档及各自 `.sha256` 文件，
权限为 `600`；归档不含 `env.sh`、配置、凭据或程序。两份归档均已核对完整文件清单、
文件大小与逐文件 SHA-256，回收两个部署目录约释放 4.28 GB。

| 环境 | tar.gz 文件 | 压缩后字节数 | SHA-256 |
| --- | --- | ---: | --- |
| `gate-intra-arb01` | `gate-intra-arb01_rocksdb_20261008T055407Z.tar.gz` | 150730111 | `093066d7667078c3f04158254e353df996794a4e042ce6316d959f7dda1bca8f` |
| `bitget-intra-arb01` | `bitget-intra-arb01_rocksdb_20261008T055407Z.tar.gz` | 519207764 | `c5968d1ba61241ed6584a6ca4a69ac0c1d14ba75ba91f5248b676859a1e00b4d` |

## 现场 socket 快照（2026-08-16）

| 私网 IP | 已建立 socket 数 |
| --- | ---: |
| `172.31.35.228` | 78 |
| `172.31.35.229` | 203 |
| `172.31.35.230` | 10 |
| `172.31.35.231` | 195 |
| `172.31.35.232` | 5 |
| `172.31.35.233` | 5 |
| `172.31.35.234` | 0 |

## 使用规则

1. 改 `local_ips`、给环境分配新 EIP、或下线占用 IP 的环境后，立刻改本文件的日期、表格和引用列表。
2. `.228` 不要改作他用。
3. `ens42` 的四个地址在写入任何 `trade_engine.toml` 之前先占表；`.234` 已由 Exec 环境共用配置。
4. `okex_mm_alpha` 现独占 `.229/.230`（原共用的 `binance_mm_alpha` 已下线退役）。
