# jp2 IP 绑定记录

日期：2026-06-26 UTC；更新：2026-06-30 UTC

## 结论

`jp2` 当前在 `ens41` 上一共绑定了 7 个私网 IPv4，对应 7 个公网 IPv4。

约定：

- `172.31.35.228` / `13.115.227.29` 固定为套利使用 IP，同时保留为主 IP、默认出口、SSH/管理入口。
- `172.31.35.231` / `52.69.78.134` 固定为资金费率使用 IP。
- `172.31.35.232` / `54.238.97.67` 和 `172.31.35.233` / `54.64.165.84` 已分配给 `gate_fr_arb02`、`binance_fr_arb04` 两个基础盘子共同使用。
- 新业务如果需要独立 source IP，优先从未使用池选择。

当前未使用池：

- `172.31.35.234` / `54.64.228.233`

## AWS ENI 映射

主机：

- SSH 别名：`jp2`
- hostname：`ip-172-31-35-228`
- 网卡：`ens41`
- ENI：`eni-046788fa6ed28d9ab`
- MAC：`06:52:65:8f:d7:37`

| 私网 IP | 公网 IP | 状态 | 当前用途 |
| --- | --- | --- | --- |
| `172.31.35.228` | `13.115.227.29` | 已使用 / 固定 | 套利使用 IP；同时是主 IP、默认出口、SSH/管理入口；`binance-intra-arb01`、`bitget-intra-arb01`、`okex-intra-arb01` 当前也显式配置在该 IP |
| `172.31.35.229` | `52.193.90.33` | 已使用 | `binance_mm_alpha` 的 `trade_engine.toml local_ips[0]` |
| `172.31.35.230` | `54.238.72.43` | 已使用 | `binance_mm_alpha` 的 `trade_engine.toml local_ips[1]` |
| `172.31.35.231` | `52.69.78.134` | 已分配 / 固定 | 资金费率使用 IP；`binance_fr_arb01-03`、`gate_fr_arb01`、`bitget_fr_arb02`、`okex_fr_arb01` 的 `trade_engine.toml` 写为 `.231/.231` |
| `172.31.35.232` | `54.238.97.67` | 已分配 | `gate_fr_arb02`、`binance_fr_arb04` 的 `trade_engine.toml local_ips[0]` |
| `172.31.35.233` | `54.64.165.84` | 已分配 | `gate_fr_arb02`、`binance_fr_arb04` 的 `trade_engine.toml local_ips[1]` |
| `172.31.35.234` | `54.64.228.233` | 未使用 | 无显式配置引用，实时 socket 数为 0 |

## 本机绑定状态

`ip -br addr` 显示 `ens41` 上已有：

```text
172.31.35.229/20
172.31.35.230/20
172.31.35.231/20
172.31.35.232/20
172.31.35.233/20
172.31.35.234/20
172.31.35.228/20
```

`/etc/netplan/50-cloud-init.yaml` 中持久化的是 secondary IP：

```text
172.31.35.229/20
172.31.35.230/20
172.31.35.231/20
172.31.35.232/20
172.31.35.233/20
172.31.35.234/20
```

`172.31.35.228/20` 是 DHCP 主地址。

## 当前使用证据

配置引用：

```text
~/binance-intra-arb01/trade_engine.toml:
  local_ips = ["172.31.35.228", "172.31.35.228"]
  binance_um_whitelist_ip = "172.31.35.228"

~/bitget-intra-arb01/trade_engine.toml:
  local_ips = ["172.31.35.228", "172.31.35.228"]

~/binance_mm_alpha/trade_engine.toml:
  local_ips = ["172.31.35.229", "172.31.35.230"]

~/binance_fr_arb01/trade_engine.toml:
  local_ips = ["172.31.35.231", "172.31.35.231"]

~/binance_fr_arb02/trade_engine.toml:
  local_ips = ["172.31.35.231", "172.31.35.231"]

~/binance_fr_arb03/trade_engine.toml:
  local_ips = ["172.31.35.231", "172.31.35.231"]

~/binance_fr_arb04/trade_engine.toml:
  local_ips = ["172.31.35.232", "172.31.35.233"]

~/gate_fr_arb01/trade_engine.toml:
  local_ips = ["172.31.35.231", "172.31.35.231"]

~/gate_fr_arb02/trade_engine.toml:
  local_ips = ["172.31.35.232", "172.31.35.233"]

~/bitget_fr_arb02/trade_engine.toml:
  local_ips = ["172.31.35.231", "172.31.35.231"]

~/okex_fr_arb01/trade_engine.toml:
  local_ips = ["172.31.35.231", "172.31.35.231"]

~/okex-intra-arb01/trade_engine.toml:
  local_ips = ["172.31.35.228", "172.31.35.228"]
```

实时 socket 聚合：

| 私网 IP | socket 数 | 进程概况 |
| --- | ---: | --- |
| `172.31.35.228` | 631 | `spread_pbs`、`trade_engine`、`account_monitor`、`trade_signal`、`nginx`、`sshd`、`systemd-network` |
| `172.31.35.229` | 10 | `trade_engine`、`account_monitor`、`trade_signal` |
| `172.31.35.230` | 10 | `trade_engine`、`account_monitor` |
| `172.31.35.231` | 0 | 无 |
| `172.31.35.232` | 预留连接口 | `gate_fr_arb02`、`binance_fr_arb04` 已配置，待交易链路启动后建立连接 |
| `172.31.35.233` | 预留连接口 | `gate_fr_arb02`、`binance_fr_arb04` 已配置，待交易链路启动后建立连接 |
| `172.31.35.234` | 0 | 无 |

说明：socket 聚合采集于修改资金费率配置前；资金费率进程重启后才会实际从 `.231` 建立新连接。

## 资金费率 IP 更新

2026-06-26 已在 `jp2` 上修改以下环境的 `trade_engine.toml`，统一为：

```toml
local_ips = ["172.31.35.231", "172.31.35.231"]
```

已修改：

- `~/binance_fr_arb01/trade_engine.toml`
- `~/binance_fr_arb02/trade_engine.toml`
- `~/binance_fr_arb03/trade_engine.toml`
- `~/gate_fr_arb01/trade_engine.toml`
- `~/bitget_fr_arb02/trade_engine.toml`
- `~/okex_fr_arb01/trade_engine.toml`

2026-06-30 已将两个基础盘子改为共同使用 `.232/.233`：

```toml
local_ips = ["172.31.35.232", "172.31.35.233"]
```

已修改：

- `~/gate_fr_arb02/trade_engine.toml`
- `~/binance_fr_arb04/trade_engine.toml`

同日已更新两个基础盘子的 `trade_engine`、`pre_trade`、`trade_signal` 和对应 `account_monitor` 二进制。

2026-06-26 新增部署了 `~/gate_fr_arb02` 和 `~/binance_fr_arb04` 空 key 壳子，仅启动 config server 和 viz server，未启动交易链路。

| 环境 | config server | viz server | dashboard 预留 | persist 预留 |
| --- | ---: | ---: | ---: | --- |
| `gate_fr_arb02` | `20022` | `20122` | `20172` | `127.0.0.1:50050` |
| `binance_fr_arb04` | `20034` | `20134` | `20144` | `127.0.0.1:50052` |

2026-06-26 另在 `jp2` 上部署了空 key 的 `~/okex-intra-arb01` 壳子，按套利 IP 约定写为：

```toml
local_ips = ["172.31.35.228", "172.31.35.228"]
```

## 使用规则

1. `.228` 固定为套利使用 IP，同时保持为主 IP、默认出口和管理入口。
2. `.229` 和 `.230` 当前由 `binance_mm_alpha` 使用。
3. `.231` 固定为已运行资金费率盘子使用 IP。
4. `.232` 和 `.233` 当前由 `gate_fr_arb02`、`binance_fr_arb04` 两个基础盘子共同使用。
5. `.234` 是可分配池。分配后需要同步更新本文件。
6. 如需验证公网出口，使用 `curl --interface <private-ip> https://checkip.amazonaws.com`，不要直接把公网 IP 加到 Linux 网卡上。
