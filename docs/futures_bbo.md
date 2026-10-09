# Gate / Bitget 独立合约 BBO

`spread_pbs --venue gate-bitget-futures-bbo --core <core>` 在一个绑核、单线程进程内接收 Gate 和 Bitget 的 USDT 合约 BBO。两家分别保留双路 WebSocket、序号去重和错峰重连，复用 SBE 借用解析及 IPC 发布逻辑。

该入口强制只订阅 Gate `futures.book_ticker` 和 Bitget `books1`。即使 `mkt_cfg.yaml` 或 `SPREAD_PBS_ENABLE_*` 打开其他行情，也不创建成交、深度、ticker 发布器或额外连接。币种列表从两家的合约接口取得，不与现货/杠杆币种取交集；启动和重连刷新使用相同范围。当前范围是两家 USDT 合约，不含币本位合约。

## IPC

| 行情 | 服务名 |
| --- | --- |
| Gate 合约 BBO | `futures_bbo/gate-futures/ask_bid_spread` |
| Bitget 合约 BBO | `futures_bbo/bitget-futures/ask_bid_spread` |

消息仍是现有 128 字节载荷中的 `AskBidSpreadMsg`，币种采用内部格式（如 `BTCUSDT`），时间戳为微秒。订阅方显式选择 `futures_bbo` 根路径。现有 FR 使用的 `spread_pbs/...` 可同时运行，启动此进程不会切换 FR 的数据源。

`--test` 改用 `futures_bbo_test/...`，与正式独立通道也隔离。`SPREAD_PBS_SYMBOLS=BTCUSDT,ETHUSDT` 可限制订阅范围；不设置时订阅完整可交易 USDT 合约列表。

## 本地验证与部署入口

```bash
cargo build --release --bin spread_pbs
SPREAD_PBS_SYMBOLS=BTCUSDT,ETHUSDT RUST_LOG=info \
  ./target/release/spread_pbs --venue gate-bitget-futures-bbo --core <可用核心> --test
```

沿用 `config/mkt_cfg.yaml` 的源 IP 和重连周期配置。部署目录名为 `gate-bitget-futures-bbo`，例如 `~/spread_pbs/gate-bitget-futures-bbo/`；其中放置独立的 `spread_pbs` 发布产物、`env.sh`、`config/mkt_cfg.yaml` 和 `config/iceoryx2.toml`，将仓库 `scripts/spread_pbs/start_spread_pbs.sh`、`stop_spread_pbs.sh` 放到该目录的 `scripts/` 下。

`env.sh` 必须显式设置 `SPREAD_PBS_CORE`，此入口不指定生产默认核心。环境内使用 `./scripts/start_spread_pbs.sh` 和 `./scripts/stop_spread_pbs.sh` 管理，supervisor 名称为 `spp_gate_bitget_futures_bbo`。正式部署仍遵循仓库的本地 release 构建、校验上传、原子安装流程，并同步 CPU/IP 运维文档。

每家每 30 秒记录 BBO 发布数、序号丢弃数、已见币种数；每 1024 条成功 BBO 采样本地接收至 IPC 发布耗时，输出均值和最大值；同时记录交易所消息时间戳至本机接收的均值、最小值、最大值及负值数，不维护延迟分位数结构。后者使用当前解析器的时间字段（Bitget 为 `sts`；Gate 为撮合引擎的 BBO 更新时间 `t`），包含交易所内部耗时、网络传输及双方时钟差，不等于纯网络延迟；负值保留，不静默丢弃。两家共享此进程的一个核心；高峰下两家 BBO 仍会竞争该核心，实际余量需部署后测量。


## JP 部署

2026-10-09 03:04 UTC 已在 `jp-meta-elvpn` 发布，代码提交 `78150e7a`。

- 环境：`/home/ubuntu/spread_pbs/gate-bitget-futures-bbo`。
- CPU：16；primary / secondary 源 IP：`172.31.46.90` / `172.31.46.91`，均走 ens42 / table 101。
- 启动时查询到 Gate 1028、Bitget 816 个可交易 USDT 合约，未设置币种过滤。
- 产物 SHA-256：`4c8c6319fb97a60f34f8fd4fbe04b2551bb04f8f6a53f6077dce99ff13a02b90`；本地、上传后及运行中的 `/proc/<pid>/exe` 校验一致。
- 通过环境内 `./scripts/start_spread_pbs.sh` 启动；日志为 `~/.pmdaemon/logs/spp_gate_bitget_futures_bbo-error.log`。
- `gate-both` / `bitget-both` 原行情进程未重启，FR 继续消费原通道。

查看最新采样统计：

```bash
ssh jp-meta-elvpn 'tail -n 30 ~/.pmdaemon/logs/spp_gate_bitget_futures_bbo-error.log'
```


## 首次线上延迟观察

2026-10-09 03:04:38–03:09:38 UTC，排除启动后前 30 秒，汇总随后 10 个完整 30 秒窗口。每 1024 条成功发布的 BBO 采样一次，两路去重后统计，消息较活跃的币种权重较高。

| 交易所及统计起点 | 样本数 | 起点至本机接收均值 | 最小值 | 采样最大值 | 接收至 IPC 均值 / 采样最大值 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Gate：撮合引擎 `t` | 703 | 约 2.16 ms | 0.420 ms | 210.880 ms | < 1 µs / 2 µs |
| Bitget：流服务推送 `sts` | 829 | 约 7.35 ms | 1.968 ms | 157.280 ms | < 1 µs / 2 µs |

两家统计起点不同，不代表同口径网络延迟排名。所有采样最大值也不等于全量消息最大值或 P99。窗口日志均值截断为整数微秒，因此本地均值显示 0 的正确解释为低于 1 µs。本地处理统计从用户态 WS 完整消息接收点开始，未覆盖此前的内核/调度排队；到达统计还包含双方时钟差。本机 chrony 对 AWS NTP 偏差在核验时低于 1 µs，未声称与交易所时钟严格对齐。

同期十次 30 秒 CPU 采样：平均 57.25% 单核，最高 59.80%，RSS 40.82 MiB。两家四条连接持续发布，无运行错误日志；保留原有每 5 分钟去重水位重置行为。运行中的原 Gate/Bitget FR 行情 PID 均未变化。

原始窗口和汇总数据：[futures_bbo_latency.json](../artifacts/futures_bbo_latency.json)。

## 官方接口核对（2026-10-09）

- [Bitget SBE BBO](https://www.bitget.com/docs/uta/websocket/sbe/sbe-bbo)：当前 `books1` 为实时推送，`ts` 是撮合时间（格式为微秒、精度为毫秒），`sts` 为流服务推送时间。本进程使用 `sts` 计算到达延迟，尚未单独统计 `sts-ts`，因此不能把当前 Bitget 数字称为撮合至到达总耗时。BBO 支持 auto-culling，序号跳跃不能直接断言为网络丢包。
- [Bitget Quick Start](https://www.bitget.com/docs/uta/quick-start)：VIP Line 绕过 CDN，SBE 地址为 `wss://vip-ws.bitget.com/v3/ws/public/sbe`；Lo-La 高速地址为 `wss://vip-ws-uta-pub-a.bitget.com/v3/ws/public/sbe`，高可用地址为 `wss://vip-ws-uta.bitget.com/v3/ws/public/sbe`。这些为需申请资格的 VIP/机构线路，当前部署仍使用普通入口，未声称已获接入资格或已验证提速。
- [Bitget 更新日志](https://www.bitget.com/legacy-docs/uta/changelog)：2026-05-19 SBE 增加 `sts/category`，当前 BBO 解析已使用；2026-09-29 预告 `books50` 于 10 月底改为 snapshot + incremental，当前 `books1` 独立服务不受该深度变更影响。
- [Gate Futures WS](https://www.gate.com/docs/developers/futures/ws/en/) 推荐 SBE，当前使用的 `futures.book_ticker` 为实时 BBO；[官方生产 XML](https://github.com/gate/gatews/blob/master/sbe/schemas/prod/gate_fex_ws_latest.xml) 的 schemaId/version 为 1/1，BBO 中 `time` 为 WS 发送时间、`t` 为撮合引擎更新时间。本进程目前使用 `t`，所以 Gate 与 Bitget 的统计起点不同，不宜直接作为同口径网络时延排名。

后续优先实验：在资格满足后对比 Bitget 普通/VIP/Lo-La 相同币种、相同 seq 的到达时差；在官方 DNS 地址范围内让 Gate 双路连接选择不同 IP，再比较先到率及尾延迟。Bitget 单连接当前覆盖全部 816 个合约，可在同进程内按币种拆分连接测试；官方通用 WS 指南建议每连接少于 50 个频道，拆分需一并遵守连接与订阅限速。以上为待测建议，本轮部署未更改既有 FR 线路或共享 NIC 参数。
