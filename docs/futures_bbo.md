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

每家每 30 秒记录 BBO 发布数、序号丢弃数、已见币种数；每 1024 条成功 BBO 采样本地接收至 IPC 发布耗时，输出均值和最大值；同时记录交易所消息时间戳至本机接收的均值、最小值、最大值及负值数，不维护延迟分位数结构。后者使用当前解析器的时间字段（Bitget 为 `sts`；Gate 为 BBO 事件时间），包含交易所内部耗时、网络传输及双方时钟差，不等于纯网络延迟；负值保留，不静默丢弃。两家共享此进程的一个核心；高峰下两家 BBO 仍会竞争该核心，实际余量需部署后测量。
