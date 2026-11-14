# Hyperliquid 逐订单行情

两个项目提供独立、显式启用的逐订单入口，共用 `crates/hyperliquid_l4` 的源码及
二进制协议。源是节点侧 `l4Book`，输出是保留订单身份、精确价量与队列优先级的
紧凑 L3 快照/变更流。现有公共 API 的 BBO、L2、成交和指标入口继续独立运行。

## 数据源准备

官方公共 `wss://api.hyperliquid.xyz/ws` 不提供 `l4Book`。需要：

1. 非验证节点，记录 fills、order statuses 和 raw book diffs，按区块输出，并启用
   本地 info 请求；不需要成为验证者或质押。
2. 与 [order_book_server](https://github.com/hyperliquid-dex/order_book_server)
   协议兼容的服务，或提供相同协议的第三方数据源。

节点安装、磁盘和出口资源单独安排；本次仅实现采集、发布和消费能力，没有部署节点。
上游示例目前不支持现货订单簿、未触发的条件单。HIP-3 使用原生 `dex:coin`，
以数据源实际暴露的市场为准；不能用现货 `@index` 或内部拼接 symbol 代替。

## mkt_signal

在已同步的生产分支 `arbmm` 本地构建这个独立 workspace crate，无需编译交易或
CTP 程序，也不在运行主机编译：

```bash
cargo build --release -p hyperliquid_l4 --features ipc --bin hyperliquid_l4_pbs
```

运行时显式提供节点服务地址、原生 coin 和核绑定：

```bash
export HYPERLIQUID_L4_WS_URL='ws://NODE_SERVICE:8000/ws'
export HYPERLIQUID_L4_COINS='BTC,ETH'
taskset -c "$HYPERLIQUID_L4_CORE" ./hyperliquid_l4_pbs \
  --service dat_pbs/hyperliquid-futures/l4
```

输出为 iceoryx2 的精确长度 `[u8]` slice，默认 service 是
`dat_pbs/hyperliquid-futures/l4`。这是独立的订单级通道；现有
`dat_pbs/.../incremental` 的档位消费者不能直接读取。多核分片时，每进程提供互不
重叠的 coin 列表、独立核和 service 名；同一 service 只允许一个 publisher。
沿用项目的市场数据公共 service 命名，不加账户 `IPC_NAMESPACE` 前缀。

## crypto_proxy

按照仓库约定，在 `hk_root` 构建新二进制：

```bash
ssh hk_root 'source /root/.cargo/env && cd /root/crypto_proxy && cargo build --release --features hyperliquid-l4 --bin hyperliquid_l4_proxy -j2'
```

默认构建和 `cargo run` 仍使用原有 proxy 入口。新入口需显式 feature 和独立
ZMQ PUB 地址，不读取、覆盖生产节点的 YAML：

```bash
export HYPERLIQUID_L4_WS_URL='ws://NODE_SERVICE:8000/ws'
export HYPERLIQUID_L4_COINS='BTC,ETH'
taskset -c "$HYPERLIQUID_L4_CORE" ./hyperliquid_l4_proxy \
  --bind ipc:///tmp/hyperliquid-l4.ipc,tcp://127.0.0.1:19040
```

同一 PUB socket 可绑定多个显式地址。不要绑定已有 processor 的 L2 端口：
新消息类型为 1040，旧 processor/writer 没有逐单订单簿语义。本次不增加 Kafka
writer 或修改 C++ processor；需要归档时须单独实现并验证对应消费者。

## 参数与消费约束

两入口共享 `--ws-url`、`--fallback-ws-url`、`--coins`、`--reserve-events`、
`--max-events`、`--max-frame-bytes`、`--chunk-events`、`--resnapshot-secs`。
备用源切换必须重新取完整快照，不能拼接不同节点的订单历史。
`--max-source-lag-ms` 默认 10000：拒绝超过十秒的旧区块及超过五秒的未来区块。
只有明确回放历史或测试时才设为 0，不能把新的本地接收时间当作节点已追平。

默认预留 4096 条、每帧最多 100 万条记录、源帧最多 64 MiB、每输出块 256 条。
大快照首次到达时会在配置上限内扩容；重复峰值复用缓冲。按实际峰值调整预留和
上限，输出分块总长不超过 64 KiB。二进制数值为整数 e8，时间统一为微秒。

消费者使用共享 crate 的 `Header::decode`、`Event::decode` 和可选
`book::OrderBook`。同通道多币种先按 header 的原生 coin 分流，再交给各自的
OrderBook。完整快照/区块收到最后分块后才可见；缺块、跨 epoch 增量、
错误原数量和无效优先级锚点会撤销 readiness。另须使用 `is_fresh` 或等价策略
检查新鲜度。公网或节点连接断开、解析失败、关闭采集器均会发布 Reset。

ZMQ PUB 和 IPC 使用有界队列，不承诺慢消费者无损。跨批次 sequence 和 chunk
检查负责识别缺口；最后一次丢包仍需靠新鲜度发现。中途订阅的消费者须等下次
完整快照，默认 300 秒定时重新获取；要求立即就绪时先启动消费者，或重连源。
单币 source height 可以合法跳块，不能把不连续高度直接判断为漏单。

## 验证与维护

```bash
# mkt_signal：独立测试，无交易和 CTP 执行
cargo test -p hyperliquid_l4 --features ipc
cargo bench -p hyperliquid_l4 --bench decode

# crypto_proxy：hk_root 的隔离 worktree，共用 target 缓存，仅运行测试
CARGO_TARGET_DIR=/root/crypto_proxy/target cargo test --release --features hyperliquid-l4 --bin hyperliquid_l4_proxy --test hyperliquid_l4 -j2
```

完整协议、测试范围和性能测试口径见
[共享 crate 说明](../crates/hyperliquid_l4/README.md)。两仓库的 crate 必须逐文件一致，
修改时同步源码、夹具、测试和 benchmark，分别在各自锁定依赖下验证。
合并源码不激活任何生产进程；尚无实际节点源，当前验证使用本地模拟服务与合成
原生协议数据，不将其表述为真实链上行情验收。

## 2026-11-14 离线验收记录

`mkt_signal` 独立 crate 的 16 项测试通过，Clippy 全目标开启 IPC 且
`-D warnings` 通过，Release 二进制构建及 CLI 检查通过。
`crypto_proxy` 在 `hk_root` 的隔离 worktree 验证：新增 ZMQ/共享协议链路
16 项测试通过，原主程序 170 项通过、9 项既有外部测试忽略；新入口 Release
构建及 CLI 检查通过。未重启或部署生产进程。

本地 AMD EPYC 7642、Rust 1.98.0、sonic-rs 0.5.5，workspace 默认 Release
（`opt-level=3`、thin LTO、4 codegen units）下，一轮合成数据基准如下：

| 工作负载 | 条数/帧 | 条/秒 | 帧 p50 | 帧 p99 | 预热后堆分配/帧 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 快照解析、验证、归一化 | 1,000 | 1,626,514 | 580.6 µs | 950.9 µs | 0 |
| 快照解析、验证、归一化 | 100,000 | 1,610,248 | 61.80 ms | 67.28 ms | 0 |
| 数量更新解析、验证、归一化 | 1,000 | 2,764,464 | 361.6 µs | 397.0 µs | 0 |
| 数量更新解析、二进制编码、订单簿应用 | 1,000 | 2,323,158 | 430.6 µs | 442.9 µs | 0 |

快照原始 JSON 分别为 291,998 / 29,389,000 字节；数量更新使用已存在订单的
原数量/新数量变更，未把新单日期解析成本冒充数量更新成本。基准在测量前预热，
同时断言堆分配为 0。它只覆盖表内函数和合成字段分布，不包含网络、TLS、节点
重建、IPC/ZMQ 传输、慢消费者或生产突发分布；不代表真实行情端到端吞吐或延迟。
