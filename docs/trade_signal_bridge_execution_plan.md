# Trade Signal Bridge Execution Plan

## Goal

在不改 `dat_pbs` 当前 subscriber 上限、不大规模重启现有消费者的前提下，引入一个本地 `ipc -> ipc` 行情中转层：

- 上游继续从 `dat_pbs/<venue>/...` 发布
- bridge 本地订阅上游
- bridge 本地重新发布到 `bridge/<venue>/...`
- `trade_signal`、`pre_trade`、`rolling_metrics` 逐步切到 `bridge/<venue>/...`

优先级：

1. 先打通本地 bridge 能力
2. 先切 `trade_signal`
3. 再切 `pre_trade`
4. 最后切 `rolling_metrics`

---

## Current State

### Current direct subscribers

- `trade_signal`
  - 直接订阅 `dat_pbs/{venue}/ask_bid_spread`
  - 直接订阅 `dat_pbs/{venue}/derivatives`
- `pre_trade`
  - 直接订阅 `dat_pbs/binance-futures/derivatives`
- `rolling_metrics`
  - 直接订阅 `dat_pbs/{venue}/ask_bid_spread`

### Current bridge limitations

- 现有 `ipc_bridge` 只支持：
  - `iceoryx -> ZMQ -> iceoryx`
- 当前不支持：
  - `from.node == to.node == self_id` 的本地 `ipc -> ipc`
- 当前 bridge publisher/subscriber 创建时没有 route 级别的：
  - `max_subscribers`
  - `history_size`
  - `subscriber_max_buffer_size`

### Current payload sizes

- `ask_bid_spread`: `128`
- `derivatives`: `128`

说明：

- `size = 128` 表示单条 IceOryx 消息 payload 的固定容量是 `128 bytes`
- 它不是 subscriber 数量
- bridge 目标 service 的 `size` 必须与消息类型兼容，不能配小

---

## Phase Plan

## Phase 1: Add Local IPC-to-IPC Bridge

### Objective

让 `ipc_bridge` 支持本地 route：

- `dat_pbs/binance-futures/ask_bid_spread -> bridge/binance-futures/ask_bid_spread`
- `dat_pbs/binance-futures/derivatives -> bridge/binance-futures/derivatives`

### Required code changes

#### 1. Extend bridge route handling

文件：

- `src/bridge/mod.rs`

改动：

- 不再跳过 `from.node == to.node == self_id`
- 对于本地 route：
  - 不创建 ZMQ sender/receiver
  - 直接创建本地 IceOryx subscriber
  - 直接创建本地 IceOryx publisher
  - 建立本地 forwarding task

#### 2. Extend bridge config schema

文件：

- `src/bridge/cfg.rs`

改动：

- 给 `RouteEndpoint` 增加可选字段：
  - `max_publishers`
  - `max_subscribers`
  - `history_size`
  - `subscriber_max_buffer_size`

要求：

- 都是 optional
- 不破坏旧 config

#### 3. Extend bridge IceOryx publisher/subscriber creation

文件：

- `src/bridge/iceoryx.rs`

改动：

- 允许 route 级别配置传入 publisher/subscriber service builder
- 对目标 bridge service 显式设置：
  - `max_publishers = 1`
  - `max_subscribers = 64`
  - `history_size = 50` 或按 route 配
  - `subscriber_max_buffer_size = 8192`

### Deliverable

两份可运行的本地 bridge config：

- `config/ipc_bridge_jp.yaml`
- `config/ipc_bridge_hk.yaml`

包含 route：

- `dat_pbs/binance-futures/ask_bid_spread -> bridge/binance-futures/ask_bid_spread`
- `dat_pbs/binance-futures/derivatives -> bridge/binance-futures/derivatives`

---

## Phase 2: Switch Trade Signal to Bridge

### Objective

只改 `trade_signal`，让它优先从 `bridge/...` 订阅，而不是直接订阅 `dat_pbs/...`

### Required code changes

#### 1. Add service-root override to MktChannel

文件：

- `src/funding_rate/mkt_channel.rs`

改动：

- 现在是硬编码：
  - `dat_pbs/{slug}/ask_bid_spread`
  - `dat_pbs/{slug}/derivatives`
- 改成可配置 service root：
  - 默认 `dat_pbs`
  - 可切成 `bridge`

建议接口：

- 读 env，例如：
  - `MKT_CHANNEL_SERVICE_ROOT=bridge`
- 或显式函数参数

推荐先用 env，改动最小。

#### 2. Add runtime wiring in trade_signal

文件：

- `src/bin/trade_signal.rs`

改动：

- 支持启用 bridge 模式
- 默认保持原行为
- bridge 模式下：
  - `ask_bid_spread` 从 `bridge/{venue}/ask_bid_spread`
  - `derivatives` 从 `bridge/{venue}/derivatives`

### Deliverable

可以只重启 bridge + `trade_signal` 完成切换，不动 `dat_pbs` 和其他消费者。

---

## Phase 3: Switch Pre Trade Derivatives to Bridge

### Objective

只改 `pre_trade` 的 derivatives 订阅，不动它的 account/pub 逻辑。

### Required code changes

文件：

- `src/pre_trade/monitor_channel.rs`

改动：

- 当前硬编码：
  - `dat_pbs/binance-futures/derivatives`
- 改成可 override：
  - 默认 `dat_pbs/binance-futures/derivatives`
  - bridge 模式下用 `bridge/binance-futures/derivatives`

注意：

- `pre_trade` 不需要切 `ask_bid_spread`
- 只切 derivatives

---

## Phase 4: Switch Rolling Metrics AskBid to Bridge

### Objective

让 `rolling_metrics` 从 `bridge/<venue>/ask_bid_spread` 订阅。

### Required code changes

文件：

- `src/common/iceoryx_subscriber.rs`
- `src/bin/rolling_metrics.rs`

改动：

- 当前 `MultiChannelSubscriber` 硬编码拼接：
  - `dat_pbs/{topic_prefix}/{channel}`
- 需要支持配置 service root：
  - 默认 `dat_pbs`
  - 可设为 `bridge`

建议：

- 给 `SubscribeParams` 增加 `service_root`
- 默认值仍然是 `dat_pbs`
- `rolling_metrics` 启动时按参数或 env 传入 `bridge`

注意：

- `rolling_metrics` 当前只订阅 `ask_bid_spread`
- 它不消费 `derivatives`

---

## Recommended Execution Order

1. 实现 Phase 1，本地 `ipc -> ipc` bridge 能力
2. 增加 bridge config，并本地启动 bridge
3. 实现 Phase 2，只切 `trade_signal`
4. 验证：
   - `trade_signal` 不再直接占用 `dat_pbs/.../derivatives`
   - `bridge/.../derivatives` subscriber 数可以拉高到 `64`
5. 实现 Phase 3，切 `pre_trade`
6. 实现 Phase 4，切 `rolling_metrics`

---

## Suggested Runtime Names

为了区分原始行情和桥接行情，建议 service 名称固定为：

- `bridge/binance-futures/ask_bid_spread`
- `bridge/binance-futures/derivatives`

以后如果扩展到其他 venue，继续复用：

- `bridge/<venue>/ask_bid_spread`
- `bridge/<venue>/derivatives`

---

## Validation Checklist

### Bridge phase validation

- bridge 进程启动成功
- 本地 route 没有走 ZMQ
- `bridge/binance-futures/ask_bid_spread` 有消息流入
- `bridge/binance-futures/derivatives` 有消息流入

### Trade signal validation

- `trade_signal` 日志中订阅源切到 `bridge/...`
- 决策逻辑正常工作
- `dat_pbs/binance-futures/derivatives` subscriber 压力下降

### Pre trade validation

- `pre_trade` 日志中 derivatives 订阅源切到 `bridge/...`
- 不再报 `ExceedsMaxSupportedSubscribers`

### Rolling metrics validation

- `rolling_metrics` 正常收到两侧 `ask_bid_spread`
- Redis 输出和原逻辑一致

---

## Risks

### Risk 1: bridge service parameters mismatch

如果 `bridge/...` 的 payload size / history / subscriber 参数和 consumer 打开方式不一致，会出现同类 IceOryx 打不开的问题。

### Risk 2: partial migration causes mixed roots

如果有的服务仍订阅 `dat_pbs`，有的服务订阅 `bridge`，需要明确日志区分，避免排障时混淆。

### Risk 3: rolling_metrics common subscriber is hardcoded

`rolling_metrics` 不是简单改一行常量，需要先扩展公共订阅器。

---

## First Concrete Task

第一步建议只做：

- Phase 1
- Phase 2

即：

- 先让 `ipc_bridge` 支持本地 `ipc -> ipc`
- 再只切 `trade_signal`

这是最小可交付版本，也是当前最值得优先验证的方案。
