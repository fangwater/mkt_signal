# CKBUSDT Binance FR Arb03 对冲请求序列覆盖故障记录（2026-07-13）

## 事件范围

本文记录 `binance_fr_arb03` 中 `CKBUSDT` 现货平仓成交后，合约对冲量未完整执行并逐批累积为敞口的问题。

排查时间：

- `2026-07-13` UTC
- 运行环境：`binance_fr_arb03`
- 现货腿：Binance Portfolio Margin / Margin
- 对冲腿：Binance USD-M Futures

主要证据来源：

- FR snapshot：`http://13.115.227.29:4191/fr/binance_fr_arb03/snapshot`
- `/home/ubuntu/.pmdaemon/logs/fr_pt_bn_arb03-error.log`
- `/home/ubuntu/.pmdaemon/logs/fr_te_bn_arb03-error.log`
- `/home/ubuntu/.pmdaemon/logs/fr_am_bn_arb03-error.log`
- `scripts/cancel_binance_pm_orders.py` 的 dry-run 输出

线上排查只执行了只读操作，没有撤单、补单或重启进程。排查完成后在仓库中实施了
ArbHedge single-flight 修复，但没有部署到线上环境。

## 结论摘要

这次问题不是 Binance 拒绝合约订单，也不是合约限价单长期未成交。

实际情况是：

1. CKB 现货平仓限价单分批成交。
2. 每笔现货终态都会触发一次 ArbHedge 状态查询，并产生不同的 `request_seq`。
3. `ArbHedgeStrategy` 只保存一个共享的 `pending_hedge_request_seq`。
4. 多笔成交短时间连续触发查询时，后一个请求会覆盖前一个 expected sequence。
5. 回复到达后，只有最新序号能够通过校验，较早回复被当作 `stale/duplicate` 丢弃。
6. 每个批次最终只执行了部分合约市价对冲，未执行部分留在 `pending_hedge_queue`，逐批累积为 `1,654,554 CKB` 的待对冲敞口。

因此，这是多次批量成交中分别漏掉一部分对冲，并非某一次一次性丢失全部对冲量。

## 现场状态

排查时 snapshot 中 CKB 数据为：

```text
open_qty              = 7,834,881.68556 CKB
hedge_qty             = -9,475,565 CKB
net_qty               = -1,640,683.31444 CKB
arb_due_hedge_qty     = -1,654,554 CKB
arb_pending_hedge_qty = -1,654,554 CKB
arb_hedge_is_taker    = true
```

按当时价格计算，净敞口约为 `-1,470 USDT`。

订单 dry-run 显示：

```text
Symbol          UM open  Margin open
--------------------------------------
CKBUSDT               0           10
```

含义：

- `UM open = 0`：不存在未成交的 CKB 合约挂单。
- `Margin open = 10`：存在 10 笔 CKB 现货 SELL 限价单。
- 合约对冲使用市价单，创建后立即成交，因此不会长期显示在 open orders 中。

## 数量核对

日志中的逐批成交和合约执行量如下：

| UTC 时间 | 现货 SELL 成交量 | 合约 BUY 成交量 | 本批少对冲量 |
| --- | ---: | ---: | ---: |
| `00:05:24` | `10 x 109,649 = 1,096,490` | `109,709` | `986,781` |
| `03:25:50` | `111,607` | `111,794` | `-187` |
| `03:26:34` | `4 x 111,482 = 445,928` | `111,731` | `334,197` |
| `03:27:42` | `4 x 111,358 = 445,432` | `111,669` | `333,763` |
| **合计** | | | **`1,654,554`** |

计算结果与 snapshot 完全一致：

```text
986,781 - 187 + 334,197 + 333,763 = 1,654,554 CKB
```

这证明当前 pending 不是展示误差，也不是仓位快照延迟，而是历史各批次少执行的合约对冲量精确累积而成。

## 合约订单实际执行情况

账户监控日志明确记录了以下 CKB UM Futures 市价 BUY：

| UTC 时间 | client_order_id | 数量 | 最终状态 |
| --- | --- | ---: | --- |
| `00:05:24` 至 `00:39:10` | `98920071853768705` | `109,709` | `FILLED` |
| `03:25:50` | `98920071853768706` | `111,794` | `FILLED` |
| `03:26:34` | `98920071853768707` | `111,731` | `FILLED` |
| `03:27:42` | `98920071853768708` | `111,669` | `FILLED` |

这些订单的 `price=0` 是本系统表示 Binance Futures 市价单的方式。日志中没有发现 Binance 对上述订单的业务拒绝。

所以“没有看到合约挂单”本身是正常现象；异常在于每个批次只生成了一笔不足以覆盖整批现货成交量的合约市价单。

## 请求序列覆盖过程

以同一批 4 笔现货成交为例，策略可能依次执行：

```text
现货成交 1 -> 发送 request_seq=12 -> pending expected=12
现货成交 2 -> 发送 request_seq=13 -> pending expected=13
现货成交 3 -> 发送 request_seq=14 -> pending expected=14
现货成交 4 -> 发送 request_seq=15 -> pending expected=15
```

当回复稍后依次到达时，策略内只剩最新的 `expected=15`：

```text
reply 12 != expected 15 -> drop stale/duplicate
reply 13 != expected 15 -> drop stale/duplicate
reply 14 != expected 15 -> drop stale/duplicate
reply 15 == expected 15 -> 接受并创建合约订单
```

实际日志与该过程一致：

```text
ArbHedgeStrategy: strategy_id=23031624 drop stale/duplicate ArbHedge reply:
symbol=CKBUSDT request_seq=12 expected_request_seq=15

ArbHedgeStrategy: strategy_id=23031624 drop stale/duplicate ArbHedge reply:
symbol=CKBUSDT request_seq=13 expected_request_seq=15

ArbHedgeStrategy: strategy_id=23031624 drop stale/duplicate ArbHedge reply:
symbol=CKBUSDT request_seq=14 expected_request_seq=15
```

后续批次同样出现：

```text
request_seq=16 expected_request_seq=19
request_seq=17 expected_request_seq=19
request_seq=18 expected_request_seq=19
```

## 代码根因

相关实现位于：

- `src/strategy/arb_hedge_strategy.rs`

发送查询时，每次生成新序号并覆盖单一 pending 字段：

```rust
let request_seq = self.next_hedge_request_seq();
self.pending_hedge_request_seq = Some(request_seq);
```

回复处理时，只接受与当前单一 pending 序号完全相同的回复：

```rust
let Some(expected_request_seq) = self.pending_hedge_request_seq else {
    return;
};

if ctx.request_seq != expected_request_seq {
    // drop stale/duplicate reply
    return;
}
```

因此，只要前一个请求尚未返回时又发送了新请求，前一个请求就失去被接受的可能。

需要特别注意：

- 不是每个请求发送时的 expected 都一样。
- 每个请求发送时 expected 会依次变为 `12/13/14/15`。
- 问题发生在回复阶段：所有回复都与当时共享字段中的最新值 `15` 比较。

理论上，如果最新请求的回复一定覆盖完整累计待对冲量，丢弃旧回复可能仍然安全。但本次日志证明，最新回复只生成了约一笔现货成交量的合约订单，没有覆盖同批其他成交，因此旧回复被丢弃后对应对冲量实际丢失。

## 当前第二层问题

少执行的数量仍保留在：

```text
arb_due_hedge_qty     = -1,654,554
arb_pending_hedge_qty = -1,654,554
```

按设计，`handle_period_clock()` 应当周期调用 `try_send_due_hedge_query()`，继续处理已经 due 的 pending 数量。但现场在最后一笔合约成交后仍长期保留上述 pending，没有继续将其排空。

这说明除了请求序列覆盖外，还需要单独验证周期重试路径为何没有把剩余 due hedge 补齐。可能相关的检查点包括：

- period clock 是否持续到达该 CKB strategy；
- lazy taker 模型是否持续返回 `Hold`；
- `next_query_ts_us` 是否被错误推迟；
- 是否存在新的 pending request 阻止后续重试；
- query 回复是否仍被后续 request sequence 覆盖。

这一层尚未通过当前日志完全定因，不能简单归结为 Binance 下单失败。

## 现货挂单上限的作用

pre_trade 日志持续出现：

```text
ArbCloseStrategy: symbol=CKBUSDT side=SELL
当前平仓限价挂单数=10，达到平仓方向上限 10
```

该风控解释了为什么不会继续创建更多 CKB 现货 SELL 限价单，但它不是合约对冲缺失的直接原因。

当前状态是：

- 已有 10 笔现货 SELL 挂单，占满平仓方向订单上限；
- 已成交的现货数量没有被完整合约 BUY 对冲；
- 缺失对冲量继续留在 ArbHedge pending 队列。

## 修复选择

本次选择 single-flight，而不是保存多个并发 query：同一 strategy 只保留一个有效 inflight，
新增成交继续累计 pending。该方案保留线性 seq，避免多个累计数量快照相互重叠，并通过
独立 timeout 防止回复丢失后永久阻塞。

乱序回复、period clock 排空和最终仓位对齐仍需要在部署后的真实批量成交场景中继续观察。

## 最终判断

本事件的主要故障模式为：

```text
多笔现货集中成交
    -> 连续触发多个 ArbHedge request
    -> 单一 expected request_seq 被最新请求覆盖
    -> 较早回复被判定 stale/duplicate
    -> 每批只执行部分合约市价对冲
    -> 未执行量逐批累积
    -> 最终形成 1,654,554 CKB 待对冲量
```

合约订单并非完全没有执行，而是每个集中成交批次只执行了一部分。当前证据能够精确证明漏对冲量的累计过程和 request sequence 覆盖现象。

## 修复实施记录

修复位于 `src/strategy/arb_hedge_strategy.rs`，实现内容如下：

1. 保留线性递增的 `request_seq` 作为请求关联 ID。
2. 将单一 `pending_hedge_request_seq` 替换为带发送时间和 deadline 的 `InflightHedgeQuery`。
3. 同一 ArbHedge strategy 同一时刻最多允许一个有效 query 在途。
4. 在途期间发生的新 opening-leg terminal 只累计 `pending_hedge_queue`，不再发送新 query，也不覆盖当前 seq。
5. backward query publish 成功后才登记 inflight；publisher 不可用或 publish 失败不会留下假在途状态。
6. query timeout 独立设置为 3 秒。deadline 到达后先退休旧 seq，再允许按最新 pending 重发。
7. 迟到或重复回复只有在 seq 与当前 inflight 完全匹配时才接受，旧回复不能清除新的 inflight。
8. direct taker 和 lazy model update 同样不能覆盖已有异步 query。

状态流转：

```text
Idle -> publish query(seq=N) -> InFlight(seq=N, deadline)
InFlight + 新现货成交 -> 只累计 pending，保持 seq=N
匹配回复 -> 清除 inflight -> borrow pending 并创建合约订单
timeout -> 退休 seq=N -> 基于最新 pending 重发 seq=N+1
迟到 reply(seq=N) -> 与当前 inflight 不匹配 -> 丢弃
```

新增测试：

- `inflight_hedge_query_coalesces_multiple_open_terminals`
- `inflight_hedge_query_retires_only_at_deadline`
- `late_hedge_reply_cannot_clear_new_inflight_query`

验证结果：

```text
cargo test --lib strategy::arb_hedge_strategy::tests
24 passed; 0 failed

cargo check --bin pre_trade
Finished successfully
```

修复尚未部署。部署前后仍需确认批量成交时不再发生 seq 覆盖，并观察剩余 pending 是否由 period clock 完整排空。
