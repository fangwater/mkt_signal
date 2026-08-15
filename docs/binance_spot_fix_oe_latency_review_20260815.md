# Binance Spot FIX 下单延迟评审与修复

日期：2026-08-15

## 背景与结论

多天实测（含 2026-08-15 的 maker 挂单延迟分析
`binance_intra_arb01_latency_7d_6h_20260815.ipynb`）显示 Binance spot 下单走 FIX OE
比走 WS API 更慢，与"FIX 普遍更快"的先验相反。

结论分两层：

1. 本仓库 FIX OE 客户端（`crates/trade_engine/src/binance_fix.rs`）存在三个具体实现
   差距，全部相对于自己的 WS 下单路径，且恰好都在 maker 撤改单 burst 工况下惩罚
   FIX。本次已修复，见"本次修复内容"。
2. Binance FIX OE 本身并不承诺比 WS API 下单更快。官方文档只承诺 ExecutionReport
   push 性能与独立限额池；社区实测甚至观察到 FIX 行情流比 WS 慢，且有从业者称
   Binance FIX 构建在其 WS 基础设施之上。修复客户端问题后合理预期是 FIX ≈ WS，
   FIX 的真实价值在独立限额（10,000 msg/10s）与会话语义，不在单笔延迟。

"FIX 更快"的经验来自传统金融（交易所原生 FIX 直连撮合）与厂商营销，对加密交易所
的网关型 FIX 实现不直接成立，需要逐所实测。

## 测量口径

主口径来自 `uniform_orders.parquet`：

- `create_ts`：新订单请求首次 publish 的本地时间（微秒）。
- `update_ts`：交易所订单事件时间（毫秒精度，存为微秒）。
- maker 挂单延迟 = `NEW.update_ts - create_ts`，覆盖 trade_engine 路由、socket
  发送、网络、交易所网关到撮合确认全过程。FIX ER 的 TransactTime(60) 与 WS
  response 的 transactTime 同为毫秒精度，两条路径可比。
- 2026-08-15 快照：margin NEW ack（`update_minus_create`）p50 约 0.94-1.3ms。

该口径对 FIX/WS 公平：两条路径都从同一个 `create_ts` 起算，终点都是交易所侧
事件时间戳。

## 根因分析

### 根因一：FIX OE socket 完全未调优（影响最大）

WS 下单连接建立时执行 `tune_tcp_stream`（`crates/trade_engine/src/ws_client.rs`
`trade_engine_tcp_tuning()`）：

```text
TCP_NODELAY      = on
TCP_QUICKACK     = on
TCP_USER_TIMEOUT = 30000ms
SO_BUSY_POLL     = 8us
```

而 FIX 的 `connect_tcp_addr` 是裸 `TcpSocket::connect`，四项全缺。

机制上最致命的是缺 TCP_NODELAY：Nagle 算法生效时，若上一段小包尚未被 ACK，
新的小包会滞留内核等待；对端 delayed ACK 最多再拖 40ms。maker 策略的
挂单 + 撤单/改单经常在一个 RTT 窗口内连发，第二条消息恰好命中 Nagle 最坏情况，
直接体现在挂单延迟的 p99 尾部。缺 SO_BUSY_POLL 则让 FIX 响应读路径每次多走
完整的中断 + epoll 唤醒（几微秒到几十微秒）。

对照：行情侧 FIX SBE 客户端（`src/spread_pbs/binance_fix_sbe.rs`）调用了
`tune_tcp_stream`，说明这是 OE 客户端的遗漏而非有意设计。

### 根因二：Logon 硬编码 MessageHandling(25035)=SEQUENTIAL

原代码 `build_logon` 写死 `25035=2`（SEQUENTIAL），要求网关严格按 MsgSeqNum
顺序把本会话消息送进撮合。官方文档明确：

> `UNORDERED(1)` should offer better performance when there are multiple
> messages in flight from the client to the server.

高频撤改单必然多消息 in-flight，SEQUENTIAL 在交易所侧引入串行化头部阻塞，
与根因一叠加（都在 burst 工况发力）。

### 根因三：ResponseMode(25036) 未设置，默认 EVERYTHING

官方文档：默认模式下，每条 OE 会话会收到**全账户**所有成功的
ExecutionReport / ListStatus 推送，包括其他 FIX 会话和非 FIX 渠道（WS/REST）
下的单。本系统成交与订单状态另有 `account_monitor` user stream 全量覆盖，
FIX 会话只需要请求 ack；全账户 ER 推送在一个未调优 socket 上放大了读路径
负载，并与下单发送在同一个 select loop 里互相排队。

### 次要差距（微秒级）

- WS 是 4 条连接轮询（`BINANCE_BASIC_WS_CONNECTIONS = 4`），FIX 原来只有
  1 条会话扛全部流量（含全账户 ER 推送）。
- FIX 报文构造为 `Vec<(u32, String)>` + `String` 拼接 + `chrono` 格式化
  SendingTime；WS 侧是 `itoa` 优化的 `build_typed_order_payload_fast`。
- 读路径 `read_fix_message` 每取一条消息做一次 `Vec::drain` 头部搬移。
- inflight 登记为三个 `HashMap<String, _>`，`remove_seq_for_key` O(n)。

单项 0.5-2us，合计每单约 2-5us，不是 ms 级差距的主因。

## 交易所侧公开信息

- 官方 fix-api.md 对性能的表述仅有："FIX API should give better performance
  for ExecutionReport <8> push"，从未承诺下单 ack 延迟更低。
- FIX OE 独立限额：10,000 msg/10s 每会话；每账户最多 10 条并发连接，
  30 秒内最多 15 次连接尝试；SenderCompID 必须在账户活跃会话间唯一
  （`^[a-zA-Z0-9-_]{1,8}$`）。
- 社区实测（Petio Petrov, 2025）：FIX 行情流比 WS 慢约 2ms；评论区从业者称
  "Binance FIX is built on top of their current websocket infrastructure,
  not the other way around"。
- 未按时发送 Logout 断开会导致该 SenderCompID 在 2x HeartBtInt 内不可重新
  建会话（多会话轮换时需注意）。

## 本次修复内容

三处修改均在 `crates/trade_engine/src/`：

1. socket 调优（`binance_fix.rs`）：FIX TCP 连接建立后调用
   `tune_tcp_stream`，参数与 WS 下单连接完全一致（NODELAY + QUICKACK +
   USER_TIMEOUT 30s + SO_BUSY_POLL 8us），复用 `ws_client.rs` 的
   `trade_engine_tcp_tuning()`。
2. Logon 参数（`binance_fix.rs`）：`25035=1`（UNORDERED）；新增
   `25036=2`（ONLY_ACKS）。ONLY_ACKS 下本会话请求的 ack（含成功/失败的
   ExecutionReport、OrderCancelReject）仍会返回，只是不再推送全账户 ER；
   现有代码对首个 ack 之后的 ER 本来就按 unsolicited 丢弃，行为不变。
3. 多会话 RR（`binance_fix.rs` + `engine.rs`）：
   - 会话数固定 2 条（代码常量 `FIX_OE_SESSIONS`，不做 env 开关）。
   - 每条会话独立 TCP + Logon + seq 空间，SenderCompID 唯一：未设置
     `BINANCE_FIX_SENDER_COMP_ID` 时每会话随机生成；设置了则截断到
     7 字符后追加会话序号。
   - `engine.rs` req_worker 对 FIX 请求按 round-robin 选第一条
     `logged_on` 的会话入队；全部不可用才回 503（原行为是单会话不可用即 503，
     多会话同时提升了可用性）。

### 配置项汇总

```bash
BINANCE_SPOT_FIX_ENABLED=on          # 既有开关，默认 off
BINANCE_FIX_SENDER_COMP_ID=<可选>    # 既有；每会话自动派生唯一后缀
BINANCE_FIX_OE_URL=<可选>            # 既有，默认 tcp+tls://fix-oe.binance.com:9000
```

会话数写死在代码里（`FIX_OE_SESSIONS = 2`），无运行时开关。

## 微秒级优化（第二批，已实施）

- FIX 报文构造改为 `FixMessageWriter`：body/out 双缓冲复用，tag 与整数经
  `itoa` 写入，价格/数量用 `QuantizedValue::write_decimal_to` 直写，
  热路径（新单/撤单）零中间 String 分配；logon/logout/heartbeat 冷路径
  保持原一次性构造。
- SendingTime 改 `FixTimeFormatter`：缓存 `YYYYMMDD-` 日期前缀（每 UTC 日
  重算一次），每条消息只手工格式化 HH:MM:SS.mmm，输出与 chrono
  `%Y%m%d-%H:%M:%S%.3f` 逐字节一致（有对照测试）。
- inflight 登记改 `InflightFixTable`：三个 `FastHashMap<i64, _>`
  （`client_order_id` 本来就是 i64），seq 内嵌在记录里做双向索引，
  任一键删除 O(1)；原 `remove_seq_for_key` O(n) 扫描删除。
- 读缓冲改 `FixReadBuffer`（读指针 + 按需 compact）：取走一条消息只前移
  指针，仅在跨 read 有残留时做一次小段搬移，消掉每条消息的头部 memmove。
- 参数校验通过后才分配 MsgSeqNum，避免校验失败烧号造成 seq gap
  （保持原语义）。

明确不做（除非后续 trace 埋点证明值得）：req_worker 直接内联 send 的
socket 读写分离改造。req_worker 到 FIX/WS 任务之间的一次队列 + 唤醒约
1-3us，改动大、影响面广，收益需先由 send_start/send_done 埋点证实。

## 验证方法

1. per-transport A/B：响应 body 已带 `"transport":"fix"`；把 transport 维度
   带进 uniform_orders（或先用日志聚合），同时段交替路由，分 transport 统计
   `update_ts - create_ts` 分位数。样本要求见
   `docs/order_latency_chain_test_plan.md`（保留 n/negative/p50/p99/max）。
2. 内核 RTT 对比：`ss -ti` 分别看 fix-oe.binance.com 与 ws-api.binance.com
   连接的 rtt；若两域名不在同一 POP，RTT 差是物理事实，客户端无法弥补。
3. 把现有 `read_tcp_retrans_snapshot`（TCP_INFO 采样）接到 FIX socket，
   对齐 WS 的重传/RTT 健康度监控（未在本次范围内）。
4. 多会话生效验证：日志应出现多条
   `Binance Spot FIX logon successful sender_comp_id=...`，且
   `routed Binance spot order to FIX` 的 session 索引轮换。

## 风险提示

- News<B> 当前按会话终止处理（触发重连）。官方语义是维护前每 10s 推送一次、
  持续 10 分钟，期望客户端重建会话，现行为可接受。
- 会话数固定 2 条：连接尝试限额 15 次/30s，会话过多在网络抖动集中重连时
  可能触发 -1034。

## trade_engine 线程模型评审与路由调度修复

### 现有模型

- 主线程（`TRADE_ENGINE_CORE`）：current_thread tokio + LocalSet，运行
  req_worker、query router、全部 WS/FIX 连接任务、响应 publish。
- te-ipc 线程（`TRADE_ENGINE_IPC_CORE`）：busy-poll iceoryx 订阅，经 SPSC
  队列交给主线程。

模型本身合理：ingest 与网络工作隔离，SPSC 拾取亚微秒级，同 L3 绑核让
跨核缓存行传递成本最低。真正的问题在主线程内部的协作调度。

### 发现：路由任务的 spin 窗口阻塞发送与响应（已修复）

req_worker 把订单 push 进连接任务队列并 notify 后不会立刻让出线程，而是
回到循环头继续空转，需空转满 `TE_ROUTER_IDLE_SPIN_ITERS`（原默认 1024）
次才 `yield_now` 一次。每次空转迭代含 control drain + SPSC try_recv
（约 20-50ns），1024 次约 20-50us。单线程 runtime 上：

- 被 notify 的 WS/FIX 任务拿不到 CPU，socket 写被推迟整个 spin 窗口；
- IO driver 只在任务 poll 边界运行，交易所 ack 的本地处理同样被推迟；
- query router 是第二个同构自旋任务，最坏情况两个窗口叠加。

因此"路由到发送"的真实手递延迟最坏是几十微秒量级，远大于队列 + 唤醒
本身的 ~0.3-0.5us。修复（`engine.rs`，req_worker 与 query router 同改）：

1. 路由过消息后，SPSC 一排空立即 `yield_now`（burst 仍先排空再让出，
   不损失批量性）；
2. 空转预算默认 1024 -> 64，idle 时对同线程任务的最大阻塞从 ~40us 压到
   ~2-3us，SPSC 拾取仍是亚微秒级；`TE_ROUTER_IDLE_SPIN_ITERS` 可覆盖。

注：tokio 的 `yield_now` 唤醒是 deferred 语义（先跑 driver 和其他就绪
任务），所以 yield 循环不会饿死 IO driver，此修复同时改善发送和响应两个
方向。pre_trade / trade_signal 的同构复查已完成，结论见下文专节：
两者结构不同，无同类病灶。

### 读写分离内联 send 的结论：暂缓

调度修复后，req_worker 到连接任务的残余成本只剩一次本地任务切换
（~0.3-0.5us）。读写分离要解决的正是这半微秒，但代价是：

- tungstenite 无锁 split 需要"共享流 + 每次 poll 内短借用（poll_fn）+
  路由器驱动写完成"的结构，Binance session-logon 门控、断线 pending
  队列、限频冷却、ping/pong、查询发送都要跟着改所有权；
- ws_client.rs 被五个交易所共用，回归面大，属于实盘核心路径。

判定：收益 <=0.5us、风险高，先不做。触发条件：按
`docs/order_latency_chain_test_plan.md` 加上 `send_start/send_done`
埋点后，若"路由完成 -> socket 写开始"仍稳定 >1us 再启动。设计要点已在
上文（共享流、per-poll 借用、单写者出站字节队列由路由循环驱动）。

### te-ipc 双线程双核的定性结论

以当前职责划分（te-ipc 只做 iceoryx -> SPSC 搬运），第二个线程对下单
延迟的贡献约等于零：订单真正等待的是主线程空闲（req_worker 与连接任务
都在主线程），te-ipc 提前搬运并不改变这一点，反而增加一跳 SPSC 和
19<->20 的缓存行往返（~0.2-0.5us）。它的真实价值只剩背压隔离：主线程被
大 JSON 解析/重连卡住几百微秒时持续排空 iceoryx，避免 pre_trade 发布端
队列打满。

两条演进路线（先埋 send_start/send_done trace 再选）：

1. 合并单线程：req_worker 直接 poll iceoryx，省掉 SPSC 一跳；空出的
   core 20 最优用途是承接 ens41 下单队列的 IRQ/XPS（替换当前跨 L3 的
   core 45），把网络唤醒路径变成同 L3。延迟中性偏正，改动涉及全交易所
   ingest。
2. 重划职责（终局形态）：保留双线程，但改成"热/冷"切分——ingest +
   路由 + socket 写在一个线程，读/解析/重连/REST 在另一个线程。这才能
   把"解析卡顿拖累下单"的尾延迟路径真正消掉；代价是 TLS/WS 状态跨线程
   共享（真锁 + Send 化），工程量大。
3. 不建议：保持现有职责、仅把写半移到 te-ipc 线程。锁争用与重连协调的
   复杂度都来了，却只省半微秒。

选择依据：trace 若显示"路由完成 -> socket 写开始"p99 有百微秒级毛刺
（解析卡顿证据）-> 路线 2；若干净 -> 路线 1 或维持现状。

## pre_trade / trade_signal 同构复查结论

结构判定：两者都没有 trade_engine 那种"路由 spin 阻塞发送任务"的病灶，
不需要同类修复。

- pre_trade 是单个内联 reactor：每轮循环按优先级直接排空全部 iceoryx
  通道（signal 优先级最高），idle spin 不挡拾取——下一轮迭代就会重新
  poll 全部通道。1024 空转只推迟冷路径 tokio 任务（通知、shutdown），
  不动。
- trade_signal 是多监听任务 + 决策后内联 publish：无 spin 阶梯（纯
  yield 循环），publish 不经队列手递。

实测拆段（jp-meta 在线日志 `arb_open_path`，2026-08-15 ~11:09 UTC，
p50，微秒）：

```text
ts_publish_minus_generation   12-15   trade_signal 决策打点->publish 开始（大头）
  其中 ts_open_before_tlen     9-10   决策计算：from_key/查表/quote plan/context
  其中 ts_open_tlen_query_gate  2     tlen gate 查询
ts_signal_publish_cost          1     iceoryx publish 本身
iceoryx 传输 + pre_trade 拾取  2-5    由 pt_receive(18) 减上述得出，健康
pt_receive_minus_generation    18     合计（与 jp2 部署期 14-16us 同量级）
```

结论：14-18us 的大头（9-10us）是 trade_signal 决策路径内部的计算成本，
不是调度/IPC 问题。已在 spread-arb open 路径加三段拆分埋点（纯观测）：

```text
ts_open_from_key_base_cost    from_key 字符串构建
ts_open_state_and_plan_cost   状态查表 + quote plan 构建
ts_open_context_build_cost    symbol 规整 + tlen 查表 + context 构造
```

部署后看一轮日志即可归因；预期 from_key 格式化与 per-level context
分配是主要嫌疑，届时做针对性优化（复用 buffer / 减少 format!）。

## 主机核查结果（jp-meta / ip-172-31-35-228，2026-08-15）

只读检查，未做任何变更。

### clocksource：已是 tsc，无需处理

```text
/sys/devices/system/clocksource/clocksource0/current_clocksource = tsc
available = tsc hpet acpi_pm
CPU = Intel Xeon Platinum 8488C
flags: constant_tsc nonstop_tsc tsc_known_freq rdtscp
```

注意 sysfs 正确路径是 `/sys/devices/system/clocksource/`（不带 `cpu/`）。

### GRO：两块网卡都开着，建议关（未执行）

```text
ens41: generic-receive-offload on, tcp-segmentation-offload off, LRO off[fixed]
ens42: 同上
```

GRO 会把 ack/小包在 NAPI 层聚合，推迟 epoll 唤醒。下单 lane（ens41）
优先关；行情 lane（ens42）同样受益于更低的单包时延，但包率高、CPU 开销
会上升，建议分开评估。执行命令（需要时）：

```bash
sudo ethtool -K ens41 gro off
sudo ethtool -K ens42 gro off   # 观察 CPU 后再定
```

持久化：并入现有 `hfq-low-latency-network.service` 的 ExecStart，或按
接口加一条 systemd oneshot。回滚 `gro on` 即可。

### C1：热核未禁用，建议按核禁（未执行）

```text
cpuidle driver = intel_idle
所有核: state0=POLL(0us) state1=C1(1us, disable=0)
governor = performance
```

busy-poll 的核（te-ipc、spread_pbs、路由自旋）几乎不进 idle，此项主要
保护偶发让出 CPU 的时刻（tokio park、行情间隙）。只禁热路径核，保持
其他核可进 C1，避免全机 POLL 的功耗/发热问题：

```bash
# binance-intra-arb01: 16-20; binance_mm_alpha: 27-31（按需扩展其他环境块）
for c in 16 17 18 19 20 27 28 29 30 31; do
  echo 1 | sudo tee /sys/devices/system/cpu/cpu$c/cpuidle/state1/disable
done
```

持久化用 systemd oneshot；回滚写 0。`/dev/cpu_dma_latency` 为 root-only
（600），如走 PM QoS 路线需要进程以特权持有 fd，不如按核禁用直接。

## 附：架构与线上 layout 评审要点（同日评审）

整体判断：进程按 signal → pre_trade → trade_engine 拆分、全链路 iceoryx2
SPSC + busy poll、te-ipc 独立绑核、热路径按 L3 岛整岛分配、双 ENI 行情/下单
分流 + 源策略路由、ENA IRQ 独核 + irqbalance off、nosmt + C-state 限制 +
busy_read/busy_poll=50 + ENA adaptive off，方向正确，无需推倒。

待复查项（按预期收益排序）：

1. persist_manager 池（core 15）与行情岛 8-15 同 L3：RocksDB 写入/compaction
   持续污染 spread_pbs/depth_pub 的 L3，建议挪到 housekeeping 或行情外的岛。
2. ens41 的 16 条 Tx/Rx 队列全压 core 45（ens42→46 同理）：行情突发时单核
   softirq 排队。既然单核收全部队列，可用 `ethtool -L` 把队列数减到 2-4 条
   提升中断路径 cache 命中；给下单核所在 TX 队列设 XPS 减少跨核 doorbell。
3. model_pub（core 47）与 ENA IRQ 核 45/46 同 L3 岛：ONNX 推理吃 L3/带宽，
   若行情尾延迟与推理负载相关则挪走。
4. `pt_receive_minus_generation` 14-18us 已用在线指标拆解完毕，见上文
   "pre_trade / trade_signal 同构复查结论"：大头在 trade_signal 决策计算
   （9-10us），IPC 与 pre_trade 拾取只占 2-5us。
5. 主机层可选项：clocksource / GRO / C1 已核查，见上文"主机核查结果"；
   kTLS TX offload 收益 1-3us 放最后。
