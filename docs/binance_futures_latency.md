# Binance Futures 延迟观测

最后更新：2026-10-13 07:18 UTC。

## 接收线程日志与双窗口选线

Binance Futures `bookticker` 角色使用有界异步日志：接收线程保留 env_logger 的
RUST_LOG 过滤、格式和记录时刻，磁盘写入由独立 `bbo-log-writer` 线程完成。
`--log-writer-core` 默认 0，必须与接收线程不同；启动时显式重新设置 worker affinity，
不会继承 taskset 的 CPU9。队列最多 1024 条，每条最多 64 KiB，队列满或超长记录
直接丢弃，磁盘恢复后输出 `spread_async_log_loss` 累积丢失/写失败计数。
出现该标记时诊断覆盖不完整，不得把缺少的分钟作为延迟改善；退出不等待磁盘线程，
因此进程退出可丢失尚未写出的队尾。时间戳、BTC 统计范围与行情发布语义不变。
market 及其他交易所角色不使用此 logger。

候选首轮固定被替换 slot，第二轮验证同一替换方案是否仍通过原有阈值，
不再要求它第二轮也排名第一。两轮 coverage、匹配数量、P50/配对收益、P99、
连接代次、行情新鲜度、时钟和 CPU 保护仍生效；不允许第二轮临时换替换对象。

## 同机诊断实验结果

独立项目已完成 BTC-only/783 币同 TCP 交换订阅、源 IP × 远端组合及新连接复测、
ens42 RX 中断合并 ABAB（四阶段各 120 秒）。详细样本、测量边界与恢复证据：
`../hfq_latency_lab/docs/ticker_latency_diagnosis.md`。

- 订阅范围交叉的 64,175 个共同事件覆盖率均 100%，全币减单币先到差中位数为
  -28.983 / +38.189 μs，暂不支持据此拆分全币订阅；旁路未包含生产 IPC。
- 不同连接的尾部差异显著，但固定 IP 组合的新连接排名并不稳定，不应永久硬编码。
- 关闭 RX 合并的两个旁路阶段均改善，生产双路未稳定复现；已恢复原网卡参数。
- 06:33:23.089–25.690 的 14 个采样点直接观察到主接收线程处于 D 状态、
  `wait_transaction_locked`，单条 TCP 内核未读队列达 1,315,836 字节。
  stdout/stderr 直接写根盘 ext4；同步日志是强嫌疑，后续 180 秒只读栈捕获未复现，
  尚不能确定具体阻塞 syscall，也未证明之前 reset 的因果方向。
- 06:11/12 两轮候选的同一个替换方案均过阈值，却因两轮 winner 从 0 变 1 被拒绝。
  后续可固定首轮替换对象并复验其阈值，避免仅因相对排名变化丢弃有效候选；
  此诊断阶段没有修改生产选优算法；后续修复见上节，健康/尾部保护不变。

按 Quantech/Sytus 公开页面的指标组织统计，范围仅 **Binance USD-M BTCUSDT**，同时覆盖 Ticker / Depth / Trade。当前代码订阅分别为 `btcusdt@bookTicker`、`btcusdt@depth@0ms`、`btcusdt@trade`。不混入 COIN-M、其他币种或其他交易所。

## 口径

- `Q`：`spread_pbs/ws.rs` 中 WebSocket `read.next().await` 返回后已有的微秒 wall-clock 时间；记录时复用它，不在解析或发布后重新取 Q。该边界包括内核、TLS、WS 解码和调度，不能叫纯网络传播时间。
- `E` / `T`：原始 Binance JSON 对应字段，毫秒转换为微秒；不使用标准化 payload 的单 timestamp、不 fallback、不交换 E/T。缺失/非法时间戳只增加 missing，绝不补零。
- `EventToClient_P50/P99 = percentile(Q−E)`；`TxToClient_P50/P99 = percentile(Q−T)`。输出单位毫秒。
- 样本为生产去重后**成功发布**的消息。Ticker 用 bookTicker，Trade 用原始 trade，不接受 aggTrade；Depth 一个完整源消息只计一次，即使 IPC 分片。失败、重复、过期消息不纳入。保留负值与长尾。
- UTC 对齐的 60 秒非重叠窗口，正负 HDR 直方图、输入分辨率 1 μs、3 位有效数字。HDR 桶近似，不能把输出微秒精度理解为交易所毫秒时间戳的精度。首个不完整窗口有 `partial=true`，空桶分位数为 null。
- 时钟回退到当前桶之前的样本计入 `clock_regression_samples`；统计线程来不及输出或跨过空桶时计入 `dropped_windows`。缺失窗口不补零。停机时尚未完成的桶不伪装成完整分钟。

这是**可解释的基准口径，不是已证实的供应商严格同口径**。对方图表的 60 秒 Maximum 聚合的是其已计算的 P50/P99；原分位数窗口、准确 stream、Depth 频率、Trade/aggTrade、连接数及去重规则未公开。页面有早晚时间戳的通用说明，本实现保留命名字段 E/T；是否适用同一规则也需确认。不同主机的时钟误差仍存在。

## 开销与控制

每条被接受的 BTC 消息只扫描必要头字段并更新两个直方图；统计本身不增加连接、线程、逐条 clock 读取、逐条日志、逐条持久化或阶段延迟。正常分钟切换复用预分配缓冲；异常超出初始 60 秒直方图范围时允许扩容，避免裁掉尾部。现有 30 秒 stats tick 输出完成的分钟，所以日志可能比桶结束晚最多约 30 秒（调度阻塞可更久），对齐必须用 JSON 桶时间，不能用日志行时间。

`SPREAD_PBS_BINANCE_LATENCY_WINDOW_SECS` 默认 `60`，设 `0` 可关闭；其他值启动时报错。短窗口不能在现有 30 秒输出节奏下可靠保留，因此没有开放伪配置。bookticker 角色只输出 Ticker，market 角色只输出 Depth / Trade。统计只在 native USD-M 实例创建。

Binance futures 的 ZMQ PUB 混合 ingress KLL、SUB 的 link/e2e KLL 停用；wire timestamp 和行情转发保持原合同。其他 venue 的既有统计保留。停用代码需对应二进制发布后才在该主机生效。

日志行以 `binance_feed_latency ` 开头，后接 JSON。除四个延迟值外，保留样本数、missing、negative、partial、丢桶/回退及进程启动时间，供判定数据是否可用。这些是分钟质量信息，不是额外的逐笔观测。

## 独立采集与比较

独立项目：`/home/fanghaizhou/hfq_latency_lab`。外部公开数据每分钟采集；JP 采集经 SSH **只读**两个 `spp_bn_fu_{market,bookticker}-error.log`，保存文件游标，传输过滤后的分钟 JSON。采集机处理 SQLite、比较与 HFQ 导出，行情核不落逐笔数据。

```bash
cd /home/fanghaizhou/hfq_latency_lab
.venv/bin/python -m hfq_latency.cli collect-jp
.venv/bin/python -m hfq_latency.cli compare --hours 24
```

`data/exports/jp_comparison.json` 只比较同一分钟/频道/时间原点/分位数；partial、同桶多次启动、缺失字段、丢桶和回退桶排除，缺失外部值不配对。输出差值仅表示两个已上报统计量的差，不是逐事件收益或某段耗时的分布。`jp_hfq_parameters.json` 导出本机完整桶的 P50/P99（μs），负值情景显式标记不可直接作为非负延迟分布锚点。

正式严格对标还需供应商确认统计合同，最好在同一主机同一时钟下按事件 ID 配对；只有聚合 P50/P99 无法完成该配对，也无法恢复完整延迟分布。当前不为此新增逐笔长期观测。

## 2026-10-13 发布验证

已于 04:04 UTC 在 JP 行情环境完成切换：market PID 1784924 / CPU14，
bookticker PID 1787779 / CPU9，relay PID 1792862 / CPU5；运行中二进制哈希
与本地 release 一致。现有 env-local 脚本保持运营配置，只补观测开关透传。
原文件旁保留 `.before-latency-20261013` 备份；具体产物、源码补丁与哈希在独立项目
`data/deploy/20261013-latency/`。基础提交 `60872429` 加该目录记录的本次源码补丁。

三个频道已观察到完整分钟、非零样本，missing、负值、clock_regression、dropped_windows
在初次验收桶中均为 0。独立项目的外部与 JP 两个用户级 timer 已启用。
ZMQ SUB 的停用代码已构建验证，但此次未发布到执行主机；本次上线范围只含 JP。

验证：解析器 178 项、主库 1,284 项、relay 3 项、独立采集 12 项测试通过。
本机 release 合成微基准，接受的 BTC 消息额外 E/T 扫描及两次 HDR 更新约 267.2 ns/条，
不含周期输出或真实线上负载，不能当作线上端到端开销保证。

## Ticker 双连接选优机制

实现位于 `src/spread_pbs/binance_bbo_selection.rs`，仅作用于 native USD-M
`bookticker` 角色。复制 `config/binance_bbo_selection.toml` 到该运行目录即启用管理器；
未配置的部署保留原连接循环。配置路径可由 `SPREAD_PBS_BINANCE_BBO_SELECTION_CONFIG`
指定。显式路径不存在或启动配置非法时拒绝启动；热加载失败保留上次有效配置并告警。

- 正常两条全币 bookTicker 连接发布，复用现有序列高水位去重；每轮最多增加一条
  **完整相同订阅**的影子连接，影子不发布。通过后原 TCP 原地晋升，再关闭被替代连接。
- 每 5 分钟刷新域名的 IPv4 DNS，枚举现有两个 source IP × 当前最多 16 个远端；
  保留 TLS 域名/SNI 验证。10 分钟未成功刷新 DNS 时暂停新试验，不永久硬编码本次试验 IP。
  第一轮启动后约 30 秒，普通试验完成/退出后冷却 15 分钟；必要的 DNS 恢复后重新等待 60 秒再试验。
- 影子必须完成全部订阅 ACK、持续有行情和 BTC 更新，并预热至少 10 秒。
  在两个连续 60 秒窗口，按 BTC 的同一 `u` 及 E/T/b/B/a/A 匹配三路，
  比较原双路 `min(Q0,Q1)` 与替换一条后的 `min(Qother,Qcandidate)`。
  首窗固定被替代连接、次窗复验同一方案（无须再次排名第一），且 P50 至少改善 10 μs、逐事件配对改善
  截断到 ±1 ms 后均值至少 5 μs、P99 恶化不超过 50 μs。
- 只在试验期间解析 BTC 评分，确定性抽取约 1/8 更新，固定 8,192 个哈希槽。
  哈希冲突保留较新的更新；覆盖率是**保留槽内**三路交集/并集，每窗共同样本至少 500、
  覆盖率至少 98%。缺失、内容冲突、负 Q−E、任一评估连接重连或断流都不会触发晋升。
  此为有界连接筛选样本，不是全量丢包审计，也不替代正式分钟延迟统计。
- 复用现有 Q，不逐帧新增时钟、逐笔日志或文件。每秒一次 process CPU/时钟检查；
  进程连续 3 秒超过单核 85% 就退出试验，负载高时不开始试验。该量不包含其他进程和
  NIC IRQ 的 CPU。检测 wall−monotonic 跳变超过 500 μs 后暂停评分 60 秒。
- 普通断线恢复、币种订阅变更和最长 6 小时的连接轮换独立于性能试验，采用域名重新连接、
  每次只替换一条，维护健康连接时要求另一条健康，至少间隔 20 秒；不把这种维护记为延迟优化。
  币种列表继续按原 `restart_duration_secs` 刷新（当前 1,800 秒）。

配置每 30 秒异步重读：`observe` 只评估和记录 `would_promote`；`auto` 才自动选优；
`off` 关闭试验但保留上述必要维护。修改配置会取消当前试验。日志
`binance_bbo_selection` 包含连接唯一 ID、实际 peer/source、候选窗口和晋升/退出原因；
每 30 秒状态摘要，不保存逐条数据。BTC 必须在该部署订阅范围内。
异步读取配置/更新任务之后重新取得检查时刻，避免把等待期间到达的数据误判为未来时间。

BTC 选优结果用于该条全币连接；**不保证每个其他币同样改善，也不证明达到了 Sytus 口径或收益**。
试验增加第三路期间自身存在开销，选优分数是同机同期三路环境内的配对结果，
正式收益需继续看晋升后正常双路运行的分钟统计。

角色操作使用环境内脚本：

```bash
cd ~/spread_pbs/binance-futures
./scripts/stop_spread_pbs.sh --role bookticker
./scripts/start_spread_pbs.sh --role bookticker
```

只操作该角色；它还承载既有 COIN-M BBO，因此重启时 COIN-M BBO 也会短暂停顿。
market 角色无需重启。快速停用选优可原子修改配置为 `mode = "off"`，无需重启；
完全恢复原连接循环需移开该配置并用环境内脚本重启 bookticker。

### 2026-10-13 选优机制上线验收

最终修复版于 05:41 UTC 通过环境内角色脚本启动，bookticker PID 2753841 / CPU9。
二进制 SHA-256 为 `4b26c5440d49988bc57a0a2c8a2520f2389ea0719a512d0ba974e114f145d13d`，
基础提交 `347dacbe` 加本次记录的补丁；发布证据在独立项目
`data/deploy/20261013-bbo-selection/`。两条 active 和影子均实际收齐 4/4 订阅回执。
配置热读取后已不再出现缓存检查时间导致的误重连；真实 WS reset 仍可能发生。
第一次完整试验尝试期间 active 连接于 05:43:24 reset，评估被丢弃，未误晋升。
正常双路 CPU 样本约 0.52–0.65 核，影子期间约 0.75–0.78 核；只是该时段观测，
不是容量保证，也尚不能宣称生产延迟已经改善。

发布重启对下游有真实影响：05:40:45–05:41:00 的 bookticker 切换造成约 15 秒
上游空窗，el01 SUB 在 05:40:58 记录无 BBO 13,354 ms，可触发 Manager 的 5 秒告警。
JP PUB PID 1792862、el01 SUB PID 405428 未重启，TCP 连接仍 ESTABLISHED；
el01 在 05:42:56 的窗口接收并发布 190,138 条 BBO，05:44 UTC 原监控程序
`--once --dry-run` 返回 `CTA monitor: OK`，覆盖 BTC/ETH/SOL/XRP。
这证明当时行情活性恢复，不等于转发链路无损：SUB 同时有序号缺口，尚未定位原因。
后续模式切换只热加载，正常候选晋升沿用已就绪的连接。

验证：最终行情相关测试 178 通过、1 忽略，仓库/现场定制角色脚本各 4 项通过；
通用 WS 回执修复后的全库测试 1,261 通过、3 忽略；本地 release 构建、上传及运行中 SHA 一致。

05:45:34 UTC 已热加载 `mode = "auto"`，bookticker PID 保持 2753841。
正常双路发布，下一轮仍须满足两窗共同改善和全部质量门槛才允许晋升。
可热改 `mode = "off"` 暂停试验；无需停止 ZMQ 或重启行情进程。

05:47:15 UTC 首个自动评估窗口共同样本 1,592，保留样本覆盖率 100%、无内容/时钟异常。
选择替换 slot 0 的组合：Q−E P50 1,512 → 1,415 μs（改善 97 μs），
P99 3,632 → 2,361 μs，截断配对平均改善 72.26 μs。
第二窗口内现有 slot 1 连接发生真实 WS reset，05:47:23 DNS 恢复并取消候选评估。
没有发生性能晋升；这是一窗内的候选成绩，不能作为已上线持续收益。
el01 05:47:56 窗口继续接收并发布 178,899 条 BBO。
