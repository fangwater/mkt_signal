# LSEG 美国股票 RAW replay

本目录实现通用 RAW 的 zstd staging、消息审计和完整 RocksDB replay。所有已知外层消息都会进入固定长度二进制消息；没有 `deferred` 或通用可变长 payload。

- 原始 gzip 始终只读，四个 part 并行流式解压；
- zstd shard 只在完整外层消息边界切分，并校验声明 FID 数；
- `QuoteMsg` 按 `RIC + 秒` 做跨 shard last-merge；
- `BID_1/BID_2/ASK_1/ASK_2` 是报价历史 Ripple，不是盘口档位；合法 ripple-only 消息校验后丢弃并计数；
- 所有 FID value/enum 均为空的 `CLOSING_RUN` 在确认 FID 已知后丢弃并计数，不生成无信息 State value；
- 仅含年度/52 周高低价格、日期或触发标志的 `UNSPECIFIED` 消息丢弃并计数；
- Trade、Correction、State、ClosingRun、Refresh 保持逐条；
- RocksDB 列族为 `v:{RIC}:{venue}`，不能唯一归属 venue 的消息进入 `i:{RIC}`；
- 新的 FID/FID 名称直接失败，未完成的 `.building` 被清理，正式目录不会发布。
- 任何未分类消息或非法 Ripple 模板直接失败，不存在 `unparsed` 跳过路径。
- 生产 replay 失败时先删除自身未完成的 `.building`，再 panic；错误日志保留，正式目录不会发布。
- `quote_reference.py` 和 `event_reference.py` 是 Python 正确性基线，与 Rust 共用 golden bytes。

方向实现以 `direction_reference.py` 为基线，Rust 在逐事件 replay 阶段写定
B/S/N；场外 N 优先，其余 ORDER_SIDE=1/2 强制取反，再走本场所触价、NBBO、
midpoint、tick、前序证据、默认 B。method/estimated/forced 随 TradeMsg 保存。
完整口径与限制见 `../preprocess/data_format/lseg/usstock_raw_trade_side.md`
（相对仓库根目录）。导出只读取保存的方向，并恢复跨 venue 的 source_order。
RAW 分钟表是源逐笔 print 的直接导出。取消、前日行情和通用 restatement 消息
完整保留在 RocksDB，但不会改写已发布的逐笔 print：本批 `CAN_TRD_ID`/`PD_TRDID`
无法与 `TRADE_ID` 无歧义逐笔关联。每个 manifest 明确 `raw_trade_corrections_applied=false`
及此策略；不得把该 RAW 输出描述为已撤销/更正净额的 time-and-sales。

分钟导出只写独立的 `baseline_data_1m_raw`，不覆盖 `baseline_data_1m`。有效
TradeMsg 的 N 已严格限定为 off-exchange reporting，因此 RAW 分钟表直接写
`off_exchange_volume/off_exchange_amount/off_exchange_count`。大小单
与 CME 同口径：每个 RIC 的纽约交易日自然月使用上一自然月 RTH 单笔名义成交额
的精确线性 P50/P90；场外参与 `large/medium/small_order` 总桶，不进入方向桶。
12 列和月度阈值 audit 写入 RAW 输出；首个没有上月样本的月份 12 列为 0。
exporter 强制两个输出根目录名分别为 `backtest_1s_raw` 和
`baseline_data_1m_raw`，参数缺少 `_raw` 后缀时直接失败。

正式 RAW 发布使用 `--raw-only`，因此不会按 RIC 混入覆盖不完整的 staged LL2。
RAW 的 L1 bid/ask 和成交照常输出；LL2 深度字段为 null，`book_depth=0`。

RAW replay 使用有序读线程和固定 RIC worker，不再按 shard 独立维护状态。
`direction_calendar` 为冻结连续区间 CSV（open_ts/close_ts，UTC 秒半开区间）；
无可用区间时禁用盘口/tick 历史复用并明确标记。并行吞吐尚未做全量压测，
本次实现不自动重启生产作业，预先存在的输出目录不会被启动失败清理删除。

现有 shard 可混合多个 RIC，manifest 仅记录 shard 首尾 RIC，不能直接用来
安全构造 RIC 任务。后续性能优化采用两阶段：先并行解析并生成保留逐事件、
source_order 的按 RIC staging 和完整 segment manifest，再按 RIC 顺序 replay、
跨 RIC 并行。不能在当前秒级 RocksDB 完成后补方向，因为秒内早期 Quote
已经丢失。parsed staging 已在包含 ARKG.BAT/ARKK.BAT 的真实混合 shard 上
验证：数据区与直接 replay 逐字节一致；首次 parser+replay 因额外 I/O 略慢，
但 staging 可复用，单独 replay 快 34.1%。启动时会并行复核所有 segment 的
SHA-256，具体测量记录见口径文档。

## 运行

```bash
cd crates/usstock_lseg_raw_replay
# staging
cargo run --release --locked -- --config config.toml

# 完整 manifest 发布后构建和校验生产库
./scripts/run_rocksdb_replay.sh raw_rocksdb.toml
cargo run --release --locked --bin usstock_lseg_raw_verify -- \
  --rocksdb-dir /path/to/period-rocksdb

# 打印每个 msg_type 的固定长度和完整字段 offset
cargo run --locked --bin usstock_lseg_raw_schema
```

The workspace owns the single `Cargo.lock`. The production wrapper uses the
crate-local `target/` directory by default; set `USSTOCK_RAW_TARGET_DIR` to
override it.

生产 replay 只通过上述脚本启动。脚本强制构建指定 binary，并拒绝运行早于 Rust 源码的旧可执行文件；不要直接调用 `target/release/usstock_lseg_raw_rocksdb`。

显式 `inputs` 只用于诊断，metadata 状态为 `diagnostic-complete`；只有读取完整 staging `manifest.json` 的构建才标为 `complete`。

## 固定 ABI

key 固定 17 字节：`msg_type:u8 | ts_utc_ns:u64(be) | source_order:u64(be)`。RIC 和 venue 已由列族表达，value 不重复保存 kind/type/RIC/venue，也不使用 presence bitmap。

高频消息使用紧凑 typed value：

- `0x01 QuoteMsg`：40 字节；
- `0x02 TradeMsg`：112 字节；保留 ORDER_ID/ORDER_SIDE/PRNTYP/HELD_T_IND/TIMACT_MS，方向及 N 原因见 `../../../preprocess/data_format/lseg/usstock_raw_trade_side.md`。旧 64 字节库必须重建；
- `0x10 TradeCancelMsg`：72 字节；
- `0x11 PreviousDayTradeMsg`：72 字节；
- `0x20 QuoteStateMsg`：24 字节。

低频 Correction/State/Refresh 使用按业务类型固定的 source-exact slot layout。value 前 16 字节为 `source_ts_utc_ns, source_order`；之后每个字段固定 32 字节：24 字节源值和 8 字节 enum string。layout 已固定字段顺序，不逐条保存 FID/字段名。全 `0xff` 表示本次 patch 没有该字段；全 `0x00` 表示源明确给出空值；因此不需要 bitmap，且两种状态可逆区分。

首批四个审计 shard 观察到 303 个 FID、82 种组合。生产 parser 以这 303 个 FID 为严格边界，允许已知字段形成新组合；组合式 correction 会拆成多个固定消息。例如“取消 + 统计重算”同时生成 `TradeCancelMsg` 和 `TradeRestatementCorrectionMsg`。真正的新 FID 仍会硬失败。

公司行为修订按字段语义路由。独立 `DIVPAYDATE`、`EXDIVDATE` 或 `CUM_EX_MKR` patch 进入 `CorporateActionCorrectionMsg`，不会落入成交重算；其固定 value 为 144 字节。

`UPDATE / UNSPECIFIED` 的 `YIELD + PERATIO + DIVPAYDATE` 组合保留为单条 112 字节 `ValuationStateMsg`，不因支付日字段重复生成第二条公司行为消息。

包含 `OFF_CLOSE` 的 `UPDATE / CORRECTION` 复用 304 字节 `OfficialCloseStateMsg`。它在 RocksDB 中追加历史事件；消费时仅覆盖本次出现的固定槽位，未出现槽位保持旧状态，只有 `REFRESH` 重置整份状态。

省略配置时 replay 的代码默认值是 `workers = 16`；当前生产配置
`raw_rocksdb_parsed.toml` 明确使用 `workers = 32`。生产先由并行 parser 生成
按 RIC 分区且保留跨 part/shard `source_order` 的 parsed staging，再把每个 RIC
固定分给一个 replay worker 严格顺序处理；不同 RIC 并发维护 Quote 状态、
`WriteBatch` 和 census，共同写入同一座 `<rocksdb>.building`。首次创建列族时
使用共享锁，同秒 Quote 由 RocksDB merge 按 `source_order` 确定性选择最后一条。
全部 worker 成功退出后才统一 finalize Quote、写 metadata、删除临时列族并
原子发布，不会为每个 worker 生成独立数据库。

正式入口对目标持有进程级文件锁。第二个同目标进程会在写库前失败，且没有清理权；任一 worker 返回错误或发生 panic 时，主线程先 join 全部 worker，再清理本进程的 `.building`。

真实 part 0 / shard 0 smoke 共读取 8,646,914 条源消息，全部编码，`deferred_messages=0`。5,731,603 条 Quote 压为 130,074 个秒级快照和 370 条 QuoteState；另写入 2,915,345 条逐笔事件。独立 verifier 全量通过，库为 127 MB。完整 smoke 保存在：

```text
/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/usstock_raw_rocksdb_smoke_part0_shard0
```
