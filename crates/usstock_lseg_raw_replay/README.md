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
- `0x02 TradeMsg`：64 字节；
- `0x10 TradeCancelMsg`：72 字节；
- `0x11 PreviousDayTradeMsg`：72 字节；
- `0x20 QuoteStateMsg`：24 字节。

低频 Correction/State/Refresh 使用按业务类型固定的 source-exact slot layout。value 前 16 字节为 `source_ts_utc_ns, source_order`；之后每个字段固定 32 字节：24 字节源值和 8 字节 enum string。layout 已固定字段顺序，不逐条保存 FID/字段名。全 `0xff` 表示本次 patch 没有该字段；全 `0x00` 表示源明确给出空值；因此不需要 bitmap，且两种状态可逆区分。

首批四个审计 shard 观察到 303 个 FID、82 种组合。生产 parser 以这 303 个 FID 为严格边界，允许已知字段形成新组合；组合式 correction 会拆成多个固定消息。例如“取消 + 统计重算”同时生成 `TradeCancelMsg` 和 `TradeRestatementCorrectionMsg`。真正的新 FID 仍会硬失败。

公司行为修订按字段语义路由。独立 `DIVPAYDATE`、`EXDIVDATE` 或 `CUM_EX_MKR` patch 进入 `CorporateActionCorrectionMsg`，不会落入成交重算；其固定 value 为 144 字节。

`UPDATE / UNSPECIFIED` 的 `YIELD + PERATIO + DIVPAYDATE` 组合保留为单条 112 字节 `ValuationStateMsg`，不因支付日字段重复生成第二条公司行为消息。

包含 `OFF_CLOSE` 的 `UPDATE / CORRECTION` 复用 304 字节 `OfficialCloseStateMsg`。它在 RocksDB 中追加历史事件；消费时仅覆盖本次出现的固定槽位，未出现槽位保持旧状态，只有 `REFRESH` 重置整份状态。

生产 replay 默认 `workers = 16`。16 个 shard worker 各自顺序解压、解析并维护本地 Quote 状态、`WriteBatch` 和 census，共同写入同一座 `<rocksdb>.building`；首次创建列族时使用共享锁。跨 shard 的同秒 Quote 由 RocksDB merge 按 `source_order` 确定性选择最后一条。全部 worker 成功退出后才统一 finalize Quote、写 metadata、删除临时列族并原子发布，不会生成 16 座库。

正式入口对目标持有进程级文件锁。第二个同目标进程会在写库前失败，且没有清理权；任一 worker 返回错误或发生 panic 时，主线程先 join 全部 worker，再清理本进程的 `.building`。

真实 part 0 / shard 0 smoke 共读取 8,646,914 条源消息，全部编码，`deferred_messages=0`。5,731,603 条 Quote 压为 130,074 个秒级快照和 370 条 QuoteState；另写入 2,915,345 条逐笔事件。独立 verifier 全量通过，库为 127 MB。完整 smoke 保存在：

```text
/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/usstock_raw_rocksdb_smoke_part0_shard0
```
