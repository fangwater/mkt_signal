# CME TAS replay

`cme_tas_replay` 读本机 LSEG Tick History Time and Sales 的 gzip part，把可打印成交、Special、Quote、RIC 更名、价格笼子写成定长 `msg`，写入**一座** RocksDB。不是 JSONL，不写 ClickHouse，不写 parquet。日结算价由独立的 `cme_tas_settlement_scan` 补扫，不能当作成交。小时 notional KLL 是独立工具 `cme_tas_kll`，只读 `cme_trade`，见 [cme_tas_kll.md](cme_tas_kll.md)。

默认 `workers = 32`：一进程读 `periods` 里全部年份的 gzip part，直接往正式目录续写。当前配置是 2024、2025、2026 H1 共 32 个 part，一个 worker 对应一个 part。`workers = 1` 仍只读第一个 period 的 `part_index`。源 part 是多段 gzip 拼在一起的，必须用 `MultiGzDecoder`；只解第一段会在大约一百万行处假装读完。解压用 `zlib-ng`。热路径按表头下标取格、缓存 RIC 是否落在 51 根上，并只扫剩余禁列，不再对每行做 294 列按名查找。

并行细节见 preprocess [tas_replay_parallel.md](../../preprocess/data_format/lseg/tas_replay_parallel.md)。

## 源

本机 TAS 三个 period：

```text
/mnt/hdd-raid5-72t/liang_torch/lseg_data/future/normalised/
  shanghai_evolution_futures_time_and_sales_ric_list_0_tas_2024-01-01_2025-01-01/   # 10 part
  shanghai_evolution_futures_time_and_sales_ric_list_0_tas_2025-01-01_2026-01-01/   # 13 part
  shanghai_evolution_futures_time_and_sales_ric_list_0_tas_2026-01-01_2026-06-01/   # 9 part
```

Part 按 `#RIC` 字典序切，不是按日。Part 0 先是指数 RIC，再是期货。

## RocksDB

```text
/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb
```

一座库，多年共用。空目录就建；已有别的 `period` 就追加。列出的每个 `period` 已在 `replay_meta` 里标成 `done`，或上次停在 `writing`，立刻失败，避免同 key 被覆盖。`max_source_rows` / `max_tradedays` 冒烟只能写空目录，不写水位。同一 `#RIC` 在 `cme_trade` / `cme_quote` 里按 key 扫就是时间序，不必跨库归并。不要再开第二个进程写这个目录。

| CF | 第一版 |
| --- | --- |
| `cme_trade` | 写。`kind=1`，80 字节，可打印成交 |
| `cme_special` | 写。`kind=2`，80 字节，`Special Trades[USER]`，价/盘口为哨兵 |
| `cme_quote` | 写。`kind=3`，64 字节，L1 盘口 |
| `symbology_change` | 写。`kind=4`，64 字节，RIC 更名：本包已见 `ADF26` → `ADF26^2` |
| `cme_price_limit` | 写。`kind=5`，48 字节，当日涨跌停。价量都空的 Trade 上出现 `UpLim` 和/或 `LoLim`。只有一侧也写，缺的一侧用价格哨兵 |
| `cme_settlement` | 由独立扫描器写。`kind=6`，40 字节，保留 `Settlement Price` 与源 `Date`；不进成交、KLL 或 Backtest |
| `replay_meta` | 写。每个 TAS `period` 一条水位：`writing` → `done` |
| `settlement_scan_meta` | 独立结算扫描的 `settlement_period:<period>` 水位：`writing` → `done` |
| Status / Correction / Auction / 指数 | 只分类计数，不写 |

Key（大端，按 RIC + 时间可扫）：

```text
ric[16] | ts_utc_ns:u64 BE | part:u16 BE | seq:u32 BE
```

`seq` 是**该 part、该 CF** 内同一 RIC、同一纳秒里的源行序。排序用 `Date-Time`，不用 `exch_time`。布局见 preprocess 的 [tas_bin_msg.md](../../preprocess/data_format/lseg/tas_bin_msg.md)。

## 处理不了的行

单行情行不再让进程 panic。未知 `Type`、期货禁列突然有值、坏数字、一侧有价没量的 Quote、只有价的 Trade、key 乱序，都写一行到：

```text
/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/logs/cme_tas_replay_unparsed.log
```

该行不写库，记 `unparsed_skip`，继续。之后对着这个文件改口径。主日志只打前 20 条 `unparsed`，后面只进这个文件，避免国债 `Implied Yield` 把主日志打满。配置坏了、gzip 截断、旁边还有 `*.building`、本 `period` 已是 `done` / `writing`，仍退出码 1，因为那不是单行问题。

价量都空、也没有涨跌停的 `Trade` 丢掉，不算错。只有一侧 `UpLim`/`LoLim` 的空 Trade 仍写 `cme_price_limit`，缺的一侧用价格哨兵。`Quote` 上的 `UpLim`/`LoLim` 允许出现，不写进 `cme_quote`，记 `price_limit_ignored`。有量没价、又不是 `Special Trades[USER]` 的 `Trade` 丢掉，记 `drop_volume_only_trade`，不写。`Special Trades[USER]` 没有 `Volume` 丢掉，记 `drop_special_no_volume`，不写。`Settle IV[USER]` 丢掉，记 `drop_settle_iv`，不写。`Type=Auction` 丢掉，记 `cme_auction`，不写。`Type=Reference Change` 丢掉，记 `reference_change`，不写。`Quote` 上的 `Implied Yield` 允许出现，不写进 `cme_quote`，记 `implied_yield_ignored`；没有完整一侧仍丢掉，记 `drop_empty_quote`。买卖都空的 `Quote` 丢掉，记 `drop_empty_quote`，不写。指数 RIC（`.` 开头）分类为 `index_print`，不写。未落到研究路由 51 个品种根上的期货（`NGLND*`、`LCO*`、迷你 `SIL` 等）记 `unmapped_skip`，不写。清单和中文见 preprocess [research_roots.md](../../preprocess/data_format/lseg/research_roots.md)。匹配是 `^{根}[FGHJKMNQUVXZ]\d{1,2}$`；`ADF26^2` 先剥历史后缀再比。`rics = []` 不再表示“全包都写”，只表示不再叠加精确 RIC 白名单。

## 运行

在 `mkt_signal` 根目录：

```bash
cargo run --release -p cme_tas_replay -- --config config/cme_tas_replay.toml
```

全量并行默认 `max_tradedays = 0`。不要给每个 worker 再套 5 天窗口。`workers = 1` 冒烟时才用 `part_index` 和可选的 `max_tradedays`。

冒烟（限制源行数）：

```bash
cargo run --release -p cme_tas_replay -- --config config/cme_tas_replay.toml --max-source-rows 20000
```

日志默认打开，只追加写

```text
/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/logs/cme_tas_replay.log
```

不要把同一条再打到 stderr，后台把 stderr 重定向进这个文件时会变成两遍。启动一行、每 100 万源行走一次进度、结束汇总。某一行 parse / classify 失败时写进 `unparsed_path`，主日志只打前 20 条。不要只写 RIC 和时间。看文件：

```bash
tail -f /mnt/nvme-raid0-28t/fanghaizhou/lseg_data/logs/cme_tas_replay.log
```
