# Tardis 月度大小单阈值

直接读取 Tardis trades gzip CSV，按市场/币对计算单笔 `price * amount`
的 P50/P90（USDT）。复用 `rolling_common::kll_quantile`，逐小时构建 KLL，
最后按小时顺序合并；固定容量 512，属于近似分位数。

```bash
cargo build --release -p tardis_order_size
target/release/tardis_order_size \
  --root /mnt/nfs/30.133_xt_crypto/TardisSource \
  --month 2026-09 \
  --symbols-file config/crypto_order_size_symbols_43.txt \
  --output /path/to/existing-parent/order_size_2026-09 \
  --workers 8
```

`--month` 是**样本月**，省略时为运行时上一 UTC 自然月；2026-09 的阈值用于
2026-10。默认同时计算 `binance-futures,binance`，可以用 `--exchanges` 缩小
市场范围，或用 `--symbols BTCUSDT,ETHUSDT` 替代名单。分位点通过
`--medium-quantile` / `--large-quantile` 设置，默认 0.5 / 0.9。

输入根目录布局：

```text
binance_usd_btcusdt/trades/binance-futures_trades_2026-09-01_BTCUSDT.csv.gz
binance_spot_btcusdt/trades/binance_trades_2026-09-01_BTCUSDT.csv.gz
```

小单 `<P50`，中单 `[P50,P90)`，大单 `>=P90`。每条有效成交等权，买卖方向
共用阈值，无 ID 去重。按微秒 `timestamp` 归属 UTC 月；非有限或非正的
price/amount/notional 跳过，跨回已关闭小时的迟到成交跳过，均单独计数。
维持现有 KLL 的 f32 分位点、加权线性插值和确定性压缩算法。

全月交付要求每天的源文件齐全、每个小时至少一个有效样本。文件缺失、
缺小时、CSV schema/身份错误或 gzip 校验失败均不发布该市场/币对阈值。
这比只检查目录存在严格；新上市/无成交币对会明确报告覆盖不足。

输出父目录须存在，输出目录必须是新目录且位于源根以外：

- `thresholds.csv`：成功市场/币对的金额阈值、样本数、有效小时数。
- `results.json`：总状态与全部结果；失败时 `status=partial` 且进程退出码非零。
- `audit/{exchange}_{symbol}.json`：每个任务的结果或错误、输入文件大小/mtime/
  SHA-256、行数及过滤计数、月度 KLL 摘要。
- `manifest.json`：执行参数、完整输入路径清单、UTC 窗口和工具源码指纹。

仅写上述新输出目录，不写 ClickHouse/Redis，也不启动交易进程。不同市场
独立存储，不使用 SID 后缀。KLL 未保存全部原始值，不能将结果称为精确排序分位数。

验证与基准：

```bash
python tests/scripts/test_tardis_order_size.py \
  --binary target/release/tardis_order_size \
  --report /tmp/tardis_order_size_validation.json
```

固定输入由测试中的确定性公式生成；Python 基线位于
`scripts/tardis_order_size_reference.py`。比较统计、阈值、全部 KLL levels 和
源哈希，覆盖压缩/合并、月界、乱序、无效数值、缺文件、空小时、损坏 gzip、
市场隔离、并行确定性与输出保护；同时报告精确分位数误差和 Python/Rust 耗时。
Python 基线会保留全部样本并排序，仅用于小规模验证。
