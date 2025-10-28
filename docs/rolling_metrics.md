# rolling_metrics 服务说明

本文档记录 `rolling_metrics` 二进制服务的当前功能，便于理解价差滑窗指标的生成流程与外部依赖。

## 1. 功能概览

- 订阅 Iceoryx 中现货与合约两个交易所的 `AskBidSpread` 渠道，实时获取双边行情。
- 计算两个市场之间的价差收益率（`spot_bid` vs `swap_ask`、`spot_ask` vs `swap_bid`），并对每个交易对维护独立的滑窗序列。
- 根据 Redis 中的运行参数，对滑窗数据计算分位点阈值、最新价差值等统计信息。
- 周期性地将结果写入 Redis HASH，供下游风控或策略模块读取。

## 2. 数据流程

1. **行情接入**  
   - 使用 `MultiChannelSubscriber` 同时监听 `spot_exchange` 与 `swap_exchange` 的 `AskBidSpread` 数据。  
   - 数据按 `"{spot}_{swap}::{SYMBOL}"` 组合生成唯一键，例如 `binance_binance-futures::BTCUSDT`。

2. **滑窗管理**  
   - 服务使用一个 `DashMap` 按键维护 `SymbolSeries`，内部以 lock-free `RingBuffer` 存储最近 `max_length` 条价差收益率。  
   - 写入端只覆盖旧数据，计算线程可以无锁地复制最近 `rolling_window` 条样本。  
   - 当 `swap_exchange = binance-futures` 时，每隔 30 分钟通过 Unix Domain Socket（默认解析 `config/mkt_cfg.yaml` 中的 `symbol_socket` 路径，读取 `binance-futures.sock`）刷新一次 USDT 永续合约列表：  
     - 新符号到达行情消息时会即时创建滑窗；  
     - 刷新结果中缺失的符号会从内存中回收对应的 `RingBuffer`，避免旧符号长期占用内存。  
     - 带三位及以上数字前缀的乘数合约（如 `1000PEPEUSDT`）会被过滤，避免与现货标的单位不一致。  
     - 被移除的符号会同步触发 Redis `rolling_metrics_thresholds` 中对应 field 的 `HDEL`，确保打印脚本不再看到遗留行。

3. **指标计算**  
   - 单独的计算线程按 `refresh_sec` 周期运行：  
     - 从滑窗复制样本，若样本数 ≥ `min_periods`，使用 `quantiles_linear_select_unstable` 计算配置要求的分位点。  
     - 生成包含 `bidask_sr`、`askbid_sr`、对应上下分位点、样本量、时间戳等字段的 JSON 文本。

4. **结果落地**  
   - 计算线程把 `(field, json)` 列表发送到写线程，由写线程通过 `HSET` 写入 Redis。  
   - `field` 为交易对键名，默认写入 `rolling_metrics_thresholds` HASH，可通过配置覆盖；该 HASH 汇总所有符号的最新阈值。

## 3. 配置与热更新

- **来源**：启动时和运行中均从 Redis HASH（默认 `rolling_metrics_params`）加载配置。  
- **主要字段**：
  - `MAX_LENGTH`：每个符号环形缓冲的容量上限；
  - `refresh_sec`：计算周期；
  - `reload_param_sec`：配置重新拉取间隔；
  - `output_hash_key`：结果 HASH 名称（可选）；
  - `factors`：JSON 对象，键为因子名（如 `bidask`、`askbid` 等），每个因子可独立配置：
    - `resample_interval_ms`：采样周期；
    - `rolling_window`：计算分位点时使用的样本数量；
    - `min_periods`：判定分位有效的最小样本量；
    - `quantiles`：需要输出的分位数列表（0~1 之间，可为空表示只输出实时值）。
- **热更新**：后台任务按 `reload_param_sec` 周期读取配置，更新滑窗容量并记录日志，不需要重启服务。配置变化会以 `debug` 日志输出一次即可。
- 可使用 `mkt_signal/scripts/print_rolling_metrics_params.py` 查看 Redis 中的当前配置值（JSON 格式）。

## 4. Redis 输出示例

默认写入 `rolling_metrics_thresholds` HASH，字段内容为 JSON：

```json
{
  "symbol_pair": "binance_binance-futures::ETHUSDT",
  "base_symbol": "ETHUSDT",
  "update_tp": 1712220000123,
  "sample_size": 95000,
  "bidask_sr": 0.00012,
  "askbid_sr": 0.00009,
  "bidask_lower": -0.00045,
  "bidask_upper": 0.00030,
  "askbid_lower": -0.00028,
  "askbid_upper": 0.00055
}
```

- 下游读取时可通过比较最新价差与对应分位点阈值，判断跨市价差是否达到做单条件。
- 提供脚本 `mkt_signal/scripts/print_rolling_metrics_thresholds.py` 以三线表格式打印 HASH 内容，缺失值会显示为 `-` 并保留行，便于排查。
