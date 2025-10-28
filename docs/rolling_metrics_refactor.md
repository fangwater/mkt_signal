# Rolling Metrics 多因子重构说明

本文档整理了 rolling_metrics 服务近期关于采样、滑窗与分位输出的改动，便于后续配置维护与联调。

## 1. 变更概览
- **多因子支持**：配置直接通过 `factors` 映射定义任意指标（bidask、askbid、mid_spot、mid_swap、spread 等），每个因子独立配置采样周期、滑窗与最小样本数。
- **自适应采样**：消费行情时，根据因子配置执行重采样（支持 1 s、10 s 等灵活窗口），数值写入各自的环形缓冲区。
- **输出结构升级**：Redis `rolling_metrics_thresholds` 中的 JSON 现在携带 `*_quantiles` 列表以及最新的 `mid_price_spot`、`mid_price_swap`、`spread_rate`。
- **脚本同步更新**：同步、打印脚本全部对齐新 schema，可拆分因子维度打印三线表。
- **移除默认分位**：服务不再内建默认上下分位，若因子未配置 `quantiles` 列表则不会计算阈值。

## 2. 配置结构（Redis HASH `rolling_metrics_params`）

常用字段：

```text
MAX_LENGTH          环形缓冲容量
refresh_sec         计算线程刷新周期
reload_param_sec    配置热加载周期
output_hash_key     阈值输出的 HASH 名称
factors             因子配置（JSON 对象）
```

`factors` 示例：

```json
{
  "bidask": {
    "resample_interval_ms": 1000,
    "rolling_window": 100000,
    "min_periods": 90000,
    "quantiles": [5, 70]
  },
  "spread": {
    "resample_interval_ms": 10000,
    "rolling_window": 60000,
    "min_periods": 40000,
    "quantiles": [30, 95]
  },
  "mid_spot": {
    "resample_interval_ms": 1000,
    "rolling_window": 100000,
    "min_periods": 90000,
    "quantiles": []
  }
}
```

字段含义：
- `resample_interval_ms`：重采样桶间隔，支持 1 秒 / 10 秒等。
- `rolling_window`：滑窗长度（条数）。
- `min_periods`：产生阈值所需的最少样本条数。
- `quantiles`：分位数列表，支持整数（按百分位解释，如 5 → 0.05）或 0–1 之间的小数。为空则只保留原始指标，不输出阈值。

> 注意：服务端不再注入默认上下分位，请在 Redis 中显式写入各因子的列表。

## 3. 运行时逻辑
1. **行情接入**：现货与合约通道分别写入 `SymbolQuotes`，待双边就绪后进入采样。
2. **重采样器**：`FactorResampleState` 维护每个因子的当前桶、样本与上次值，按照配置周期对数据做均值聚合与前向填充，写入 `SymbolSeries` 对应的环形缓冲。
3. **中间指标**：实时计算
   - `mid_price_spot = (ask_spot + bid_spot) / 2`
   - `mid_price_swap = (ask_swap + bid_swap) / 2`
   - `spread_rate = (mid_price_spot - mid_price_swap) / mid_price_spot`
   结果存放在 `SymbolSeries` 的原子字段，供输出阶段引用。
4. **计算线程**：定期遍历所有 symbol，根据因子配置提取滑窗样本，达到 `min_periods` 后计算分位，最后写入 Redis。

## 4. 输出结构（Redis HASH `rolling_metrics_thresholds`）

每个字段 `spot_swap::symbol` 的 JSON 示例：

```json
{
  "symbol_pair": "binance_binance-futures::BTCUSDT",
  "base_symbol": "BTCUSDT",
  "update_tp": 1717747200123,
  "sample_size": 95000,
  "bidask_sr": 0.00042,
  "askbid_sr": -0.00037,
  "mid_price_spot": 68012.5,
  "mid_price_swap": 67998.7,
  "spread_rate": 0.000202,
  "bidask_quantiles": [
    {"quantile": 0.05, "threshold": -0.00015},
    {"quantile": 0.7, "threshold": 0.00032}
  ],
  "spread_quantiles": [
    {"quantile": 0.3, "threshold": 0.00005},
    {"quantile": 0.95, "threshold": 0.00041}
  ]
}
```

- 缺少样本时 `threshold` 会返回 `null`。
- 若因子未配置分位，`*_quantiles` 字段将被省略。

## 5. 配套脚本

| 脚本 | 作用 | 要点 |
| --- | --- | --- |
| `scripts/sync_rolling_metrics_params.py` | 同步配置到 Redis | 现要求 `factors` 对象；会校验间隔/窗口/分位合法性；支持 `--dry-run` |
| `scripts/print_rolling_metrics_params.py` | 打印配置三线表 | 第一张表显示通用参数，第二张按照因子输出采样与分位 |
| `scripts/print_rolling_metrics_thresholds.py` | 打印阈值三线表 | 针对 bidask、askbid、midprice_spot、midprice_swap、spread 各生成一张表 |
| `scripts/binance_arb_price_spread_threshold.py` | Binance 套利阈值巡检 | 输出基础指标 + 因子分位列（列名形如 `binance_midprice_spot_5`） |

## 6. 迁移建议
1. **更新 Redis 配置**：使用新版 `sync_rolling_metrics_params.py` 写入包含 `factors` 的配置。
2. **回归验证**：
   - 观察服务日志，确认读取到新的因子摘要；
   - 通过 `print_rolling_metrics_thresholds.py` 检查输出列是否符合期望。
3. **下游对齐**：如有消费者依赖旧字段（`bidask_lower/upper` 等），需改为读取 `bidask_quantiles` 里的有序列表。
4. **监控调整**：若监控引用了旧字段名，需同步更新指标映射。

如需新增因子，只需在 Redis 配置的 `factors` 对象中新增条目，rolling_metrics 将自动创建对应环形缓冲并输出结果。***
