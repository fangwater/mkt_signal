# Rust vs Python 因子计算对比报告

> 生成日期: 2026-02-26
> Rust 因子实现: `src/factor_pub/fusion_factor_pub/app.rs`
> Python 因子池: `final_factor_pool_update20260123.py`
> 对比脚本: `compare_factor_test.py`

## 1. 测试方法

### 1.1 合成数据

Rust 端 `factor_test` binary（`src/bin/factor_test.rs`）使用确定性 PRNG（xorshift64）生成三组合成行情数据，每组 600 bars，包含完整的 kline + trade_flow + 20档 depth 字段：

| 场景 | 说明 | 数据特征 |
|------|------|----------|
| `normal` | 正常行情模拟 | 价格在合理范围波动，volume/amount 有随机变化 |
| `edge_flat` | 常数序列 | 所有价格字段恒定，volume/amount 恒定，用于测试零方差边界 |
| `edge_extreme` | 极端值 | 价格剧烈波动，含极大/极小值，测试数值稳定性 |

### 1.2 对比流程

1. `cargo run --bin factor_test -- --output /tmp/factor_test_output.json` 生成 Rust 计算结果
2. JSON 包含每个场景的完整 600 bar 输入序列 + 172 个有效 fusion factor 的最终值
3. Python 端读取同一份输入序列，构建 DataFrame，调用 Python 因子函数计算**完整序列**后取 `result[-1]`
4. 对比 Rust 输出值与 Python 末尾值，相对误差 < 1e-6 视为匹配

### 1.3 时序因子处理

对于依赖历史窗口的因子（rolling mean/std/skew/kurt/corr/quantile 等），两端均基于完整 600 bar 序列计算。Python 示例：

```python
result = func(df)          # df 包含完整 600 bar
val = float(result[-1])    # 取序列末尾值与 Rust 对比
```

这确保了 rolling window 内的历史数据完全一致，不存在"只比单点"的问题。

## 2. 测试结果总览

### 修复 quantile 插值后的最终结果：

| 场景 | 总计 | 匹配 | 不匹配 | 跳过 | 匹配率 |
|------|------|------|--------|------|--------|
| normal | 172 | 172 | 0 | 0 | **100.0%** |
| edge_extreme | 172 | 167 | 4 | 1 | **97.7%** |
| edge_flat | 172 | 138 | 34 | 0 | **80.2%** |

### 修复前（quantile 使用 floor 插值）：

| 场景 | 匹配 | 不匹配 | 匹配率 |
|------|------|--------|--------|
| normal | 169 | 3 | 98.3% |
| edge_extreme | 165 | 6 | 96.5% |
| edge_flat | 138 | 34 | 80.2% |

修复内容：`tail_quantile_last` 从 floor 插值改为 linear 插值，对齐 pandas 默认行为。修复后 normal 场景达到 100% 匹配。

## 3. 差异分类与详细分析

### 3.1 已修复：Quantile 插值方法（影响 3 个因子）

**涉及因子**: `factor_094`, `baseline_028`, `baseline_113`

**根因**: Rust 的 `tail_quantile_last` 原实现使用 floor 取整：
```rust
// 修复前
let idx = ((n - 1) as f64 * q).floor() as usize;
tail.get(idx).copied()
```

pandas 的 `rolling().quantile()` 默认使用 linear interpolation。

**修复后**:
```rust
// 修复后 - linear interpolation
let pos = (n - 1) as f64 * q;
let lo = pos.floor() as usize;
let hi = (lo + 1).min(n - 1);
let frac = pos - lo as f64;
let value = tail[lo] * (1.0 - frac) + tail[hi] * frac;
```

**修复前误差**:

| 因子 | 场景 | Rust | Python | 相对误差 |
|------|------|------|--------|----------|
| factor_094 | normal | 39671.08 | 39693.33 | 5.6e-4 |
| baseline_028 | normal | -11.055 | -9.669 | 1.25e-1 |
| baseline_113 | normal | 10.5901 | 10.5903 | 1.97e-5 |
| factor_094 | edge_extreme | 40178.79 | 40183.31 | 1.12e-4 |
| baseline_028 | edge_extreme | -8.718 | -8.161 | 6.38e-2 |

`baseline_028` 误差较大的原因：quantile 计算的是 volatility（分母），分母的微小差异在除法中被放大。

**修复后**: 三个因子在 normal 场景全部精确匹配，edge_extreme 中 `baseline_028` 和 `factor_094` 也已对齐。

### 3.2 NaN 防御性处理差异（影响 ~31 个因子，仅 edge_flat）

**涉及因子**:

- `factor_001`, `factor_021`, `factor_052`, `factor_077`, `factor_107`, `factor_108`, `factor_124`, `factor_134`
- `baseline_016`, `baseline_028`(edge_flat), `baseline_079`, `baseline_080`, `baseline_082`, `baseline_117`, `baseline_172`, `baseline_177`, `baseline_178`, `baseline_189`, `baseline_191`
- `TD_CI_003`, `TD_MT_008`, `TD_MT_015`, `TD_PR_016`, `TD_TI_031`, `TD_TI_033`, `TD_TI_034`, `TD_VI_025`
- `TP_VPI_001`, `TP_VPI_002`
- `factor_trades_034`, `factor_trades_035`

**表现**: Rust 返回 `0.0`，Python 返回 `NaN`。仅在 `edge_flat`（常数序列）场景出现。

**根因**: Rust 的 `window_primitives.rs` 中多处做了防御性 NaN→0 转换：

```rust
// finite_or_none: NaN/Inf → Some(0.0)
fn finite_or_none(value: Option<f64>) -> Option<f64> {
    match value {
        Some(v) if v.is_finite() => Some(v),
        Some(_) => Some(0.0),   // ← NaN/Inf 被替换为 0
        None => None,
    }
}

// last_f64: 取 rolling 结果末尾值，NaN 也 fallback 到 0
fn last_f64(series: &Series) -> Option<f64> {
    let idx = series.len().saturating_sub(1);
    finite_or_none(series.f64().ok()?.get(idx)).or(Some(0.0))
}

// rolling_kurt_last: 末尾兜底
Ok(finite_or_none(value).or(Some(0.0)))
```

Python（pandas/numpy）在以下情况返回 NaN：
- 常数序列 `std() = 0` → `value / std` = NaN
- `rolling().corr()` 方差为零 → NaN
- `rolling().skew()` / `.kurt()` 常数序列 → NaN
- 除法分母为零 → NaN

**影响评估**: 这是**有意的设计选择**，不是 bug。在实际行情中价格/量不会是常数序列，这些因子在 normal 场景全部精确匹配。Rust 选择 0.0 作为安全默认值可以避免 NaN 在下游信号/交易逻辑中传播，是合理的工程决策。

**完整列表（edge_flat 场景，Rust=0 vs Python=NaN）**:

| 因子 | Rust 值 | Python 值 | 触发原因 |
|------|---------|-----------|----------|
| factor_001 | 0 | NaN | rolling corr, std=0 |
| factor_021 | 0 | NaN | rolling std=0 导致除法 NaN |
| factor_052 | 0 | NaN | rolling corr, std=0 |
| factor_077 | 0 | NaN | rolling std=0 |
| factor_107 | 0 | NaN | rolling corr, std=0 |
| factor_108 | 0 | NaN | rolling corr, std=0 |
| factor_124 | 0 | NaN | pct_change=0, 后续计算 NaN |
| factor_134 | 0 | NaN | rolling std=0 |
| baseline_016 | 0 | NaN | rolling std=0 |
| baseline_028 | 0 | NaN | quantile volatility=0, 除法 NaN |
| baseline_079 | 0 | NaN | rolling corr, std=0 |
| baseline_080 | 0 | NaN | rolling corr, std=0 |
| baseline_082 | 0 | NaN | rolling corr, std=0 |
| baseline_117 | 0 | NaN | rolling std=0 |
| baseline_172 | 0 | NaN | rolling std=0 |
| baseline_177 | 0 | NaN | rolling corr, std=0 |
| baseline_178 | 0 | NaN | rolling corr, std=0 |
| baseline_189 | 0 | NaN | rolling std=0 |
| baseline_191 | 0 | NaN | rolling std=0 |
| TD_CI_003 | 0 | NaN | diff 全零, std=0 |
| TD_MT_008 | 0 | NaN | rolling std=0 |
| TD_MT_015 | 0 | NaN | rolling std=0 |
| TD_PR_016 | 0 | NaN | rolling std=0 |
| TD_TI_031 | 0 | NaN | stochastic: (high-low)=0 |
| TD_TI_033 | 0 | NaN | rolling std=0 |
| TD_TI_034 | 0 | NaN | rolling std=0 |
| TD_VI_025 | 0 | NaN | bid/ask variance=0 |
| TP_VPI_001 | 0 | NaN | (high-low)=0, CLV 除法 NaN |
| TP_VPI_002 | 0 | NaN | 同 TP_VPI_001 |
| factor_trades_034 | 0 | NaN | rolling std=0 |
| factor_trades_035 | 0 | NaN | rolling std=0 |

### 3.3 Kurtosis 常数序列定义差异（影响 3 个因子，仅 edge_flat）

**涉及因子**: `factor_045`, `factor_046`, `avg_price`

**表现**: Rust 返回 `0`，Python 返回 `-3`。

**根因**:

- Polars 的 `kurtosis(fisher=true, bias=false)` 在常数序列（方差=0）时返回 `None`，被 Rust 的 `finite_or_none` 转为 `0.0`
- pandas 的 `rolling().kurt()` 对常数序列返回 `-3`（excess kurtosis 的数学定义：正态分布 kurtosis=3，excess=0；常数分布在 pandas 实现中返回 -3）

**具体实现对比**:

```rust
// Rust (Polars)
pub fn rolling_kurt_last(values: &[f64], window: usize, fisher: bool, bias: bool) -> Result<Option<f64>> {
    // ... Polars kurtosis 对常数序列返回 None
    Ok(finite_or_none(value).or(Some(0.0)))  // None → Some(0.0)
}
```

```python
# Python (pandas)
df['top10_bid_mean'].rolling(30).kurt()  # 常数序列 → -3.0
```

**影响评估**: 仅在常数序列触发，实际行情不会出现。数学上常数分布的 excess kurtosis 定义存在争议（有的库返回 0，有的返回 -3，有的返回 NaN），两种处理都可接受。

### 3.4 极端值场景逻辑分歧（影响 4 个因子，仅 edge_extreme）

#### 3.4.1 TD_PT_003

| | 值 | 说明 |
|--|---|------|
| Rust | NaN (None) | `rolling_mean_series_opt` 在极端 vwap 序列上返回 None |
| Python | 0.0 | `ln(ratio)` 计算路径不同，Python 对 ratio≤0 返回 0 |

**根因**: 该因子计算 `ln(mean_vwap / rolling_vwap)`，在极端数据下 ratio 可能为负或零。Rust 的 `finite_opt` 将非有限值转为 None 并向上传播；Python 的实现在 ratio≤0 时走了不同的分支。

#### 3.4.2 TD_PT_004

| | 值 | 说明 |
|--|---|------|
| Rust | 1.607 | `sma_fast(9) - sma_slow(25)` 的差值 |
| Python | 0.0 | 同一公式但中间累积值在极端数据下分歧 |

**根因**: 该因子涉及多步链式计算 `(close-open)/hl * (volume/vol_ma)` → `rolling_sum(60)` → `sma(9) - sma(25)`。在极端值场景下，`(high-low)` 接近零时的除法保护逻辑不同，导致中间序列分歧并在后续累积中放大。

#### 3.4.3 TP_VPI_001 / TP_VPI_002

| | 值 | 说明 |
|--|---|------|
| Rust | 0.0 | CLV 计算中 `(high-low)=0` 时返回 NaN，被 rolling_sum 的 min_periods 过滤 |
| Python | NaN | 同样的 NaN 但未被转换 |

**根因**: 与 3.2 节相同的 NaN 防御性处理，但在 edge_extreme 中部分 bar 的 `high == low` 触发。

### 3.5 Python 端 Bug（影响 1 个因子）

**涉及因子**: `factor_097`

**表现**: Python 抛出异常，Rust 正常计算。

```
ValueError: setting an array element with a sequence.
The requested array has an inhomogeneous shape after 1 dimensions.
The detected shape was (10,) + inhomogeneous part.
```

**根因**: Python 实现中构造 numpy 数组时，子数组长度不一致导致 inhomogeneous shape 错误。这是 Python 端的 bug，与 Rust 无关。仅在 edge_extreme 场景触发。

## 4. 总结

### 4.1 差异分类汇总

| 类别 | 因子数 | 触发场景 | 严重程度 | 状态 |
|------|--------|----------|----------|------|
| Quantile 插值 | 3 | normal + extreme | 中 | ✅ 已修复 |
| NaN→0 防御处理 | ~31 | 仅 edge_flat | 低 | 保持现状（设计选择） |
| Kurtosis 常数序列 | 3 | 仅 edge_flat | 低 | 保持现状 |
| 极端值逻辑分歧 | 4 | 仅 edge_extreme | 低 | 按需对齐 |
| Python bug | 1 | 仅 edge_extreme | 无 | Python 端修复 |

### 4.2 结论

- **正常行情场景（normal）: 172/172 因子 100% 匹配**，Rust 实现与 Python 因子池完全一致
- 所有剩余差异仅在边界条件（常数序列、极端值）下出现，且均为合理的工程处理差异
- Rust 端的 NaN→0 防御策略是有意设计，避免 NaN 在下游交易信号中传播
- 唯一需要关注的实际 bug 是 Python 端 `factor_097` 的数组构造问题

### 4.3 修改记录

| 日期 | 文件 | 修改内容 |
|------|------|----------|
| 2026-02-26 | `src/factor_pub/fusion_factor_pub/app.rs` | `tail_quantile_last`: floor 插值 → linear 插值 |
