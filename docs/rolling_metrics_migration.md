# Rolling Metrics 配置独立化迁移指南

> 注意：本文基于早期按 exchange 维度的接口，相关脚本已重命名为 `print_rolling_metrics_*` / `sync_rolling_metrics_*`，并改为 `--open-venue/--hedge-venue` 参数。下文示例请据此调整后再使用。

## 变更概述

现在每个交易所都有**独立的** rolling_metrics 配置，不再共享配置。

### 变更前（旧版本）
- 所有交易所共享配置 hash key：
  - `rolling_metrics_params` - 配置参数
  - `rolling_metrics_thresholds` - 输出结果
  - `funding_rate_thresholds` - 资金费率阈值

### 变更后（新版本）
- 每个交易所独立配置 hash key：
  - `rolling_metrics_params_{exchange}` - 配置参数
  - `rolling_metrics_thresholds_{exchange}` - 输出结果
  - `funding_rate_thresholds_{exchange}` - 资金费率阈值

## 支持的交易所

- `binance`
- `okex`
- `bybit`
- `bitget`
- `gate`

## 使用方法

### 1. 初始化配置

为每个交易所初始化配置（使用 sync 脚本）：

```bash
# 初始化 binance 现货/合约配置
python scripts/rolling_metrics/sync_rolling_metrics_params.py --open-venue binance-margin --hedge-venue binance-futures

# 初始化 okex 现货/合约配置
python scripts/rolling_metrics/sync_rolling_metrics_params.py --open-venue okex-margin --hedge-venue okex-futures

# 自定义参数
python scripts/rolling_metrics/sync_rolling_metrics_params.py --open-venue binance-margin --hedge-venue binance-futures \
    --max-length 200000 \
    --refresh-sec 60

# 预览而不写入（dry-run）
python scripts/rolling_metrics/sync_rolling_metrics_params.py --open-venue binance-margin --hedge-venue binance-futures --dry-run
```

### 2. 查看配置

查看特定交易所的配置：

```bash
# 查看 binance margin/futures 配置
python scripts/rolling_metrics/print_rolling_metrics_params.py --open-venue binance-margin --hedge-venue binance-futures

# 查看 okex 配置
python scripts/rolling_metrics/print_rolling_metrics_params.py --open-venue okex-margin --hedge-venue okex-futures

# 只查看特定 factor
python scripts/rolling_metrics/print_rolling_metrics_params.py --open-venue binance-margin --hedge-venue binance-futures --prefix bidask_
```

### 3. 同步资金费率阈值

为每个交易所同步资金费率阈值：

```bash
# 同步 binance 资金费率阈值
python scripts/sync_funding_rate_thresholds.py --exchange binance

# 同步 okex 资金费率阈值
python scripts/sync_funding_rate_thresholds.py --exchange okex
```

### 4. 查看配置

```bash
# 查看 rolling_metrics 配置
python scripts/rolling_metrics/print_rolling_metrics_params.py --open-venue binance-margin --hedge-venue binance-futures

# 查看 rolling_metrics 输出结果
python scripts/rolling_metrics/print_rolling_metrics_thresholds.py --open-venue binance-margin --hedge-venue binance-futures

# 查看资金费率阈值
python scripts/print_funding_rate_thresholds.py --exchange binance
```

### 5. 启动 rolling_metrics 服务

启动时**必须指定** `--exchange` 参数：

```bash
# 启动 binance rolling_metrics
cargo run --bin rolling_metrics -- --exchange binance

# 启动 okex rolling_metrics
cargo run --bin rolling_metrics -- --exchange okex

# 自定义配置 hash key（可选）
cargo run --bin rolling_metrics -- \
    --exchange binance \
    --params-hash-key custom_params_binance \
    --output-hash-key custom_output_binance
```

### 6. 启动使用资金费率阈值的服务

对于需要读取资金费率阈值的服务（如 funding_rate 策略），通过**环境变量**指定 exchange：

```bash
# 设置环境变量指定 funding_rate_thresholds key
export FUNDING_THRESHOLD_REDIS_KEY="funding_rate_thresholds_binance"

# 启动服务（会自动读取 funding_rate_thresholds_binance）
cargo run --bin your_funding_rate_service

# 或者在命令行直接指定
FUNDING_THRESHOLD_REDIS_KEY="funding_rate_thresholds_okex" \
    cargo run --bin your_funding_rate_service
```

支持的环境变量：
- `FUNDING_THRESHOLD_REDIS_KEY` - 资金费率阈值 key（默认 `funding_rate_thresholds`）
- `SPREAD_THRESHOLD_REDIS_KEY` - 价差阈值 key（默认 `binance_spread_thresholds`）

## Redis Key 命名规范

### 配置参数 (params)
- Binance: `rolling_metrics_params_binance`
- OKEx: `rolling_metrics_params_okex`
- Bybit: `rolling_metrics_params_bybit`
- Bitget: `rolling_metrics_params_bitget`
- Gate: `rolling_metrics_params_gate`

### 输出结果 (thresholds)
- Binance: `rolling_metrics_thresholds_binance`
- OKEx: `rolling_metrics_thresholds_okex`
- Bybit: `rolling_metrics_thresholds_bybit`
- Bitget: `rolling_metrics_thresholds_bitget`
- Gate: `rolling_metrics_thresholds_gate`

### 资金费率阈值
- Binance: `funding_rate_thresholds_binance`
- OKEx: `funding_rate_thresholds_okex`
- Bybit: `funding_rate_thresholds_bybit`
- Bitget: `funding_rate_thresholds_bitget`
- Gate: `funding_rate_thresholds_gate`

## 数据隔离

### 数据 Prefix 格式
rolling_metrics 的数据通过 prefix 在 Redis 中区分：

- Binance: `binance_binance-futures::{symbol}::...`
- OKEx: `okex_okex-swap::{symbol}::...`
- Bybit: `bybit-spot_bybit::{symbol}::...`
- Bitget: `bitget_bitget-futures::{symbol}::...`
- Gate: `gate_gate-futures::{symbol}::...`

### 完整的 Redis Field 示例

```
rolling_metrics_thresholds_binance:
  binance_binance-futures::BTCUSDT::bidask::0.5    = "0.00012"
  binance_binance-futures::ETHUSDT::askbid::0.99   = "0.00034"

rolling_metrics_thresholds_okex:
  okex_okex-swap::BTC-USDT-SWAP::bidask::0.5       = "0.00015"
  okex_okex-swap::ETH-USDT-SWAP::askbid::0.99      = "0.00038"
```

## 迁移步骤

如果你已有旧版本的配置，需要执行以下迁移步骤：

### 1. 备份旧配置

```bash
# 导出旧配置
redis-cli --raw HGETALL rolling_metrics_params > backup_params.txt
redis-cli --raw HGETALL rolling_metrics_thresholds > backup_thresholds.txt
redis-cli --raw HGETALL funding_rate_thresholds > backup_funding.txt
```

### 2. 为每个交易所创建新配置（open/hedge 组合）

```bash
# 为每个交易所初始化配置
for exchange in binance okex bybit bitget gate; do
    python scripts/rolling_metrics/sync_rolling_metrics_params.py --open-venue ${exchange}-margin --hedge-venue ${exchange}-futures
    python scripts/sync_funding_rate_thresholds.py --exchange ${exchange}
done
```

### 3. 更新启动脚本

确保 rolling_metrics 启动脚本都添加了 `--open-venue/--hedge-venue` 参数：

```bash
# 旧版本（会失败或取默认）
cargo run --bin rolling_metrics   # 缺少 open/hedge

# 新版本（正确）
cargo run --bin rolling_metrics -- --open-venue binance-margin --hedge-venue binance-futures
```

### 4. 清理旧数据（可选）

确认新配置正常运行后，可以删除旧的共享配置：

```bash
redis-cli DEL rolling_metrics_params
redis-cli DEL funding_rate_thresholds
# 注意：不要删除 rolling_metrics_thresholds，它包含历史数据
```

## 故障排除

### 问题：启动时提示找不到配置

**错误信息：**
```
未找到参数或 HASH 'rolling_metrics_params_binance-margin_binance-futures' 为空
```

**解决方案：**
```bash
python scripts/rolling_metrics/sync_rolling_metrics_params.py --open-venue binance-margin --hedge-venue binance-futures
```

### 问题：脚本提示缺少 --open-venue/--hedge-venue 参数

**错误信息：**
```
error: the following required arguments were not provided:
  --open-venue <...>
  --hedge-venue <...>
```

**解决方案：**
所有脚本现在都需要指定 open/hedge，例如：
```bash
python scripts/rolling_metrics/print_rolling_metrics_params.py --open-venue binance-margin --hedge-venue binance-futures
```

### 问题：多个交易所的数据混在一起

**检查方法：**
```bash
# 查看 Redis 中的所有 rolling_metrics key
redis-cli KEYS "rolling_metrics*"
```

**预期输出：**
```
rolling_metrics_params_binance-margin_binance-futures
rolling_metrics_params_okex-margin_okex-futures
rolling_metrics_thresholds_binance-margin_binance-futures
rolling_metrics_thresholds_okex-margin_okex-futures
```

## 配置参数说明

### 通用参数
- `MAX_LENGTH`: 滑窗最大长度（默认 150000）
- `refresh_sec`: 计算刷新间隔（秒，默认 60）
- `reload_param_sec`: 配置重载间隔（秒，默认 3）

### Factor 配置
每个 factor（bidask, askbid, spread）都有：
- `resample_interval_ms`: 重采样间隔（毫秒，默认 1000）
- `rolling_window`: 滑窗大小（默认 100000）
- `min_periods`: 最小周期数（默认 90000）
- `quantiles`: 分位数列表（默认 [0.001, 0.01, 0.05, 0.5, 0.95, 0.99, 0.999]）

## 相关文件

### Python 脚本
- `scripts/rolling_metrics/sync_rolling_metrics_params.py` - 同步 rolling_metrics 配置
- `scripts/rolling_metrics/print_rolling_metrics_params.py` - 查看 rolling_metrics 配置
- `scripts/sync_funding_rate_thresholds.py` - 同步资金费率阈值
- `scripts/print_funding_rate_thresholds.py` - 查看资金费率阈值
- `scripts/rolling_metrics/print_rolling_metrics_thresholds.py` - 查看 rolling_metrics 输出结果

### Rust 代码
- `src/bin/rolling_metrics.rs` - rolling_metrics 主程序
- `src/rolling_metrics/config.rs` - rolling_metrics 配置模块
- `src/funding_rate/config_loader.rs` - 统一配置加载器（支持环境变量）
- `src/funding_rate/fr_threshold_loader.rs` - 资金费率阈值加载器

## 总结：两个脚本的职责

### `rolling_metrics/sync_rolling_metrics_params.py` - 统计计算配置
- **写入**: `rolling_metrics_params_{open}_{hedge}`
- **用途**: 配置如何计算滑窗统计量
- **内容**: MAX_LENGTH, refresh_sec, factors (bidask, askbid, spread)
- **使用者**: rolling_metrics 进程

### `sync_funding_rate_thresholds.py` - 交易策略阈值
- **写入**: `funding_rate_thresholds_{exchange}`
- **用途**: 配置交易策略的决策阈值
- **内容**: 8h/4h 的正反套开平仓阈值
- **使用者**: 交易策略进程

两个脚本分开是为了：
1. **职责分离**：统计层 vs 策略层
2. **独立维护**：统计参数和交易阈值可以独立调整
3. **更新频率不同**：统计参数相对稳定，交易阈值需要频繁调整
