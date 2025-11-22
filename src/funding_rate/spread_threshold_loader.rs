//! 价差阈值加载器
//!
//! 从 Redis 加载价差阈值并更新到 SpreadFactor 单例。
//! Redis Hash 格式：
//! - Key: `binance_spread_thresholds`
//! - Fields (每个 symbol 有 8 个字段):
//!   - `{symbol}_forward_open_mm`: 正套开仓MM阈值
//!   - `{symbol}_forward_open_mt`: 正套开仓MT阈值
//!   - `{symbol}_forward_cancel_mm`: 正套撤单MM阈值
//!   - `{symbol}_forward_cancel_mt`: 正套撤单MT阈值
//!   - `{symbol}_backward_open_mm`: 反套开仓MM阈值
//!   - `{symbol}_backward_open_mt`: 反套开仓MT阈值
//!   - `{symbol}_backward_cancel_mm`: 反套撤单MM阈值
//!   - `{symbol}_backward_cancel_mt`: 反套撤单MT阈值
//!
//! 注意：
//! - 默认使用币安现货和合约，venue 固定为 BinanceMargin 和 BinanceUm
//! - forward_close 和 backward_close 在代码中通过调用对应的 open 方法实现，无需单独加载

use anyhow::Result;
use log::{info, warn};
use std::collections::HashMap;

use super::spread_factor::SpreadFactor;
use crate::signal::common::TradingVenue;

/// 阈值缓存：用于收集同一 symbol 的 mm 和 mt 阈值
struct ThresholdCache {
    mm: Option<f64>,
    mt: Option<f64>,
}

impl ThresholdCache {
    fn new() -> Self {
        Self { mm: None, mt: None }
    }

    fn is_complete(&self) -> bool {
        self.mm.is_some() && self.mt.is_some()
    }

    fn get_pair(&self) -> Option<(f64, f64)> {
        if self.is_complete() {
            Some((self.mm.unwrap(), self.mt.unwrap()))
        } else {
            None
        }
    }
}

/// 从 Redis Hash 加载价差阈值
///
/// # 参数
/// - `hash_map`: Redis HGETALL 返回的 HashMap
///
/// # 返回
/// - `Ok(())`: 加载成功
/// - `Err(e)`: 加载失败
pub fn load_from_redis(hash_map: HashMap<String, String>) -> Result<()> {
    let spread_factor = SpreadFactor::instance();

    // 按 symbol 分组收集阈值
    let mut symbols_thresholds: HashMap<
        String,
        HashMap<String, ThresholdCache>, // operation_key -> cache
    > = HashMap::new();

    // 第一轮：收集所有阈值到缓存
    for (key, value) in hash_map.iter() {
        // 解析 key: "{symbol}_{direction}_{operation}_{mode}"
        // 需要找到最后3个下划线分隔的部分
        let parts: Vec<&str> = key.split('_').collect();
        if parts.len() < 4 {
            warn!(
                "跳过无效的价差阈值 key: {} (格式应为 symbol_direction_operation_mode)",
                key
            );
            continue;
        }

        // mode 是最后一部分
        let mode_str = parts[parts.len() - 1]; // "mm" or "mt"
        let operation_str = parts[parts.len() - 2]; // "open", "cancel", "close"
        let direction_str = parts[parts.len() - 3]; // "forward", "backward"

        // symbol 是前面所有部分（可能包含下划线，如 BTCUSDT）
        let symbol = parts[0..parts.len() - 3].join("_");

        // 解析阈值
        let threshold = match value.parse::<f64>() {
            Ok(v) => v,
            Err(err) => {
                warn!("解析阈值失败: {} = {} (err: {})", key, value, err);
                continue;
            }
        };

        // 构建操作 key: "{direction}_{operation}"
        let op_key = format!("{}_{}", direction_str, operation_str);

        // 存入缓存
        let symbol_map = symbols_thresholds
            .entry(symbol.clone())
            .or_insert_with(HashMap::new);
        let cache = symbol_map.entry(op_key).or_insert_with(ThresholdCache::new);

        match mode_str.to_uppercase().as_str() {
            "MM" => cache.mm = Some(threshold),
            "MT" => cache.mt = Some(threshold),
            _ => {
                warn!("未知的模式: {} (key: {})", mode_str, key);
            }
        }
    }

    // 第二轮：应用完整的阈值对
    let mut loaded_count = 0;

    for (symbol, operations) in symbols_thresholds.iter() {
        let venue1 = TradingVenue::BinanceMargin;
        let venue2 = TradingVenue::BinanceUm;

        for (op_key, cache) in operations.iter() {
            if !cache.is_complete() {
                warn!(
                    "Symbol {} 的 {} 阈值不完整 (mm={:?}, mt={:?})，跳过",
                    symbol, op_key, cache.mm, cache.mt
                );
                continue;
            }

            let (mm_threshold, mt_threshold) = cache.get_pair().unwrap();

            // 解析操作类型
            let parts: Vec<&str> = op_key.split('_').collect();
            if parts.len() != 2 {
                continue;
            }

            let direction_str = parts[0];
            let operation_str = parts[1];

            match (direction_str, operation_str) {
                ("forward", "open") => {
                    spread_factor.set_forward_open_threshold(
                        venue1,
                        symbol,
                        venue2,
                        symbol,
                        mm_threshold,
                        mt_threshold,
                    );
                    loaded_count += 1;
                }
                ("forward", "cancel") => {
                    spread_factor.set_forward_open_cancel_threshold(
                        venue1,
                        symbol,
                        venue2,
                        symbol,
                        mm_threshold,
                        mt_threshold,
                    );
                    loaded_count += 1;
                }
                ("backward", "open") => {
                    spread_factor.set_backward_open_threshold(
                        venue1,
                        symbol,
                        venue2,
                        symbol,
                        mm_threshold,
                        mt_threshold,
                    );
                    loaded_count += 1;
                }
                ("backward", "cancel") => {
                    spread_factor.set_backward_cancel_threshold(
                        venue1,
                        symbol,
                        venue2,
                        symbol,
                        mm_threshold,
                        mt_threshold,
                    );
                    loaded_count += 1;
                }
                _ => {
                    warn!("未知的方向/操作组合: {} (symbol: {})", op_key, symbol);
                }
            }
        }
    }

    info!("✅ 价差阈值已加载: {} 条", loaded_count);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_spread_thresholds() {
        let mut hash_map = HashMap::new();
        hash_map.insert("BTCUSDT_forward_open_mm".to_string(), "0.0002".to_string());
        hash_map.insert("BTCUSDT_forward_open_mt".to_string(), "0.0003".to_string());
        hash_map.insert(
            "BTCUSDT_forward_cancel_mm".to_string(),
            "0.0004".to_string(),
        );
        hash_map.insert(
            "BTCUSDT_forward_cancel_mt".to_string(),
            "0.0005".to_string(),
        );

        let result = load_from_redis(hash_map);
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_incomplete_thresholds() {
        let mut hash_map = HashMap::new();
        // 只有 mm 没有 mt
        hash_map.insert("BTCUSDT_forward_open_mm".to_string(), "0.0002".to_string());

        let result = load_from_redis(hash_map);
        assert!(result.is_ok()); // 应该跳过不完整的配置但不报错
    }

    #[test]
    fn test_load_invalid_value() {
        let mut hash_map = HashMap::new();
        hash_map.insert("BTCUSDT_forward_open_mm".to_string(), "invalid".to_string());

        let result = load_from_redis(hash_map);
        assert!(result.is_ok()); // 应该跳过无效的值但不报错
    }

    #[test]
    fn test_load_symbol_with_underscore() {
        let mut hash_map = HashMap::new();
        // 测试带下划线的 symbol
        hash_map.insert("BTC_USDT_forward_open_mm".to_string(), "0.0002".to_string());
        hash_map.insert("BTC_USDT_forward_open_mt".to_string(), "0.0003".to_string());

        let result = load_from_redis(hash_map);
        assert!(result.is_ok());
    }
}
