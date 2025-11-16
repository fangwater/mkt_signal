//! 资金费率阈值加载器
//!
//! 从 Redis 加载资金费率阈值并更新到 FundingRateFactor 单例。
//! Redis Hash 格式：
//! - Key: `funding_rate_thresholds`
//! - Fields (8 个阈值，不区分 MM/MT):
//!   - `8h_forward_open`: 8h 正套开仓阈值
//!   - `8h_forward_close`: 8h 正套平仓阈值
//!   - `8h_backward_open`: 8h 反套开仓阈值
//!   - `8h_backward_close`: 8h 反套平仓阈值
//!   - `4h_forward_open`: 4h 正套开仓阈值
//!   - `4h_forward_close`: 4h 正套平仓阈值
//!   - `4h_backward_open`: 4h 反套开仓阈值
//!   - `4h_backward_close`: 4h 反套平仓阈值

use anyhow::Result;
use log::{info, warn};
use std::collections::HashMap;

use super::common::FactorMode;
use super::funding_rate_factor::FundingRateFactor;
use super::rate_fetcher::FundingRatePeriod;

/// 从 Redis Hash 加载资金费率阈值
///
/// # 参数
/// - `hash_map`: Redis HGETALL 返回的 HashMap
///
/// # 返回
/// - `Ok(())`: 加载成功
/// - `Err(e)`: 加载失败
pub fn load_from_redis(hash_map: HashMap<String, String>) -> Result<()> {
    let funding_factor = FundingRateFactor::instance();
    let mut loaded_count = 0;

    // 遍历所有字段并解析
    for (key, value) in hash_map.iter() {
        // 解析 key: "{period}_{direction}_{operation}"
        let parts: Vec<&str> = key.split('_').collect();
        if parts.len() != 3 {
            warn!(
                "跳过无效的资金费率阈值 key: {} (格式应为 period_direction_operation)",
                key
            );
            continue;
        }

        let period_str = parts[0]; // "8h" or "4h"
        let direction_str = parts[1]; // "forward" or "backward"
        let operation_str = parts[2]; // "open" or "close"

        // 解析周期
        let period = match period_str {
            "8h" => FundingRatePeriod::Hours8,
            "4h" => FundingRatePeriod::Hours4,
            _ => {
                warn!("未知的资金费率周期: {} (key: {})", period_str, key);
                continue;
            }
        };

        // 解析阈值
        let threshold = match value.parse::<f64>() {
            Ok(v) => v,
            Err(err) => {
                warn!("解析阈值失败: {} = {} (err: {})", key, value, err);
                continue;
            }
        };

        // 根据方向和操作更新阈值（同时更新 MM 和 MT 模式）
        match (direction_str, operation_str) {
            ("forward", "open") => {
                funding_factor.update_forward_open_threshold(period, FactorMode::MM, threshold);
                funding_factor.update_forward_open_threshold(period, FactorMode::MT, threshold);
                loaded_count += 1;
            }
            ("forward", "close") => {
                funding_factor.update_forward_close_threshold(period, FactorMode::MM, threshold);
                funding_factor.update_forward_close_threshold(period, FactorMode::MT, threshold);
                loaded_count += 1;
            }
            ("backward", "open") => {
                funding_factor.update_backward_open_threshold(period, FactorMode::MM, threshold);
                funding_factor.update_backward_open_threshold(period, FactorMode::MT, threshold);
                loaded_count += 1;
            }
            ("backward", "close") => {
                funding_factor.update_backward_close_threshold(period, FactorMode::MM, threshold);
                funding_factor.update_backward_close_threshold(period, FactorMode::MT, threshold);
                loaded_count += 1;
            }
            _ => {
                warn!(
                    "未知的方向/操作组合: {}_{} (key: {})",
                    direction_str, operation_str, key
                );
            }
        }
    }

    info!("✅ 资金费率阈值已加载: {} 条", loaded_count);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_fr_thresholds() {
        let mut hash_map = HashMap::new();
        hash_map.insert("8h_forward_open".to_string(), "0.0001".to_string());
        hash_map.insert("8h_forward_close".to_string(), "-0.0001".to_string());
        hash_map.insert("4h_forward_open".to_string(), "0.00005".to_string());
        hash_map.insert("4h_backward_open".to_string(), "-0.00005".to_string());

        let result = load_from_redis(hash_map);
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_invalid_key() {
        let mut hash_map = HashMap::new();
        hash_map.insert("invalid_key".to_string(), "0.0001".to_string());
        hash_map.insert("8h_invalid_operation".to_string(), "0.0001".to_string());

        let result = load_from_redis(hash_map);
        assert!(result.is_ok()); // 应该跳过无效的 key 但不报错
    }

    #[test]
    fn test_load_invalid_value() {
        let mut hash_map = HashMap::new();
        hash_map.insert("8h_forward_open".to_string(), "invalid".to_string());

        let result = load_from_redis(hash_map);
        assert!(result.is_ok()); // 应该跳过无效的值但不报错
    }

    #[test]
    fn test_load_unknown_period() {
        let mut hash_map = HashMap::new();
        hash_map.insert("1h_forward_open".to_string(), "0.0001".to_string());

        let result = load_from_redis(hash_map);
        assert!(result.is_ok()); // 应该跳过未知周期但不报错
    }
}
