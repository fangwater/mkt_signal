//! Kline Publisher 配置模块

use anyhow::Result;
use serde::Deserialize;
use tokio::fs;

/// K线时间配置
#[derive(Debug, Deserialize, Clone)]
pub struct KlineTimingConfig {
    /// K线周期（毫秒）
    pub period_ms: u64,
    /// 封bar延迟（微秒）
    pub close_delay_us: u64,
}

impl Default for KlineTimingConfig {
    fn default() -> Self {
        Self {
            period_ms: 5_000,
            close_delay_us: 500,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    kline_timing: KlineTimingConfig,
}

/// Kline Publisher 配置
#[derive(Debug, Clone)]
pub struct KlinePubConfig {
    pub kline_timing: KlineTimingConfig,
}

impl KlinePubConfig {
    /// 从配置文件加载配置
    pub async fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path).await?;
        let config_file: ConfigFile = serde_yaml::from_str(&content)?;

        Ok(Self {
            kline_timing: config_file.kline_timing,
        })
    }
}
