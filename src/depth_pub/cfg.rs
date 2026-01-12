//! Depth Publisher 配置模块

use anyhow::Result;
use serde::Deserialize;
use tokio::fs;

/// 深度档位开关配置
#[derive(Debug, Deserialize, Clone)]
pub struct DepthLevelsConfig {
    pub enable_depth5: bool,
    pub enable_depth20: bool,
    pub enable_depth50: bool,
}

impl Default for DepthLevelsConfig {
    fn default() -> Self {
        Self {
            enable_depth5: true,
            enable_depth20: true,
            enable_depth50: false,
        }
    }
}

/// 推送配置
#[derive(Debug, Deserialize, Clone)]
pub struct PushConfig {
    pub min_push_interval_ms: u64,
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            min_push_interval_ms: 100,
        }
    }
}

/// 配置文件结构
#[derive(Debug, Deserialize)]
struct ConfigFile {
    depth_levels: DepthLevelsConfig,
    push_config: PushConfig,
}

/// Depth Publisher 配置
#[derive(Debug, Clone)]
pub struct DepthPubConfig {
    pub depth_levels: DepthLevelsConfig,
    pub push_config: PushConfig,
}

impl DepthPubConfig {
    /// 从配置文件加载配置
    pub async fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path).await?;
        let config_file: ConfigFile = serde_yaml::from_str(&content)?;

        Ok(Self {
            depth_levels: config_file.depth_levels,
            push_config: config_file.push_config,
        })
    }
}
