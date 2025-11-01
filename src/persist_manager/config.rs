use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::persist_manager::signal::SignalPersistCfg;
use crate::persist_manager::storage::RocksDbConfig;

#[derive(Debug, Clone, Deserialize)]
pub struct PersistManagerCfg {
    #[serde(default)]
    pub rocksdb: RocksDbConfig,
    #[serde(default)]
    pub signal: SignalPersistCfg,
    #[serde(default)]
    pub http: HttpConfig,
    #[serde(default)]
    pub execution: ExecutionPersistCfg,
    #[serde(default)]
    pub um_execution: UmOrderPersistCfg,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpConfig {
    #[serde(default = "default_http_bind")]
    pub bind: String,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind: default_http_bind(),
        }
    }
}

fn default_http_bind() -> String {
    "0.0.0.0:8088".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionPersistCfg {
    #[serde(default = "default_execution_enabled")]
    pub enabled: bool,
    #[serde(default = "default_execution_channel")]
    pub channel: String,
    #[serde(default = "default_execution_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
}

impl Default for ExecutionPersistCfg {
    fn default() -> Self {
        Self {
            enabled: true,
            channel: default_execution_channel(),
            retry_backoff_ms: default_execution_retry_backoff_ms(),
        }
    }
}

fn default_execution_enabled() -> bool {
    true
}

fn default_execution_retry_backoff_ms() -> u64 {
    200
}

fn default_execution_channel() -> String {
    crate::account::execution_record::MARGIN_EXECUTION_RECORD_CHANNEL.to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct UmOrderPersistCfg {
    #[serde(default = "default_um_execution_enabled")]
    pub enabled: bool,
    #[serde(default = "default_um_execution_channel")]
    pub channel: String,
    #[serde(default = "default_um_execution_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
}

impl Default for UmOrderPersistCfg {
    fn default() -> Self {
        Self {
            enabled: true,
            channel: default_um_execution_channel(),
            retry_backoff_ms: default_um_execution_retry_backoff_ms(),
        }
    }
}

fn default_um_execution_enabled() -> bool {
    true
}

fn default_um_execution_retry_backoff_ms() -> u64 {
    200
}

fn default_um_execution_channel() -> String {
    crate::account::order_update_record::UM_ORDER_UPDATE_RECORD_CHANNEL.to_string()
}

impl PersistManagerCfg {
    pub async fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let data = tokio::fs::read(&path)
            .await
            .with_context(|| format!("failed to read config {:?}", path.as_ref()))?;
        let content = String::from_utf8(data)
            .with_context(|| format!("config {:?} is not valid UTF-8", path.as_ref()))?;
        let cfg: Self = toml::from_str(&content)
            .with_context(|| format!("failed to parse config {:?}", path.as_ref()))?;
        Ok(cfg)
    }
}
