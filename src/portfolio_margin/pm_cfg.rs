//! 账户监控（统一账户 PM）TOML 配置解析
//!
//! 主要字段：
//! - `[general]`：ws_session_max_secs（会话最长时长），primary/secondary_local_ip（本地绑定IP）
//! - `[iceoryx.pm]`：max_subscribers（PM发布通道配置，现已固定为 4，仅保留字段兼容旧配置；历史缓存固定 1024）
//! - `[exchanges.binance.ws/rest]`、`[exchanges.okex.ws]`：现已硬编码为官方 PM 地址，字段仅为兼容旧配置
//!
//! 注意：如果 ws.pm 以 `/ws` 结尾，程序将直接在其后追加 listenKey；
//! 否则程序会默认追加 `/ws/{listenKey}`。
use anyhow::Result;
use serde::Deserialize;
use tokio::fs;

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ChannelCfg {
    pub history_size: Option<usize>,
    pub max_subscribers: Option<usize>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct IceoryxCfg {
    pub pm: Option<ChannelCfg>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct WsCfg {
    pub pm: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct RestCfg {
    pub pm: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ExchangeCfg {
    pub ws: Option<WsCfg>,
    pub rest: Option<RestCfg>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ExchangesCfg {
    pub binance: Option<ExchangeCfg>,
    pub okex: Option<ExchangeCfg>,
    pub bybit: Option<ExchangeCfg>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GeneralCfg {
    pub ws_session_max_secs: Option<u64>,
    pub primary_local_ip: Option<String>,
    pub secondary_local_ip: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AccountTomlCfg {
    pub general: GeneralCfg,
    pub iceoryx: Option<IceoryxCfg>,
    pub exchanges: ExchangesCfg,
}

impl AccountTomlCfg {
    pub async fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path).await?;
        let cfg: Self = toml::from_str(&content)?;
        Ok(cfg)
    }
}
