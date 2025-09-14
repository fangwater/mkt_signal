//! 账户监控（统一账户 PM）TOML 配置解析
//!
//! 主要字段：
//! - `[general]`：ws_session_max_secs（会话最长时长），primary/secondary_local_ip（本地绑定IP）
//! - `[iceoryx.pm]`：history_size、max_subscribers（PM发布通道配置）
//! - `[exchanges.binance.ws]`：pm（用户数据WS base，如 `wss://.../pm` 或 `.../pm/ws`）
//! - `[exchanges.binance.rest]`：pm（用户数据REST base，如 `https://papi.binance.com`）
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
