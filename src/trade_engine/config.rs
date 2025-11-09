use anyhow::Result;
use serde::Deserialize;
use std::{net::IpAddr, path::Path};

#[derive(Debug, Clone, Deserialize)]
pub struct TradeEngineCfg {
    pub order_req_service: String,
    pub order_resp_service: String,
    pub exchange: Option<String>,
    pub rest: RestCfg,
    #[serde(default)]
    pub transport: TradeTransport,
    pub ws: Option<WsCfg>,
    pub limits: LimitsCfg,
    pub network: NetworkCfg,
    pub accounts: AccountsCfg,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TradeTransport {
    Rest,
    Ws,
}

impl Default for TradeTransport {
    fn default() -> Self {
        TradeTransport::Rest
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RestCfg {
    pub base_url: String,
    pub timeout_ms: Option<u64>,
    pub recv_window_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WsCfg {
    pub urls: Vec<String>,
    pub connect_timeout_ms: Option<u64>,
    pub ping_interval_ms: Option<u64>,
    pub max_inflight: Option<usize>,
    pub login_payload: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LimitsCfg {
    pub account_per_min: Option<u32>,    // default 1200
    pub ip_weight_per_min: Option<u32>,  // default 6000
    pub warn_ratio: Option<f32>,         // default 0.8
    pub cooldown_ms_429: Option<u64>,    // default 60_000
    pub ban_backoff_ms_418: Option<u64>, // default 120_000
}

#[derive(Debug, Clone, Deserialize)]
pub struct NetworkCfg {
    pub local_ips: Vec<IpAddr>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccountsCfg {
    pub keys: Vec<ApiKey>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiKey {
    pub name: String,
    pub key: String,
    pub secret: String,
}

impl TradeEngineCfg {
    pub async fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let data = tokio::fs::read_to_string(path).await?;
        let cfg: Self = toml::from_str(&data)?;
        Ok(cfg)
    }
}

impl LimitsCfg {
    pub fn account_limit(&self) -> u32 {
        self.account_per_min.unwrap_or(1200)
    }
    pub fn ip_weight_limit(&self) -> u32 {
        self.ip_weight_per_min.unwrap_or(6000)
    }
    pub fn warn_ratio(&self) -> f32 {
        self.warn_ratio.unwrap_or(0.8)
    }
    pub fn cooldown_429(&self) -> u64 {
        self.cooldown_ms_429.unwrap_or(60_000)
    }
    pub fn ban_backoff_418(&self) -> u64 {
        self.ban_backoff_ms_418.unwrap_or(120_000)
    }
}
