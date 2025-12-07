use serde::Deserialize;

/// Binance API 密钥配置
#[derive(Debug, Clone, Deserialize)]
pub struct ApiKey {
    pub name: String,
    pub key: String,
    pub secret: String,
}

/// 硬编码的 REST 配置常量
pub struct RestConstants;

impl RestConstants {
    pub const BINANCE_BASE_URL: &'static str = "https://papi.binance.com";
    pub const TIMEOUT_MS: u64 = 10_000;
    pub const RECV_WINDOW_MS: u64 = 5_000;
}

/// 硬编码的限流配置常量
pub struct LimitConstants;

impl LimitConstants {
    pub const ACCOUNT_PER_MIN: u32 = 1200;
    pub const IP_WEIGHT_PER_MIN: u32 = 6000;
    pub const WARN_RATIO: f32 = 0.8;
    pub const COOLDOWN_MS_429: u64 = 60_000;
    pub const BAN_BACKOFF_MS_418: u64 = 120_000;
}

/// 硬编码的 WebSocket 配置常量
pub struct WsConstants;

impl WsConstants {
    pub const CONNECT_TIMEOUT_MS: u64 = 5_000;
    pub const PING_INTERVAL_MS: u64 = 15_000;
    pub const MAX_INFLIGHT: usize = 128;

    /// OKX 交易 WebSocket URL (business 频道，用于下单)
    pub const OKEX_BUSINESS_WS_URL: &'static str = "wss://ws.okx.com:8443/ws/v5/business";
}

/// 默认的本地 IP 列表（用于 OKEx 双连接）
pub const DEFAULT_LOCAL_IPS: [&str; 2] = ["172.31.33.133", "172.31.46.90"];
