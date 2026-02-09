//! Gate.io WebSocket 私有频道鉴权模块
//!
//! Gate.io 私有频道需要通过 WebSocket 发送带有签名的订阅消息进行鉴权。
//! 与 OKX 不同，Gate.io 不需要单独的 login 消息，而是在订阅消息中携带鉴权信息。
//!
//! 签名算法: HMAC-SHA512(channel={channel}&event={event}&time={timestamp}, secretKey)

use hmac::{Hmac, Mac};
use sha2::Sha512;

type HmacSha512 = Hmac<Sha512>;

/// Gate.io API 凭证
#[derive(Clone)]
pub struct GateCredentials {
    pub api_key: String,
    pub secret_key: String,
}

impl GateCredentials {
    pub fn new(api_key: String, secret_key: String) -> Self {
        Self {
            api_key,
            secret_key,
        }
    }

    /// 从环境变量加载凭证
    pub fn from_env() -> anyhow::Result<Self> {
        let api_key = std::env::var("GATE_API_KEY")
            .map_err(|_| anyhow::anyhow!("GATE_API_KEY not set"))?
            .trim()
            .to_string();
        let secret_key = std::env::var("GATE_API_SECRET")
            .map_err(|_| anyhow::anyhow!("GATE_API_SECRET not set"))?
            .trim()
            .to_string();

        Ok(Self::new(api_key, secret_key))
    }

    /// 生成 WebSocket 签名
    /// sign = Hex(HMAC-SHA512(channel={channel}&event={event}&time={timestamp}, secretKey))
    pub fn sign(&self, channel: &str, event: &str, timestamp: i64) -> String {
        let message = format!("channel={}&event={}&time={}", channel, event, timestamp);

        let mut mac =
            HmacSha512::new_from_slice(self.secret_key.as_bytes()).expect("HMAC can take any size");
        mac.update(message.as_bytes());
        let result = mac.finalize();

        // Gate.io 使用十六进制编码
        hex::encode(result.into_bytes())
    }

    /// 生成带鉴权的订阅消息
    pub fn build_subscribe_message(&self, channel: &str, payload: Vec<&str>) -> serde_json::Value {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let sign = self.sign(channel, "subscribe", timestamp);

        serde_json::json!({
            "time": timestamp,
            "channel": channel,
            "event": "subscribe",
            "payload": payload,
            "auth": {
                "method": "api_key",
                "KEY": self.api_key,
                "SIGN": sign
            }
        })
    }
}

/// Gate.io WebSocket 私有频道 URL
pub struct GatePrivateWsUrls;

impl GatePrivateWsUrls {
    /// 统一账户 WebSocket URL
    pub const UNIFIED: &'static str = "wss://ws.gate.com/v4/ws/unified";

    /// 现货 WebSocket URL
    pub const SPOT: &'static str = "wss://api.gateio.ws/ws/v4/";

    /// USDT 合约 WebSocket URL
    pub const FUTURES_USDT: &'static str = "wss://fx-ws.gateio.ws/v4/ws/usdt";

    /// BTC 合约 WebSocket URL
    pub const FUTURES_BTC: &'static str = "wss://fx-ws.gateio.ws/v4/ws/btc";
}

/// 构建统一账户资产详情订阅消息
pub fn build_unified_asset_subscribe_message(credentials: &GateCredentials) -> serde_json::Value {
    credentials.build_subscribe_message("unified.asset_detail", vec!["!all"])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_generation() {
        let creds = GateCredentials::new("test_api_key".to_string(), "test_secret".to_string());

        let timestamp = 1716796362i64;
        let sign = creds.sign("unified.asset_detail", "subscribe", timestamp);

        // 签名应该是十六进制编码
        assert!(!sign.is_empty());
        // 应该是 128 字符 (SHA512 = 64 bytes = 128 hex chars)
        assert_eq!(sign.len(), 128);
    }

    #[test]
    fn test_subscribe_message_format() {
        let creds = GateCredentials::new("test_api_key".to_string(), "test_secret".to_string());

        let msg = creds.build_subscribe_message("unified.asset_detail", vec!["!all"]);

        assert_eq!(msg["channel"], "unified.asset_detail");
        assert_eq!(msg["event"], "subscribe");
        assert!(msg["payload"].is_array());
        assert_eq!(msg["payload"][0], "!all");
        assert_eq!(msg["auth"]["method"], "api_key");
        assert_eq!(msg["auth"]["KEY"], "test_api_key");
        assert!(msg["auth"]["SIGN"].is_string());
        assert!(msg["time"].is_number());
    }
}
