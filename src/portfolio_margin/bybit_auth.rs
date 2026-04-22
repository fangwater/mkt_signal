//! Bybit V5 private WebSocket 鉴权与订阅辅助。
//!
//! 官方鉴权格式：
//! - op = "auth"
//! - args = [api_key, expires, sign]
//! - sign = hex(HMAC_SHA256("GET/realtime" + expires, secret))

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct BybitCredentials {
    pub api_key: String,
    pub secret_key: String,
}

impl BybitCredentials {
    pub fn new(api_key: String, secret_key: String) -> Self {
        Self {
            api_key,
            secret_key,
        }
    }

    pub fn from_env() -> anyhow::Result<Self> {
        let api_key = std::env::var("BYBIT_API_KEY")
            .map_err(|_| anyhow::anyhow!("BYBIT_API_KEY not set"))?
            .trim()
            .to_string();
        let secret_key = std::env::var("BYBIT_API_SECRET")
            .map_err(|_| anyhow::anyhow!("BYBIT_API_SECRET not set"))?
            .trim()
            .to_string();
        Ok(Self::new(api_key, secret_key))
    }

    pub fn sign(&self, expires: i64) -> String {
        let payload = format!("GET/realtime{}", expires);
        let mut mac =
            HmacSha256::new_from_slice(self.secret_key.as_bytes()).expect("HMAC accepts any size");
        mac.update(payload.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    pub fn build_auth_message(&self) -> serde_json::Value {
        let expires = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 10_000;
        let sign = self.sign(expires);
        serde_json::json!({
            "op": "auth",
            "args": [self.api_key, expires, sign]
        })
    }
}

pub struct BybitPrivateWsUrls;

impl BybitPrivateWsUrls {
    pub const PRIVATE: &'static str = "wss://stream.bybit.com/v5/private";
    pub const PRIVATE_TESTNET: &'static str = "wss://stream-testnet.bybit.com/v5/private";
}

pub fn build_wallet_subscribe_message() -> serde_json::Value {
    serde_json::json!({
        "op": "subscribe",
        "args": ["wallet"]
    })
}

pub fn build_position_subscribe_message() -> serde_json::Value {
    serde_json::json!({
        "op": "subscribe",
        "args": ["position"]
    })
}

pub fn build_order_subscribe_message() -> serde_json::Value {
    serde_json::json!({
        "op": "subscribe",
        "args": ["order"]
    })
}

pub fn build_execution_subscribe_message() -> serde_json::Value {
    serde_json::json!({
        "op": "subscribe",
        "args": ["execution"]
    })
}

pub fn build_fast_execution_subscribe_message() -> serde_json::Value {
    serde_json::json!({
        "op": "subscribe",
        "args": ["execution.fast"]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_generation_is_hex() {
        let creds = BybitCredentials::new("key".to_string(), "secret".to_string());
        let sign = creds.sign(1_700_000_000_000);
        assert_eq!(sign.len(), 64);
        assert!(sign.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn auth_message_shape() {
        let creds = BybitCredentials::new("key".to_string(), "secret".to_string());
        let msg = creds.build_auth_message();
        assert_eq!(msg["op"], "auth");
        assert_eq!(msg["args"].as_array().map(|v| v.len()), Some(3));
    }

    #[test]
    fn fast_execution_subscribe_message_shape() {
        let msg = build_fast_execution_subscribe_message();
        assert_eq!(msg["op"], "subscribe");
        assert_eq!(msg["args"][0], "execution.fast");
    }
}
