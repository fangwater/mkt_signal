//! OKX WebSocket 私有频道鉴权模块
//!
//! OKX 私有频道需要通过 WebSocket 内发送 login 消息进行鉴权，
//! 与 Binance 的 listenKey 机制不同。
//!
//! 签名算法: Base64(HMAC-SHA256(timestamp + "GET" + "/users/self/verify", secretKey))

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// OKX API 凭证
#[derive(Clone)]
pub struct OkexCredentials {
    pub api_key: String,
    pub secret_key: String,
    pub passphrase: String,
}

impl OkexCredentials {
    pub fn new(api_key: String, secret_key: String, passphrase: String) -> Self {
        Self {
            api_key,
            secret_key,
            passphrase,
        }
    }

    /// 从环境变量加载凭证
    pub fn from_env() -> anyhow::Result<Self> {
        let api_key = std::env::var("OKX_API_KEY")
            .map_err(|_| anyhow::anyhow!("OKX_API_KEY not set"))?
            .trim()
            .to_string();
        let secret_key = std::env::var("OKX_API_SECRET")
            .map_err(|_| anyhow::anyhow!("OKX_API_SECRET not set"))?
            .trim()
            .to_string();
        let passphrase = std::env::var("OKX_PASSPHRASE")
            .map_err(|_| anyhow::anyhow!("OKX_PASSPHRASE not set"))?
            .trim()
            .to_string();

        Ok(Self::new(api_key, secret_key, passphrase))
    }

    /// 生成登录签名
    /// sign = Base64(HMAC-SHA256(timestamp + "GET" + "/users/self/verify", secretKey))
    pub fn sign(&self, timestamp: &str) -> String {
        let message = format!("{}GET/users/self/verify", timestamp);

        let mut mac =
            HmacSha256::new_from_slice(self.secret_key.as_bytes()).expect("HMAC can take any size");
        mac.update(message.as_bytes());
        let result = mac.finalize();

        BASE64.encode(result.into_bytes())
    }

    /// 生成登录消息 JSON
    pub fn build_login_message(&self) -> serde_json::Value {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string();

        let sign = self.sign(&timestamp);

        serde_json::json!({
            "op": "login",
            "args": [{
                "apiKey": self.api_key,
                "passphrase": self.passphrase,
                "timestamp": timestamp,
                "sign": sign
            }]
        })
    }
}

/// OKX WebSocket 私有频道 URL
pub struct OkexPrivateWsUrls;

impl OkexPrivateWsUrls {
    /// 私有频道 WebSocket URL (实盘)
    pub const PRIVATE: &'static str = "wss://ws.okx.com:8443/ws/v5/private";

    /// 私有频道 WebSocket URL (模拟盘)
    pub const PRIVATE_DEMO: &'static str = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999";

    /// 业务频道 WebSocket URL (实盘) - 用于订单相关
    pub const BUSINESS: &'static str = "wss://ws.okx.com:8443/ws/v5/business";

    /// 业务频道 WebSocket URL (模拟盘)
    pub const BUSINESS_DEMO: &'static str = "wss://wspap.okx.com:8443/ws/v5/business?brokerId=9999";
}

/// 构建订单频道订阅消息
pub fn build_orders_subscribe_message(inst_type: &str) -> serde_json::Value {
    serde_json::json!({
        "op": "subscribe",
        "args": [{
            "channel": "orders",
            "instType": inst_type
        }]
    })
}

/// 构建账户余额和持仓频道订阅消息
pub fn build_balance_and_position_subscribe_message() -> serde_json::Value {
    serde_json::json!({
        "op": "subscribe",
        "args": [{
            "channel": "balance_and_position"
        }]
    })
}

/// 构建持仓频道订阅消息
pub fn build_positions_subscribe_message(inst_type: &str) -> serde_json::Value {
    serde_json::json!({
        "op": "subscribe",
        "args": [{
            "channel": "positions",
            "instType": inst_type
        }]
    })
}

/// 构建账户频道订阅消息
pub fn build_account_subscribe_message() -> serde_json::Value {
    serde_json::json!({
        "op": "subscribe",
        "args": [{
            "channel": "account"
        }]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_generation() {
        let creds = OkexCredentials::new(
            "test_api_key".to_string(),
            "test_secret".to_string(),
            "test_passphrase".to_string(),
        );

        let timestamp = "1704876947";
        let sign = creds.sign(timestamp);

        // 签名应该是 base64 编码的
        assert!(!sign.is_empty());
        // Base64 解码应该成功
        assert!(BASE64.decode(&sign).is_ok());
    }

    #[test]
    fn test_login_message_format() {
        let creds = OkexCredentials::new(
            "test_api_key".to_string(),
            "test_secret".to_string(),
            "test_passphrase".to_string(),
        );

        let msg = creds.build_login_message();

        assert_eq!(msg["op"], "login");
        assert!(msg["args"].is_array());
        assert_eq!(msg["args"][0]["apiKey"], "test_api_key");
        assert_eq!(msg["args"][0]["passphrase"], "test_passphrase");
        assert!(msg["args"][0]["timestamp"].is_string());
        assert!(msg["args"][0]["sign"].is_string());
    }

    #[test]
    fn test_orders_subscribe_message() {
        let msg = build_orders_subscribe_message("SWAP");

        assert_eq!(msg["op"], "subscribe");
        assert_eq!(msg["args"][0]["channel"], "orders");
        assert_eq!(msg["args"][0]["instType"], "SWAP");
    }
}
