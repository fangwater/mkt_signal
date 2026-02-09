//! Bitget UTA WebSocket 私有频道鉴权模块
//!
//! UTA 私有频道通过 WS 内发送 login 消息鉴权。
//!
//! 签名算法（与官方示例一致）：
//! sign = Base64(HMAC-SHA256(timestamp + "GET" + "/user/verify", secretKey))

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Bitget API 凭证
#[derive(Clone)]
pub struct BitgetCredentials {
    pub api_key: String,
    pub secret_key: String,
    pub passphrase: String,
}

impl BitgetCredentials {
    pub fn new(api_key: String, secret_key: String, passphrase: String) -> Self {
        Self {
            api_key,
            secret_key,
            passphrase,
        }
    }

    /// 从环境变量加载凭证
    pub fn from_env() -> anyhow::Result<Self> {
        let api_key = std::env::var("BITGET_API_KEY")
            .map_err(|_| anyhow::anyhow!("BITGET_API_KEY not set"))?
            .trim()
            .to_string();
        let secret_key = std::env::var("BITGET_API_SECRET")
            .map_err(|_| anyhow::anyhow!("BITGET_API_SECRET not set"))?
            .trim()
            .to_string();
        let passphrase = std::env::var("BITGET_API_PASSPHRASE")
            .map_err(|_| anyhow::anyhow!("BITGET_API_PASSPHRASE not set"))?
            .trim()
            .to_string();

        Ok(Self::new(api_key, secret_key, passphrase))
    }

    /// 生成登录签名
    pub fn sign(&self, timestamp: &str) -> String {
        let message = format!("{}GET/user/verify", timestamp);
        let mut mac =
            HmacSha256::new_from_slice(self.secret_key.as_bytes()).expect("HMAC can take any size");
        mac.update(message.as_bytes());
        let result = mac.finalize();
        BASE64.encode(result.into_bytes())
    }

    /// 生成登录消息 JSON
    pub fn build_login_message(&self) -> serde_json::Value {
        // Bitget WS 登录常用秒级时间戳
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

/// Bitget 私有频道 WebSocket URL
pub struct BitgetPrivateWsUrls;

impl BitgetPrivateWsUrls {
    /// UTA 私有频道（v3）
    pub const PRIVATE: &'static str = "wss://ws.bitget.com/v3/ws/private";
}

/// 构建 UTA account 频道订阅消息
pub fn build_account_subscribe_message() -> serde_json::Value {
    serde_json::json!({
        "op": "subscribe",
        "args": [{
            "instType": "UTA",
            "topic": "account"
        }]
    })
}

/// 构建 UTA position 频道订阅消息
pub fn build_positions_subscribe_message() -> serde_json::Value {
    serde_json::json!({
        "op": "subscribe",
        "args": [{
            "instType": "UTA",
            "topic": "position"
        }]
    })
}

/// 构建 UTA order 频道订阅消息（覆盖统一账户订单）
pub fn build_orders_subscribe_message() -> Vec<serde_json::Value> {
    vec![serde_json::json!({
        "op": "subscribe",
        "args": [{
            "instType": "UTA",
            "topic": "order"
        }]
    })]
}
