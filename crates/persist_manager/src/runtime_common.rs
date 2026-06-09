use std::collections::HashMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use log::info;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

pub const SIGNAL_PAYLOAD: usize = 4_096;
pub const SPREAD_PAYLOAD_BYTES: usize = 128;
pub const ASK_BID_SPREAD_MSG_TYPE: u32 = 1015;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct RedisSettings {
    #[serde(default = "default_redis_host")]
    pub host: String,
    #[serde(default = "default_redis_port")]
    pub port: u16,
    #[serde(default)]
    pub db: i64,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub prefix: Option<String>,
}

impl Default for RedisSettings {
    fn default() -> Self {
        Self {
            host: default_redis_host(),
            port: default_redis_port(),
            db: 0,
            username: None,
            password: None,
            prefix: None,
        }
    }
}

fn default_redis_host() -> String {
    "127.0.0.1".to_string()
}

const fn default_redis_port() -> u16 {
    6379
}

impl RedisSettings {
    pub fn connection_url(&self) -> String {
        let auth = match (&self.username, &self.password) {
            (Some(user), Some(pass)) => format!("{}:{}@", encode(user), encode(pass)),
            (Some(user), None) => format!("{}:@", encode(user)),
            (None, Some(pass)) => format!(":{}@", encode(pass)),
            (None, None) => String::new(),
        };
        format!("redis://{}{}:{}/{}", auth, self.host, self.port, self.db)
    }

    fn prefixed_key(&self, key: &str) -> String {
        match &self.prefix {
            Some(prefix) if !prefix.is_empty() => format!("{}{}", prefix, key),
            _ => key.to_string(),
        }
    }
}

fn encode(raw: &str) -> String {
    urlencoding::encode(raw).to_string()
}

pub struct RedisClient {
    settings: RedisSettings,
    manager: ConnectionManager,
}

impl fmt::Debug for RedisClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisClient")
            .field("host", &self.settings.host)
            .field("port", &self.settings.port)
            .field("db", &self.settings.db)
            .finish()
    }
}

impl RedisClient {
    pub async fn connect(settings: RedisSettings) -> Result<Self> {
        let url = settings.connection_url();
        let client = redis::Client::open(url.clone())?;
        let manager = ConnectionManager::new(client)
            .await
            .with_context(|| format!("connect redis failed: {}", url))?;

        info!(
            "redis connected host={} port={} db={} prefix={:?}",
            settings.host, settings.port, settings.db, settings.prefix
        );

        Ok(Self { settings, manager })
    }

    fn key(&self, key: &str) -> String {
        self.settings.prefixed_key(key)
    }

    pub async fn get_string(&mut self, key: &str) -> Result<Option<String>> {
        let full_key = self.key(key);
        let value: Option<String> = self.manager.get(full_key).await?;
        Ok(value)
    }

    pub async fn get_json<T>(&mut self, key: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        match self.get_string(key).await? {
            Some(text) => serde_json::from_str(&text)
                .with_context(|| format!("parse redis json failed: key={}", key))
                .map(Some),
            None => Ok(None),
        }
    }

    #[allow(dead_code)]
    pub async fn set_json<T>(&mut self, key: &str, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        let text = serde_json::to_string(value)
            .with_context(|| format!("serialize redis json failed: key={}", key))?;
        let full_key = self.key(key);
        self.manager.set::<_, _, ()>(full_key, text).await?;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn hgetall_map(&mut self, key: &str) -> Result<HashMap<String, String>> {
        let full_key = self.key(key);
        let map: HashMap<String, String> = self.manager.hgetall(full_key).await?;
        Ok(map)
    }
}

pub fn build_service_name(base_name: &str) -> String {
    if base_name.starts_with("dat_pbs/")
        || base_name.starts_with("spread_pbs/")
        || base_name.starts_with("bridge/")
        || base_name.starts_with("factor_pub/")
    {
        return base_name.to_string();
    }

    let namespace = std::env::var("IPC_NAMESPACE")
        .expect("IPC_NAMESPACE must be set to isolate iceoryx services");
    format!("{}/{}", namespace, base_name)
}

pub fn get_timestamp_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or_default()
}

pub fn normalize_symbol_for_whitelist(symbol: &str, venue: order_common::TradingVenue) -> String {
    let mut cleaned = symbol.to_uppercase().replace(['-', '_'], "");
    if matches!(
        venue,
        order_common::TradingVenue::OkexMargin | order_common::TradingVenue::OkexFutures
    ) && cleaned.ends_with("SWAP")
    {
        cleaned.truncate(cleaned.len().saturating_sub(4));
    }
    cleaned
}

pub fn spread_symbol(payload: &[u8]) -> Option<&str> {
    let symbol_len = u32::from_le_bytes(payload.get(4..8)?.try_into().ok()?) as usize;
    std::str::from_utf8(payload.get(8..8 + symbol_len)?).ok()
}

pub fn spread_timestamp(payload: &[u8]) -> Option<i64> {
    let symbol_len = u32::from_le_bytes(payload.get(4..8)?.try_into().ok()?) as usize;
    let offset = 8usize.checked_add(symbol_len)?;
    Some(i64::from_le_bytes(
        payload.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

pub fn spread_f64(payload: &[u8], idx: usize) -> Option<f64> {
    let symbol_len = u32::from_le_bytes(payload.get(4..8)?.try_into().ok()?) as usize;
    let offset = 8usize
        .checked_add(symbol_len)?
        .checked_add(8)?
        .checked_add(idx.checked_mul(8)?)?;
    Some(f64::from_le_bytes(
        payload.get(offset..offset + 8)?.try_into().ok()?,
    ))
}
