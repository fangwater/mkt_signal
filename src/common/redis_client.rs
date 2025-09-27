use std::fmt;

use anyhow::{Context, Result};
use log::info;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

/// 通用的 Redis 连接配置
#[derive(Debug, Clone, Deserialize)]
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
    /// 拼装 redis:// 连接串
    pub fn connection_url(&self) -> String {
        let auth = match (&self.username, &self.password) {
            (Some(user), Some(pass)) => format!("{}:{}@", encode(user), encode(pass)),
            (Some(user), None) => format!("{}:@", encode(user)),
            (None, Some(pass)) => format!(":{}@", encode(pass)),
            (None, None) => String::new(),
        };
        format!("redis://{}{}:{}/{}", auth, self.host, self.port, self.db)
    }

    fn prefixed_key<'a>(&self, key: &'a str) -> String {
        match &self.prefix {
            Some(prefix) if !prefix.is_empty() => format!("{}{}", prefix, key),
            _ => key.to_string(),
        }
    }
}

fn encode(raw: &str) -> String {
    urlencoding::encode(raw).to_string()
}

/// Redis 连接包装，提供 JSON 读写的便捷方法
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
    /// 建立新的连接管理器
    pub async fn connect(settings: RedisSettings) -> Result<Self> {
        let url = settings.connection_url();
        let client = redis::Client::open(url.clone())?;
        let manager = ConnectionManager::new(client)
            .await
            .with_context(|| format!("连接 Redis 失败: {}", url))?;

        info!(
            "Redis 已连接 host={} port={} db={} prefix={:?}",
            settings.host, settings.port, settings.db, settings.prefix
        );

        Ok(Self { settings, manager })
    }

    pub fn settings(&self) -> &RedisSettings {
        &self.settings
    }

    fn key(&self, key: &str) -> String {
        self.settings.prefixed_key(key)
    }

    /// 获取字符串值
    pub async fn get_string(&mut self, key: &str) -> Result<Option<String>> {
        let full_key = self.key(key);
        let value: Option<String> = self.manager.get(full_key).await?;
        Ok(value)
    }

    /// 写入字符串值
    pub async fn set_string(&mut self, key: &str, value: &str) -> Result<()> {
        let full_key = self.key(key);
        self.manager.set::<_, _, ()>(full_key, value).await?;
        Ok(())
    }

    /// 获取 JSON，并反序列化
    pub async fn get_json<T>(&mut self, key: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        match self.get_string(key).await? {
            Some(text) => {
                let parsed = serde_json::from_str(&text)
                    .with_context(|| format!("解析 Redis JSON 失败: key={}", key))?;
                Ok(Some(parsed))
            }
            None => Ok(None),
        }
    }

    /// 将结构体序列化为 JSON 并写入
    pub async fn set_json<T>(&mut self, key: &str, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        let text = serde_json::to_string(value)
            .with_context(|| format!("序列化 Redis JSON 失败: key={}", key))?;
        self.set_string(key, &text).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_connection_url() {
        let mut settings = RedisSettings::default();
        assert_eq!(settings.connection_url(), "redis://127.0.0.1:6379/0");
        settings.username = Some("user".into());
        settings.password = Some("pass".into());
        assert_eq!(
            settings.connection_url(),
            "redis://user:pass@127.0.0.1:6379/0"
        );
    }
}
