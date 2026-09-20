use std::fmt;

use anyhow::{Context, Result};
use log::info;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Commands};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;

/// 通用的 Redis 连接配置
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

/// Redis 连接包装，提供 JSON 读写的便捷方法
pub struct RedisClient {
    settings: RedisSettings,
    manager: ConnectionManager,
}

pub struct BlockingRedisClient {
    settings: RedisSettings,
    connection: redis::Connection,
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

    /// Atomically write and delete a related set of namespaced keys.
    pub async fn atomic_write(
        &mut self,
        values: &[(String, String)],
        delete_keys: &[String],
    ) -> Result<()> {
        if values.is_empty() && delete_keys.is_empty() {
            return Ok(());
        }
        let mut pipe = redis::pipe();
        pipe.atomic();
        for (key, value) in values {
            pipe.set(self.key(key), value);
        }
        for key in delete_keys {
            pipe.del(self.key(key));
        }
        let _: () = pipe.query_async(&mut self.manager).await?;
        Ok(())
    }

    /// Move one member between JSON-array indexes and write its state atomically.
    pub async fn atomic_move_json_index_member(
        &mut self,
        source_key: &str,
        destination_key: &str,
        member: &str,
        state_key: &str,
        state_value: &str,
    ) -> Result<()> {
        const SCRIPT: &str = r#"
local source = cjson.decode(redis.call('GET', KEYS[1]) or '[]')
local destination = cjson.decode(redis.call('GET', KEYS[2]) or '[]')
local next_source = {}
for _, value in ipairs(source) do
    if value ~= ARGV[1] then table.insert(next_source, value) end
end
local found = false
for _, value in ipairs(destination) do
    if value == ARGV[1] then found = true end
end
if not found then table.insert(destination, ARGV[1]) end
table.sort(next_source)
table.sort(destination)
local source_json = #next_source == 0 and '[]' or cjson.encode(next_source)
local destination_json = #destination == 0 and '[]' or cjson.encode(destination)
redis.call('SET', KEYS[1], source_json)
redis.call('SET', KEYS[2], destination_json)
redis.call('SET', KEYS[3], ARGV[2])
return 1
"#;
        let _: i32 = redis::Script::new(SCRIPT)
            .key(self.key(source_key))
            .key(self.key(destination_key))
            .key(self.key(state_key))
            .arg(member)
            .arg(state_value)
            .invoke_async(&mut self.manager)
            .await?;
        Ok(())
    }

    /// 获取哈希表的所有字段（值为字符串）
    pub async fn hgetall_map(&mut self, key: &str) -> Result<HashMap<String, String>> {
        let full_key = self.key(key);
        let map: HashMap<String, String> = self.manager.hgetall(full_key).await?;
        Ok(map)
    }

    /// 批量写入哈希多个字段（值为字符串）。
    pub async fn hset_multiple_str(
        &mut self,
        key: &str,
        entries: &[(String, String)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let full_key = self.key(key);
        // Inference of (F, V) = (String, String); ignore return value
        let _: () = self.manager.hset_multiple(full_key, entries).await?;
        Ok(())
    }

    /// 从哈希中删除多个字段。
    pub async fn hdel_fields(&mut self, key: &str, fields: &[String]) -> Result<()> {
        if fields.is_empty() {
            return Ok(());
        }
        let full_key = self.key(key);
        // redis::AsyncCommands 的 hdel 可接受切片；忽略返回值
        let _: () = self.manager.hdel(full_key, fields).await?;
        Ok(())
    }
}

impl fmt::Debug for BlockingRedisClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockingRedisClient")
            .field("host", &self.settings.host)
            .field("port", &self.settings.port)
            .field("db", &self.settings.db)
            .finish()
    }
}

impl BlockingRedisClient {
    pub fn connect(settings: RedisSettings) -> Result<Self> {
        let url = settings.connection_url();
        let client = redis::Client::open(url.clone())?;
        let connection = client
            .get_connection()
            .with_context(|| format!("连接 Redis 失败: {}", url))?;

        info!(
            "Redis blocking connected host={} port={} db={} prefix={:?}",
            settings.host, settings.port, settings.db, settings.prefix
        );

        Ok(Self {
            settings,
            connection,
        })
    }

    fn key(&self, key: &str) -> String {
        self.settings.prefixed_key(key)
    }

    pub fn get_string(&mut self, key: &str) -> Result<Option<String>> {
        let full_key = self.key(key);
        let value: Option<String> = self.connection.get(full_key)?;
        Ok(value)
    }

    pub fn hgetall_map(&mut self, key: &str) -> Result<HashMap<String, String>> {
        let full_key = self.key(key);
        let map: HashMap<String, String> = self.connection.hgetall(full_key)?;
        Ok(map)
    }

    pub fn set_string(&mut self, key: &str, value: &str) -> Result<()> {
        let full_key = self.key(key);
        self.connection.set::<_, _, ()>(full_key, value)?;
        Ok(())
    }

    pub fn set_json<T>(&mut self, key: &str, value: &T) -> Result<()>
    where
        T: serde::Serialize + ?Sized,
    {
        let text = serde_json::to_string(value)?;
        self.set_string(key, &text)
    }
}
