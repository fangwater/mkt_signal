use anyhow::{Context, Result};
use log::info;
use redis::aio::ConnectionManager;
use runtime_common::redis_client::RedisSettings;
use serde::{Deserialize, Serialize};

use super::cfg::RedisKeyType;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RedisSyncValue {
    Missing,
    Hash(Vec<(Vec<u8>, Vec<u8>)>),
    String(Vec<u8>),
}

pub struct RedisSyncClient {
    manager: ConnectionManager,
    prefix: Option<String>,
}

impl RedisSyncClient {
    pub async fn connect(settings: &RedisSettings) -> Result<Self> {
        let client = redis::Client::open(settings.connection_url()).with_context(|| {
            format!(
                "build Redis client failed: host={} port={} db={}",
                settings.host, settings.port, settings.db
            )
        })?;
        let manager = ConnectionManager::new(client).await.with_context(|| {
            format!(
                "connect Redis failed: host={} port={} db={}",
                settings.host, settings.port, settings.db
            )
        })?;
        info!(
            "bridge Redis connected: host={} port={} db={} prefix={:?}",
            settings.host, settings.port, settings.db, settings.prefix
        );
        Ok(Self {
            manager,
            prefix: settings.prefix.clone(),
        })
    }

    pub async fn read(&mut self, key: &str, key_type: RedisKeyType) -> Result<RedisSyncValue> {
        let key = self.key(key);
        match key_type {
            RedisKeyType::Hash => {
                let entries: Vec<(Vec<u8>, Vec<u8>)> = redis::cmd("HGETALL")
                    .arg(&key)
                    .query_async(&mut self.manager)
                    .await
                    .with_context(|| format!("HGETALL failed: key={key}"))?;
                if entries.is_empty() {
                    Ok(RedisSyncValue::Missing)
                } else {
                    Ok(RedisSyncValue::Hash(entries))
                }
            }
            RedisKeyType::String => {
                let value: Option<Vec<u8>> = redis::cmd("GET")
                    .arg(&key)
                    .query_async(&mut self.manager)
                    .await
                    .with_context(|| format!("GET failed: key={key}"))?;
                Ok(value
                    .map(RedisSyncValue::String)
                    .unwrap_or(RedisSyncValue::Missing))
            }
        }
    }

    pub async fn apply(
        &mut self,
        key: &str,
        expected_type: RedisKeyType,
        value: RedisSyncValue,
    ) -> Result<()> {
        let key = self.key(key);
        match value {
            RedisSyncValue::Missing => {
                redis::cmd("DEL")
                    .arg(&key)
                    .query_async::<()>(&mut self.manager)
                    .await
                    .with_context(|| format!("DEL failed: key={key}"))?;
            }
            RedisSyncValue::Hash(entries) => {
                anyhow::ensure!(
                    expected_type == RedisKeyType::Hash,
                    "Redis sync type mismatch: target={expected_type:?} payload=hash"
                );
                let mut pipe = redis::pipe();
                pipe.atomic().cmd("DEL").arg(&key).ignore();
                if !entries.is_empty() {
                    pipe.cmd("HSET").arg(&key);
                    for (field, value) in entries {
                        pipe.arg(field).arg(value);
                    }
                    pipe.ignore();
                }
                pipe.query_async::<()>(&mut self.manager)
                    .await
                    .with_context(|| format!("replace Redis hash failed: key={key}"))?;
            }
            RedisSyncValue::String(value) => {
                anyhow::ensure!(
                    expected_type == RedisKeyType::String,
                    "Redis sync type mismatch: target={expected_type:?} payload=string"
                );
                redis::cmd("SET")
                    .arg(&key)
                    .arg(value)
                    .query_async::<()>(&mut self.manager)
                    .await
                    .with_context(|| format!("SET failed: key={key}"))?;
            }
        }
        Ok(())
    }

    fn key(&self, key: &str) -> String {
        match &self.prefix {
            Some(prefix) if !prefix.is_empty() => format!("{prefix}{key}"),
            _ => key.to_string(),
        }
    }
}

pub fn encode(value: &RedisSyncValue) -> Result<Vec<u8>> {
    bincode::serialize(value).context("encode Redis sync payload failed")
}

pub fn decode(payload: &[u8]) -> Result<RedisSyncValue> {
    bincode::deserialize(payload).context("decode Redis sync payload failed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_hash_payload() {
        let value = RedisSyncValue::Hash(vec![
            (b"BTCUSDT".to_vec(), br#"{"ready":true}"#.to_vec()),
            (b"ETHUSDT".to_vec(), br#"{"ready":false}"#.to_vec()),
        ]);
        assert_eq!(decode(&encode(&value).unwrap()).unwrap(), value);
    }

    #[test]
    fn round_trips_missing_payload() {
        let value = RedisSyncValue::Missing;
        assert_eq!(decode(&encode(&value).unwrap()).unwrap(), value);
    }
}
