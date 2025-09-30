use anyhow::Result;
use bytes::Bytes;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use log::debug;

use crate::pre_trade::order_manager::Order;

const KEY_ORDERS: &str = "orders";
const KEY_STRATEGIES: &str = "strategies";
const KEY_SAVED_AT: &str = "saved_at";

#[derive(Clone)]
pub struct RedisStore {
    conn: ConnectionManager,
    prefix: String,
}

impl RedisStore {
    pub async fn connect(url: &str, prefix: &str) -> Result<Self> {
        let client = redis::Client::open(url)?;
        let conn = client.get_tokio_connection_manager().await?;
        Ok(Self { conn, prefix: prefix.to_string() })
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}:{}", self.prefix, suffix)
    }

    pub async fn save_snapshot(
        &mut self,
        orders: &[Order],
        strategies: &[StrategyRecord],
    ) -> Result<()> {
        let orders_key = self.key(KEY_ORDERS);
        let strategies_key = self.key(KEY_STRATEGIES);
        let ts_key = self.key(KEY_SAVED_AT);
        let orders_blob = bincode::serialize(orders)?;
        let strategies_blob = bincode::serialize(strategies)?;
        let ts = chrono::Utc::now().timestamp();

        debug!(
            "redis save_snapshot: prefix={} orders={} strategies={}",
            self.prefix,
            orders.len(),
            strategies.len()
        );
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("SET").arg(orders_key).arg(orders_blob).ignore()
            .cmd("SET").arg(strategies_key).arg(strategies_blob).ignore()
            .cmd("SET").arg(ts_key).arg(ts).ignore();
        pipe.query_async(&mut self.conn).await?;
        debug!("redis save_snapshot: done prefix={} ts={} ", self.prefix, ts);
        Ok(())
    }

    pub async fn load_orders(&mut self) -> Result<Vec<Order>> {
        let key = self.key(KEY_ORDERS);
        let bytes: Option<Vec<u8>> = self.conn.get(key).await?;
        match bytes {
            Some(b) => Ok(bincode::deserialize(&b)?),
            None => Ok(Vec::new()),
        }
    }

    pub async fn load_strategies(&mut self) -> Result<Vec<StrategyRecord>> {
        let key = self.key(KEY_STRATEGIES);
        let bytes: Option<Vec<u8>> = self.conn.get(key).await?;
        match bytes {
            Some(b) => Ok(bincode::deserialize(&b)?),
            None => Ok(Vec::new()),
        }
    }
}

/// 通用策略快照：不同策略类型使用不同的 `type_name`，`payload` 由各策略自定义序列化
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyRecord {
    pub id: i32,
    pub type_name: String,
    #[serde(with = "bytes_ser")] // 序列化为紧凑的 bytes
    pub payload: Bytes,
}

mod bytes_ser {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BytesVisitor;
        impl<'de> serde::de::Visitor<'de> for BytesVisitor {
            type Value = Bytes;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a byte array")
            }
            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Bytes::copy_from_slice(v))
            }
            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Bytes::from(v))
            }
        }
        deserializer.deserialize_bytes(BytesVisitor)
    }
}
