use anyhow::Result;
use log::debug;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;

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
        let conn = client.get_connection_manager().await?;
        Ok(Self {
            conn,
            prefix: prefix.to_string(),
        })
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}:{}", self.prefix, suffix)
    }

    pub async fn save_snapshot(&mut self, orders: &[Order]) -> Result<()> {
        let orders_key = self.key(KEY_ORDERS);
        let strategies_key = self.key(KEY_STRATEGIES);
        let ts_key = self.key(KEY_SAVED_AT);
        let orders_blob = bincode::serialize(orders)?;
        let ts = chrono::Utc::now().timestamp();

        debug!(
            "redis save_snapshot: prefix={} orders={}",
            self.prefix,
            orders.len(),
        );
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("SET")
            .arg(orders_key)
            .arg(orders_blob)
            .ignore()
            .cmd("DEL")
            .arg(strategies_key)
            .ignore()
            .cmd("SET")
            .arg(ts_key)
            .arg(ts)
            .ignore();
        pipe.query_async::<_, ()>(&mut self.conn).await?;
        debug!(
            "redis save_snapshot: done prefix={} ts={} ",
            self.prefix, ts
        );
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
}
