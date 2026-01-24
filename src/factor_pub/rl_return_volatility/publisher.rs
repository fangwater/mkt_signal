//! RL Return Volatility 发布模块

use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use redis::Commands;
use std::time::{Duration, Instant};

use crate::common::redis_client::RedisSettings;
use crate::common::mkt_msg::RlReturnVolatilityMsg;

const FACTOR_MAX_BYTES: usize = 256;
const FACTOR_NAME: &str = "rl_return_volatility";
const REDIS_WARN_INTERVAL_SECS: u64 = 60;

struct RedisKvWriter {
    settings: RedisSettings,
    client: redis::Client,
    conn: Option<redis::Connection>,
    last_warn: Instant,
}

impl RedisKvWriter {
    fn new(settings: RedisSettings) -> Result<Self> {
        let url = settings.connection_url();
        let client = redis::Client::open(url)?;
        Ok(Self {
            settings,
            client,
            conn: None,
            last_warn: Instant::now() - Duration::from_secs(REDIS_WARN_INTERVAL_SECS),
        })
    }

    fn write(&mut self, key: &str, payload: &[u8]) {
        if self.conn.is_none() {
            if let Err(err) = self.connect() {
                self.warn_throttled("connect", &err);
                return;
            }
        }

        let full_key = self.prefixed_key(key);
        let Some(conn) = self.conn.as_mut() else {
            return;
        };

        if let Err(err) = conn.set::<_, _, ()>(full_key, payload) {
            self.warn_throttled("set", &err);
            self.conn = None;
        }
    }

    fn connect(&mut self) -> redis::RedisResult<()> {
        let conn = self.client.get_connection()?;
        self.conn = Some(conn);
        Ok(())
    }

    fn prefixed_key(&self, key: &str) -> String {
        match &self.settings.prefix {
            Some(prefix) if !prefix.is_empty() => format!("{}{}", prefix, key),
            _ => key.to_string(),
        }
    }

    fn warn_throttled(&mut self, action: &str, err: &redis::RedisError) {
        if self.last_warn.elapsed() >= Duration::from_secs(REDIS_WARN_INTERVAL_SECS) {
            warn!("Redis {} failed: {}", action, err);
            self.last_warn = Instant::now();
        }
    }
}

pub struct FactorPublisher {
    venue_slug: String,
    publisher: Publisher<ipc::Service, [u8; FACTOR_MAX_BYTES], ()>,
    redis_writer: Option<RedisKvWriter>,
    published: u64,
    dropped: u64,
}

impl FactorPublisher {
    pub fn new(venue_slug: &str) -> Result<Self> {
        let node_name = format!(
            "factor_pub_{}_rl_return_vol",
            venue_slug.replace('-', "_")
        );
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service_path = format!("factor_pub/{}/rl_return_volatility", venue_slug);
        let service = node
            .service_builder(&ServiceName::new(&service_path)?)
            .publish_subscribe::<[u8; FACTOR_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .history_size(128)
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;

        info!(
            "FactorPublisher created for {}: {}",
            venue_slug, service_path
        );

        let redis_writer = match RedisKvWriter::new(RedisSettings::default()) {
            Ok(writer) => Some(writer),
            Err(err) => {
                warn!("Redis writer init failed: {}", err);
                None
            }
        };

        Ok(Self {
            venue_slug: venue_slug.to_string(),
            publisher,
            redis_writer,
            published: 0,
            dropped: 0,
        })
    }

    pub fn publish(&mut self, msg: &RlReturnVolatilityMsg) -> bool {
        let bytes: Bytes = msg.to_bytes();
        if let Some(writer) = self.redis_writer.as_mut() {
            let key = format!("{}_{}_{}", FACTOR_NAME, self.venue_slug, msg.symbol);
            writer.write(&key, bytes.as_ref());
        }

        if bytes.len() > FACTOR_MAX_BYTES {
            warn!(
                "Factor payload size {} exceeds max {}",
                bytes.len(),
                FACTOR_MAX_BYTES
            );
            self.dropped += 1;
            return false;
        }

        let mut buffer = [0u8; FACTOR_MAX_BYTES];
        buffer[..bytes.len()].copy_from_slice(&bytes);

        match self.publisher.loan_uninit() {
            Ok(sample) => {
                let sample = sample.write_payload(buffer);
                if sample.send().is_ok() {
                    self.published += 1;
                    return true;
                }
            }
            Err(_) => {}
        }

        self.dropped += 1;
        false
    }

    pub fn log_stats(&mut self) {
        info!(
            "FactorPublisher[{}] stats: published={}, dropped={}",
            self.venue_slug, self.published, self.dropped
        );
        self.published = 0;
        self.dropped = 0;
    }
}
