//! 交易因子发布模块

use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use redis::Commands;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::common::mkt_msg::RlReturnVolatilityMsg;
use crate::common::redis_client::RedisSettings;

const FACTOR_MAX_BYTES: usize = 256;
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

#[derive(Default)]
struct FactorStats {
    published: u64,
    dropped: u64,
}

pub struct TradeFactorPublisher {
    venue_slug: String,
    publishers: HashMap<String, Publisher<ipc::Service, [u8; FACTOR_MAX_BYTES], ()>>,
    redis_writer: Option<RedisKvWriter>,
    stats: HashMap<String, FactorStats>,
}

impl TradeFactorPublisher {
    pub fn new(venue_slug: &str, factor_names: &[String]) -> Result<Self> {
        let mut publishers = HashMap::new();
        let mut stats = HashMap::new();

        for factor_name in factor_names {
            let node_name = format!(
                "trade_factor_pub_{}_{}",
                venue_slug.replace('-', "_"),
                factor_name.replace('-', "_")
            );
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;

            let service_path = format!("factor_pub/{}/{}", venue_slug, factor_name);
            let service = node
                .service_builder(&ServiceName::new(&service_path)?)
                .publish_subscribe::<[u8; FACTOR_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(10)
                .history_size(128)
                .open_or_create()?;

            let publisher = service.publisher_builder().create()?;

            info!(
                "TradeFactorPublisher created for {}: {}",
                factor_name, service_path
            );

            publishers.insert(factor_name.clone(), publisher);
            stats.insert(factor_name.clone(), FactorStats::default());
        }

        let redis_writer = match RedisKvWriter::new(RedisSettings::default()) {
            Ok(writer) => Some(writer),
            Err(err) => {
                warn!("Redis writer init failed: {}", err);
                None
            }
        };

        Ok(Self {
            venue_slug: venue_slug.to_string(),
            publishers,
            redis_writer,
            stats,
        })
    }

    pub fn publish_factor(
        &mut self,
        factor_name: &str,
        symbol: &str,
        value: f64,
        timestamp_ms: i64,
        ready: bool,
    ) -> bool {
        let msg = RlReturnVolatilityMsg::create(symbol.to_string(), value, timestamp_ms, ready);
        let bytes: Bytes = msg.to_bytes();

        if let Some(writer) = self.redis_writer.as_mut() {
            let key = format!("{}_{}_{}", factor_name, self.venue_slug, symbol);
            writer.write(&key, bytes.as_ref());
        }

        let Some(publisher) = self.publishers.get_mut(factor_name) else {
            warn!("Missing publisher for factor_name={}", factor_name);
            self.bump_dropped(factor_name);
            return false;
        };

        if bytes.len() > FACTOR_MAX_BYTES {
            warn!(
                "Factor payload size {} exceeds max {} for factor={} symbol={}",
                bytes.len(),
                FACTOR_MAX_BYTES,
                factor_name,
                symbol
            );
            self.bump_dropped(factor_name);
            return false;
        }

        let mut buffer = [0u8; FACTOR_MAX_BYTES];
        buffer[..bytes.len()].copy_from_slice(&bytes);

        match publisher.loan_uninit() {
            Ok(sample) => {
                let sample = sample.write_payload(buffer);
                if sample.send().is_ok() {
                    self.bump_published(factor_name);
                    return true;
                }
            }
            Err(_) => {}
        }

        self.bump_dropped(factor_name);
        false
    }

    pub fn log_stats(&mut self) {
        for (factor_name, st) in self.stats.iter_mut() {
            info!(
                "TradeFactorPublisher[{}:{}] stats: published={}, dropped={}",
                self.venue_slug, factor_name, st.published, st.dropped
            );
            st.published = 0;
            st.dropped = 0;
        }
    }

    fn bump_published(&mut self, factor_name: &str) {
        if let Some(st) = self.stats.get_mut(factor_name) {
            st.published += 1;
        }
    }

    fn bump_dropped(&mut self, factor_name: &str) {
        if let Some(st) = self.stats.get_mut(factor_name) {
            st.dropped += 1;
        }
    }
}
