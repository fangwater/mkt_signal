//! 因子发布模块

use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::collections::HashMap;

use crate::common::mkt_msg::FactorValueMsg;
use crate::factor_pub::factor_index::factor_name_to_index;

const FACTOR_MAX_BYTES: usize = 256;

#[derive(Default)]
struct FactorStats {
    published: u64,
    dropped: u64,
}

pub struct FactorPublisher {
    venue_slug: String,
    factor_indices: HashMap<String, u16>,
    publishers: HashMap<String, Publisher<ipc::Service, [u8; FACTOR_MAX_BYTES], ()>>,
    stats: HashMap<String, FactorStats>,
}

impl FactorPublisher {
    pub fn new(venue_slug: &str, factor_names: &[String]) -> Result<Self> {
        let mut factor_indices = HashMap::new();
        let mut publishers = HashMap::new();
        let mut stats = HashMap::new();

        for factor_name in factor_names {
            let Some(factor_index) = factor_name_to_index(factor_name.as_str()) else {
                anyhow::bail!("unknown factor name for index mapping: {}", factor_name);
            };

            let node_name = format!(
                "factor_pub_{}_{}",
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
                "FactorPublisher created for {}: {} (factor_index={})",
                factor_name, service_path, factor_index
            );

            factor_indices.insert(factor_name.clone(), factor_index);
            publishers.insert(factor_name.clone(), publisher);
            stats.insert(factor_name.clone(), FactorStats::default());
        }

        Ok(Self {
            venue_slug: venue_slug.to_string(),
            factor_indices,
            publishers,
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
        let Some(factor_index) = self.factor_indices.get(factor_name).copied() else {
            warn!("Missing factor index for factor_name={}", factor_name);
            self.bump_dropped(factor_name);
            return false;
        };

        let msg = FactorValueMsg::create_with_factor_index(
            symbol.to_string(),
            value,
            timestamp_ms,
            ready,
            factor_index,
        );
        let bytes: Bytes = msg.to_bytes();

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
                "FactorPublisher[{}:{}] stats: published={}, dropped={}",
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
