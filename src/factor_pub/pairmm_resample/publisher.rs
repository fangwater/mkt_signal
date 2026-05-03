use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};

use super::cfg::PairMmResampleConfig;
use crate::common::mkt_msg::PairMmResampleMsg;

const FACTOR_MAX_BYTES: usize = 256;
const BUFFER_SIZE: usize = 8192;
const HISTORY_SIZE: usize = 100;
const MAX_SUBSCRIBERS: usize = 10;

pub struct PairMmPublisher {
    venue_slug: String,
    publisher: Publisher<ipc::Service, [u8; FACTOR_MAX_BYTES], ()>,
    published: u64,
    dropped: u64,
}

impl PairMmPublisher {
    pub fn new(venue_slug: &str, _config: &PairMmResampleConfig) -> Result<Self> {
        let node_name = format!(
            "factor_pub_{}_pairmm_resample",
            venue_slug.replace('-', "_")
        );
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service_path = format!("factor_pub/{}/pairmm_resample", venue_slug);
        let service = node
            .service_builder(&ServiceName::new(&service_path)?)
            .publish_subscribe::<[u8; FACTOR_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(MAX_SUBSCRIBERS)
            .history_size(HISTORY_SIZE)
            .subscriber_max_buffer_size(BUFFER_SIZE)
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;

        info!(
            "PairMmPublisher created for {}: {} (history_size={}, max_subscribers={})",
            venue_slug, service_path, HISTORY_SIZE, MAX_SUBSCRIBERS
        );

        Ok(Self {
            venue_slug: venue_slug.to_string(),
            publisher,
            published: 0,
            dropped: 0,
        })
    }

    pub fn publish(&mut self, msg: &PairMmResampleMsg) -> bool {
        let bytes: Bytes = match msg.to_bytes() {
            Ok(v) => v,
            Err(err) => {
                warn!("PairMmResampleMsg encode failed: {err}");
                self.dropped += 1;
                return false;
            }
        };

        if bytes.len() > FACTOR_MAX_BYTES {
            warn!(
                "PairMmResample payload size {} exceeds max {}",
                bytes.len(),
                FACTOR_MAX_BYTES
            );
            self.dropped += 1;
            return false;
        }

        let mut buffer = [0u8; FACTOR_MAX_BYTES];
        buffer[..bytes.len()].copy_from_slice(&bytes);

        if let Ok(sample) = self.publisher.loan_uninit() {
            let sample = sample.write_payload(buffer);
            if sample.send().is_ok() {
                self.published += 1;
                return true;
            }
        }

        self.dropped += 1;
        false
    }

    pub fn log_stats(&mut self) {
        info!(
            "PairMmPublisher[{}] stats: published={}, dropped={}",
            self.venue_slug, self.published, self.dropped
        );
        self.published = 0;
        self.dropped = 0;
    }
}
