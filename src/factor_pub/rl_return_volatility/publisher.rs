//! RL Return Volatility 发布模块

use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};

use crate::common::mkt_msg::RlReturnVolatilityMsg;

const FACTOR_MAX_BYTES: usize = 256;

pub struct FactorPublisher {
    venue_slug: String,
    publisher: Publisher<ipc::Service, [u8; FACTOR_MAX_BYTES], ()>,
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

        Ok(Self {
            venue_slug: venue_slug.to_string(),
            publisher,
            published: 0,
            dropped: 0,
        })
    }

    pub fn publish(&mut self, msg: &RlReturnVolatilityMsg) -> bool {
        let bytes: Bytes = msg.to_bytes();
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
