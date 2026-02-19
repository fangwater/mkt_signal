//! Trade flow feature 发布器

use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};

use crate::common::trade_flow_feature_msg::TradeFlowFeatureMsg;

pub const TRADE_FLOW_FEATURE_MAX_BYTES: usize = 1024;
const SUBSCRIBER_MAX_BUFFER_SIZE: usize = 8192;
const HISTORY_SIZE: usize = 128;

pub struct TradeFlowFeaturePublisher {
    venue_slug: String,
    publisher: Publisher<ipc::Service, [u8; TRADE_FLOW_FEATURE_MAX_BYTES], ()>,
    published: u64,
    dropped: u64,
}

impl TradeFlowFeaturePublisher {
    pub fn new(venue_slug: &str) -> Result<Self> {
        let node_name = format!("trade_flow_feature_pub_{}", venue_slug.replace('-', "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service_path = format!("factor_pub/{}/trade_flow_feature", venue_slug);
        let service = node
            .service_builder(&ServiceName::new(&service_path)?)
            .publish_subscribe::<[u8; TRADE_FLOW_FEATURE_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .history_size(HISTORY_SIZE)
            .subscriber_max_buffer_size(SUBSCRIBER_MAX_BUFFER_SIZE)
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;

        info!(
            "TradeFlowFeaturePublisher created: venue={} service={} history_size={} subscriber_max_buffer_size={}",
            venue_slug,
            service_path,
            service.static_config().history_size(),
            service.static_config().subscriber_max_buffer_size()
        );

        Ok(Self {
            venue_slug: venue_slug.to_string(),
            publisher,
            published: 0,
            dropped: 0,
        })
    }

    pub fn publish(&mut self, msg: &TradeFlowFeatureMsg) -> bool {
        let bytes: Bytes = match msg.to_bytes() {
            Ok(v) => v,
            Err(err) => {
                warn!("TradeFlowFeatureMsg encode failed: {}", err);
                self.dropped += 1;
                return false;
            }
        };

        if bytes.len() > TRADE_FLOW_FEATURE_MAX_BYTES {
            warn!(
                "TradeFlowFeature payload size {} exceeds max {} for symbol={}",
                bytes.len(),
                TRADE_FLOW_FEATURE_MAX_BYTES,
                msg.symbol
            );
            self.dropped += 1;
            return false;
        }

        let mut buffer = [0u8; TRADE_FLOW_FEATURE_MAX_BYTES];
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
            "TradeFlowFeaturePublisher[{}] stats: published={}, dropped={}",
            self.venue_slug, self.published, self.dropped
        );
        self.published = 0;
        self.dropped = 0;
    }
}
