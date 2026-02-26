use anyhow::Result;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;

pub const FUSION_FACTOR_PAYLOAD_MAX_BYTES: usize = 4096;

pub struct FusionFactorPublisher {
    publisher: Publisher<ipc::Service, [u8; FUSION_FACTOR_PAYLOAD_MAX_BYTES], ()>,
    published_count: u64,
    dropped_count: u64,
}

impl FusionFactorPublisher {
    pub fn new(node_name: &str, service_path: &str) -> Result<Self> {
        let node = NodeBuilder::new()
            .name(&NodeName::new(node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(service_path)?)
            .publish_subscribe::<[u8; FUSION_FACTOR_PAYLOAD_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .subscriber_max_buffer_size(8192)
            .history_size(128)
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;
        Ok(Self {
            publisher,
            published_count: 0,
            dropped_count: 0,
        })
    }

    pub fn publish(&mut self, data: &[u8]) -> bool {
        if data.len() > FUSION_FACTOR_PAYLOAD_MAX_BYTES {
            self.dropped_count = self.dropped_count.saturating_add(1);
            return false;
        }

        let mut buffer = [0u8; FUSION_FACTOR_PAYLOAD_MAX_BYTES];
        buffer[..data.len()].copy_from_slice(data);

        match self.publisher.loan_uninit() {
            Ok(sample) => {
                let sample = sample.write_payload(buffer);
                if sample.send().is_ok() {
                    self.published_count = self.published_count.saturating_add(1);
                    true
                } else {
                    self.dropped_count = self.dropped_count.saturating_add(1);
                    false
                }
            }
            Err(_) => {
                self.dropped_count = self.dropped_count.saturating_add(1);
                false
            }
        }
    }

    pub fn published_count(&self) -> u64 {
        self.published_count
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped_count
    }
}
