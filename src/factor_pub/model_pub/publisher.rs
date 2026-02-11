use anyhow::Result;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;

pub const MODEL_PAYLOAD_MAX_BYTES: usize = 256;

pub struct ModelPublisher {
    publisher: Publisher<ipc::Service, [u8; MODEL_PAYLOAD_MAX_BYTES], ()>,
}

impl ModelPublisher {
    pub fn new(node_name: &str, service_path: &str) -> Result<Self> {
        let node = NodeBuilder::new()
            .name(&NodeName::new(node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(service_path)?)
            .publish_subscribe::<[u8; MODEL_PAYLOAD_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .history_size(128)
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;
        Ok(Self { publisher })
    }

    pub fn publish(&self, data: &[u8]) -> bool {
        if data.len() > MODEL_PAYLOAD_MAX_BYTES {
            return false;
        }

        let mut buffer = [0u8; MODEL_PAYLOAD_MAX_BYTES];
        buffer[..data.len()].copy_from_slice(data);

        match self.publisher.loan_uninit() {
            Ok(sample) => {
                let sample = sample.write_payload(buffer);
                sample.send().is_ok()
            }
            Err(_) => false,
        }
    }
}
