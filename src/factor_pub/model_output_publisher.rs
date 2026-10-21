use anyhow::Result;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;

use mkt_parsers::msg::model_ipc::MODEL_PAYLOAD_MAX_BYTES;

const MODEL_OUTPUT_SUBSCRIBER_MAX_BUFFER_SIZE: usize = 256;
const MODEL_OUTPUT_MAX_SUBSCRIBERS: usize = 32;

/// Publishes one logical model stream to a dedicated `model_output/*` service.
pub struct ModelPublisher {
    publisher: Publisher<ipc::Service, [u8; MODEL_PAYLOAD_MAX_BYTES], ()>,
}

impl ModelPublisher {
    pub fn new(node_name: &str, service_path: &str) -> Result<Self> {
        Self::with_capacity(
            node_name,
            service_path,
            MODEL_OUTPUT_SUBSCRIBER_MAX_BUFFER_SIZE,
            128,
        )
    }

    pub fn with_capacity(
        node_name: &str,
        service_path: &str,
        subscriber_buffer_size: usize,
        history_size: usize,
    ) -> Result<Self> {
        let node = NodeBuilder::new()
            .name(&NodeName::new(node_name)?)
            .create::<ipc::Service>()?;

        Self::from_node(&node, service_path, subscriber_buffer_size, history_size)
    }

    fn from_node(
        node: &Node<ipc::Service>,
        service_path: &str,
        subscriber_buffer_size: usize,
        history_size: usize,
    ) -> Result<Self> {
        let service = node
            .service_builder(&ServiceName::new(service_path)?)
            .publish_subscribe::<[u8; MODEL_PAYLOAD_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(MODEL_OUTPUT_MAX_SUBSCRIBERS)
            .subscriber_max_buffer_size(subscriber_buffer_size)
            .history_size(history_size)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::factor_pub::cta_spread_factor_model_1m_pub::volatility::VOLATILITY_BUFFER_SIZE;
    use mkt_parsers::msg::mkt_msg::{ModelMsg, MODEL_STATUS_OK};
    use std::collections::HashSet;

    #[test]
    fn full_market_burst_and_late_subscription_preserve_every_symbol() {
        let directory = std::env::temp_dir().join(format!("grid_vol_ipc_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let mut config = iceoryx2::config::Config::default();
        config
            .global
            .set_root_path(&Path::new(directory.as_os_str().as_encoded_bytes()).unwrap());
        {
            let node = NodeBuilder::new()
                .config(&config)
                .create::<ipc::Service>()
                .unwrap();
            let service_name = format!("model_output/grid_burst_test_{}", uuid::Uuid::new_v4());
            let publisher = ModelPublisher::from_node(
                &node,
                &service_name,
                VOLATILITY_BUFFER_SIZE,
                VOLATILITY_BUFFER_SIZE,
            )
            .unwrap();
            let service = node
                .service_builder(&ServiceName::new(&service_name).unwrap())
                .publish_subscribe::<[u8; MODEL_PAYLOAD_MAX_BYTES]>()
                .open()
                .unwrap();
            let subscriber = service
                .subscriber_builder()
                .buffer_size(VOLATILITY_BUFFER_SIZE)
                .create()
                .unwrap();
            for i in 0..1536 {
                let message = ModelMsg::create(
                    format!("COIN{i}USDT"),
                    1_791_590_400_000,
                    1_791_590_400_000,
                    i,
                    0.01,
                    None,
                    true,
                    MODEL_STATUS_OK,
                    Vec::new(),
                    Vec::new(),
                );
                assert!(publisher.publish(&message.to_bytes().unwrap()));
            }
            let late = service
                .subscriber_builder()
                .buffer_size(VOLATILITY_BUFFER_SIZE)
                .create()
                .unwrap();
            // Publishers discover new subscribers on the next send and then
            // deliver retained history, just as the next live market bar does.
            let next = ModelMsg::create(
                "COIN0USDT".into(),
                1_791_590_405_000,
                1_791_590_405_000,
                1536,
                0.02,
                None,
                true,
                MODEL_STATUS_OK,
                Vec::new(),
                Vec::new(),
            );
            assert!(publisher.publish(&next.to_bytes().unwrap()));
            for reader in [&subscriber, &late] {
                let mut symbols = HashSet::new();
                while let Some(sample) = reader.receive().unwrap() {
                    let message = ModelMsg::from_bytes(sample.payload()).unwrap();
                    assert!(message.score_ready);
                    symbols.insert(message.symbol);
                }
                assert_eq!(symbols.len(), 1536);
            }
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
}
