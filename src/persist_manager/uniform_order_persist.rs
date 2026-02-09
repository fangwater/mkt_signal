use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Buf;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::service::ipc;
use log::{info, warn};

use crate::persist_manager::iceoryx::{create_record_subscriber, trim_uniform_order_payload};
use crate::persist_manager::storage::RocksDbStore;
use crate::pre_trade::UNIFORM_ORDER_RECORD_CHANNEL;

pub(crate) const CF_UNIFORM_ORDER: &str = "uniform_orders";

pub fn required_column_families() -> &'static [&'static str] {
    &[CF_UNIFORM_ORDER]
}

pub struct UniformOrderPersistor {
    subscriber:
        Subscriber<ipc::Service, [u8; crate::common::iceoryx_publisher::SIGNAL_PAYLOAD], ()>,
    store: Arc<RocksDbStore>,
}

impl UniformOrderPersistor {
    pub fn new(store: Arc<RocksDbStore>) -> Result<Self> {
        let subscriber = create_record_subscriber(UNIFORM_ORDER_RECORD_CHANNEL)?;
        Ok(Self { subscriber, store })
    }

    pub async fn run(self) -> Result<()> {
        info!(
            "uniform order persistor started on channel {}",
            UNIFORM_ORDER_RECORD_CHANNEL
        );
        loop {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = trim_uniform_order_payload(sample.payload());
                    if !payload.is_empty() {
                        if payload.len() < 8 {
                            warn!("uniform order payload too short: {} bytes", payload.len());
                            continue;
                        }
                        let mut cursor = &payload[..];
                        let ts = cursor.get_i64_le() as u64;
                        let key = format!("{:020}", ts);
                        info!(
                            "persist uniform order: key={} payload_len={}",
                            key,
                            payload.len()
                        );
                        let _ = self
                            .store
                            .put(CF_UNIFORM_ORDER, key.as_bytes(), payload.as_ref());
                    }
                }
                Ok(None) => tokio::task::yield_now().await,
                Err(err) => {
                    warn!("uniform order receive error: {err}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
}
