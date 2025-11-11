use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::service::ipc;
use log::{info, warn};

use crate::common::time_util::get_timestamp_us;
use crate::persist_manager::iceoryx::{create_signal_record_subscriber, trim_payload};
use crate::persist_manager::storage::RocksDbStore;
use crate::pre_trade::ORDER_UPDATE_RECORD_CHANNEL;

const CF_ORDER_UPDATE: &str = "order_updates";

pub fn required_column_families() -> &'static [&'static str] {
    &[CF_ORDER_UPDATE]
}

pub struct OrderUpdatePersistor {
    subscriber: Subscriber<ipc::Service, [u8; crate::common::iceoryx_publisher::SIGNAL_PAYLOAD], ()>,
    store: Arc<RocksDbStore>,
}

impl OrderUpdatePersistor {
    pub fn new(store: Arc<RocksDbStore>) -> Result<Self> {
        let subscriber = create_signal_record_subscriber(ORDER_UPDATE_RECORD_CHANNEL)?;
        Ok(Self { subscriber, store })
    }

    pub async fn run(self) -> Result<()> {
        info!("order update persistor started on channel {}", ORDER_UPDATE_RECORD_CHANNEL);
        loop {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = trim_payload(sample.payload());
                    if !payload.is_empty() {
                        let ts = get_timestamp_us() as u64;
                        let key = format!("{:020}", ts);
                        let _ = self.store.put(CF_ORDER_UPDATE, key.as_bytes(), payload.as_ref());
                    }
                }
                Ok(None) => tokio::task::yield_now().await,
                Err(err) => {
                    warn!("order update receive error: {err}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
}
