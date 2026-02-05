use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Buf;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::service::ipc;
use log::{info, warn};

use crate::persist_manager::iceoryx::{create_signal_record_subscriber, trim_order_update_payload};
use crate::persist_manager::storage::RocksDbStore;
use crate::pre_trade::{ORDER_UPDATE_RECORD_CHANNEL, ORDER_UPDATE_UNMATCHED_RECORD_CHANNEL};

pub(crate) const CF_ORDER_UPDATE: &str = "order_updates";
pub(crate) const CF_ORDER_UPDATE_UNMATCHED: &str = "order_updates_unmatched";

pub fn required_column_families() -> &'static [&'static str] {
    &[CF_ORDER_UPDATE, CF_ORDER_UPDATE_UNMATCHED]
}

pub struct OrderUpdatePersistor {
    subscriber:
        Subscriber<ipc::Service, [u8; crate::common::iceoryx_publisher::SIGNAL_PAYLOAD], ()>,
    store: Arc<RocksDbStore>,
}

impl OrderUpdatePersistor {
    pub fn new(store: Arc<RocksDbStore>) -> Result<Self> {
        let subscriber = create_signal_record_subscriber(ORDER_UPDATE_RECORD_CHANNEL)?;
        Ok(Self { subscriber, store })
    }

    pub async fn run(self) -> Result<()> {
        info!(
            "order update persistor started on channel {}",
            ORDER_UPDATE_RECORD_CHANNEL
        );
        loop {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = trim_order_update_payload(sample.payload());
                    if !payload.is_empty() {
                        // 从消息头部读取接收时间戳（前8字节）
                        if payload.len() < 8 {
                            warn!("order update payload too short: {} bytes", payload.len());
                            continue;
                        }
                        let mut cursor = &payload[..];
                        let ts = cursor.get_i64_le() as u64;
                        let key = format!("{:020}", ts);
                        info!(
                            "persist order update: key={} payload_len={}",
                            key,
                            payload.len()
                        );
                        let _ = self
                            .store
                            .put(CF_ORDER_UPDATE, key.as_bytes(), payload.as_ref());
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

pub struct OrderUpdateUnmatchedPersistor {
    subscriber:
        Subscriber<ipc::Service, [u8; crate::common::iceoryx_publisher::SIGNAL_PAYLOAD], ()>,
    store: Arc<RocksDbStore>,
}

impl OrderUpdateUnmatchedPersistor {
    pub fn new(store: Arc<RocksDbStore>) -> Result<Self> {
        let subscriber = create_signal_record_subscriber(ORDER_UPDATE_UNMATCHED_RECORD_CHANNEL)?;
        Ok(Self { subscriber, store })
    }

    pub async fn run(self) -> Result<()> {
        info!(
            "order update unmatched persistor started on channel {}",
            ORDER_UPDATE_UNMATCHED_RECORD_CHANNEL
        );
        loop {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = trim_order_update_payload(sample.payload());
                    if !payload.is_empty() {
                        if payload.len() < 8 {
                            warn!(
                                "order update unmatched payload too short: {} bytes",
                                payload.len()
                            );
                            continue;
                        }
                        let mut cursor = &payload[..];
                        let ts = cursor.get_i64_le() as u64;
                        let key = format!("{:020}", ts);
                        info!(
                            "persist order update unmatched: key={} payload_len={}",
                            key,
                            payload.len()
                        );
                        let _ = self.store.put(
                            CF_ORDER_UPDATE_UNMATCHED,
                            key.as_bytes(),
                            payload.as_ref(),
                        );
                    }
                }
                Ok(None) => tokio::task::yield_now().await,
                Err(err) => {
                    warn!("order update unmatched receive error: {err}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
}
