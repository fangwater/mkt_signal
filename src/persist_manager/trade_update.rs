use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Buf;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::service::ipc;
use log::{info, warn};

use crate::persist_manager::iceoryx::{create_record_subscriber, trim_trade_update_payload};
use crate::persist_manager::storage::RocksDbStore;
use crate::persist_manager::sync::persist_with_outbox;
use crate::pre_trade::{TRADE_UPDATE_RECORD_CHANNEL, TRADE_UPDATE_UNMATCHED_RECORD_CHANNEL};

pub(crate) const CF_TRADE_UPDATE: &str = "trade_updates";
pub(crate) const CF_TRADE_UPDATE_UNMATCHED: &str = "trade_updates_unmatched";

pub fn required_column_families() -> &'static [&'static str] {
    &[CF_TRADE_UPDATE, CF_TRADE_UPDATE_UNMATCHED]
}

pub struct TradeUpdatePersistor {
    subscriber:
        Subscriber<ipc::Service, [u8; crate::common::iceoryx_publisher::SIGNAL_PAYLOAD], ()>,
    store: Arc<RocksDbStore>,
    sync_enabled: bool,
}

impl TradeUpdatePersistor {
    pub fn new(store: Arc<RocksDbStore>, sync_enabled: bool) -> Result<Self> {
        let subscriber = create_record_subscriber(TRADE_UPDATE_RECORD_CHANNEL)?;
        Ok(Self {
            subscriber,
            store,
            sync_enabled,
        })
    }

    pub async fn run(self) -> Result<()> {
        info!(
            "trade update persistor started on channel {}",
            TRADE_UPDATE_RECORD_CHANNEL
        );
        loop {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = trim_trade_update_payload(sample.payload());
                    if !payload.is_empty() {
                        // 从消息头部读取接收时间戳（前8字节）
                        if payload.len() < 8 {
                            warn!("trade update payload too short: {} bytes", payload.len());
                            continue;
                        }
                        let mut cursor = &payload[..];
                        let ts = cursor.get_i64_le() as u64;
                        let key = format!("{:020}", ts);
                        info!(
                            "persist trade update: key={} payload_len={}",
                            key,
                            payload.len()
                        );
                        let _ = persist_with_outbox(
                            &self.store,
                            CF_TRADE_UPDATE,
                            key.as_bytes(),
                            payload.as_ref(),
                            ts as i64,
                            self.sync_enabled,
                        );
                    }
                }
                Ok(None) => tokio::task::yield_now().await,
                Err(err) => {
                    warn!("trade update receive error: {err}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
}

pub struct TradeUpdateUnmatchedPersistor {
    subscriber:
        Subscriber<ipc::Service, [u8; crate::common::iceoryx_publisher::SIGNAL_PAYLOAD], ()>,
    store: Arc<RocksDbStore>,
    sync_enabled: bool,
}

impl TradeUpdateUnmatchedPersistor {
    pub fn new(store: Arc<RocksDbStore>, sync_enabled: bool) -> Result<Self> {
        let subscriber = create_record_subscriber(TRADE_UPDATE_UNMATCHED_RECORD_CHANNEL)?;
        Ok(Self {
            subscriber,
            store,
            sync_enabled,
        })
    }

    pub async fn run(self) -> Result<()> {
        info!(
            "trade update unmatched persistor started on channel {}",
            TRADE_UPDATE_UNMATCHED_RECORD_CHANNEL
        );
        loop {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = trim_trade_update_payload(sample.payload());
                    if !payload.is_empty() {
                        if payload.len() < 8 {
                            warn!(
                                "trade update unmatched payload too short: {} bytes",
                                payload.len()
                            );
                            continue;
                        }
                        let mut cursor = &payload[..];
                        let ts = cursor.get_i64_le() as u64;
                        let key = format!("{:020}", ts);
                        info!(
                            "persist trade update unmatched: key={} payload_len={}",
                            key,
                            payload.len()
                        );
                        let _ = persist_with_outbox(
                            &self.store,
                            CF_TRADE_UPDATE_UNMATCHED,
                            key.as_bytes(),
                            payload.as_ref(),
                            ts as i64,
                            self.sync_enabled,
                        );
                    }
                }
                Ok(None) => tokio::task::yield_now().await,
                Err(err) => {
                    warn!("trade update unmatched receive error: {err}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
}
