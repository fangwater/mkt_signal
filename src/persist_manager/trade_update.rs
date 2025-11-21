use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Buf;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::service::ipc;
use log::{debug, info, warn};

use crate::persist_manager::iceoryx::{create_signal_record_subscriber, trim_payload};
use crate::persist_manager::storage::RocksDbStore;
use crate::pre_trade::TRADE_UPDATE_RECORD_CHANNEL;

pub(super) const CF_TRADE_UPDATE: &str = "trade_updates";

pub fn required_column_families() -> &'static [&'static str] {
    &[CF_TRADE_UPDATE]
}

pub struct TradeUpdatePersistor {
    subscriber:
        Subscriber<ipc::Service, [u8; crate::common::iceoryx_publisher::SIGNAL_PAYLOAD], ()>,
    store: Arc<RocksDbStore>,
}

impl TradeUpdatePersistor {
    pub fn new(store: Arc<RocksDbStore>) -> Result<Self> {
        let subscriber = create_signal_record_subscriber(TRADE_UPDATE_RECORD_CHANNEL)?;
        Ok(Self { subscriber, store })
    }

    pub async fn run(self) -> Result<()> {
        info!(
            "trade update persistor started on channel {}",
            TRADE_UPDATE_RECORD_CHANNEL
        );
        loop {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = trim_payload(sample.payload());
                    if !payload.is_empty() {
                        // 从消息头部读取接收时间戳（前8字节）
                        if payload.len() < 8 {
                            warn!("trade update payload too short: {} bytes", payload.len());
                            continue;
                        }
                        let mut cursor = &payload[..];
                        let ts = cursor.get_i64_le() as u64;
                        let key = format!("{:020}", ts);
                        debug!(
                            "persist trade update: key={} payload_len={}",
                            key,
                            payload.len()
                        );
                        let _ = self
                            .store
                            .put(CF_TRADE_UPDATE, key.as_bytes(), payload.as_ref());
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
