use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use log::{info, warn};

use crate::account::order_update_record::OrderUpdateRecordMessage;
use crate::persist_manager::config::UmOrderPersistCfg;
use crate::persist_manager::iceoryx::{create_signal_record_subscriber, trim_payload};
use crate::persist_manager::storage::RocksDbStore;

const CF_BINANCE_UM_ORDER_UPDATE: &str = "executions_binance_um";

pub fn required_column_families() -> &'static [&'static str] {
    &[CF_BINANCE_UM_ORDER_UPDATE]
}

pub struct UmOrderUpdatePersistor {
    cfg: UmOrderPersistCfg,
    store: Arc<RocksDbStore>,
    subscriber: iceoryx2::port::subscriber::Subscriber<
        iceoryx2::service::ipc::Service,
        [u8; crate::common::iceoryx_publisher::SIGNAL_PAYLOAD],
        (),
    >,
}

impl UmOrderUpdatePersistor {
    pub fn new(cfg: UmOrderPersistCfg, store: Arc<RocksDbStore>) -> Result<Self> {
        let subscriber = create_signal_record_subscriber(&cfg.channel)
            .context("failed to create UM order update subscriber")?;
        Ok(Self {
            cfg,
            store,
            subscriber,
        })
    }

    pub async fn run(self) -> Result<()> {
        info!(
            "UM order update persistor listening on channel {}",
            self.cfg.channel
        );
        loop {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = trim_payload(sample.payload());
                    if payload.is_empty() {
                        continue;
                    }
                    if let Err(err) = self.handle_payload(payload.clone()) {
                        warn!("failed to persist UM order update: {err:#}");
                    }
                }
                Ok(None) => tokio::task::yield_now().await,
                Err(err) => {
                    warn!("UM order update subscriber error: {err}");
                    tokio::time::sleep(Duration::from_millis(self.cfg.retry_backoff_ms)).await;
                }
            }
        }
    }

    fn handle_payload(&self, payload: Bytes) -> Result<()> {
        let record = OrderUpdateRecordMessage::from_bytes(payload.clone())?;
        let key = format!(
            "{:020}_{:016}_{:016}",
            record.recv_ts_us.max(0),
            record.order_id,
            record.trade_id
        );
        self.store
            .put(CF_BINANCE_UM_ORDER_UPDATE, key.as_bytes(), payload.as_ref())
            .context("failed to append UM order update record")
    }
}
