use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use log::{info, warn};

use crate::account::execution_record::ExecutionRecordMessage;
use crate::persist_manager::config::ExecutionPersistCfg;
use crate::persist_manager::iceoryx::{create_signal_record_subscriber, trim_payload};
use crate::persist_manager::storage::RocksDbStore;

const CF_BINANCE_MARGIN_EXECUTION: &str = "executions_binance_margin";

pub fn required_column_families() -> &'static [&'static str] {
    &[CF_BINANCE_MARGIN_EXECUTION]
}

pub struct ExecutionPersistor {
    cfg: ExecutionPersistCfg,
    store: Arc<RocksDbStore>,
    subscriber: iceoryx2::port::subscriber::Subscriber<
        iceoryx2::service::ipc::Service,
        [u8; crate::common::iceoryx_publisher::SIGNAL_PAYLOAD],
        (),
    >,
}

impl ExecutionPersistor {
    pub fn new(cfg: ExecutionPersistCfg, store: Arc<RocksDbStore>) -> Result<Self> {
        let subscriber = create_signal_record_subscriber(&cfg.channel)
            .context("failed to create execution record subscriber")?;
        Ok(Self {
            cfg,
            store,
            subscriber,
        })
    }

    pub async fn run(self) -> Result<()> {
        info!(
            "execution persistor listening on channel {}",
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
                        warn!("failed to persist execution record: {err:#}");
                    }
                }
                Ok(None) => tokio::task::yield_now().await,
                Err(err) => {
                    warn!("execution subscriber error: {err}");
                    tokio::time::sleep(Duration::from_millis(self.cfg.retry_backoff_ms)).await;
                }
            }
        }
    }

    fn handle_payload(&self, payload: Bytes) -> Result<()> {
        let record = ExecutionRecordMessage::from_bytes(payload.clone())?;
        let key = format!(
            "{:020}_{:016}_{:016}",
            record.recv_ts_us.max(0),
            record.order_id,
            record.update_id
        );
        self.store
            .put(
                CF_BINANCE_MARGIN_EXECUTION,
                key.as_bytes(),
                payload.as_ref(),
            )
            .context("failed to append execution record")
    }
}
