use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::service::ipc;
use log::{info, warn};

use crate::common::time_util::get_timestamp_us;
use crate::persist_manager::iceoryx::{create_signal_record_subscriber, trim_payload};
use crate::persist_manager::storage::RocksDbStore;
use crate::signal::record::{SignalRecordMessage, PRE_TRADE_SIGNAL_RECORD_CHANNEL};
use crate::signal::trade_signal::SignalType;

const CF_ARB_OPEN: &str = "signals_arb_open";
const CF_ARB_HEDGE: &str = "signals_arb_hedge";
const CF_ARB_CANCEL: &str = "signals_arb_cancel";
const CF_ARB_CLOSE: &str = "signals_arb_close";

pub fn required_column_families() -> &'static [&'static str] {
    &[CF_ARB_OPEN, CF_ARB_HEDGE, CF_ARB_CANCEL, CF_ARB_CLOSE]
}

pub struct SignalPersistor {
    subscriber:
        Subscriber<ipc::Service, [u8; crate::common::iceoryx_publisher::SIGNAL_PAYLOAD], ()>,
    store: Arc<RocksDbStore>,
}

impl SignalPersistor {
    pub fn new(store: Arc<RocksDbStore>) -> Result<Self> {
        let subscriber = create_signal_record_subscriber(PRE_TRADE_SIGNAL_RECORD_CHANNEL)?;
        Ok(Self { subscriber, store })
    }

    pub async fn run(self) -> Result<()> {
        info!(
            "signal persistor started on channel {}",
            PRE_TRADE_SIGNAL_RECORD_CHANNEL
        );
        loop {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = trim_payload(sample.payload());
                    if !payload.is_empty() {
                        let _ = self.handle_payload(payload.clone());
                    }
                }
                Ok(None) => tokio::task::yield_now().await,
                Err(err) => {
                    warn!("signal receive error: {err}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    fn handle_payload(&self, payload: Bytes) -> Result<()> {
        let record = SignalRecordMessage::from_bytes(payload.clone())?;
        let Some(cf_name) = column_family_for_signal(&record.signal_type) else {
            return Ok(());
        };
        let ts = if record.timestamp_us > 0 {
            record.timestamp_us as u64
        } else {
            get_timestamp_us() as u64
        };
        let key = format!("{:020}_{:010}", ts, record.strategy_id);
        self.store.put(cf_name, key.as_bytes(), payload.as_ref())?;
        Ok(())
    }
}

fn column_family_for_signal(signal_type: &SignalType) -> Option<&'static str> {
    match signal_type {
        SignalType::ArbOpen => Some(CF_ARB_OPEN),
        SignalType::ArbHedge => Some(CF_ARB_HEDGE),
        SignalType::ArbCancel => Some(CF_ARB_CANCEL),
        SignalType::ArbClose => Some(CF_ARB_CLOSE),
    }
}
