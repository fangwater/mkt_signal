use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use serde::Deserialize;

use crate::common::time_util::get_timestamp_us;
use crate::persist_manager::iceoryx::{create_signal_record_subscriber, trim_payload};
use crate::persist_manager::storage::RocksDbStore;
use crate::signal::record::{SignalRecordMessage, PRE_TRADE_SIGNAL_RECORD_CHANNEL};
use crate::signal::trade_signal::SignalType;

#[derive(Debug, Clone, Deserialize)]
pub struct SignalPersistCfg {
    #[serde(default = "default_signal_enabled")]
    pub enabled: bool,
    #[serde(default = "default_signal_channel")]
    pub channel: String,
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
}

impl Default for SignalPersistCfg {
    fn default() -> Self {
        Self {
            enabled: true,
            channel: default_signal_channel(),
            retry_backoff_ms: default_retry_backoff_ms(),
        }
    }
}

fn default_signal_enabled() -> bool {
    true
}

fn default_signal_channel() -> String {
    PRE_TRADE_SIGNAL_RECORD_CHANNEL.to_string()
}

fn default_retry_backoff_ms() -> u64 {
    200
}

const CF_ARB_OPEN: &str = "signals_arb_open";
const CF_ARB_HEDGE: &str = "signals_arb_hedge";
const CF_ARB_CANCEL: &str = "signals_arb_cancel";
const CF_ARB_CLOSE: &str = "signals_arb_close";

pub fn required_column_families() -> &'static [&'static str] {
    &[
        CF_ARB_OPEN,
        CF_ARB_HEDGE,
        CF_ARB_CANCEL,
        CF_ARB_CLOSE,
    ]
}

pub struct SignalPersistor {
    cfg: SignalPersistCfg,
    subscriber:
        Subscriber<ipc::Service, [u8; crate::common::iceoryx_publisher::SIGNAL_PAYLOAD], ()>,
    store: Arc<RocksDbStore>,
}

impl SignalPersistor {
    pub fn new(cfg: SignalPersistCfg, store: Arc<RocksDbStore>) -> Result<Self> {
        let subscriber = create_signal_record_subscriber(&cfg.channel)?;
        Ok(Self {
            cfg,
            subscriber,
            store,
        })
    }

    pub async fn run(self) -> Result<()> {
        info!("signal persistor listening on channel {}", self.cfg.channel);
        loop {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = trim_payload(sample.payload());
                    if payload.is_empty() {
                        continue;
                    }
                    if let Err(err) = self.handle_payload(payload.clone()) {
                        warn!(
                            "failed to persist signal from {}: {err:#}",
                            self.cfg.channel
                        );
                    }
                }
                Ok(None) => tokio::task::yield_now().await,
                Err(err) => {
                    warn!("error receiving from channel {}: {err}", self.cfg.channel);
                    tokio::time::sleep(Duration::from_millis(self.cfg.retry_backoff_ms)).await;
                }
            }
        }
    }

    fn handle_payload(&self, payload: Bytes) -> Result<()> {
        let record = SignalRecordMessage::from_bytes(payload.clone())
            .context("failed to decode signal record message")?;
        let Some(cf_name) = column_family_for_signal(&record.signal_type) else {
            debug!("unsupported signal type {:?}", record.signal_type);
            return Ok(());
        };

        let ts: u64 = if record.timestamp_us > 0 {
            record.timestamp_us as u64
        } else {
            get_timestamp_us() as u64
        };
        let key = format!("{:020}_{:010}", ts, record.strategy_id);
        self.store
            .put(cf_name, key.as_bytes(), payload.as_ref())
            .context("failed to append signal record")?;
        debug!(
            "persisted signal {:?} strategy_id={} ts_us={}",
            record.signal_type, record.strategy_id, ts
        );
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
