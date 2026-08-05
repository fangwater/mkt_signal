use std::sync::Arc;

use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::service::ipc;
use log::warn;

use crate::iceoryx::{create_record_subscriber, trim_order_queue_position_payload};
use crate::polling::{PollStats, MAX_DRAIN_PER_CHANNEL};
use crate::storage::RocksDbStore;
use crate::sync::persist_with_outbox;
use persist_common::{OrderQueuePositionRecord, ORDER_QUEUE_POSITION_RECORD_CHANNEL};

pub(crate) const CF_ORDER_QUEUE_POSITION: &str = "order_queue_positions";

pub fn required_column_families() -> &'static [&'static str] {
    &[CF_ORDER_QUEUE_POSITION]
}

pub struct OrderQueuePositionPersistor {
    subscriber: Subscriber<ipc::Service, [u8; crate::runtime_common::SIGNAL_PAYLOAD], ()>,
    store: Arc<RocksDbStore>,
    sync_enabled: bool,
}

impl OrderQueuePositionPersistor {
    pub fn new(store: Arc<RocksDbStore>, sync_enabled: bool) -> Result<Self> {
        let subscriber = create_record_subscriber(ORDER_QUEUE_POSITION_RECORD_CHANNEL)?;
        Ok(Self {
            subscriber,
            store,
            sync_enabled,
        })
    }

    pub(crate) fn poll_available(&self) -> PollStats {
        let mut stats = PollStats::default();
        for _ in 0..MAX_DRAIN_PER_CHANNEL {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    stats.record_received();
                    let Some(payload) = trim_order_queue_position_payload(sample.payload()) else {
                        warn!("invalid padded order-position lifecycle payload");
                        continue;
                    };
                    let record = match OrderQueuePositionRecord::from_bytes(&payload) {
                        Ok(record) => record,
                        Err(err) => {
                            warn!("invalid order-position lifecycle record: {err}");
                            continue;
                        }
                    };
                    let key = record_key(&record);
                    if let Err(err) = persist_with_outbox(
                        &self.store,
                        CF_ORDER_QUEUE_POSITION,
                        key.as_bytes(),
                        payload.as_ref(),
                        record.recv_ts_us,
                        self.sync_enabled,
                    ) {
                        warn!(
                            "persist order-position lifecycle failed: client_order_id={} err={err:#}",
                            record.client_order_id
                        );
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    warn!("order-position lifecycle receive error: {err}");
                    stats.record_error();
                    break;
                }
            }
        }
        stats
    }
}

fn record_key(record: &OrderQueuePositionRecord) -> String {
    format!(
        "{:020}:{:02x}:{:016x}:{:016x}:{:02x}",
        u64::try_from(record.recv_ts_us).unwrap_or_default(),
        record.venue,
        record.client_order_id as u64,
        record.update_tp as u64,
        record.action.to_u8(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use persist_common::OrderQueuePositionAction;

    #[test]
    fn record_key_keeps_timestamp_prefix_and_event_identity() {
        let record = OrderQueuePositionRecord {
            recv_ts_us: 123,
            account_id: "acct".to_string(),
            venue: 2,
            action: OrderQueuePositionAction::Filled,
            create_tp: 1,
            update_tp: 122,
            local_tp: 123,
            client_order_id: 42,
            tlen: 3.0,
            backlen: 2.0,
            inpos: 1.0,
        };
        let key = record_key(&record);
        assert!(key.starts_with("00000000000000000123:"));
        assert!(key.ends_with(":03"));
    }
}
