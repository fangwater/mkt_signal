use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use iceoryx2::port::publisher::Publisher;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::service::ipc;
use log::warn;
use persist_common::rapidx_execution::{
    ExecutionRecord, ACK_BYTES, ACK_CHANNEL, MAX_BYTES, RECORD_CHANNEL,
};

use crate::iceoryx::{
    create_sized_record_publisher, create_sized_record_subscriber_with_max_publishers,
};
use crate::polling::{PollStats, MAX_DRAIN_PER_CHANNEL};
use crate::runtime_common::get_timestamp_us;
use crate::storage::RocksDbStore;
use crate::sync::persist_with_outbox_sync;

pub const CF_RAPIDX_EXECUTION: &str = "rapidx_executions";

pub fn required_column_families() -> &'static [&'static str] {
    &[CF_RAPIDX_EXECUTION]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::{CF_SYNC_META, CF_SYNC_OUTBOX};
    use persist_common::rapidx_execution::ExecutionEvidence;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn record(rest: bool) -> ExecutionRecord {
        ExecutionRecord {
            portfolio: "123".into(),
            exchange: "BINANCE".into(),
            execution: ExecutionEvidence {
                transaction_id: "tx".into(),
                order_id: "ord".into(),
                client_order_id: "cli".into(),
                symbol: "BINANCE_PERP_BTC_USDT".into(),
                side: "BUY".into(),
                quantity: "1".into(),
                price: "2".into(),
                timestamp_ms: 1,
                realized_pnl: "0".into(),
                signed_fee: (!rest).then(|| "0".into()),
                signed_fee_currency: (!rest).then(|| "".into()),
                fee: rest.then(|| "0".into()),
                fee_currency: rest.then(|| "".into()),
                reported_rebate: rest.then(|| "0".into()),
                rebate_currency: rest.then(|| "".into()),
                rest,
            },
        }
    }

    fn open_store(with_outbox: bool) -> (RocksDbStore, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "rapidx_persist_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut cfs = vec![CF_RAPIDX_EXECUTION];
        if with_outbox {
            cfs.extend([CF_SYNC_OUTBOX, CF_SYNC_META]);
        }
        (
            RocksDbStore::open(path.to_str().unwrap(), &cfs, false).unwrap(),
            path,
        )
    }

    #[test]
    fn persists_idempotently_across_restart_without_rewriting_outbox() {
        let (store, path) = open_store(true);
        let ws = record(false);
        let rest = record(true);
        assert_eq!(
            persist_record(&store, &ws, true).unwrap(),
            ws.ack().unwrap()
        );
        assert_eq!(
            store.scan(CF_SYNC_OUTBOX, None, false, None).unwrap().len(),
            1
        );
        persist_record(&store, &ws, true).unwrap();
        assert_eq!(
            store.scan(CF_SYNC_OUTBOX, None, false, None).unwrap().len(),
            1
        );
        drop(store);
        let store = RocksDbStore::open(
            path.to_str().unwrap(),
            &[CF_RAPIDX_EXECUTION, CF_SYNC_OUTBOX, CF_SYNC_META],
            false,
        )
        .unwrap();
        assert_eq!(
            persist_record(&store, &ws, true).unwrap(),
            ws.ack().unwrap()
        );
        assert_eq!(
            store.scan(CF_SYNC_OUTBOX, None, false, None).unwrap().len(),
            1
        );
        persist_record(&store, &rest, true).unwrap();
        assert_eq!(
            store.scan(CF_SYNC_OUTBOX, None, false, None).unwrap().len(),
            2
        );
        assert_eq!(
            store
                .scan(CF_RAPIDX_EXECUTION, None, false, None)
                .unwrap()
                .len(),
            2
        );
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn conflict_does_not_overwrite_durable_evidence() {
        let (store, path) = open_store(true);
        let original = record(false);
        persist_record(&store, &original, true).unwrap();
        let mut changed = original.clone();
        changed.execution.price = "3".into();
        assert!(persist_record(&store, &changed, true).is_err());
        assert_eq!(
            store
                .get(CF_RAPIDX_EXECUTION, &original.stable_key().unwrap())
                .unwrap()
                .unwrap(),
            original.to_json_bytes().unwrap()
        );
        assert_eq!(
            store.scan(CF_SYNC_OUTBOX, None, false, None).unwrap().len(),
            1
        );
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn failed_sync_batch_does_not_write_fact_or_return_ack() {
        let (store, path) = open_store(false);
        let item = record(false);
        assert!(persist_record(&store, &item, true).is_err());
        assert!(store
            .get(CF_RAPIDX_EXECUTION, &item.stable_key().unwrap())
            .unwrap()
            .is_none());
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
}

pub struct RapidXExecutionPersistor {
    subscriber: Subscriber<ipc::Service, [u8; MAX_BYTES], ()>,
    ack_publisher: Publisher<ipc::Service, [u8; ACK_BYTES], ()>,
    store: Arc<RocksDbStore>,
    sync_enabled: bool,
}

impl RapidXExecutionPersistor {
    pub fn new(store: Arc<RocksDbStore>, sync_enabled: bool) -> Result<Self> {
        Ok(Self {
            subscriber: create_sized_record_subscriber_with_max_publishers(RECORD_CHANNEL, 32)?,
            ack_publisher: create_sized_record_publisher("persist_acks", ACK_CHANNEL)?,
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
                    if let Err(err) = self.persist_and_ack(sample.payload()) {
                        warn!("reject RapidX execution without ACK: {err:#}");
                        stats.record_error();
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    warn!("RapidX execution receive error: {err}");
                    stats.record_error();
                    break;
                }
            }
        }
        stats
    }

    fn persist_and_ack(&self, payload: &[u8]) -> Result<()> {
        let record =
            ExecutionRecord::from_ipc_payload(payload).context("decode RapidX execution record")?;
        let ack = persist_record(&self.store, &record, self.sync_enabled)?;
        self.send_ack(ack)
    }
}

fn persist_record(
    store: &RocksDbStore,
    record: &ExecutionRecord,
    sync_enabled: bool,
) -> Result<[u8; ACK_BYTES]> {
    let key = record.stable_key()?;
    let canonical = record
        .to_json_bytes()
        .context("canonicalize RapidX execution record")?;
    match store.get(CF_RAPIDX_EXECUTION, &key)? {
        Some(existing) if existing == canonical => record.ack(),
        Some(_) => Err(anyhow!("RapidX execution stable-key conflict")),
        None => {
            persist_with_outbox_sync(
                store,
                CF_RAPIDX_EXECUTION,
                &key,
                &canonical,
                get_timestamp_us(),
                sync_enabled,
            )
            .context("persist RapidX execution and sync outbox")?;
            record.ack()
        }
    }
}

impl RapidXExecutionPersistor {
    fn send_ack(&self, ack: [u8; ACK_BYTES]) -> Result<()> {
        self.ack_publisher
            .loan_uninit()
            .context("loan RapidX execution ACK")?
            .write_payload(ack)
            .send()
            .context("send RapidX execution ACK")?;
        Ok(())
    }
}
