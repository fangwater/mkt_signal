use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::service::ipc;
use log::{info, warn};
use tokio::time::Instant;

use crate::bbo_spread::BboSpreadStore;
use crate::iceoryx::{create_record_subscriber, trim_uniform_order_payload};
use crate::polling::{PollStats, MAX_DRAIN_PER_CHANNEL};
use crate::storage::RocksDbStore;
use crate::sync::persist_with_outbox;
use persist_common::{SIGNAL_BBO_BINARY_LEN, UNIFORM_ORDER_RECORD_CHANNEL};

pub(crate) const CF_UNIFORM_ORDER: &str = "uniform_orders";

pub fn required_column_families() -> &'static [&'static str] {
    &[CF_UNIFORM_ORDER]
}

pub struct UniformOrderPersistor {
    subscriber: Subscriber<ipc::Service, [u8; crate::runtime_common::SIGNAL_PAYLOAD], ()>,
    store: Arc<RocksDbStore>,
    bbo_store: Option<Arc<BboSpreadStore>>,
    bbo_enrich_delay: Duration,
    sync_enabled: bool,
}

pub(crate) struct PendingUniformOrder {
    key: String,
    payload: Bytes,
    symbol: String,
    update_ts: i64,
    due_at: Instant,
}

impl UniformOrderPersistor {
    pub fn new(store: Arc<RocksDbStore>, sync_enabled: bool) -> Result<Self> {
        let subscriber = create_record_subscriber(UNIFORM_ORDER_RECORD_CHANNEL)?;
        Ok(Self {
            subscriber,
            store,
            bbo_store: None,
            bbo_enrich_delay: Duration::ZERO,
            sync_enabled,
        })
    }

    pub fn new_with_bbo_spread(
        store: Arc<RocksDbStore>,
        bbo_store: Arc<BboSpreadStore>,
        bbo_enrich_delay: Duration,
        sync_enabled: bool,
    ) -> Result<Self> {
        let mut this = Self::new(store, sync_enabled)?;
        this.bbo_store = Some(bbo_store);
        this.bbo_enrich_delay = bbo_enrich_delay;
        Ok(this)
    }

    pub(crate) fn poll_available(&self, pending: &mut VecDeque<PendingUniformOrder>) -> PollStats {
        let mut stats = PollStats::default();
        for _ in 0..MAX_DRAIN_PER_CHANNEL {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    stats.record_received();
                    let Some(payload) = trim_uniform_order_payload(sample.payload()) else {
                        warn!(
                            "uniform order payload has invalid base or signal_bbo length: {}",
                            sample.payload().len()
                        );
                        stats.record_error();
                        continue;
                    };
                    match PendingUniformOrder::new(payload, self.bbo_enrich_delay) {
                        Ok(Some(order)) => pending.push_back(order),
                        Ok(None) => {}
                        Err(err) => warn!("uniform order payload decode failed: {err:#}"),
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    warn!("uniform order receive error: {err}");
                    stats.record_error();
                    break;
                }
            }
        }

        self.flush_due(pending);
        stats
    }

    fn flush_due(&self, pending: &mut VecDeque<PendingUniformOrder>) {
        let now = Instant::now();
        while matches!(pending.front(), Some(order) if order.due_at <= now) {
            let Some(order) = pending.pop_front() else {
                break;
            };

            let bbo_spread = self
                .bbo_store
                .as_ref()
                .map(|store| store.format_spread_for_symbol(&order.symbol, order.update_ts))
                .unwrap_or_default();
            let payload = append_bbo_spread(order.payload, &bbo_spread);
            info!(
                "persist uniform order: key={} payload_len={} bbo_spread_len={}",
                order.key,
                payload.len(),
                bbo_spread.len()
            );
            if let Err(err) = persist_with_outbox(
                &self.store,
                CF_UNIFORM_ORDER,
                order.key.as_bytes(),
                payload.as_ref(),
                order.update_ts,
                self.sync_enabled,
            ) {
                warn!("persist uniform order failed key={}: {err:#}", order.key);
            }
        }
    }
}

impl PendingUniformOrder {
    fn new(payload: Bytes, delay: Duration) -> Result<Option<Self>> {
        if payload.is_empty() {
            return Ok(None);
        }
        if payload.len() < 8 {
            return Err(anyhow!(
                "uniform order payload too short: {} bytes",
                payload.len()
            ));
        }

        let mut cursor = &payload[..];
        let ts = cursor.get_i64_le() as u64;
        let key = format!("{:020}", ts);
        let (symbol, update_ts) = decode_uniform_order_symbol_update_ts(&payload)?;
        Ok(Some(Self {
            key,
            payload,
            symbol,
            update_ts,
            due_at: Instant::now() + delay,
        }))
    }
}

pub(crate) fn next_idle_sleep(pending: &VecDeque<PendingUniformOrder>) -> Duration {
    let max_sleep = crate::polling::idle_sleep();
    match pending.front() {
        Some(order) => {
            let now = Instant::now();
            if order.due_at <= now {
                Duration::ZERO
            } else {
                (order.due_at - now).min(max_sleep)
            }
        }
        None => max_sleep,
    }
}

fn decode_uniform_order_symbol_update_ts(payload: &[u8]) -> Result<(String, i64)> {
    let mut cursor = Bytes::copy_from_slice(payload);
    skip(&mut cursor, 8, "uniform order recv_ts_us")?;

    if cursor.remaining() < 2 {
        return Err(anyhow!(
            "payload too short to read uniform order symbol_len"
        ));
    }
    let symbol_len = cursor.get_u16_le() as usize;
    if cursor.remaining() < symbol_len {
        return Err(anyhow!(
            "payload too short to read uniform order symbol (need {symbol_len}, have {})",
            cursor.remaining()
        ));
    }
    let symbol = String::from_utf8_lossy(cursor.copy_to_bytes(symbol_len).as_ref()).into_owned();

    skip(&mut cursor, 8, "uniform order create_ts")?;
    if cursor.remaining() < 8 {
        return Err(anyhow!("payload too short to read uniform order update_ts"));
    }
    let update_ts = cursor.get_i64_le();
    Ok((symbol, update_ts))
}

fn skip(cursor: &mut Bytes, len: usize, field: &str) -> Result<()> {
    if cursor.remaining() < len {
        return Err(anyhow!(
            "payload too short to read {field} (need {len}, have {})",
            cursor.remaining()
        ));
    }
    cursor.advance(len);
    Ok(())
}

fn append_bbo_spread(payload: Bytes, bbo_spread: &str) -> Bytes {
    let bbo_bytes = bbo_spread.as_bytes();
    let len = bbo_bytes.len().min(u16::MAX as usize);
    let signal_start = payload.len().saturating_sub(SIGNAL_BBO_BINARY_LEN);
    let mut out = BytesMut::with_capacity(payload.len() + 2 + len);
    out.extend_from_slice(&payload[..signal_start]);
    out.put_u16_le(len as u16);
    out.extend_from_slice(&bbo_bytes[..len]);
    out.extend_from_slice(&payload[signal_start..]);
    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_bbo_spread_uses_u16_length_prefix() {
        let mut raw = vec![1, 2, 3];
        raw.extend_from_slice(&[7; SIGNAL_BBO_BINARY_LEN]);
        let payload = Bytes::from(raw);
        let out = append_bbo_spread(payload, "1,2,3");
        assert_eq!(&out[..3], &[1, 2, 3]);
        assert_eq!(u16::from_le_bytes([out[3], out[4]]), 5);
        assert_eq!(&out[5..10], b"1,2,3");
        assert_eq!(&out[10..], &[7; SIGNAL_BBO_BINARY_LEN]);
    }
}
