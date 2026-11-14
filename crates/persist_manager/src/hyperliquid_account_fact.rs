use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::service::ipc;
use log::{info, warn};
use mkt_parsers::msg::basic_account_msg::{
    split_basic_account_event, BasicAccountEventType, BASIC_ACCOUNT_EVENT_HEADER_LEN,
};
use mkt_parsers::msg::hyperliquid_account_msg::{
    HyperliquidBasicFillMsg, HyperliquidBasicOrderMsg, HyperliquidFactIdentity,
    HyperliquidFundingMsg, HyperliquidLedgerMsg, HyperliquidTwapHistoryMsg,
    HyperliquidTwapSliceFillMsg,
};
use mkt_parsers::msg::hyperliquid_native_msg::HyperliquidNativeEventMsg;
use persist_common::{
    hyperliquid_account_fact_value_digest, HyperliquidAccountFactAck,
    HYPERLIQUID_ACCOUNT_FACT_ACK_CHANNEL, HYPERLIQUID_ACCOUNT_FACT_ACK_MAX_BYTES,
    HYPERLIQUID_ACCOUNT_FACT_MAX_BYTES, HYPERLIQUID_ACCOUNT_FACT_RECORD_CHANNEL,
    HYPERLIQUID_ACCOUNT_FACT_STABLE_KEY_BYTES,
};

use crate::iceoryx::{create_sized_record_publisher, create_sized_record_subscriber};
use crate::polling::{PollStats, MAX_DRAIN_PER_CHANNEL};
use crate::runtime_common::get_timestamp_us;
use crate::storage::RocksDbStore;
use crate::sync::persist_with_outbox_sync;

pub(crate) const CF_HYPERLIQUID_ACCOUNT_FACT: &str = "hyperliquid_account_facts";
const ACK_PUBLISHER_PREFIX: &str = "persist_acks";
const PERSIST_RETRY_DELAY: Duration = Duration::from_millis(250);
const PENDING_FACT_CAPACITY: usize = 256;

pub fn required_column_families() -> &'static [&'static str] {
    &[CF_HYPERLIQUID_ACCOUNT_FACT]
}

pub struct HyperliquidAccountFactPersistor {
    subscriber: Subscriber<ipc::Service, [u8; HYPERLIQUID_ACCOUNT_FACT_MAX_BYTES], ()>,
    ack_publisher: Publisher<ipc::Service, [u8; HYPERLIQUID_ACCOUNT_FACT_ACK_MAX_BYTES], ()>,
    store: Arc<RocksDbStore>,
    sync_enabled: bool,
    pending: PendingFactQueue,
}

impl HyperliquidAccountFactPersistor {
    pub fn new(store: Arc<RocksDbStore>, sync_enabled: bool) -> Result<Self> {
        Ok(Self {
            subscriber: create_sized_record_subscriber(HYPERLIQUID_ACCOUNT_FACT_RECORD_CHANNEL)?,
            ack_publisher: create_sized_record_publisher(
                ACK_PUBLISHER_PREFIX,
                HYPERLIQUID_ACCOUNT_FACT_ACK_CHANNEL,
            )?,
            store,
            sync_enabled,
            pending: PendingFactQueue::default(),
        })
    }

    pub(crate) fn poll_available(&mut self) -> PollStats {
        let mut stats = PollStats::default();
        for _ in 0..MAX_DRAIN_PER_CHANNEL {
            match self.drive_pending(Instant::now()) {
                Ok(true) => continue,
                Ok(false) if !self.pending.is_empty() => break,
                Ok(false) => {}
                Err(err) => {
                    warn!("deliver Hyperliquid account fact failed; retained for retry: {err:#}");
                    stats.record_error();
                    break;
                }
            }
            if self.pending.is_full() {
                break;
            }
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    stats.record_received();
                    match decode_fact_record(sample.payload()) {
                        Ok(record) => {
                            if let Err(err) = self.pending.push(record.into_owned()) {
                                warn!("retain Hyperliquid account fact failed closed: {err:#}");
                                stats.record_error();
                                break;
                            }
                        }
                        Err(err) => {
                            // The sender retains this request until an ACK. Rejecting it here
                            // therefore cannot advance the sender's durable cursor.
                            warn!("reject invalid Hyperliquid account fact without ACK: {err:#}");
                            stats.record_error();
                            break;
                        }
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    warn!("Hyperliquid account fact receive error: {err}");
                    stats.record_error();
                    break;
                }
            }
        }
        stats
    }

    fn drive_pending(&mut self, now: Instant) -> Result<bool> {
        let store = self.store.clone();
        let sync_enabled = self.sync_enabled;
        let ack_publisher = &self.ack_publisher;
        let delivered = self.pending.drive_front(
            now,
            |record| {
                persist_with_outbox_sync(
                    &store,
                    CF_HYPERLIQUID_ACCOUNT_FACT,
                    &record.key,
                    &record.payload,
                    get_timestamp_us(),
                    sync_enabled,
                )
                .context("sync-write Hyperliquid fact and replication outbox")
            },
            |ack| publish_ack(ack_publisher, ack),
        )?;
        if let Some(record) = delivered {
            info!(
                "persisted and ACKed Hyperliquid account fact: event={:?} venue_time={} payload_len={}",
                record.event_type,
                record.venue_time,
                record.payload.len()
            );
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

fn publish_ack(
    publisher: &Publisher<ipc::Service, [u8; HYPERLIQUID_ACCOUNT_FACT_ACK_MAX_BYTES], ()>,
    ack: &HyperliquidAccountFactAck,
) -> Result<()> {
    let sample = publisher
        .loan_uninit()
        .context("loan Hyperliquid fact ACK sample")?;
    let sample = sample.write_payload(ack.to_ipc_payload());
    sample.send().context("send Hyperliquid fact ACK")?;
    Ok(())
}

#[derive(Debug)]
struct OwnedFactRecord {
    event_type: BasicAccountEventType,
    venue_time: i64,
    key: [u8; HYPERLIQUID_ACCOUNT_FACT_STABLE_KEY_BYTES],
    payload: Bytes,
    ack: HyperliquidAccountFactAck,
}

#[derive(Debug)]
struct PendingFactRecord {
    record: OwnedFactRecord,
    durable: bool,
}

#[derive(Debug)]
struct PendingFactQueue {
    entries: VecDeque<PendingFactRecord>,
    next_retry_at: Instant,
}

impl Default for PendingFactQueue {
    fn default() -> Self {
        Self {
            entries: VecDeque::new(),
            next_retry_at: Instant::now(),
        }
    }
}

impl PendingFactQueue {
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn is_full(&self) -> bool {
        self.entries.len() >= PENDING_FACT_CAPACITY
    }

    fn push(&mut self, record: OwnedFactRecord) -> Result<()> {
        if self.is_full() {
            anyhow::bail!(
                "Hyperliquid fact persistence queue is full: capacity={PENDING_FACT_CAPACITY}"
            );
        }
        self.entries.push_back(PendingFactRecord {
            record,
            durable: false,
        });
        Ok(())
    }

    fn drive_front<P, A>(
        &mut self,
        now: Instant,
        mut persist: P,
        mut publish_ack: A,
    ) -> Result<Option<OwnedFactRecord>>
    where
        P: FnMut(&OwnedFactRecord) -> Result<()>,
        A: FnMut(&HyperliquidAccountFactAck) -> Result<()>,
    {
        if self.entries.is_empty() || now < self.next_retry_at {
            return Ok(None);
        }

        if !self.entries.front().expect("checked non-empty").durable {
            let persist_result = persist(&self.entries.front().expect("checked non-empty").record);
            if let Err(err) = persist_result {
                self.next_retry_at = now + PERSIST_RETRY_DELAY;
                return Err(err);
            }
            self.entries.front_mut().expect("checked non-empty").durable = true;
        }

        let ack_result = publish_ack(&self.entries.front().expect("checked non-empty").record.ack);
        if let Err(err) = ack_result {
            self.next_retry_at = now + PERSIST_RETRY_DELAY;
            return Err(err);
        }
        self.next_retry_at = now;
        Ok(self.entries.pop_front().map(|pending| pending.record))
    }
}

#[derive(Debug)]
struct DecodedFactRecord<'a> {
    event_type: BasicAccountEventType,
    venue_time: i64,
    key: [u8; 36],
    payload: &'a [u8],
    identity: HyperliquidFactIdentity,
}

impl DecodedFactRecord<'_> {
    fn into_owned(self) -> OwnedFactRecord {
        let payload = Bytes::copy_from_slice(self.payload);
        let ack = HyperliquidAccountFactAck {
            account_hash: self.identity.account_hash,
            monitor_id: self.identity.monitor_id,
            fact_seq: self.identity.fact_seq,
            stable_key: self.key,
            value_digest: hyperliquid_account_fact_value_digest(&self.key, &payload),
        };
        OwnedFactRecord {
            event_type: self.event_type,
            venue_time: self.venue_time,
            key: self.key,
            payload,
            ack,
        }
    }
}

fn decode_fact_record(payload: &[u8]) -> Result<DecodedFactRecord<'_>> {
    let (event_type, _, body) =
        split_basic_account_event(payload).context("invalid basic account event envelope")?;
    let (venue_time, stable_key, identity) = match event_type {
        BasicAccountEventType::HyperliquidNativeEvent => {
            let msg = HyperliquidNativeEventMsg::from_bytes(body)?;
            (msg.observed_at_us, msg.stable_venue_key(), msg.identity)
        }
        BasicAccountEventType::OrderUpdate => {
            let msg = HyperliquidBasicOrderMsg::from_bytes(body)
                .context("decode Hyperliquid order lifecycle fact")?;
            (msg.event_time, msg.stable_venue_key(), msg.fact_identity())
        }
        BasicAccountEventType::HyperliquidFill => {
            let msg = HyperliquidBasicFillMsg::from_bytes(body)
                .context("decode Hyperliquid fill fact")?;
            (msg.event_time, msg.stable_venue_key(), msg.fact_identity())
        }
        BasicAccountEventType::HyperliquidFunding => {
            let msg = HyperliquidFundingMsg::from_bytes(body)
                .context("decode Hyperliquid funding fact")?;
            (msg.event_time, msg.stable_venue_key(), msg.fact_identity())
        }
        BasicAccountEventType::HyperliquidLedger => {
            let msg =
                HyperliquidLedgerMsg::from_bytes(body).context("decode Hyperliquid ledger fact")?;
            (msg.event_time, msg.stable_venue_key(), msg.fact_identity())
        }
        BasicAccountEventType::HyperliquidTwapSliceFill => {
            let msg = HyperliquidTwapSliceFillMsg::from_bytes(body)
                .context("decode Hyperliquid TWAP slice association fact")?;
            (msg.event_time, msg.stable_venue_key(), msg.fact_identity())
        }
        BasicAccountEventType::HyperliquidTwapHistory => {
            let msg = HyperliquidTwapHistoryMsg::from_bytes(body)
                .context("decode Hyperliquid TWAP history fact")?;
            (msg.event_time, msg.stable_venue_key(), msg.fact_identity())
        }
        other => anyhow::bail!("unsupported Hyperliquid account fact event {other:?}"),
    };
    let mut key = [0_u8; 36];
    key[..4].copy_from_slice(&(event_type as u32).to_be_bytes());
    key[4..].copy_from_slice(&stable_key);
    let used_len = BASIC_ACCOUNT_EVENT_HEADER_LEN
        .checked_add(body.len())
        .context("Hyperliquid fact envelope length overflow")?;
    if identity.monitor_id == 0 || identity.fact_seq == 0 {
        anyhow::bail!("Hyperliquid account fact has zero delivery identity");
    }
    Ok(DecodedFactRecord {
        event_type,
        venue_time,
        key,
        payload: &payload[..used_len],
        identity,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::basic_account_msg::{BasicAccountEventMsg, BasicAccountScope};
    use mkt_parsers::msg::hyperliquid_account_msg::{
        HyperliquidBasicOrderMsg, HyperliquidFactIdentity, HyperliquidTwapHistoryMsg,
        HyperliquidTwapSliceFillMsg,
    };
    use std::cell::Cell;

    fn wrap(event_type: BasicAccountEventType, body: bytes::Bytes) -> bytes::Bytes {
        BasicAccountEventMsg::create(event_type, BasicAccountScope::HyperliquidUnified, body)
            .to_bytes()
    }

    #[test]
    fn native_fact_preserves_payload_and_exact_ack_across_epochs() {
        use mkt_parsers::msg::hyperliquid_native_msg::HyperliquidNativeSource;
        let msg = HyperliquidNativeEventMsg::create(
            1_725_000_000_000_000, HyperliquidNativeSource::Liquidation, "lid:7".into(),
            &serde_json::json!({"lid":7,"liquidated_account_value":"-1.000000001","extra":{"a":true}}),
        ).unwrap();
        let encode = |monitor_id, fact_seq| {
            wrap(
                BasicAccountEventType::HyperliquidNativeEvent,
                msg.clone()
                    .with_fact_identity(HyperliquidFactIdentity {
                        account_hash: [7; 32],
                        monitor_id,
                        fact_seq,
                    })
                    .to_bytes(),
            )
        };
        let first = encode(1, 2);
        let second = encode(3, 4);
        let record = decode_fact_record(&first).unwrap().into_owned();
        let other = decode_fact_record(&second).unwrap().into_owned();
        assert_eq!(record.key, other.key);
        assert_ne!(record.ack.value_digest, other.ack.value_digest);
        assert_eq!(record.payload, first);
        assert_eq!(record.ack.fact_seq, 2);
        let mut padded = first.to_vec();
        padded.resize(HYPERLIQUID_ACCOUNT_FACT_MAX_BYTES, 0);
        let decoded = decode_fact_record(&padded).unwrap().into_owned();
        assert_eq!(decoded.payload, first);
        assert_eq!(decoded.ack.value_digest, record.ack.value_digest);
    }

    #[test]
    fn funding_key_is_stable_across_monitor_epochs() {
        let base = HyperliquidFundingMsg::create(
            1_725_000_000_000,
            "BTC".to_string(),
            "-1.25".to_string(),
            "2.0".to_string(),
            "0.0001".to_string(),
        );
        let first = base.clone().with_fact_identity(HyperliquidFactIdentity {
            account_hash: [7; 32],
            monitor_id: 11,
            fact_seq: 2,
        });
        let second = base.with_fact_identity(HyperliquidFactIdentity {
            account_hash: [7; 32],
            monitor_id: 12,
            fact_seq: 99,
        });
        let first = wrap(BasicAccountEventType::HyperliquidFunding, first.to_bytes());
        let second = wrap(BasicAccountEventType::HyperliquidFunding, second.to_bytes());

        let first = decode_fact_record(&first).unwrap();
        let second = decode_fact_record(&second).unwrap();
        assert_eq!(first.key, second.key);
        assert_ne!(first.payload, second.payload);
    }

    #[test]
    fn order_and_twap_facts_are_accepted_with_epoch_stable_keys() {
        let identity = |monitor_id, fact_seq| HyperliquidFactIdentity {
            account_hash: [8; 32],
            monitor_id,
            fact_seq,
        };
        let order = HyperliquidBasicOrderMsg::create(
            13,
            1_725_000_000_000,
            "BTCUSDC".to_string(),
            71,
            72,
            "0x00000000000000000000000000000048".to_string(),
            1,
            1,
            0,
            1,
            1,
            60_000.0,
            1.0,
            0.0,
            "open".to_string(),
        );
        let first = wrap(
            BasicAccountEventType::OrderUpdate,
            order.clone().with_fact_identity(identity(1, 1)).to_bytes(),
        );
        let second = wrap(
            BasicAccountEventType::OrderUpdate,
            order.with_fact_identity(identity(2, 9)).to_bytes(),
        );
        assert_eq!(
            decode_fact_record(&first).unwrap().key,
            decode_fact_record(&second).unwrap().key
        );

        let slice = HyperliquidTwapSliceFillMsg::create(
            13,
            1_725_000_000_001,
            "BTC".to_string(),
            "BTCUSDC".to_string(),
            71,
            81,
            "0xfill".to_string(),
            91,
        )
        .with_fact_identity(identity(1, 2));
        assert_eq!(
            decode_fact_record(&wrap(
                BasicAccountEventType::HyperliquidTwapSliceFill,
                slice.to_bytes()
            ))
            .unwrap()
            .event_type,
            BasicAccountEventType::HyperliquidTwapSliceFill
        );

        let history = HyperliquidTwapHistoryMsg::create(
            1_788_587_622,
            Some(91),
            "0x1111111111111111111111111111111111111111".to_string(),
            "BTC".to_string(),
            "B".to_string(),
            "1.0".to_string(),
            "0.5".to_string(),
            "30000.0".to_string(),
            60,
            false,
            true,
            1_788_581_510_182,
            None,
            None,
            None,
            "activated".to_string(),
            None,
        )
        .with_fact_identity(identity(1, 3));
        assert_eq!(
            decode_fact_record(&wrap(
                BasicAccountEventType::HyperliquidTwapHistory,
                history.to_bytes()
            ))
            .unwrap()
            .event_type,
            BasicAccountEventType::HyperliquidTwapHistory
        );
    }

    #[test]
    fn rejects_state_rows_on_the_factual_channel() {
        let payload = BasicAccountEventMsg::create(
            BasicAccountEventType::BalanceUpdate,
            BasicAccountScope::HyperliquidUnified,
            bytes::Bytes::new(),
        )
        .to_bytes();
        assert!(decode_fact_record(&payload)
            .unwrap_err()
            .to_string()
            .contains("unsupported Hyperliquid account fact"));
    }

    #[test]
    fn pending_fact_is_retained_until_sync_store_and_ack_both_succeed() {
        let stable_key = [5_u8; HYPERLIQUID_ACCOUNT_FACT_STABLE_KEY_BYTES];
        let payload = Bytes::from_static(b"durable-fact");
        let record = OwnedFactRecord {
            event_type: BasicAccountEventType::HyperliquidFunding,
            venue_time: 17,
            key: stable_key,
            ack: HyperliquidAccountFactAck {
                account_hash: [4; 32],
                monitor_id: 8,
                fact_seq: 9,
                stable_key,
                value_digest: hyperliquid_account_fact_value_digest(&stable_key, &payload),
            },
            payload,
        };
        let mut queue = PendingFactQueue::default();
        queue.push(record).unwrap();
        let now = Instant::now();
        let persist_calls = Cell::new(0);
        let ack_calls = Cell::new(0);

        assert!(queue
            .drive_front(
                now,
                |_| {
                    persist_calls.set(persist_calls.get() + 1);
                    anyhow::bail!("store unavailable")
                },
                |_| {
                    ack_calls.set(ack_calls.get() + 1);
                    Ok(())
                },
            )
            .is_err());
        assert_eq!(queue.entries.len(), 1);
        assert!(!queue.entries.front().unwrap().durable);
        assert_eq!(persist_calls.get(), 1);
        assert_eq!(ack_calls.get(), 0);

        assert!(queue
            .drive_front(
                now + PERSIST_RETRY_DELAY / 2,
                |_| {
                    persist_calls.set(persist_calls.get() + 1);
                    Ok(())
                },
                |_| {
                    ack_calls.set(ack_calls.get() + 1);
                    Ok(())
                },
            )
            .unwrap()
            .is_none());
        assert_eq!(persist_calls.get(), 1);
        assert_eq!(ack_calls.get(), 0);

        assert!(queue
            .drive_front(
                now + PERSIST_RETRY_DELAY,
                |_| {
                    persist_calls.set(persist_calls.get() + 1);
                    Ok(())
                },
                |_| {
                    ack_calls.set(ack_calls.get() + 1);
                    anyhow::bail!("ACK transport unavailable")
                },
            )
            .is_err());
        assert_eq!(queue.entries.len(), 1);
        assert!(queue.entries.front().unwrap().durable);
        assert_eq!(persist_calls.get(), 2);
        assert_eq!(ack_calls.get(), 1);

        let delivered = queue
            .drive_front(
                now + PERSIST_RETRY_DELAY * 2,
                |_| {
                    persist_calls.set(persist_calls.get() + 1);
                    Ok(())
                },
                |_| {
                    ack_calls.set(ack_calls.get() + 1);
                    Ok(())
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(delivered.payload, Bytes::from_static(b"durable-fact"));
        assert!(queue.is_empty());
        assert_eq!(
            persist_calls.get(),
            2,
            "durable value must not be rewritten"
        );
        assert_eq!(ack_calls.get(), 2);
    }

    #[test]
    fn decoded_ack_digest_covers_the_exact_trimmed_value() {
        let msg = HyperliquidFundingMsg::create(
            1_725_000_000_000,
            "ETH".to_string(),
            "0.2".to_string(),
            "1.0".to_string(),
            "0.0002".to_string(),
        )
        .with_fact_identity(HyperliquidFactIdentity {
            account_hash: [2; 32],
            monitor_id: 3,
            fact_seq: 4,
        });
        let wrapped = wrap(BasicAccountEventType::HyperliquidFunding, msg.to_bytes());
        let mut padded = wrapped.to_vec();
        padded.resize(HYPERLIQUID_ACCOUNT_FACT_MAX_BYTES, 0);
        let owned = decode_fact_record(&padded).unwrap().into_owned();
        assert_eq!(owned.payload.as_ref(), wrapped.as_ref());
        assert_eq!(
            owned.ack.value_digest,
            hyperliquid_account_fact_value_digest(&owned.key, wrapped.as_ref())
        );
        assert_ne!(
            owned.ack.value_digest,
            hyperliquid_account_fact_value_digest(&owned.key, &padded)
        );
    }
}
