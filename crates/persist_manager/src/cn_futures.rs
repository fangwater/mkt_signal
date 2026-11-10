//! Embedded domestic order persistence. One writer owns the store; no IPC/runtime dependency.
use crate::{RocksDbStore, RocksDbTuning};
use anyhow::{ensure, Context, Result};
use persist_common::cn_futures::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, path::Path};

pub const META: &str = "cn_trading_meta";
pub const EVENTS: &str = "cn_trading_events";
pub const ORDERS: &str = "cn_uniform_orders";
pub const TRADES: &str = "cn_trade_facts";
pub const COLUMN_FAMILIES: &[&str] = &[META, EVENTS, ORDERS, TRADES];

#[derive(Debug, Serialize, Deserialize)]
pub struct Evidence {
    pub sequence: u64,
    pub received_us: u64,
    pub value: Value,
    /// A bad projection never overwrites good state; raw evidence remains durable.
    pub projection_error: Option<String>,
}

type NativeWrite = (String, Vec<u8>, Vec<u8>);
type OrderUpdates = HashMap<PersistedOrderKey, NativeOrderRecord>;
type Projection = (Vec<NativeWrite>, OrderUpdates);

pub struct NativeStore {
    store: RocksDbStore,
    sequence: u64,
    orders: HashMap<PersistedOrderKey, NativeOrderRecord>,
}
fn key<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(value)?)
}
fn write<T: Serialize>(cf: &str, key: Vec<u8>, value: &T) -> Result<(String, Vec<u8>, Vec<u8>)> {
    Ok((cf.to_string(), key, serde_json::to_vec(value)?))
}

impl NativeStore {
    pub fn open(path: &Path, identity: &StoreIdentity) -> Result<Self> {
        ensure!(path.is_absolute(), "native RocksDB path must be absolute");
        let store = RocksDbStore::open_with_existing_cfs_and_tuning(
            path.to_str().context("non-UTF8 native RocksDB path")?,
            &COLUMN_FAMILIES
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
            true,
            &RocksDbTuning {
                block_cache_bytes: Some(8 * 1024 * 1024),
                write_buffer_size_bytes: Some(1024 * 1024),
                db_write_buffer_size_bytes: Some(16 * 1024 * 1024),
                max_write_buffer_number: Some(2),
            },
        )?;
        if let Some(existing) = store.get(META, b"identity")? {
            ensure!(
                serde_json::from_slice::<StoreIdentity>(&existing)? == *identity,
                "native database account/adapter/endpoint identity mismatch"
            );
        } else {
            ensure!(
                store.scan(ORDERS, None, false, Some(1))?.is_empty(),
                "native database missing account identity"
            );
            store.put_sync(META, b"identity", &serde_json::to_vec(identity)?)?;
        }
        let sequence = store
            .get(META, b"sequence")?
            .map(|s| serde_json::from_slice(&s))
            .transpose()?
            .unwrap_or(0);
        let orders = store
            .scan(ORDERS, None, false, None)?
            .into_iter()
            .map(|(_, v)| -> Result<_> {
                let r: NativeOrderRecord = serde_json::from_slice(&v)?;
                Ok((r.identity.clone(), r))
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            store,
            sequence,
            orders,
        })
    }

    pub fn reference_floors(&self) -> HashMap<(String, String), u64> {
        let mut floors = HashMap::new();
        for identity in self.orders.keys() {
            if let Ok(reference) = identity.native.order_ref.parse::<u64>() {
                if let Some(next) = reference.checked_add(1) {
                    let floor = floors
                        .entry((
                            identity.trading_day.clone(),
                            identity.native.session.clone(),
                        ))
                        .or_insert(1);
                    *floor = (*floor).max(next);
                }
            }
        }
        floors
    }

    /// Evidence, dedup facts and current order state commit in one synced WAL batch.
    pub fn append(
        &mut self,
        received_us: u64,
        value: Value,
        mutation: NativeMutation,
    ) -> Result<()> {
        let sequence = self
            .sequence
            .checked_add(1)
            .context("native evidence sequence exhausted")?;
        let mut evidence = Evidence {
            sequence,
            received_us,
            value,
            projection_error: None,
        };
        let projected = self.project(sequence, mutation);
        let (mut writes, updates) = match projected {
            Ok(result) => result,
            Err(error) => {
                evidence.projection_error = Some(error.to_string());
                self.store.put_many_sync(&[
                    write(EVENTS, sequence.to_be_bytes().to_vec(), &evidence)?,
                    write(META, b"sequence".to_vec(), &sequence)?,
                ])?;
                self.sequence = sequence;
                return Err(error);
            }
        };
        writes.push(write(EVENTS, sequence.to_be_bytes().to_vec(), &evidence)?);
        writes.push(write(META, b"sequence".to_vec(), &sequence)?);
        self.store.put_many_sync(&writes)?;
        self.sequence = sequence;
        self.orders.extend(updates);
        Ok(())
    }

    fn project(&self, seq: u64, mutation: NativeMutation) -> Result<Projection> {
        let mut updates = HashMap::new();
        for mut record in mutation.orders {
            ensure!(
                record.identity.native == record.order.key
                    && record.identity.venue == record.order.request.venue,
                "inconsistent persisted native order identity"
            );
            ensure!(
                record.order.request.lots > 0
                    && record.order.filled >= 0
                    && record.order.filled <= record.order.request.lots,
                "invalid persisted native order quantity"
            );
            if let Some(old) = self.orders.get(&record.identity) {
                ensure!(
                    old.order.request.symbol == record.order.request.symbol
                        && old.order.request.side == record.order.request.side
                        && old.order.request.offset == record.order.request.offset
                        && old.order.request.lots == record.order.request.lots
                        && old.order.request.price_type == record.order.request.price_type
                        && old.order.request.time_in_force == record.order.request.time_in_force,
                    "native identity reused with conflicting order terms"
                );
                ensure!(
                    old.order.system_id.is_empty() || record.order.system_id == old.order.system_id,
                    "native order system identity changed"
                );
                ensure!(
                    record.order.filled >= old.order.filled
                        && record.order.reported_filled >= old.order.reported_filled,
                    "durable native order fill quantity regressed"
                );
                ensure!(
                    !old.order.status.terminal() || record.order.status == old.order.status,
                    "durable terminal native order regressed"
                );
                record.first_received_us = old.first_received_us;
                record.locally_submitted |= old.locally_submitted;
                record.submitted_intent = old.submitted_intent.clone().or(record.submitted_intent);
                record.instrument = record.instrument.or_else(|| old.instrument.clone());
                if old.send_state == SendState::Observed {
                    record.send_state = SendState::Observed;
                }
            }
            if mutation.observed.contains(&record.identity) {
                record.send_state = SendState::Observed;
            }
            record.evidence_seq = seq;
            updates.insert(record.identity.clone(), record);
        }
        if let Some(day) = mutation.reconciliation_day {
            for (identity, old) in &self.orders {
                let unresolved = old.locally_submitted
                    && (!old.order.status.terminal()
                        || matches!(
                            old.send_state,
                            SendState::Prepared
                                | SendState::Uncertain
                                | SendState::SubmittedLocally
                        ));
                if unresolved {
                    ensure!(identity.trading_day == day && updates.contains_key(identity),
                        "durable local order absent from complete recovery: {} {} session={} ref={}; reconciliation required",
                        identity.trading_day, identity.venue.as_str(), identity.native.session, identity.native.order_ref);
                }
            }
        }
        let mut writes = vec![];
        let mut batch_trades: HashMap<Vec<u8>, NativeTradeRecord> = HashMap::new();
        for mut trade in mutation.trades {
            ensure!(
                trade.fill.lots > 0 && trade.fill.price.is_finite() && trade.fill.price > 0.0,
                "invalid persisted factual fill"
            );
            let identity = key(&trade.fill.identity)?;
            let old = if let Some(old) = batch_trades.get(&identity) {
                Some(old.clone())
            } else {
                self.store
                    .get(TRADES, &identity)?
                    .map(|v| serde_json::from_slice(&v))
                    .transpose()?
            };
            if let Some(old) = old {
                let a = &old.fill;
                let b = &trade.fill;
                ensure!(
                    a.identity == b.identity
                        && a.system_id == b.system_id
                        && a.offset == b.offset
                        && a.lots == b.lots
                        && a.price == b.price
                        && a.order_ref == b.order_ref
                        && (a.order_key.is_none()
                            || b.order_key.is_none()
                            || a.order_key == b.order_key),
                    "conflicting duplicate native trade fact"
                );
                ensure!(
                    old.commission.is_none()
                        || trade.commission.is_none()
                        || old.commission == trade.commission,
                    "conflicting native trade commission"
                );
                ensure!(
                    old.native_time.is_none()
                        || trade.native_time.is_none()
                        || old.native_time == trade.native_time,
                    "conflicting native trade time"
                );
                trade.fill.order_key = old.fill.order_key.or(trade.fill.order_key);
                trade.commission = old.commission.or(trade.commission);
                trade.commission_currency = old.commission_currency.or(trade.commission_currency);
                trade.native_time = old.native_time.or(trade.native_time);
                trade.received_us = old.received_us;
                trade.evidence_seq = old.evidence_seq;
            } else {
                trade.evidence_seq = seq;
            }
            batch_trades.insert(identity, trade);
        }
        for (key, trade) in batch_trades {
            writes.push(write(TRADES, key, &trade)?);
        }
        for (identity, order) in &updates {
            writes.push(write(ORDERS, key(identity)?, order)?);
        }
        Ok((writes, updates))
    }
}

/// Uses the same store API as existing exports. The trading process must be stopped,
/// or callers can open a RocksDB secondary and pass it to `export_store`.
pub fn export(path: &Path, output: &Path) -> Result<()> {
    let store =
        RocksDbStore::open_read_only(path.to_str().context("non-UTF8 path")?, COLUMN_FAMILIES)?;
    export_store(&store, output)
}
pub fn export_store(store: &RocksDbStore, output: &Path) -> Result<()> {
    use std::io::Write;
    std::fs::create_dir_all(output)?;
    for cf in COLUMN_FAMILIES {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(output.join(format!("{cf}.jsonl")))?;
        // Stream bounded batches; evidence history can grow far beyond memory.
        store.scan_range_batches(cf, b"", &[255], 512, |entries| {
            for (_, value) in entries {
                file.write_all(&value)?;
                file.write_all(b"\n")?;
            }
            Ok(())
        })?;
        file.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static ID: AtomicU64 = AtomicU64::new(0);
    struct Temp(std::path::PathBuf);
    impl Temp {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                    "cn-persist-test-{}-{}-{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos(),
                    ID.fetch_add(1, Ordering::Relaxed)
                )))
        }
        fn open(&self) -> NativeStore {
            NativeStore::open(&self.0, &identity()).unwrap()
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn identity() -> StoreIdentity {
        StoreIdentity {
            adapter: "yida".into(),
            account_hash: "account-scope-a".into(),
        }
    }
    fn order() -> NativeOrderRecord {
        let key = OrderKey {
            session: "yida:20261110:group:3".into(),
            order_ref: "17".into(),
        };
        let intent = Intent {
            venue: Venue::Shfe,
            symbol: "cu2612C106000".into(),
            side: Side::Buy,
            offset: Offset::Open,
            lots: 2,
            price_type: PriceType::Market,
            price_ticks: 0,
            time_in_force: TimeInForce::Fak,
            execution_route: Some(ExecutionRoute {
                mode: ConnectionSelection::Fixed,
                connection_id: Some(2),
            }),
        };
        NativeOrderRecord {
            identity: PersistedOrderKey {
                trading_day: "20261110".into(),
                venue: Venue::Shfe,
                native: key.clone(),
            },
            order: Order {
                request: intent.clone(),
                key,
                system_id: String::new(),
                limit_price: 0.0,
                status: OrderStatus::Pending,
                filled: 0,
                reported_filled: 0,
                cancelled: 0,
                reserved_today: 0,
                reserved_yesterday: 0,
                local_cash_held: 50000.0,
                cash_reserved_per_lot: 25000.0,
                cancel_pending: false,
                owned: true,
            },
            submitted_intent: Some(intent),
            locally_submitted: true,
            send_state: SendState::Prepared,
            instrument: None,
            first_received_us: 100,
            updated_us: 100,
            evidence_seq: 0,
        }
    }
    fn trade() -> NativeTradeRecord {
        NativeTradeRecord {
            fill: Fill {
                identity: TradeIdentity {
                    venue: Venue::Shfe,
                    symbol: "cu2612C106000".into(),
                    trading_day: "20261110".into(),
                    trade_id: "9007199254740997".into(),
                    side: Side::Buy,
                },
                order_ref: "17".into(),
                order_key: Some(order().identity.native),
                system_id: "9007199254740993".into(),
                offset: Offset::Open,
                lots: 1,
                price: 1320.0,
            },
            commission: Some(52.660000000000004),
            commission_currency: Some("CNY".into()),
            native_time: Some(110001234),
            received_us: 110,
            evidence_seq: 0,
        }
    }
    fn update(record: NativeOrderRecord) -> NativeMutation {
        NativeMutation {
            orders: vec![record],
            ..Default::default()
        }
    }
    fn append(store: &mut NativeStore, mutation: NativeMutation) -> Result<()> {
        store.append(123, serde_json::json!({"type":"fixture"}), mutation)
    }
    fn read_order(store: &NativeStore, r: &NativeOrderRecord) -> NativeOrderRecord {
        serde_json::from_slice(
            &store
                .store
                .get(ORDERS, &key(&r.identity).unwrap())
                .unwrap()
                .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn out_of_order_facts_deduplicate_reopen_and_export_with_native_terms() {
        let path = Temp::new();
        {
            let mut store = path.open();
            let mut r = order();
            append(&mut store, update(r.clone())).unwrap();
            // Factual fill can arrive before the exchange acknowledgement.
            append(
                &mut store,
                NativeMutation {
                    trades: vec![trade()],
                    ..Default::default()
                },
            )
            .unwrap();
            r.order.system_id = trade().fill.system_id;
            r.order.status = OrderStatus::Cancelled;
            r.order.filled = 1;
            r.order.reported_filled = 1;
            r.order.cancelled = 1;
            let mut mutation = update(r.clone());
            mutation.observed.push(r.identity.clone());
            append(&mut store, mutation).unwrap();
            let mut replay = trade();
            replay.received_us = 999;
            replay.commission = None;
            replay.native_time = None;
            append(
                &mut store,
                NativeMutation {
                    trades: vec![replay],
                    ..Default::default()
                },
            )
            .unwrap();
        }
        let store = path.open();
        assert_eq!(
            store.store.scan(TRADES, None, false, None).unwrap().len(),
            1
        );
        let rows = store.store.scan(TRADES, None, false, None).unwrap();
        let t: NativeTradeRecord = serde_json::from_slice(&rows[0].1).unwrap();
        assert_eq!(t.commission, Some(52.660000000000004));
        assert_eq!(t.native_time, Some(110001234));
        assert_eq!(t.received_us, 110);
        let r = read_order(&store, &order());
        assert_eq!(r.order.filled, 1);
        assert_eq!(r.order.cancelled, 1);
        assert_eq!(r.send_state, SendState::Observed);
        assert_eq!(r.submitted_intent.unwrap().time_in_force, TimeInForce::Fak);
        let output = Temp::new();
        export_store(&store.store, &output.0).unwrap();
        let exported = std::fs::read_to_string(output.0.join(format!("{TRADES}.jsonl"))).unwrap();
        assert!(exported.contains("9007199254740997"));
        assert_eq!(exported.lines().count(), 1);
        assert!(export_store(&store.store, &output.0).is_err()); // never overwrite an audit export
    }
    #[test]
    fn conflicting_duplicate_retains_evidence_and_atomic_state() {
        let path = Temp::new();
        let mut store = path.open();
        append(&mut store, update(order())).unwrap();
        append(
            &mut store,
            NativeMutation {
                trades: vec![trade()],
                ..Default::default()
            },
        )
        .unwrap();
        let mut bad = trade();
        bad.fill.price = 999.0;
        let mut r = order();
        r.order.filled = 1;
        let mut mutation = update(r);
        mutation.trades.push(bad);
        assert!(append(&mut store, mutation)
            .unwrap_err()
            .to_string()
            .contains("conflicting duplicate"));
        assert_eq!(read_order(&store, &order()).order.filled, 0);
        let evidence = store.store.scan(EVENTS, None, true, Some(1)).unwrap();
        let evidence: Evidence = serde_json::from_slice(&evidence[0].1).unwrap();
        assert!(evidence.projection_error.is_some());
        drop(store);
        let store = path.open();
        assert_eq!(store.sequence, 3);
        assert_eq!(
            store.store.scan(TRADES, None, false, None).unwrap().len(),
            1
        );
    }
    #[test]
    fn durable_unconfirmed_request_survives_reopen_and_blocks_missing_snapshot() {
        let path = Temp::new();
        {
            let mut store = path.open();
            append(&mut store, update(order())).unwrap();
        }
        let mut store = path.open();
        let missing = NativeMutation {
            reconciliation_day: Some("20261110".into()),
            ..Default::default()
        };
        assert!(append(&mut store, missing)
            .unwrap_err()
            .to_string()
            .contains("absent from complete recovery"));
        let mut r = order();
        r.order.owned = false;
        r.order.status = OrderStatus::Working;
        r.order.system_id = "64".into();
        r.submitted_intent = None;
        r.locally_submitted = false;
        r.send_state = SendState::Observed;
        let mut recovered = update(r.clone());
        recovered.reconciliation_day = Some("20261110".into());
        recovered.observed.push(r.identity.clone());
        append(&mut store, recovered).unwrap();
        let r = read_order(&store, &r);
        assert!(r.locally_submitted);
        assert!(!r.order.owned);
        assert_eq!(r.submitted_intent.unwrap().time_in_force, TimeInForce::Fak);
    }
    #[test]
    fn same_reference_is_separate_across_group_venue_and_day() {
        let path = Temp::new();
        let mut store = path.open();
        let a = order();
        let mut b = a.clone();
        b.identity.native.session = "yida:20261110:group:4".into();
        b.order.key = b.identity.native.clone();
        let mut c = a.clone();
        c.identity.venue = Venue::Ine;
        c.order.request.venue = Venue::Ine;
        c.order.request.symbol = "sc2612C640".into();
        let mut d = a.clone();
        d.identity.trading_day = "20261111".into();
        append(
            &mut store,
            NativeMutation {
                orders: vec![a, b, c, d],
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(store.orders.len(), 4);
        assert!(append(
            &mut store,
            NativeMutation {
                reconciliation_day: Some("20261112".into()),
                ..Default::default()
            }
        )
        .is_err());
    }
    #[test]
    fn identity_binding_rejects_another_account_or_adapter() {
        let path = Temp::new();
        drop(path.open());
        let mut wrong = identity();
        wrong.account_hash = "other".into();
        assert!(NativeStore::open(&path.0, &wrong).is_err());
        wrong = identity();
        wrong.adapter = "ctp".into();
        assert!(NativeStore::open(&path.0, &wrong).is_err());
    }
    #[test]
    fn recovery_rejects_changed_request_terms_and_regressing_facts() {
        let path = Temp::new();
        let mut store = path.open();
        let original = order();
        append(&mut store, update(original.clone())).unwrap();
        let mut changed = original.clone();
        changed.order.request.time_in_force = TimeInForce::Day;
        assert!(append(&mut store, update(changed)).is_err());
        let mut changed = original;
        changed.order.request.price_type = PriceType::Limit;
        assert!(append(&mut store, update(changed)).is_err());
        let mut r = order();
        r.order.filled = 2;
        r.order.reported_filled = 2;
        r.order.status = OrderStatus::Filled;
        r.send_state = SendState::Observed;
        append(&mut store, update(r.clone())).unwrap();
        let mut stale = r.clone();
        stale.order.filled = 0;
        assert!(append(&mut store, update(stale)).is_err());
        let mut stale = r;
        stale.order.status = OrderStatus::Working;
        assert!(append(&mut store, update(stale)).is_err());
    }
}
