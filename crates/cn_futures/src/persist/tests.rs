use super::*;
use crate::model::*;
use persist_common::{native_codec, uniform_fill, UnifiedOrderRecord};
use std::rc::Rc;

/// Test stand-in for the iceoryx publisher: keeps every encoded record.
pub(crate) type Records = Rc<RefCell<Vec<Vec<u8>>>>;
struct MemorySink(Records);
impl RecordSink for MemorySink {
    fn publish(&self, record: &[u8]) -> Result<()> {
        self.0.borrow_mut().push(record.to_vec());
        Ok(())
    }
}

fn absent_database() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "cn-persist-absent-{}-{}",
        std::process::id(),
        now_us()
    ))
}

pub(crate) fn channel(redactions: Vec<String>) -> (PersistChannel, Records) {
    channel_at(&absent_database(), redactions)
}

fn channel_at(database: &Path, redactions: Vec<String>) -> (PersistChannel, Records) {
    let records = Records::default();
    let channel =
        PersistChannel::start(database, redactions, Box::new(MemorySink(records.clone()))).unwrap();
    (channel, records)
}

/// A channel whose publisher always fails, as when shared memory is unavailable.
pub(crate) fn broken_channel() -> PersistChannel {
    struct Broken;
    impl RecordSink for Broken {
        fn publish(&self, _: &[u8]) -> Result<()> {
            anyhow::bail!("no shared memory")
        }
    }
    PersistChannel::start(&absent_database(), vec![], Box::new(Broken)).unwrap()
}

/// Trader-log lines of every displayed record.
pub(crate) fn displayed(records: &Records) -> Vec<Value> {
    records
        .borrow()
        .iter()
        .filter_map(|record| materialize(record).unwrap().display)
        .collect()
}

/// Stored bytes per column family, as persist_manager would append them.
pub(crate) fn stored(records: &Records, cf: &str) -> Vec<Vec<u8>> {
    records
        .borrow()
        .iter()
        .filter_map(|record| {
            let row = materialize(record).unwrap();
            row.store
                .filter(|(name, _)| *name == cf)
                .map(|(_, bytes)| bytes.to_vec())
        })
        .collect()
}

pub(crate) fn uniform_orders(records: &Records) -> Vec<UnifiedOrderRecord> {
    stored(records, store_cf::ORDERS)
        .iter()
        .map(|bytes| uniform_fill::decode(bytes).unwrap().record)
        .collect()
}

use persist_common::cn_futures::store_cf;

fn rule() -> Instrument {
    Instrument {
        venue: Venue::Shfe,
        symbol: "rb2701".into(),
        price_tick: 1.0,
        contract_multiple: 10,
        expiry_date: 20270115,
        commission: Some(CommissionRate {
            open_by_money: 0.0001,
            open_by_volume: 0.0,
            close_by_money: 0.0001,
            close_by_volume: 0.0,
            close_today_by_money: 0.0002,
            close_today_by_volume: 0.0,
        }),
        margin: None,
        min_lots: 1,
        max_lots: 100,
        market_lots: None,
        active: true,
        cash_budget_per_open_lot: 100000.0,
        option: None,
    }
}

fn core() -> crate::core::TradingCore {
    let mut core = crate::core::TradingCore::new(
        RiskLimits {
            max_order_lots: Some(10),
            max_position_lots_per_side: Some(10),
            max_working_lots: Some(10),
            max_actions_per_second: 10,
            minimum_cash_remaining: 0.0,
            enable_fak_fok: true,
            close_priority: ClosePriority::TodayFirst,
        },
        crate::yida::scope(),
    )
    .unwrap();
    core.restore(Snapshot {
        trading_day: "20261120".into(),
        session: "yida:20261120:group:3".into(),
        max_order_ref: 16,
        available_cash: 10_000_000.0,
        instruments: vec![rule()],
        positions: vec![],
        orders: vec![],
        trades: vec![],
    })
    .unwrap();
    core
}

fn intent(lots: i32) -> Intent {
    Intent {
        venue: Venue::Shfe,
        symbol: "rb2701".into(),
        side: Side::Buy,
        offset: Offset::Open,
        lots,
        price_type: PriceType::Limit,
        price_ticks: 3100,
        time_in_force: TimeInForce::Day,
        execution_route: None,
    }
}

fn fill(p: &PreparedOrder, trade_id: &str, lots: i32) -> Fill {
    Fill {
        identity: TradeIdentity {
            venue: Venue::Shfe,
            symbol: "rb2701".into(),
            trading_day: "20261120".into(),
            trade_id: Text::new(trade_id).unwrap(),
            side: Side::Buy,
        },
        order_ref: p.key.order_ref,
        order_key: Some(p.key),
        system_id: "77".into(),
        offset: Offset::Open,
        lots,
        price: 3100.0,
    }
}

#[test]
fn ctp_events_are_trader_log_only_and_errors_are_redacted_on_the_exec_side() {
    let (channel, records) = channel(vec!["fixture-secret".into()]);
    let mut hot = crate::ctp::HotEvent {
        kind: crate::ctp::ERROR,
        error: 31,
        ..Default::default()
    };
    hot.message[..20].copy_from_slice(b"bad fixture-secret!\0");
    channel
        .ctp_event(&crate::ctp::ingress::Event::Hot(hot), 125, true)
        .unwrap();
    for kind in [crate::ctp::INSTRUMENT, crate::ctp::ACCOUNT] {
        let quiet = crate::ctp::HotEvent {
            kind,
            ..Default::default()
        };
        channel
            .ctp_event(&crate::ctp::ingress::Event::Hot(quiet), 126, false)
            .unwrap();
    }
    assert_eq!(records.borrow().len(), 1, "query rows are not published");
    let raw = records.borrow()[0].clone();
    let row = materialize(&raw).unwrap();
    assert!(row.store.is_none(), "raw SDK events are never stored");
    let line = row.display.unwrap();
    assert_eq!(row.received_us, 125);
    assert_eq!(line["type"], "ctp_event");
    assert_eq!(line["shutdown_tail"], true);
    assert_eq!(line["event"]["error_code"], 31);
    assert!(!line.to_string().contains("fixture-secret"));
}

#[test]
fn yida_order_and_trade_callbacks_keep_ids_and_float_bits_in_the_trader_log() {
    let (channel, records) = channel(vec![]);
    let mut yida = crate::yida::RawEvent {
        kind: crate::yida::TRADE,
        trade_id: 9_007_199_254_740_997,
        system_id: 9_007_199_254_740_993,
        commission: 52.660000000000004,
        ..Default::default()
    };
    yida.original_trade_id[0] = 255;
    channel.yida_event(&yida, 123, false).unwrap();
    channel
        .yida_event(&crate::yida::RawEvent::default(), 124, false)
        .unwrap();
    let lines = displayed(&records);
    assert_eq!(lines.len(), 1, "account rows stay in memory");
    let event = &lines[0]["event"];
    assert_eq!(event["trade_id"].as_i64(), Some(9_007_199_254_740_997));
    assert_eq!(event["system_id"].as_i64(), Some(9_007_199_254_740_993));
    assert_eq!(
        event["commission"].as_f64().unwrap().to_bits(),
        yida.commission.to_bits()
    );
    assert_eq!(lines[0]["received_us"], 123);
}

#[test]
fn order_lifecycle_is_a_crypto_uniform_order_recorded_once_per_change() {
    let (channel, records) = channel(vec![]);
    let mut core = core();
    let p = core.prepare(intent(3), std::time::Instant::now()).unwrap();
    channel.prepared(core.trading_day(), &p, None).unwrap();
    // An unchanged report is not recorded again.
    channel.orders(&core, &[p.key]).unwrap();
    channel.state(&core).unwrap();
    let orders = uniform_orders(&records);
    assert_eq!(orders.len(), 1);
    let r = &orders[0];
    assert_eq!(r.symbol, b"rb2701");
    assert_eq!(r.venue, order_common::TradingVenue::ShfeFutures.to_u8());
    assert_eq!(r.ttype, order_common::OrderType::Limit.to_u8());
    assert_eq!(r.side, order_common::Side::Buy.to_u8());
    assert_eq!(r.status, order_common::OrderStatus::New.to_u8());
    assert_eq!((r.amount_init, r.amount_update), (3.0, 0.0));
    assert_eq!(r.from_key, EXEC_FROM_KEY.as_bytes());
    let identity = PersistedOrderKey {
        trading_day: "20261120".into(),
        venue: Venue::Shfe,
        native: p.key,
    };
    assert_eq!(r.client_order_id, identity.client_order_id());
    let ext = r.extension.as_ref().unwrap();
    assert_eq!(ext.trading_day.as_deref(), Some("20261120"));
    assert_eq!(
        ext.position_effect,
        Some(order_common::PositionEffect::Open)
    );
    assert_eq!(
        ext.client_order_text.as_deref(),
        Some(identity.client_order_text().as_str())
    );
    core.reject(&p.key).unwrap();
    channel.orders(&core, &[p.key]).unwrap();
    let orders = uniform_orders(&records);
    assert_eq!(orders.len(), 2);
    assert_eq!(orders[1].status, order_common::OrderStatus::Expired.to_u8());
    assert_eq!(orders[1].create_ts, orders[0].create_ts);
}

#[test]
fn a_new_fill_is_one_trade_update_plus_per_strategy_uniform_fills() {
    let (channel, records) = channel(vec![]);
    let mut core = core();
    let p = core.prepare(intent(3), std::time::Instant::now()).unwrap();
    channel.prepared(core.trading_day(), &p, None).unwrap();
    let f = fill(&p, "T1", 3);
    assert!(core.on_fill(&f).unwrap());
    let allocation = crate::ctp::exec::FillAllocation {
        source_id: Some("account".into()),
        batch_id: Some("batch".into()),
        target_revision: Some(1),
        market: "SHFE:rb2701".into(),
        trade: f.identity,
        lots: 3,
        price: 3100.0,
        allocations: Row::from([("cta-a".into(), 2), (UNALLOCATED.into(), 1)]),
        ledger: Row::from([("cta-a".into(), 2)]),
    };
    let events = [crate::ctp::exec::LedgerEvent::StrategyFillAllocation(
        allocation,
    )];
    channel
        .ledger(&core, &events, Some((&f, None, 500)))
        .unwrap();
    // Recovery replays the same trade; it is not recorded twice.
    channel.fill(&core, &f, None, 600, None).unwrap();
    let trades = stored(&records, store_cf::TRADES);
    assert_eq!(trades.len(), 1);
    let t = native_codec::decode_trade_update(&trades[0]).unwrap();
    assert_eq!(t.symbol, "rb2701");
    assert_eq!(t.order_id, 77);
    assert_eq!(t.cumulative_filled_quantity, 3.0);
    assert_eq!(
        t.order_status,
        Some(order_common::OrderStatus::Filled as u8)
    );
    let ext = t.extension.unwrap();
    assert_eq!(ext.trade_id.as_deref(), Some("T1"));
    assert_eq!(ext.last_filled_quantity, Some(3.0));
    // Priced from the queried rates: 3100 * 10 * 0.0001 per lot.
    assert!((ext.commission.unwrap() - 9.3).abs() < 1e-9);
    let fills: Vec<_> = uniform_orders(&records)
        .into_iter()
        .filter(|r| r.amount_update > 0.0)
        .collect();
    assert_eq!(fills.len(), 2);
    let by_key: std::collections::BTreeMap<_, _> = fills
        .iter()
        .map(|r| {
            (
                String::from_utf8(r.from_key.clone()).unwrap(),
                r.amount_update,
            )
        })
        .collect();
    assert_eq!(by_key["cta-a"], 2.0);
    assert_eq!(by_key[UNALLOCATED], 1.0);
    assert!(fills.iter().all(|r| r.price == 3100.0));
    let lifecycle = uniform_orders(&records);
    assert_eq!(
        lifecycle.last().unwrap().status,
        order_common::OrderStatus::Filled.to_u8()
    );
}

#[test]
fn crosses_are_internal_cross_uniform_orders_for_both_legs() {
    let (channel, records) = channel(vec![]);
    let core = core();
    let cross = crate::ctp::exec::StrategyCross {
        source_id: "account".into(),
        batch_id: "batch".into(),
        target_revision: 3,
        market: "SHFE:rb2701".into(),
        price: 3100.5,
        quote_received_us: 7,
        transfers: vec![cn_futures_target::ledger::Transfer {
            buyer: "cta-b".into(),
            seller: "cta-a".into(),
            lots: 2,
        }],
        ledger: Row::from([("cta-b".into(), 2), ("cta-a".into(), -2)]),
    };
    channel
        .ledger(
            &core,
            &[crate::ctp::exec::LedgerEvent::StrategyCross(cross)],
            None,
        )
        .unwrap();
    let legs = uniform_orders(&records);
    assert_eq!(legs.len(), 2);
    assert!(legs
        .iter()
        .all(|r| r.ttype == UNIFORM_ORDER_TYPE_INTERNAL_CROSS && r.amount_update == 2.0));
    assert_eq!(legs[0].from_key, b"cta-b");
    assert_eq!(legs[0].side, order_common::Side::Buy.to_u8());
    assert_eq!(legs[1].from_key, b"cta-a");
    assert_eq!(legs[1].side, order_common::Side::Sell.to_u8());
    assert_ne!(legs[0].client_order_id, legs[1].client_order_id);
}

#[test]
fn recovery_rebuilds_ledger_floors_unresolved_orders_and_known_trades_from_the_log() {
    let dir = absent_database();
    let (channel, records) = channel(vec![]);
    let mut core = core();
    let open = core.prepare(intent(1), std::time::Instant::now()).unwrap();
    channel.prepared(core.trading_day(), &open, None).unwrap();
    let filled = core.prepare(intent(2), std::time::Instant::now()).unwrap();
    channel.prepared(core.trading_day(), &filled, None).unwrap();
    let f = fill(&filled, "T9", 2);
    assert!(core.on_fill(&f).unwrap());
    let events = [crate::ctp::exec::LedgerEvent::StrategyFillAllocation(
        crate::ctp::exec::FillAllocation {
            source_id: None,
            batch_id: None,
            target_revision: None,
            market: "SHFE:rb2701".into(),
            trade: f.identity,
            lots: 2,
            price: 3100.0,
            allocations: Row::from([("cta-a".into(), 2)]),
            ledger: Row::from([("cta-a".into(), 2)]),
        },
    )];
    channel.ledger(&core, &events, Some((&f, None, 1))).unwrap();
    {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = rocksdb::DB::open_cf(&options, &dir, store_cf::ALL).unwrap();
        for (cf, rows) in [
            (store_cf::ORDERS, stored(&records, store_cf::ORDERS)),
            (store_cf::TRADES, stored(&records, store_cf::TRADES)),
        ] {
            for (index, bytes) in rows.iter().enumerate() {
                db.put_cf(
                    db.cf_handle(cf).unwrap(),
                    format!("{:020}", index + 1),
                    bytes,
                )
                .unwrap();
            }
        }
    }
    let recovery = DurableRecovery::load(&dir).unwrap();
    assert_eq!(
        recovery.ledger,
        vec![("SHFE:rb2701".to_string(), Row::from([("cta-a".into(), 2)]))]
    );
    assert_eq!(
        recovery
            .reference_floors
            .get(&("20261120".into(), "yida:20261120:group:3".into())),
        Some(&19)
    );
    assert_eq!(recovery.unresolved.len(), 1);
    assert_eq!(recovery.unresolved.iter().next().unwrap().native, open.key);
    assert!(recovery
        .known_trades
        .contains(&(Venue::Shfe, "20261120".into(), "T9".into())));
    // A new process does not record the known trade again.
    let (reopened, again) = channel_at(&dir, vec![]);
    assert_eq!(reopened.take_previous_ledger().len(), 1);
    reopened.fill(&core, &f, None, 2, None).unwrap();
    assert!(stored(&again, store_cf::TRADES).is_empty());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn strategy_outcomes_and_quote_decisions_cross_as_bincode_and_render_operator_json() {
    use crate::ctp::strategy::ActionOutcome;
    let (channel, records) = channel(vec!["secret".into()]);
    let key = OrderKey {
        session: "ctp:1:2".into(),
        order_ref: "9".into(),
    };
    channel
        .strategy_result(
            &ActionOutcome::CancelRejected {
                tag: 4,
                key,
                error: "order not cancellable secret".into(),
            },
            true,
        )
        .unwrap();
    // Non-displayed outcomes are neither stored nor published.
    channel
        .strategy_result(&ActionOutcome::CancelledLocally { tag: 5, key }, false)
        .unwrap();
    let target = std::sync::Arc::new(crate::ctp::exec::Target {
        source_id: "account".into(),
        batch_id: "batch-7".into(),
        target_revision: 7,
        trading_day: "20261120".into(),
        expires_at_us: 9,
        strategies: Default::default(),
    });
    channel
        .quote_decision(
            &crate::ctp::exec::WaitNote {
                target,
                market: "SHFE:rb2701".into(),
                reason: "maker_working",
            },
            11,
            3,
        )
        .unwrap();
    assert_eq!(records.borrow().len(), 2);
    let lines = displayed(&records);
    assert_eq!(lines[0]["type"], "strategy_cancel_rejected");
    assert_eq!(lines[0]["key"]["order_ref"], "9");
    assert_eq!(lines[0]["error"], "order not cancellable [redacted]");
    assert_eq!(lines[1]["type"], "exec_quote_decision");
    assert_eq!(lines[1]["batch_id"], "batch-7");
    assert_eq!(lines[1]["reason"], "maker_working");
    assert!(stored(&records, store_cf::ORDERS).is_empty());
}

#[test]
fn a_failed_publish_latches_and_blocks_later_records() {
    let channel = broken_channel();
    assert!(channel.record(serde_json::json!({}), true).is_err());
    assert!(channel.failed());
    assert!(channel.record(serde_json::json!({}), true).is_err());
    assert_eq!(channel.stats().failures, 1);
}

#[test]
fn records_from_another_layout_are_refused() {
    let (channel, records) = channel(vec![]);
    channel
        .record(serde_json::json!({"type":"x"}), true)
        .unwrap();
    let mut record = records.borrow()[0].clone();
    record[4] ^= 1;
    assert!(materialize(&record).is_err());
}
