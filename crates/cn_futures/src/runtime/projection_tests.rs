use super::*;
use crate::{
    core::{SingleLoop, TradingCore},
    model::*,
};
use persist_manager::cn_futures::{COLUMN_FAMILIES, EVENTS, ORDERS, TRADES};

fn fixture(label: &str) -> (std::path::PathBuf, Journal, TradingCore) {
    let path = std::env::temp_dir().join(format!(
        "cn-compact-{label}-{}-{}",
        std::process::id(),
        crate::persistence::now_us()
    ));
    let journal = Journal::start(
        &path.join("audit.jsonl"),
        64,
        vec![],
        &path.join("db"),
        crate::persistence::store_identity("compact_fixture", &[b"synthetic"]),
        false,
    )
    .unwrap();
    let rule = Instrument {
        venue: Venue::Cffex,
        symbol: "IF2612".into(),
        price_tick: 0.2,
        contract_multiple: 300,
        expiry_date: 20261218,
        commission: None,
        margin: None,
        min_lots: 1,
        max_lots: 10,
        market_lots: None,
        active: true,
        cash_budget_per_open_lot: 100.0,
        option: None,
    };
    let mut core = TradingCore::new(
        RiskLimits {
            max_order_lots: 10,
            max_position_lots_per_side: 20,
            max_working_lots: 20,
            max_actions_per_second: 20,
            minimum_cash_remaining: 0.0,
            enable_fak_fok: true,
            close_priority: ClosePriority::TodayFirst,
        },
        crate::ctp::scope(),
    )
    .unwrap();
    core.set_trading_status(Venue::Cffex, "IF", true);
    core.restore(Snapshot {
        trading_day: "20261110".into(),
        session: crate::ctp::session_key(7, 11),
        max_order_ref: 100,
        available_cash: 1000000.0,
        instruments: vec![rule],
        positions: vec![],
        orders: vec![],
        trades: vec![],
    })
    .unwrap();
    (path, journal, core)
}
fn intent() -> Intent {
    Intent {
        venue: Venue::Cffex,
        symbol: "IF2612".into(),
        side: Side::Buy,
        offset: Offset::Open,
        lots: 1,
        price_type: PriceType::Limit,
        price_ticks: 20000,
        time_in_force: TimeInForce::Day,
        execution_route: None,
    }
}
fn rows<T: serde::de::DeserializeOwned>(store: &persist_manager::RocksDbStore, cf: &str) -> Vec<T> {
    store
        .scan(cf, None, false, None)
        .unwrap()
        .into_iter()
        .map(|(_, v)| serde_json::from_slice(&v).unwrap())
        .collect()
}

#[test]
fn deferred_trade_and_order_commit_together_and_keep_the_captured_rule() {
    let (path, journal, mut core) = fixture("atomic-trade");
    let prepared = core.prepare(intent(), Instant::now()).unwrap();
    journal
        .prepared_submit(core.trading_day(), &prepared)
        .unwrap();
    let fill = Fill {
        identity: TradeIdentity {
            venue: Venue::Cffex,
            symbol: "IF2612".into(),
            trading_day: "20261110".into(),
            trade_id: "9007199254740997".into(),
            side: Side::Buy,
        },
        order_ref: prepared.key.order_ref.clone(),
        order_key: Some(prepared.key.clone()),
        system_id: "9007199254740993".into(),
        offset: Offset::Open,
        lots: 1,
        price: 4000.0000000000005,
    };
    core.on_fill(&fill).unwrap();
    journal
        .trade_for(
            &core,
            &fill,
            Some(52.660000000000004),
            Some(16230),
            123,
            true,
        )
        .unwrap();
    let mut revised = core.instrument(Venue::Cffex, "IF2612").unwrap().clone();
    revised.price_tick = 0.5;
    core.replace_instruments(vec![revised]).unwrap();
    journal.finish().unwrap();
    let store = persist_manager::RocksDbStore::open_read_only(
        path.join("db").to_str().unwrap(),
        COLUMN_FAMILIES,
    )
    .unwrap();
    let orders: Vec<persist_common::cn_futures::NativeOrderRecord> = rows(&store, ORDERS);
    let trades: Vec<persist_common::cn_futures::NativeTradeRecord> = rows(&store, TRADES);
    assert_eq!(orders.len(), 1);
    assert_eq!(trades.len(), 1);
    assert_eq!(orders[0].order.filled, 1);
    assert_eq!(orders[0].instrument.as_ref().unwrap().price_tick, 0.2);
    assert_eq!(orders[0].submitted_intent, Some(prepared.intent));
    assert_eq!(orders[0].evidence_seq, trades[0].evidence_seq);
    assert_eq!(trades[0].fill, fill);
    assert_eq!(
        trades[0].commission.unwrap().to_bits(),
        52.660000000000004f64.to_bits()
    );
    assert_eq!(trades[0].commission_currency.as_deref(), Some("CNY"));
    assert_eq!(trades[0].native_time, Some(16230));
    assert_eq!(trades[0].received_us, 123);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn multiple_order_projections_preserve_one_evidence_record_and_producer_state() {
    let (path, journal, mut core) = fixture("multi-order");
    let a = core.prepare(intent(), Instant::now()).unwrap();
    let b = core.prepare(intent(), Instant::now()).unwrap();
    journal.prepared_submit(core.trading_day(), &a).unwrap();
    journal.prepared_submit(core.trading_day(), &b).unwrap();
    let keys = [a.key.clone(), b.key.clone()];
    journal.orders(&core, &keys, &keys).unwrap();
    core.reject(&a.key).unwrap();
    core.reject(&b.key).unwrap();
    journal.finish().unwrap();
    let store = persist_manager::RocksDbStore::open_read_only(
        path.join("db").to_str().unwrap(),
        COLUMN_FAMILIES,
    )
    .unwrap();
    let orders: Vec<persist_common::cn_futures::NativeOrderRecord> = rows(&store, ORDERS);
    let events: Vec<persist_manager::cn_futures::Evidence> = rows(&store, EVENTS);
    assert_eq!(orders.len(), 2);
    assert_eq!(orders[0].evidence_seq, orders[1].evidence_seq);
    assert!(orders
        .iter()
        .all(|o| o.order.status == OrderStatus::Pending && o.order.local_cash_held == 100.0));
    assert_eq!(
        events
            .iter()
            .filter(|e| e.value["type"] == "order_projection")
            .count(),
        1
    );
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

struct Accepted;
impl Trader for Accepted {
    fn insert(&mut self, _: &PreparedOrder) -> Result<i32> {
        Ok(0)
    }
    fn cancel(&mut self, _: &PreparedCancel) -> Result<i32> {
        Ok(0)
    }
}
#[test]
fn inline_receipts_keep_the_existing_json_array_contract() {
    let (path, journal, core) = fixture("receipt");
    let mut runtime = SingleLoop {
        core,
        trader: Accepted,
        execute: true,
    };
    let receipt = runtime
        .submit_planned_with(intent(), Instant::now(), |_| Ok(()))
        .unwrap();
    assert!(!receipt.children.spilled());
    let value = serde_json::to_value(receipt).unwrap();
    assert!(value["children"].is_array());
    assert_eq!(value["children"].as_array().unwrap().len(), 1);
    assert_eq!(value["children"][0]["key"]["order_ref"], "101");
    assert_eq!(value["children"][0]["outcome"], "submitted_locally");
    journal.finish().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}
