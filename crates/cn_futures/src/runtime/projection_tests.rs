use super::*;
use crate::{
    core::{SingleLoop, TradingCore},
    model::*,
    persist::tests::{channel, displayed, uniform_orders, Records},
};
use std::sync::Arc;

fn fixture() -> (PersistChannel, Records, TradingCore) {
    let (journal, records) = channel(vec![]);
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
            max_order_lots: Some(10),
            max_position_lots_per_side: Some(20),
            max_working_lots: Some(20),
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
    (journal, records, core)
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

#[test]
fn unsent_ctp_dispatch_audit_identifies_the_blocking_check_and_target() {
    use crate::ctp::{
        exec::Target,
        strategy::{Action, ActionOutcome, CtpDispatchChecks},
    };
    let (journal, records) = channel(vec![]);
    let outcome = ActionOutcome::CtpDispatchBlocked {
        action: Action::SubmitExec {
            tag: 11,
            intent: intent(),
            target: Arc::new(Target {
                source_id: "account".into(),
                batch_id: "rb-test-batch".into(),
                target_revision: 3,
                strategies: Default::default(),
            }),
        },
        checks: CtpDispatchChecks {
            strategy_can_trade: true,
            execution_enabled: true,
            account_ready: true,
            market_data_ready: true,
            newer_contract_quote_queued: true,
            trade_event_backlog: false,
            journal_failed: false,
            trade_ingress_failed: false,
        },
        quote_received_us: 123,
        quote_generation: 2,
    };
    journal.strategy_result(&outcome, true).unwrap();
    let entry = displayed(&records).remove(0);
    assert_eq!(entry["type"], "ctp_strategy_dispatch_blocked");
    assert_eq!(entry["sent_to_sdk"], false);
    assert_eq!(entry["retryable"], true);
    assert_eq!(entry["checks"]["newer_contract_quote_queued"], true);
    assert_eq!(entry["checks"]["trade_event_backlog"], false);
    assert_eq!(entry["action"]["batch_id"], "rb-test-batch");
    assert_eq!(entry["action"]["target_revision"], 3);
    assert_eq!(entry["quote_received_us"], 123);
    assert!(
        uniform_orders(&records).is_empty(),
        "a blocked action stores nothing"
    );
}

#[test]
fn ctp_orders_carry_the_cffex_venue_and_close_effect_into_the_uniform_order() {
    let (journal, records, mut core) = fixture();
    let mut close = intent();
    close.offset = Offset::Open;
    let a = core.prepare(close).unwrap();
    let b = core.prepare(intent()).unwrap();
    journal.prepared(core.trading_day(), &a, None).unwrap();
    journal.prepared(core.trading_day(), &b, None).unwrap();
    journal.orders(&core, &[a.key, b.key]).unwrap();
    let orders = uniform_orders(&records);
    assert_eq!(orders.len(), 2, "unchanged orders are recorded once");
    assert!(orders
        .iter()
        .all(|r| r.venue == order_common::TradingVenue::CffexFutures.to_u8()));
    assert_ne!(orders[0].client_order_id, orders[1].client_order_id);
    assert_eq!(
        orders[0]
            .extension
            .as_ref()
            .unwrap()
            .client_order_text
            .as_deref(),
        Some("ctp:7:11/101")
    );
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
    let (journal, _records, core) = fixture();
    let mut runtime = SingleLoop {
        core,
        trader: Accepted,
        execute: true,
    };
    let receipt = runtime.submit_planned_with(intent(), |_| Ok(())).unwrap();
    assert!(!receipt.children.spilled());
    let value = serde_json::to_value(receipt).unwrap();
    assert!(value["children"].is_array());
    assert_eq!(value["children"].as_array().unwrap().len(), 1);
    assert_eq!(value["children"][0]["key"]["order_ref"], "101");
    assert_eq!(value["children"][0]["outcome"], "submitted_locally");
    journal.finish().unwrap();
}
