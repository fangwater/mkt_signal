use super::*;
use crate::{
    config::{Config, SymbolConfig},
    core::TradingCore,
};
use std::time::Instant;
const NOW: u64 = 1_794_877_200_000_000;
fn config() -> Config {
    let mut c: Config =
        toml::from_str(include_str!("../../../../../config/ctp_trade.example.toml")).unwrap();
    c.symbols = vec![SymbolConfig {
        venue: Venue::Shfe,
        symbol: "cu2612".into(),
        cash_budget_per_open_lot: 1000.0,
    }];
    c.risk.max_order_lots = 3;
    c.risk.max_position_lots_per_side = 10;
    c.risk.max_working_lots = 10;
    c.risk.max_actions_per_second = 20;
    c.risk.enable_fak_fok = true;
    c.ctaker = Some(Settings {
        source_id: "test-account".into(),
        max_target_lots: 10,
        max_order_lots: 3,
        quote_max_age_ms: 1000,
        target_max_age_ms: 30000,
        retry_interval_ms: 500,
    });
    c
}
fn core(long: i32, short: i32) -> TradingCore {
    let c = config();
    let mut core = TradingCore::new(c.risk, super::super::scope()).unwrap();
    core.set_trading_status(Venue::Shfe, "cu", true);
    core.restore(Snapshot {
        trading_day: "20261117".into(),
        session: "ctp:1:2".into(),
        max_order_ref: 1,
        available_cash: 1_000_000.0,
        instruments: vec![Instrument {
            venue: Venue::Shfe,
            symbol: "cu2612".into(),
            price_tick: 1.0,
            contract_multiple: 5,
            expiry_date: 20261215,
            commission: None,
            margin: None,
            min_lots: 1,
            max_lots: 100,
            market_lots: None,
            active: true,
            cash_budget_per_open_lot: 1000.0,
            option: None,
        }],
        positions: vec![
            PositionSnapshot {
                venue: Venue::Shfe,
                symbol: "cu2612".into(),
                side: Side::Buy,
                today: long,
                yesterday: 0,
            },
            PositionSnapshot {
                venue: Venue::Shfe,
                symbol: "cu2612".into(),
                side: Side::Sell,
                today: short,
                yesterday: 0,
            },
        ],
        orders: vec![],
        trades: vec![],
    })
    .unwrap();
    core
}
fn context(core: &TradingCore) -> StrategyContext<'_> {
    StrategyContext {
        core,
        can_trade: true,
    }
}
fn target(qty: i32) -> Target {
    Target {
        source_id: "test-account".into(),
        batch_id: "batch-01".into(),
        target_revision: 1,
        trading_day: "20261117".into(),
        expires_at_us: NOW + 30_000_000,
        net_targets: BTreeMap::from([("SHFE:cu2612".into(), qty)]),
    }
}
fn tick(at: u64) -> TickRecord {
    let mut e = super::super::market_data::RawEvent::default();
    e.symbol[..6].copy_from_slice(b"cu2612");
    e.exchange[..4].copy_from_slice(b"SHFE");
    e.trading_day[..8].copy_from_slice(b"20261117");
    e.bid[0] = 100.0;
    e.ask[0] = 101.0;
    e.bid_volume[0] = 5;
    e.ask_volume[0] = 5;
    e.lower_limit = 10.0;
    e.upper_limit = 200.0;
    TickRecord::capture(&e, at, 7, Venue::Shfe)
}
fn strategy(core: &TradingCore, qty: i32) -> Ctaker {
    let mut s = Ctaker::new(&config()).unwrap();
    s.on_market_state(true, 7).unwrap();
    s.accept(target(qty), context(core), NOW).unwrap();
    s
}
fn decide(s: &mut Ctaker, c: &TradingCore, t: TickRecord, now: u64) -> Vec<Action> {
    let mut a = Actions::default();
    s.decide(&t, context(c), &mut a, now).unwrap();
    a.drain().collect()
}
fn request(a: Action) -> (Intent, Arc<Target>) {
    match a {
        Action::SubmitCtaker { intent, target, .. } => (intent, target),
        _ => panic!("unexpected action"),
    }
}
#[test]
fn configuration_is_explicit_and_requires_tested_fak_and_lot_bounds() {
    let c: Config =
        toml::from_str(include_str!("../../../../../config/ctp_trade.example.toml")).unwrap();
    assert!(c.ctaker.is_none());
    let mut c = config();
    c.validate().unwrap();
    c.risk.enable_fak_fok = false;
    assert!(c.validate().is_err());
    c.risk.enable_fak_fok = true;
    c.ctaker.as_mut().unwrap().max_order_lots = 4;
    assert!(c.validate().is_err());
    assert!(NoStrategy
        .on_target(target(1), context(&core(0, 0)), NOW)
        .is_err());
}
#[test]
fn target_admission_rejects_wrong_identity_scope_day_size_lease_and_closed_gates() {
    let c = core(0, 0);
    let mut s = Ctaker::new(&config()).unwrap();
    s.on_market_state(true, 7).unwrap();
    let mut bad = vec![];
    let mut t = target(1);
    t.source_id = "another-account".into();
    bad.push(t);
    let mut t = target(1);
    t.trading_day = "20261116".into();
    bad.push(t);
    let mut t = target(1);
    t.net_targets.clear();
    bad.push(t);
    let mut t = target(1);
    t.net_targets.insert("SHFE:CU2612".into(), 0);
    bad.push(t);
    let mut t = target(1);
    t.net_targets.insert("SHFE:cu2612".into(), 11);
    bad.push(t);
    let mut t = target(1);
    t.expires_at_us = NOW;
    bad.push(t);
    let mut t = target(1);
    t.expires_at_us = NOW + 30_000_001;
    bad.push(t);
    let mut t = target(1);
    t.target_revision = 0;
    bad.push(t);
    for t in bad {
        assert!(s.accept(t, context(&c), NOW).is_err());
    }
    assert!(s
        .accept(
            target(1),
            StrategyContext {
                core: &c,
                can_trade: false
            },
            NOW
        )
        .is_err());
    assert!(s.target.is_none());
}
#[test]
fn identical_targets_are_idempotent_and_revisions_cannot_conflict_or_go_backwards() {
    let c = core(0, 0);
    let mut s = strategy(&c, 1);
    s.accept(target(1), context(&c), NOW).unwrap();
    assert!(s.accept(target(2), context(&c), NOW).is_err());
    let mut next = target(2);
    next.target_revision = 2;
    assert!(s.accept(next.clone(), context(&c), NOW).is_err());
    next.batch_id = "batch-02".into();
    s.accept(next, context(&c), NOW).unwrap();
    assert!(s.accept(target(1), context(&c), NOW).is_err());
}
#[test]
fn aggressive_bbo_limit_fak_uses_integer_ticks_and_chunk_bounds_with_frozen_batch_identity() {
    let c = core(0, 0);
    let mut s = strategy(&c, 7);
    let (i, t) = request(decide(&mut s, &c, tick(NOW), NOW).remove(0));
    assert_eq!(
        (
            i.side,
            i.offset,
            i.lots,
            i.price_ticks,
            i.price_type,
            i.time_in_force
        ),
        (
            Side::Buy,
            Offset::Open,
            3,
            101,
            PriceType::Limit,
            TimeInForce::Fak
        )
    );
    assert_eq!(*t, target(7));
    let mut s = strategy(&c, -2);
    let (i, _) = request(decide(&mut s, &c, tick(NOW), NOW).remove(0));
    assert_eq!((i.side, i.price_ticks, i.lots), (Side::Sell, 100, 2));
}
#[test]
fn opposite_gross_inventory_closes_before_new_opening_and_only_factual_fills_advance() {
    let mut c = core(0, 2);
    let mut s = strategy(&c, 1);
    let (i, _) = request(decide(&mut s, &c, tick(NOW), NOW).remove(0));
    assert_eq!((i.side, i.offset, i.lots), (Side::Buy, Offset::Close, 2));
    let children = c.plan(i).unwrap();
    assert_eq!(children[0].offset, Offset::CloseToday);
    let p = c.prepare(children[0].clone(), Instant::now()).unwrap();
    assert!(decide(&mut s, &c, tick(NOW + 1_000_000), NOW + 1_000_000).is_empty());
    c.on_fill(&Fill {
        identity: TradeIdentity {
            venue: Venue::Shfe,
            symbol: "cu2612".into(),
            trading_day: "20261117".into(),
            trade_id: "closing-trade".into(),
            side: Side::Buy,
        },
        order_ref: p.key.order_ref.clone(),
        order_key: Some(p.key.clone()),
        system_id: "sys1".into(),
        offset: Offset::CloseToday,
        lots: 2,
        price: 101.0,
    })
    .unwrap();
    let (i, _) = request(decide(&mut s, &c, tick(NOW + 1_000_000), NOW + 1_000_000).remove(0));
    assert_eq!((i.side, i.offset, i.lots), (Side::Buy, Offset::Open, 1));
}
#[test]
fn filled_order_report_without_trade_facts_remains_unresolved_and_blocks_replacement() {
    let mut c = core(0, 0);
    let mut s = strategy(&c, 2);
    let (i, _) = request(decide(&mut s, &c, tick(NOW), NOW).remove(0));
    let p = c.prepare(i, Instant::now()).unwrap();
    c.on_order(&OrderUpdate {
        key: p.key.clone(),
        symbol: "cu2612".into(),
        venue: Venue::Shfe,
        system_id: "sys1".into(),
        side: Side::Buy,
        offset: Offset::Open,
        lots: 2,
        traded: 2,
        price_type: PriceType::Limit,
        time_in_force: TimeInForce::Fak,
        limit_price: 101.0,
        status: OrderStatus::Filled,
        cancel_rejected: false,
        trading_day: "20261117".into(),
    })
    .unwrap();
    assert!(c.has_working_orders(Venue::Shfe, "cu2612"));
    assert_eq!(c.position(Venue::Shfe, "cu2612", Side::Buy).total(), 0);
    assert!(decide(&mut s, &c, tick(NOW + 1_000_000), NOW + 1_000_000).is_empty());
    let mut t = target(3);
    t.target_revision = 2;
    t.batch_id = "batch-02".into();
    assert!(s.accept(t, context(&c), NOW).is_err());
}
#[test]
fn stale_crossed_off_grid_limit_missing_depth_and_foreign_generation_quotes_do_not_submit() {
    let c = core(0, 0);
    let mut invalid = vec![];
    let mut t = tick(NOW);
    t.received_us = NOW - 1_000_001;
    invalid.push(t);
    let mut t = tick(NOW);
    t.connection_generation = 8;
    invalid.push(t);
    let mut t = tick(NOW);
    t.ask[0] = 99.0;
    invalid.push(t);
    let mut t = tick(NOW);
    t.ask[0] = 101.5;
    invalid.push(t);
    let mut t = tick(NOW);
    t.upper_limit = 100.0;
    invalid.push(t);
    let mut t = tick(NOW);
    t.ask_volume[0] = 0;
    invalid.push(t);
    let mut t = tick(NOW);
    t.received_us = NOW + 1;
    invalid.push(t);
    let mut t = tick(NOW);
    t.ask[0] = f64::MAX;
    invalid.push(t);
    for t in invalid {
        let mut s = strategy(&c, 1);
        assert!(decide(&mut s, &c, t, NOW).is_empty());
    }
}
#[test]
fn disconnected_or_expired_targets_need_a_new_batch_and_cannot_silently_resume() {
    let c = core(0, 0);
    let mut s = strategy(&c, 1);
    s.on_market_state(false, 7).unwrap();
    s.on_market_state(true, 8).unwrap();
    assert!(s.accept(target(1), context(&c), NOW).is_err());
    let mut t = tick(NOW);
    t.connection_generation = 8;
    assert!(decide(&mut s, &c, t, NOW).is_empty());
    let mut s = strategy(&c, 1);
    assert!(decide(&mut s, &c, tick(NOW + 30_000_000), NOW + 30_000_000).is_empty());
}
#[test]
fn rejection_halts_retries_and_even_idempotent_replays_cannot_rearm_it() {
    let c = core(0, 0);
    let mut s = strategy(&c, 1);
    s.on_result(
        &ActionOutcome::Rejected {
            tag: 1,
            error: "test rejection".into(),
        },
        context(&c),
    )
    .unwrap();
    assert!(decide(&mut s, &c, tick(NOW), NOW).is_empty());
    assert!(s.accept(target(1), context(&c), NOW).is_err());
    let mut t = target(1);
    t.target_revision = 2;
    t.batch_id = "batch-02".into();
    s.accept(t, context(&c), NOW).unwrap();
    assert_eq!(decide(&mut s, &c, tick(NOW), NOW).len(), 1);
}
#[test]
fn hedged_inventory_at_the_net_target_is_preserved() {
    let c = core(2, 2);
    let mut s = strategy(&c, 0);
    assert!(decide(&mut s, &c, tick(NOW), NOW).is_empty());
    assert_eq!(c.position(Venue::Shfe, "cu2612", Side::Buy).total(), 2);
}
#[test]
fn action_retry_spacing_is_bounded_even_before_local_outcome_callback() {
    let c = core(0, 0);
    let mut s = strategy(&c, 2);
    assert_eq!(decide(&mut s, &c, tick(NOW), NOW).len(), 1);
    assert!(decide(&mut s, &c, tick(NOW + 499_999), NOW + 499_999).is_empty());
}

#[test]
fn reconnect_requires_a_new_batch_instead_of_reactivating_a_stale_target() {
    let c = core(0, 0);
    let mut s = strategy(&c, 1);
    s.on_market_state(false, 7).unwrap();
    s.on_market_state(true, 8).unwrap();
    assert!(s.accept(target(1), context(&c), NOW).is_err());
    let mut q = tick(NOW);
    q.connection_generation = 8;
    assert!(decide(&mut s, &c, q, NOW).is_empty());
    let mut t = target(1);
    t.target_revision = 2;
    t.batch_id = "batch-02".into();
    s.accept(t, context(&c), NOW).unwrap();
    assert_eq!(decide(&mut s, &c, q, NOW).len(), 1);
}

#[test]
fn fake_transport_keeps_acceptance_factual_and_blocks_before_sdk_when_audit_fails() {
    use crate::{core::SingleLoop, runtime::Journal};
    #[derive(Default)]
    struct Fake {
        calls: usize,
    }
    impl Trader for Fake {
        fn insert(&mut self, _: &PreparedOrder) -> Result<i32> {
            self.calls += 1;
            Ok(0)
        }
        fn cancel(&mut self, _: &PreparedCancel) -> Result<i32> {
            anyhow::bail!("unexpected cancellation")
        }
    }
    for fail in [false, true] {
        let c = core(0, 0);
        let mut s = strategy(&c, 1);
        let action = decide(&mut s, &c, tick(NOW), NOW).remove(0);
        let mut runtime = SingleLoop {
            core: c,
            trader: Fake::default(),
            execute: true,
        };
        let root = std::env::temp_dir().join(format!(
            "ctaker-fake-{}-{}-{fail}",
            std::process::id(),
            crate::persistence::now_us()
        ));
        let journal = Journal::start(
            None,
            64,
            vec![],
            &root.join("db"),
            crate::persistence::store_identity("test", &[b"ctaker-fake"]),
            false,
        )
        .unwrap();
        if fail {
            journal
                .failed
                .store(true, std::sync::atomic::Ordering::Release);
        }
        let result =
            super::super::strategy::dispatch(&mut runtime, &journal, action, true, Instant::now());
        assert_eq!(runtime.trader.calls, usize::from(!fail));
        assert_eq!(
            runtime
                .core
                .position(Venue::Shfe, "cu2612", Side::Buy)
                .total(),
            0
        );
        if fail {
            assert!(result.is_err());
            assert!(!runtime.core.is_ready());
            let _ = journal.finish();
        } else {
            assert!(
                matches!(result.unwrap(),ActionOutcome::Submitted{receipt,..} if receipt.all_submitted_locally)
            );
            assert!(runtime.core.has_working_orders(Venue::Shfe, "cu2612"));
            journal.finish().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
