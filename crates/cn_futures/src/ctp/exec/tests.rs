use super::*;
use crate::{config::Config, core::TradingCore};
use std::collections::BTreeMap;
const NOW: u64 = 1_794_877_200_000_000;
fn config() -> Config {
    let mut c: Config =
        toml::from_str(include_str!("../../../../../config/ctp_trade.example.toml")).unwrap();
    c.risk.enable_fak_fok = true;
    c.exec = Some(Settings {
        source_id: "test-account".into(),
        quote_max_age_ms: 1000,
        retry_interval_ms: 500,
        algorithm: ExecutionAlgorithm::Ctaker,
        chase: Default::default(),
    });
    c
}

#[test]
fn a_contract_without_rules_waits_alone_and_never_blocks_the_target() {
    let c = core(0, 0);
    let settings = config().exec.unwrap();
    let mut batch = target(7);
    with_market(&mut batch, "SHFE:sn2612", 1);
    assert_eq!(
        validate_target(&settings, &batch, &c, false).unwrap(),
        [(Venue::Shfe, "cu2612", true), (Venue::Shfe, "sn2612", true)]
    );
    let mut s = Exec::new(&config()).unwrap();
    s.on_market_state(true, 7).unwrap();
    s.accept(batch, context(&c)).unwrap();
    let (intent, _) = request(decide(&mut s, &c, tick(NOW), NOW).remove(0));
    assert_eq!((intent.symbol.as_str(), intent.lots), ("cu2612", 7));
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
        strategies: BTreeMap::from([(
            "cta-a".into(),
            BTreeMap::from([("SHFE:cu2612".into(), qty)]),
        )]),
    }
}
fn roster(rows: &[(&str, i32)]) -> Target {
    let mut t = target(0);
    t.strategies = rows
        .iter()
        .map(|(name, qty)| {
            (
                name.to_string(),
                BTreeMap::from([("SHFE:cu2612".into(), *qty)]),
            )
        })
        .collect();
    t
}
fn with_market(t: &mut Target, market: &str, qty: i32) {
    for vector in t.strategies.values_mut() {
        vector.insert(market.into(), qty);
    }
}
fn fill(key: &OrderKey, id: &str, side: Side, lots: i32) -> Fill {
    Fill {
        identity: TradeIdentity {
            venue: Venue::Shfe,
            symbol: "cu2612".into(),
            trading_day: "20261117".into(),
            trade_id: Text::new(id).unwrap(),
            side,
        },
        order_ref: key.order_ref,
        order_key: Some(*key),
        system_id: Text::format(format_args!("sys-{id}")).unwrap(),
        offset: Offset::Open,
        lots,
        price: 101.0,
    }
}
fn tick(at: u64) -> TickRecord {
    tick_on(at, b"20261117")
}
fn tick_on(at: u64, trading_day: &[u8; 8]) -> TickRecord {
    let mut e = super::super::market_data::RawEvent::default();
    e.symbol[..6].copy_from_slice(b"cu2612");
    e.exchange[..4].copy_from_slice(b"SHFE");
    e.trading_day[..8].copy_from_slice(trading_day);
    e.bid[0] = 100.0;
    e.ask[0] = 101.0;
    e.bid_volume[0] = 5;
    e.ask_volume[0] = 5;
    e.lower_limit = 10.0;
    e.upper_limit = 200.0;
    TickRecord::capture(&e, at, 7, Venue::Shfe)
}
fn strategy(core: &TradingCore, qty: i32) -> Exec {
    let mut s = Exec::new(&config()).unwrap();
    s.on_market_state(true, 7).unwrap();
    s.accept(target(qty), context(core)).unwrap();
    s
}
fn decide(s: &mut Exec, c: &TradingCore, t: TickRecord, now: u64) -> Vec<Action> {
    let mut a = Actions::default();
    s.decide(&t, context(c), &mut a, now).unwrap();
    a.drain().collect()
}
fn request(a: Action) -> (Intent, Arc<Target>) {
    match a {
        Action::SubmitExec { intent, target, .. } => (intent, target),
        _ => panic!("unexpected action"),
    }
}

#[test]
fn unsent_ctp_action_is_retried_after_the_interval_with_no_target_lease() {
    for (md, trade) in [(true, false), (false, true), (true, true)] {
        let c = core(0, 0);
        let mut exec = strategy(&c, -2);
        let checks = CtpDispatchChecks {
            strategy_can_trade: true,
            execution_enabled: true,
            account_ready: true,
            market_data_ready: true,
            newer_contract_quote_queued: md,
            trade_event_backlog: trade,
            journal_failed: false,
            trade_ingress_failed: false,
        };
        assert!(!checks.can_dispatch());
        assert!(checks.can_retry());
        // The runtime withholds the action; the contract waits for its retry.
        assert_eq!(decide(&mut exec, &c, tick(NOW), NOW).len(), 1);
        let early = NOW + 499_999;
        assert!(decide(&mut exec, &c, tick(early), early).is_empty());
        let retry = NOW + 500_000;
        let (intent, original) = request(decide(&mut exec, &c, tick(retry), retry).remove(0));
        assert_eq!((intent.side, intent.lots), (Side::Sell, 2));
        assert_eq!(*original, target(-2));
        let hours_later = NOW + 3_600_000_000;
        assert_eq!(
            decide(&mut exec, &c, tick(hours_later), hours_later).len(),
            1
        );
    }
}

#[test]
fn failed_dispatch_health_check_pauses_only_until_the_retry_interval() {
    let c = core(0, 0);
    let mut exec = strategy(&c, -2);
    assert_eq!(decide(&mut exec, &c, tick(NOW), NOW).len(), 1);
    let checks = CtpDispatchChecks {
        strategy_can_trade: true,
        execution_enabled: true,
        account_ready: true,
        market_data_ready: true,
        newer_contract_quote_queued: true,
        trade_event_backlog: false,
        journal_failed: true,
        trade_ingress_failed: false,
    };
    assert!(!checks.can_retry());
    assert!(decide(&mut exec, &c, tick(NOW + 499_999), NOW + 499_999).is_empty());
    let later = NOW + 1_000_000;
    assert_eq!(decide(&mut exec, &c, tick(later), later).len(), 1);
}
#[test]
fn configuration_requires_tested_fak_and_removes_local_lot_limits() {
    let c: Config =
        toml::from_str(include_str!("../../../../../config/ctp_trade.example.toml")).unwrap();
    assert!(c.exec.is_none());
    let mut c = config();
    c.validate().unwrap();
    c.risk.enable_fak_fok = false;
    assert!(c.validate().is_err());
    c.risk.enable_fak_fok = true;
    c.risk.max_order_lots = Some(4);
    assert!(c.validate().is_err());
    assert!(NoStrategy
        .on_target(target(1), context(&core(0, 0)))
        .is_err());
}
#[test]
fn target_admission_rejects_wrong_identity_scope_and_closed_gates() {
    let c = core(0, 0);
    let mut s = Exec::new(&config()).unwrap();
    s.on_market_state(true, 7).unwrap();
    let mut bad = vec![];
    let mut t = target(1);
    t.source_id = "another-account".into();
    bad.push(t);
    let mut t = target(1);
    t.strategies.clear();
    bad.push(t);
    let mut t = target(1);
    with_market(&mut t, "SHFE:CU2612", 0);
    bad.push(t);
    let mut t = roster(&[("cta-a", 1), ("cta-b", 0)]);
    t.strategies
        .get_mut("cta-b")
        .unwrap()
        .insert("SHFE:sn2612".into(), 0);
    bad.push(t);
    let mut t = target(1);
    t.target_revision = 0;
    bad.push(t);
    for t in bad {
        assert!(s.accept(t, context(&c)).is_err());
    }
    assert!(s
        .accept(
            target(1),
            StrategyContext {
                core: &c,
                can_trade: false
            }
        )
        .is_err());
    assert!(s.target.is_none());
}
#[test]
fn identical_targets_are_idempotent_and_revisions_cannot_conflict_or_go_backwards() {
    let c = core(0, 0);
    let mut s = strategy(&c, 1);
    s.accept(target(1), context(&c)).unwrap();
    assert!(s.accept(target(2), context(&c)).is_err());
    let mut next = target(2);
    next.target_revision = 2;
    assert!(s.accept(next.clone(), context(&c)).is_err());
    next.batch_id = "batch-02".into();
    s.accept(next, context(&c)).unwrap();
    assert!(s.accept(target(1), context(&c)).is_err());
}
#[test]
fn roll_closes_the_held_month_whether_or_not_the_target_lists_it() {
    let mut c = core(1, 0);
    let old = c.instrument(Venue::Shfe, "cu2612").unwrap().clone();
    let mut new = old.clone();
    new.symbol = "cu2701".into();
    c.replace_instruments(vec![old, new]).unwrap();
    let mut s = strategy(&c, 1);
    let mut roll = target(0);
    roll.target_revision = 2;
    roll.batch_id = "roll".into();
    roll.strategies =
        BTreeMap::from([("cta-a".into(), BTreeMap::from([("SHFE:cu2701".into(), 1)]))]);
    // Omitted, the held month is an implicit zero like `SYSTEM_POSITION_CLOSE`.
    let mut implicit = strategy(&c, 1);
    implicit.accept(roll.clone(), context(&c)).unwrap();
    let (close, _) = request(decide(&mut implicit, &c, tick(NOW), NOW).remove(0));
    assert_eq!(
        (close.symbol.as_str(), close.side, close.offset),
        ("cu2612", Side::Sell, Offset::Close)
    );

    with_market(&mut roll, "SHFE:cu2612", 0);
    s.accept(roll, context(&c)).unwrap();
    let (close, _) = request(decide(&mut s, &c, tick(NOW), NOW).remove(0));
    assert_eq!(
        (close.symbol.as_str(), close.side, close.offset),
        ("cu2612", Side::Sell, Offset::Close)
    );

    let mut new_tick = tick(NOW);
    new_tick.symbol[..6].copy_from_slice(b"cu2701");
    let (open, _) = request(decide(&mut s, &c, new_tick, NOW).remove(0));
    assert_eq!(
        (open.symbol.as_str(), open.side, open.offset),
        ("cu2701", Side::Buy, Offset::Open)
    );
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
            7,
            101,
            PriceType::Limit,
            TimeInForce::Fak
        )
    );
    assert_eq!(*t, target(7));
    let mut s = strategy(&c, 107);
    let (i, t) = request(decide(&mut s, &c, tick(NOW), NOW).remove(0));
    assert_eq!(*t, target(107));
    assert_eq!(i.lots, 100);
    let mut c_for_order = core(0, 0);
    let planned = c_for_order.plan(i).unwrap();
    assert_eq!(c_for_order.prepare(planned[0]).unwrap().intent.lots, 100);
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
    let p = c.prepare(children[0]).unwrap();
    assert!(decide(&mut s, &c, tick(NOW + 1_000_000), NOW + 1_000_000).is_empty());
    c.on_fill(&Fill {
        identity: TradeIdentity {
            venue: Venue::Shfe,
            symbol: "cu2612".into(),
            trading_day: "20261117".into(),
            trade_id: "closing-trade".into(),
            side: Side::Buy,
        },
        order_ref: p.key.order_ref,
        order_key: Some(p.key),
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
fn filled_order_report_without_trade_facts_blocks_new_orders_but_not_target_replacement() {
    let mut c = core(0, 0);
    let mut s = strategy(&c, 2);
    let (i, _) = request(decide(&mut s, &c, tick(NOW), NOW).remove(0));
    let p = c.prepare(i).unwrap();
    c.on_order(&OrderUpdate {
        key: p.key,
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
    assert!(c.working_orders(Venue::Shfe, "cu2612").next().is_some());
    assert_eq!(c.position(Venue::Shfe, "cu2612", Side::Buy).total(), 0);
    assert!(decide(&mut s, &c, tick(NOW + 1_000_000), NOW + 1_000_000).is_empty());
    // Like crypto, a newer target replaces the old one at any time; the late
    // fill is attributed against the new strategy gaps.
    let mut t = target(3);
    t.target_revision = 2;
    t.batch_id = "batch-02".into();
    s.accept(t, context(&c)).unwrap();
    assert!(decide(&mut s, &c, tick(NOW + 1_000_000), NOW + 1_000_000).is_empty());
}
#[test]
fn opposite_strategy_targets_cross_at_the_mid_without_an_order() {
    let c = core(1, 0);
    let mut s = Exec::new(&config()).unwrap();
    s.restore_ledger(vec![(
        "SHFE:cu2612".into(),
        BTreeMap::from([("cta-a".into(), 1)]),
    )])
    .unwrap();
    s.on_market_state(true, 7).unwrap();
    s.accept(roster(&[("cta-a", 0), ("cta-b", 1)]), context(&c))
        .unwrap();
    assert!(decide(&mut s, &c, tick(NOW), NOW).is_empty());
    let events = s.take_ledger_events();
    assert_eq!(events.len(), 1);
    match &events[0] {
        LedgerEvent::StrategyCross(StrategyCross {
            price,
            transfers,
            ledger,
            ..
        }) => {
            assert_eq!(*price, 100.5);
            assert_eq!(
                (transfers[0].buyer.as_str(), transfers[0].seller.as_str()),
                ("cta-b", "cta-a")
            );
            assert_eq!(ledger, &BTreeMap::from([("cta-b".into(), 1)]));
        }
        other => panic!("unexpected {other:?}"),
    }
    assert!(decide(&mut s, &c, tick(NOW), NOW).is_empty());
    assert!(s.take_ledger_events().is_empty());
}
#[test]
fn cutover_moves_the_unallocated_account_position_to_strategies() {
    let c = core(2, 0);
    let mut s = Exec::new(&config()).unwrap();
    s.on_market_state(true, 7).unwrap();
    s.accept(roster(&[("cta-a", 2), ("cta-b", 0)]), context(&c))
        .unwrap();
    assert!(decide(&mut s, &c, tick(NOW), NOW).is_empty());
    match &s.take_ledger_events()[0] {
        LedgerEvent::StrategyCross(StrategyCross { transfers, .. }) => {
            assert_eq!(transfers[0].seller, cn_futures_target::ledger::UNALLOCATED);
            assert_eq!(transfers[0].lots, 2);
        }
        other => panic!("unexpected {other:?}"),
    }
}
#[test]
fn own_fills_are_attributed_to_strategy_gaps_and_foreign_fills_stay_unallocated() {
    let mut c = core(0, 0);
    let mut s = Exec::new(&config()).unwrap();
    s.on_market_state(true, 7).unwrap();
    s.accept(roster(&[("cta-a", 2), ("cta-b", 1)]), context(&c))
        .unwrap();
    let (i, _) = request(decide(&mut s, &c, tick(NOW), NOW).remove(0));
    assert_eq!(i.lots, 3);
    let p = c.prepare(i).unwrap();
    let first = fill(&p.key, "t1", Side::Buy, 2);
    assert!(c.on_fill(&first).unwrap());
    s.on_fill(&first, true);
    let foreign = fill(&p.key, "t2", Side::Buy, 1);
    s.on_fill(&foreign, false);
    let events = s.take_ledger_events();
    assert_eq!(events.len(), 2);
    match &events[0] {
        LedgerEvent::StrategyFillAllocation(FillAllocation {
            lots,
            allocations,
            batch_id,
            ..
        }) => {
            assert_eq!(*lots, 2);
            assert_eq!(batch_id.as_deref(), Some("batch-01"));
            assert_eq!(allocations, &BTreeMap::from([("cta-a".into(), 2)]));
        }
        other => panic!("unexpected {other:?}"),
    }
    match &events[1] {
        LedgerEvent::StrategyFillAllocation(FillAllocation {
            allocations,
            batch_id,
            ledger,
            ..
        }) => {
            assert!(batch_id.is_none());
            assert_eq!(
                allocations,
                &BTreeMap::from([(cn_futures_target::ledger::UNALLOCATED.into(), 1)])
            );
            assert_eq!(ledger, &BTreeMap::from([("cta-a".into(), 2)]));
        }
        other => panic!("unexpected {other:?}"),
    }
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
    // A quote of another trading day than the account's never trades.
    invalid.push(tick_on(NOW, b"20261118"));
    for t in invalid {
        let mut s = strategy(&c, 1);
        assert!(decide(&mut s, &c, t, NOW).is_empty());
    }
}
#[test]
fn targets_stand_across_md_reconnects_and_never_expire() {
    let c = core(0, 0);
    let mut s = strategy(&c, 1);
    s.on_market_state(false, 7).unwrap();
    // Quotes of the old connection are ignored once the feed is down.
    assert!(decide(&mut s, &c, tick(NOW), NOW).is_empty());
    s.on_market_state(true, 8).unwrap();
    let mut t = tick(NOW);
    t.connection_generation = 8;
    assert_eq!(decide(&mut s, &c, t, NOW).len(), 1);
    // Replaying the standing target is idempotent.
    assert_eq!(*s.accept(target(1), context(&c)).unwrap(), target(1));
    let mut s = strategy(&c, 1);
    let days_later = NOW + 3 * 86_400_000_000;
    assert_eq!(decide(&mut s, &c, tick(days_later), days_later).len(), 1);
}
#[test]
fn a_rejected_order_is_retried_on_its_contract_after_the_interval() {
    use crate::core::SingleLoop;
    use crate::persist::tests::channel;
    struct Reject;
    impl Trader for Reject {
        fn insert(&mut self, _: &PreparedOrder) -> Result<i32> {
            Ok(-3)
        }
        fn cancel(&mut self, _: &PreparedCancel) -> Result<i32> {
            anyhow::bail!("unexpected cancellation")
        }
    }
    let c = core(0, 0);
    let mut s = strategy(&c, 1);
    let mut runtime = SingleLoop {
        core: c,
        trader: Reject,
        execute: true,
    };
    let (journal, _records) = channel(vec![]);
    let action = decide(&mut s, &runtime.core, tick(NOW), NOW).remove(0);
    let outcome = super::super::strategy::dispatch(&mut runtime, &journal, action, true).unwrap();
    assert!(
        matches!(outcome, ActionOutcome::Submitted { receipt, .. } if !receipt.all_submitted_locally)
    );
    let early = NOW + 499_999;
    assert!(decide(&mut s, &runtime.core, tick(early), early).is_empty());
    let retry = NOW + 500_000;
    assert_eq!(decide(&mut s, &runtime.core, tick(retry), retry).len(), 1);
    journal.finish().unwrap();
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
fn a_newer_batch_after_reconnect_replaces_the_standing_target() {
    let c = core(0, 0);
    let mut s = strategy(&c, 1);
    s.on_market_state(false, 7).unwrap();
    s.on_market_state(true, 8).unwrap();
    let mut t = target(2);
    t.target_revision = 2;
    t.batch_id = "batch-02".into();
    s.accept(t, context(&c)).unwrap();
    let mut q = tick(NOW);
    q.connection_generation = 8;
    let (intent, target) = request(decide(&mut s, &c, q, NOW).remove(0));
    assert_eq!(
        (intent.side, intent.lots, target.target_revision),
        (Side::Buy, 2, 2)
    );
}

#[test]
fn evidence_follows_the_sdk_call_and_a_publish_failure_blocks_later_trading() {
    use crate::core::SingleLoop;
    use crate::persist::tests::{broken_channel, channel};
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
        let (journal, records) = if fail {
            (broken_channel(), Default::default())
        } else {
            channel(vec![])
        };
        let result = super::super::strategy::dispatch(&mut runtime, &journal, action, true);
        // The order is never delayed or withheld for evidence.
        assert_eq!(runtime.trader.calls, 1);
        assert!(runtime
            .core
            .working_orders(Venue::Shfe, "cu2612")
            .next()
            .is_some());
        if fail {
            assert!(result.is_err());
            assert!(!runtime.core.is_ready());
        } else {
            assert!(
                matches!(result.unwrap(),ActionOutcome::Submitted{receipt,..} if receipt.all_submitted_locally)
            );
            let orders = crate::persist::tests::uniform_orders(&records);
            assert_eq!(orders[0].from_key, b"exec:batch-01");
            assert_eq!(orders[0].amount_update, 0.0);
            journal.finish().unwrap();
        }
    }
}
#[test]
fn quote_decisions_note_each_stable_reason_once_and_ignore_the_flapping_callback_gate() {
    let c = core(0, 0);
    let mut s = strategy(&c, 1);
    let mut stale = tick(NOW);
    stale.received_us = NOW - 1_000_001;
    assert!(decide(&mut s, &c, stale, NOW).is_empty());
    let note = s.take_wait_note().unwrap();
    assert_eq!(
        (
            note.market.as_str(),
            note.reason,
            note.target.batch_id.as_str()
        ),
        ("SHFE:cu2612", "quote_stale", "batch-01")
    );
    assert!(decide(&mut s, &c, stale, NOW).is_empty());
    assert!(
        s.take_wait_note().is_none(),
        "unchanged reason is not repeated"
    );

    let mut closed = Actions::default();
    let gate = StrategyContext {
        core: &c,
        can_trade: false,
    };
    s.decide(&tick(NOW), gate, &mut closed, NOW).unwrap();
    assert!(closed.drain().next().is_none());
    assert!(
        s.take_wait_note().is_none(),
        "gate flaps are logged at dispatch"
    );

    assert_eq!(decide(&mut s, &c, tick(NOW), NOW).len(), 1);
    assert_eq!(s.take_wait_note().unwrap().reason, "order_action_emitted");
    assert!(decide(&mut s, &c, tick(NOW + 1), NOW + 1).is_empty());
    assert_eq!(s.take_wait_note().unwrap().reason, "retry_interval");

    let held = core(1, 0);
    let mut s = strategy(&held, 1);
    assert!(decide(&mut s, &held, tick(NOW), NOW).is_empty());
    assert_eq!(s.take_wait_note().unwrap().reason, "target_reached");
}
#[test]
fn assigned_legacy_position_is_a_priced_cross_and_is_not_crossed_again() {
    let c = core(2, 0);
    let mut s = Exec::new(&config()).unwrap();
    s.assign_position(&c, "cta-a", "SHFE:cu2612", 2, 98_000.0, NOW)
        .unwrap();
    match &s.take_ledger_events()[..] {
        [LedgerEvent::StrategyCross(StrategyCross {
            price,
            transfers,
            ledger,
            ..
        })] => {
            assert_eq!(*price, 98_000.0);
            assert_eq!(transfers[0].buyer, "cta-a");
            assert_eq!(transfers[0].seller, cn_futures_target::ledger::UNALLOCATED);
            assert_eq!(ledger, &BTreeMap::from([("cta-a".into(), 2)]));
        }
        other => panic!("unexpected {other:?}"),
    }
    assert!(
        s.assign_position(&c, "cta-b", "SHFE:cu2612", 1, 98_000.0, NOW)
            .is_err(),
        "nothing unallocated is left"
    );
    // With the target already matching, the first quote needs no cross.
    s.on_market_state(true, 7).unwrap();
    s.accept(roster(&[("cta-a", 2)]), context(&c)).unwrap();
    assert!(decide(&mut s, &c, tick(NOW), NOW).is_empty());
    assert!(s.take_ledger_events().is_empty());
}
#[test]
fn a_rejected_contract_keeps_backing_off_across_batches_until_its_target_changes() {
    let c = core(0, 0);
    let mut s = strategy(&c, 1);
    assert_eq!(decide(&mut s, &c, tick(NOW), NOW).len(), 1);
    s.on_rejected(Venue::Shfe, "cu2612", NOW);
    let batch = |revision: i64, qty: i32| {
        let mut t = target(qty);
        t.target_revision = revision;
        t.batch_id = format!("batch-{revision:02}");
        t
    };
    // Another batch with the same net target: the 500 ms backoff still holds.
    s.accept(batch(2, 1), context(&c)).unwrap();
    let early = NOW + 499_999;
    assert!(decide(&mut s, &c, tick(early), early).is_empty());
    let later = NOW + 500_000;
    assert_eq!(decide(&mut s, &c, tick(later), later).len(), 1);
    // A second rejection doubles the wait; a changed target clears it.
    s.on_rejected(Venue::Shfe, "cu2612", later);
    let retry = later + 999_999;
    assert!(decide(&mut s, &c, tick(retry), retry).is_empty());
    s.accept(batch(3, 2), context(&c)).unwrap();
    let (intent, _) = request(decide(&mut s, &c, tick(retry), retry).remove(0));
    assert_eq!(intent.lots, 2);
}

fn empty_roster(revision: i64) -> Target {
    let mut t = target(0);
    t.batch_id = format!("retire-{revision}");
    t.target_revision = revision;
    t.strategies.clear();
    t
}

#[test]
fn an_empty_roster_flattens_every_held_contract() {
    let c = core(0, 2);
    let mut s = Exec::new(&config()).unwrap();
    s.on_market_state(true, 7).unwrap();
    s.accept(empty_roster(1), context(&c)).unwrap();
    assert_eq!(
        s.implicit_contracts(&empty_roster(1), &c),
        [(Venue::Shfe, Symbol::new("cu2612").unwrap())]
    );
    let (close, _) = request(decide(&mut s, &c, tick(NOW), NOW).remove(0));
    assert_eq!(
        (close.side, close.offset, close.lots),
        (Side::Buy, Offset::Close, 2)
    );
}

#[test]
fn a_retired_strategy_is_closed_and_keeps_its_closing_fills() {
    let mut c = core(0, 0);
    let mut s = Exec::new(&config()).unwrap();
    s.on_market_state(true, 7).unwrap();
    s.accept(target(2), context(&c)).unwrap();
    let (open, _) = request(decide(&mut s, &c, tick(NOW), NOW).remove(0));
    let p = c.prepare(open).unwrap();
    let opened = fill(&p.key, "t1", Side::Buy, 2);
    assert!(c.on_fill(&opened).unwrap());
    s.on_fill(&opened, true);
    s.take_ledger_events();

    s.accept(empty_roster(2), context(&c)).unwrap();
    let later = NOW + 1_000_000;
    let (close, _) = request(decide(&mut s, &c, tick(later), later).remove(0));
    assert_eq!(
        (close.side, close.offset, close.lots),
        (Side::Sell, Offset::Close, 2)
    );
    let p = c.prepare(close).unwrap();
    let mut closed = fill(&p.key, "t2", Side::Sell, 2);
    closed.offset = Offset::Close;
    assert!(c.on_fill(&closed).unwrap());
    s.on_fill(&closed, true);
    match &s.take_ledger_events()[..] {
        [LedgerEvent::StrategyFillAllocation(FillAllocation {
            allocations,
            ledger,
            batch_id,
            ..
        })] => {
            assert_eq!(allocations, &BTreeMap::from([("cta-a".into(), -2)]));
            assert!(ledger.is_empty());
            assert_eq!(batch_id.as_deref(), Some("retire-2"));
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn a_holding_that_appears_after_admission_joins_at_the_next_refresh() {
    let flat = core(0, 0);
    let mut s = Exec::new(&config()).unwrap();
    s.on_market_state(true, 7).unwrap();
    s.accept(empty_roster(1), context(&flat)).unwrap();
    let held = core(1, 0);
    assert!(decide(&mut s, &held, tick(NOW), NOW).is_empty());
    s.refresh_scope(&held);
    let (close, _) = request(decide(&mut s, &held, tick(NOW), NOW).remove(0));
    assert_eq!(
        (close.side, close.offset, close.lots),
        (Side::Sell, Offset::Close, 1)
    );
}
