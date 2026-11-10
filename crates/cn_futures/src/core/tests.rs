use super::*;

#[test]
fn same_session_reconciliation_keeps_rejected_attempt_references_consumed() {
    let mut core = core();
    let request = core
        .prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .unwrap();
    core.reject(&request.key).unwrap();
    core.restore(snapshot()).unwrap();
    assert_eq!(
        core.prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
            .unwrap()
            .key
            .order_ref,
        "102"
    );
}

#[test]
fn recovery_does_not_reuse_an_observed_reference_above_the_login_maximum() {
    let mut core = core();
    let request = core
        .prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .unwrap();
    let mut s = snapshot();
    s.orders.push(update(&request, 0, OrderStatus::Working));
    core.restore(s).unwrap();
    assert_eq!(
        core.prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
            .unwrap()
            .key
            .order_ref,
        "102"
    );
}
#[test]
fn incomplete_fact_queries_cannot_restore_reported_fills() {
    let mut core = core();
    let request = core
        .prepare(intent(Side::Buy, Offset::Open, 3), Instant::now())
        .unwrap();
    let mut s = snapshot();
    s.orders.push(update(&request, 1, OrderStatus::Working));
    assert!(core.restore(s).is_err());
    assert_eq!(core.order(&request.key).unwrap().filled, 0);
}

#[test]
fn failed_pre_submit_audit_never_reaches_the_sdk() {
    let mut runtime = SingleLoop {
        core: core(),
        trader: Fake::default(),
        execute: true,
    };
    assert!(runtime
        .submit_with(
            intent(Side::Buy, Offset::Open, 1),
            Instant::now(),
            |_| bail!("audit unavailable")
        )
        .is_err());
    assert_eq!(runtime.trader.inserts, 0);
    assert_eq!(runtime.core.view().available_local_budget, 1_000_000.0);
    assert!(!runtime.core.is_ready());
}

fn limits() -> RiskLimits {
    RiskLimits {
        max_order_lots: 10,
        max_position_lots_per_side: 20,
        max_working_lots: 20,
        max_actions_per_second: 20,
        minimum_cash_remaining: 1000.0,
        enable_fak_fok: false,
        close_priority: ClosePriority::TodayFirst,
    }
}
fn snapshot() -> Snapshot {
    Snapshot {
        trading_day: "20261109".into(),
        session: crate::ctp::session_key(7, 11),
        max_order_ref: 100,
        available_cash: 1_000_000.0,
        instruments: vec![Instrument {
            option: None,
            venue: Venue::Cffex,
            symbol: "IF2612".into(),
            price_tick: 0.2,
            min_lots: 1,
            max_lots: 100,
            market_lots: None,
            active: true,
            cash_budget_per_open_lot: 10000.0,
        }],
        positions: vec![],
        orders: vec![],
        trades: vec![],
    }
}
fn core() -> TradingCore {
    let mut core = TradingCore::new(limits(), crate::ctp::scope()).unwrap();
    core.set_trading_status(crate::model::Venue::Cffex, "IF", true);
    core.restore(snapshot()).unwrap();
    core
}
fn intent(side: Side, offset: Offset, lots: i32) -> Intent {
    Intent {
        venue: Venue::Cffex,
        symbol: "IF2612".into(),
        side,
        offset,
        lots,
        price_type: PriceType::Limit,
        price_ticks: 20_000,
        time_in_force: TimeInForce::Day,
        execution_route: None,
    }
}
fn update(request: &PreparedOrder, traded: i32, status: OrderStatus) -> OrderUpdate {
    OrderUpdate {
        key: request.key.clone(),
        symbol: request.intent.symbol.clone(),
        venue: Venue::Cffex,
        system_id: format!("SYS-{}", request.key.order_ref),
        side: request.intent.side,
        offset: request.intent.offset,
        lots: request.intent.lots,
        traded,
        price_type: request.intent.price_type,
        limit_price: request.limit_price,
        status,
        cancel_rejected: false,
        trading_day: "20261109".into(),
    }
}
fn fill(request: &PreparedOrder, id: &str, lots: i32) -> Fill {
    Fill {
        identity: TradeIdentity {
            venue: Venue::Cffex,
            symbol: request.intent.symbol.clone(),
            trading_day: "20261109".into(),
            trade_id: id.into(),
            side: request.intent.side,
        },
        order_ref: request.key.order_ref.clone(),
        order_key: None,
        system_id: format!("SYS-{}", request.key.order_ref),
        offset: request.intent.offset,
        lots,
        price: request.limit_price,
    }
}
#[derive(Default)]
struct Fake {
    inserts: usize,
    cancels: usize,
    code: i32,
    uncertain: bool,
}
impl Trader for Fake {
    fn insert(&mut self, _: &PreparedOrder) -> Result<i32> {
        self.inserts += 1;
        if self.uncertain {
            bail!("uncertain")
        }
        Ok(self.code)
    }
    fn cancel(&mut self, _: &PreparedCancel) -> Result<i32> {
        self.cancels += 1;
        Ok(self.code)
    }
}

#[test]
fn supported_scope_excludes_options_coin_crypto_and_bad_months() {
    for symbol in [
        "IF2612", "IH2612", "IC2612", "IM2612", "TS2612", "TF2612", "T2612", "TL2612",
    ] {
        assert!(is_cffex_future(symbol));
    }
    for symbol in [
        "IF2613",
        "IF2600",
        "IO2612-C-4000",
        "BTCUSDT",
        "CU2612",
        "IF",
        "IF2612x",
        "中金所2612",
    ] {
        assert!(!is_cffex_future(symbol));
    }
}
#[test]
fn synchronous_acceptance_only_reserves_does_not_fill() {
    let mut runtime = SingleLoop {
        core: core(),
        trader: Fake::default(),
        execute: true,
    };
    let key = runtime
        .submit(intent(Side::Buy, Offset::Open, 3), Instant::now())
        .unwrap();
    assert_eq!(runtime.trader.inserts, 1);
    assert_eq!(runtime.core.order(&key).unwrap().remaining(), 3);
    assert_eq!(
        runtime
            .core
            .position(Venue::Cffex, "IF2612", Side::Buy)
            .total(),
        0
    );
    assert_eq!(runtime.core.view().available_local_budget, 970_000.0);
}
#[test]
fn read_only_and_pretrade_rejections_never_call_sdk() {
    let mut runtime = SingleLoop {
        core: core(),
        trader: Fake::default(),
        execute: false,
    };
    assert!(runtime
        .submit(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .is_err());
    runtime.execute = true;
    assert!(runtime
        .submit(intent(Side::Buy, Offset::Open, 11), Instant::now())
        .is_err());
    assert_eq!(runtime.trader.inserts, 0);
    assert!(runtime.core.view().orders.is_empty());
}
#[test]
fn documented_local_failure_releases_once() {
    let mut runtime = SingleLoop {
        core: core(),
        trader: Fake {
            code: -3,
            ..Fake::default()
        },
        execute: true,
    };
    assert!(runtime
        .submit(intent(Side::Buy, Offset::Open, 3), Instant::now())
        .is_err());
    let key = runtime.core.view().orders[0].key.clone();
    runtime.core.reject(&key).unwrap();
    assert_eq!(runtime.core.view().available_local_budget, 1_000_000.0);
    assert_eq!(runtime.core.order(&key).unwrap().remaining(), 0);
}
#[test]
fn uncertain_submission_keeps_reservation_and_blocks_new_orders() {
    let mut runtime = SingleLoop {
        core: core(),
        trader: Fake {
            uncertain: true,
            ..Fake::default()
        },
        execute: true,
    };
    assert!(runtime
        .submit(intent(Side::Buy, Offset::Open, 3), Instant::now())
        .is_err());
    assert!(!runtime.core.is_ready());
    assert_eq!(runtime.core.view().orders[0].remaining(), 3);
    assert_eq!(runtime.core.view().available_local_budget, 970_000.0);
}
#[test]
fn duplicate_fill_is_idempotent_and_conflicting_identity_fails() {
    let mut core = core();
    let request = core
        .prepare(intent(Side::Buy, Offset::Open, 3), Instant::now())
        .unwrap();
    let first = fill(&request, "TRADE-A", 1);
    assert!(core.on_fill(first.clone()).unwrap());
    assert!(!core.on_fill(first.clone()).unwrap());
    assert_eq!(core.position(Venue::Cffex, "IF2612", Side::Buy).today, 1);
    let mut changed = first;
    changed.lots = 2;
    assert!(core.on_fill(changed).is_err());
    assert_eq!(core.position(Venue::Cffex, "IF2612", Side::Buy).today, 1);
    core.on_fill(fill(&request, "TRADE-B", 2)).unwrap();
    assert_eq!(core.position(Venue::Cffex, "IF2612", Side::Buy).today, 3);
    assert_eq!(core.order(&request.key).unwrap().remaining(), 0);
}
#[test]
fn cumulative_order_report_does_not_invent_a_fill_or_release_pending_fill() {
    let mut core = core();
    let request = core
        .prepare(intent(Side::Buy, Offset::Open, 3), Instant::now())
        .unwrap();
    core.on_order(update(&request, 3, OrderStatus::Filled))
        .unwrap();
    assert_eq!(core.position(Venue::Cffex, "IF2612", Side::Buy).total(), 0);
    assert_eq!(core.order(&request.key).unwrap().remaining(), 3);
    core.on_fill(fill(&request, "TRADE-A", 3)).unwrap();
    core.on_order(update(&request, 0, OrderStatus::Working))
        .unwrap();
    assert_eq!(
        core.order(&request.key).unwrap().status,
        OrderStatus::Filled
    );
    assert_eq!(core.position(Venue::Cffex, "IF2612", Side::Buy).total(), 3);
}
#[test]
fn cancel_request_and_cancel_failure_do_not_release_budget() {
    let mut runtime = SingleLoop {
        core: core(),
        trader: Fake::default(),
        execute: true,
    };
    let key = runtime
        .submit(intent(Side::Buy, Offset::Open, 3), Instant::now())
        .unwrap();
    runtime.trader.code = -3;
    assert!(runtime.cancel(&key, Instant::now()).is_err());
    assert_eq!(runtime.core.order(&key).unwrap().remaining(), 3);
    assert!(!runtime.core.order(&key).unwrap().cancel_pending);
    assert_eq!(runtime.core.view().available_local_budget, 970_000.0);
}
#[test]
fn cancel_confirmation_retains_unseen_reported_fill_reservation() {
    let mut core = core();
    let request = core
        .prepare(intent(Side::Buy, Offset::Open, 3), Instant::now())
        .unwrap();
    core.prepare_cancel(&request.key, Instant::now()).unwrap();
    assert_eq!(core.order(&request.key).unwrap().remaining(), 3);
    core.on_order(update(&request, 1, OrderStatus::Cancelled))
        .unwrap();
    assert_eq!(core.order(&request.key).unwrap().remaining(), 1);
    assert_eq!(core.view().available_local_budget, 990_000.0);
    core.on_fill(fill(&request, "LATE-FILL", 1)).unwrap();
    assert_eq!(core.order(&request.key).unwrap().remaining(), 0);
    assert_eq!(core.position(Venue::Cffex, "IF2612", Side::Buy).today, 1);
}
#[test]
fn close_allocation_reserves_dates_and_releases_only_cancelled_part() {
    let mut core = core();
    let mut s = snapshot();
    s.positions.push(PositionSnapshot {
        venue: Venue::Cffex,
        symbol: "IF2612".into(),
        side: Side::Buy,
        today: 1,
        yesterday: 2,
    });
    core.restore(s).unwrap();
    let request = core
        .prepare(intent(Side::Sell, Offset::Close, 3), Instant::now())
        .unwrap();
    assert_eq!(core.position(Venue::Cffex, "IF2612", Side::Buy).frozen(), 3);
    assert!(core
        .prepare(intent(Side::Sell, Offset::Close, 1), Instant::now())
        .is_err());
    core.on_order(update(&request, 1, OrderStatus::Cancelled))
        .unwrap();
    let p = core.position(Venue::Cffex, "IF2612", Side::Buy);
    assert_eq!((p.frozen_today, p.frozen_yesterday), (1, 0));
    core.on_fill(fill(&request, "CLOSE-ONE", 1)).unwrap();
    let p = core.position(Venue::Cffex, "IF2612", Side::Buy);
    assert_eq!((p.today, p.yesterday, p.frozen()), (0, 2, 0));
}
#[test]
fn rejected_close_restores_frozen_inventory() {
    let mut core = core();
    let mut s = snapshot();
    s.positions.push(PositionSnapshot {
        venue: Venue::Cffex,
        symbol: "IF2612".into(),
        side: Side::Sell,
        today: 2,
        yesterday: 0,
    });
    core.restore(s).unwrap();
    let request = core
        .prepare(intent(Side::Buy, Offset::Close, 2), Instant::now())
        .unwrap();
    core.reject(&request.key).unwrap();
    assert_eq!(
        core.position(Venue::Cffex, "IF2612", Side::Sell).frozen(),
        0
    );
    assert_eq!(core.position(Venue::Cffex, "IF2612", Side::Sell).total(), 2);
}
#[test]
fn disconnect_preserves_state_and_cancels_remain_possible_after_reconnect() {
    let mut core = core();
    let request = core
        .prepare(intent(Side::Buy, Offset::Open, 3), Instant::now())
        .unwrap();
    core.disconnect();
    assert!(core.prepare_cancel(&request.key, Instant::now()).is_err());
    core.connected();
    assert!(!core.is_ready());
    assert_eq!(core.order(&request.key).unwrap().remaining(), 3);
    assert!(core.prepare_cancel(&request.key, Instant::now()).is_ok());
    assert!(core
        .prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .is_err());
}
#[test]
fn malformed_restore_does_not_overwrite_live_state() {
    let mut core = core();
    let request = core
        .prepare(intent(Side::Buy, Offset::Open, 3), Instant::now())
        .unwrap();
    let mut s = snapshot();
    s.available_cash = f64::NAN;
    assert!(core.restore(s).is_err());
    assert_eq!(core.order(&request.key).unwrap().remaining(), 3);
}
#[test]
fn status_unknown_inactive_and_non_allowlisted_contracts_cannot_trade() {
    let mut core = TradingCore::new(limits(), crate::ctp::scope()).unwrap();
    core.restore(snapshot()).unwrap();
    assert!(core
        .prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .is_err());
    core.set_trading_status(crate::model::Venue::Cffex, "IF", true);
    core.prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .unwrap();
    core.set_trading_status(crate::model::Venue::Cffex, "IF", false);
    assert!(core
        .prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .is_err());
    let mut other = intent(Side::Buy, Offset::Open, 1);
    other.symbol = "IF2703".into();
    assert!(core.prepare(other, Instant::now()).is_err());
}
#[test]
fn self_trade_and_shared_action_rate_are_checked_before_submission() {
    let mut core = core();
    let now = Instant::now();
    let request = core
        .prepare(intent(Side::Buy, Offset::Open, 1), now)
        .unwrap();
    assert!(core
        .prepare(intent(Side::Sell, Offset::Open, 1), now)
        .is_err());
    core.limits.max_actions_per_second = 1;
    assert!(core.prepare_cancel(&request.key, now).is_err());
    assert!(core
        .prepare_cancel(&request.key, now + Duration::from_secs(1))
        .is_ok());
}
#[test]
fn snapshot_seeds_trade_deduplication_and_existing_open_reservations() {
    let mut core = core();
    let request = core
        .prepare(intent(Side::Buy, Offset::Open, 3), Instant::now())
        .unwrap();
    let f = fill(&request, "PAST-TRADE", 1);
    let mut s = snapshot();
    s.orders.push(update(&request, 1, OrderStatus::Working));
    s.trades.push(f.clone());
    s.positions.push(PositionSnapshot {
        venue: Venue::Cffex,
        symbol: "IF2612".into(),
        side: Side::Buy,
        today: 1,
        yesterday: 0,
    });
    s.max_order_ref = 101;
    core.restore(s).unwrap();
    assert!(!core.on_fill(f).unwrap());
    assert_eq!(core.position(Venue::Cffex, "IF2612", Side::Buy).today, 1);
    assert_eq!(core.order(&request.key).unwrap().remaining(), 2);
    assert_eq!(
        core.prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
            .unwrap()
            .key
            .order_ref,
        "102"
    );
}
#[test]
fn unknown_fill_is_an_error_not_silently_dropped_or_attributed() {
    let mut core = core();
    let request = PreparedOrder {
        key: OrderKey {
            session: crate::ctp::session_key(1, 2),
            order_ref: "forced-close".into(),
        },
        intent: intent(Side::Sell, Offset::Close, 1),
        option_market_buy_ceiling: 0.0,
        limit_price: 4000.0,
    };
    assert!(core
        .on_fill(fill(&request, "VENUE-LIQUIDATION", 1))
        .is_err());
    assert_eq!(core.position(Venue::Cffex, "IF2612", Side::Buy).total(), 0);
}

#[test]
fn cancel_rejection_via_order_stream_allows_retry_without_releasing() {
    let mut core = core();
    let request = core
        .prepare(intent(Side::Buy, Offset::Open, 3), Instant::now())
        .unwrap();
    core.prepare_cancel(&request.key, Instant::now()).unwrap();
    let mut rejected = update(&request, 0, OrderStatus::Working);
    rejected.cancel_rejected = true;
    core.on_order(rejected).unwrap();
    assert_eq!(core.order(&request.key).unwrap().remaining(), 3);
    assert!(core.prepare_cancel(&request.key, Instant::now()).is_ok());
}
#[test]
fn completed_rule_omission_and_inactive_refresh_remove_permission() {
    let mut core = core();
    core.replace_instruments(vec![]).unwrap();
    assert!(core
        .prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .is_err());
    let mut rules = snapshot().instruments;
    rules[0].active = false;
    core.replace_instruments(rules).unwrap();
    assert!(core
        .prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .is_err());
}
#[test]
fn cash_poll_tightens_but_never_replenishes_local_reservations() {
    let mut core = core();
    core.prepare(intent(Side::Buy, Offset::Open, 3), Instant::now())
        .unwrap();
    core.constrain_cash(2_000_000.0).unwrap();
    assert_eq!(core.view().available_local_budget, 970_000.0);
    core.constrain_cash(5000.0).unwrap();
    assert!(core
        .prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .is_err());
}
#[test]
fn yesterday_first_cancellation_preserves_missing_fill_in_older_inventory() {
    let mut core = TradingCore::new(
        RiskLimits {
            close_priority: ClosePriority::YesterdayFirst,
            ..limits()
        },
        crate::ctp::scope(),
    )
    .unwrap();
    core.set_trading_status(crate::model::Venue::Cffex, "IF", true);
    let mut s = snapshot();
    s.positions.push(PositionSnapshot {
        venue: Venue::Cffex,
        symbol: "IF2612".into(),
        side: Side::Buy,
        today: 2,
        yesterday: 1,
    });
    core.restore(s).unwrap();
    let request = core
        .prepare(intent(Side::Sell, Offset::Close, 3), Instant::now())
        .unwrap();
    core.on_order(update(&request, 1, OrderStatus::Cancelled))
        .unwrap();
    let p = core.position(Venue::Cffex, "IF2612", Side::Buy);
    assert_eq!((p.frozen_today, p.frozen_yesterday), (0, 1));
    core.on_fill(fill(&request, "OLD-CLOSE", 1)).unwrap();
    let p = core.position(Venue::Cffex, "IF2612", Side::Buy);
    assert_eq!((p.today, p.yesterday, p.frozen()), (2, 0, 0));
}

fn cn_snapshot() -> Snapshot {
    let mut s = snapshot();
    s.session = crate::yida::session_key(20261109);
    s.instruments[0].venue = Venue::Shfe;
    s.instruments[0].symbol = "cu2612".into();
    s.instruments.push(Instrument {
        venue: Venue::Ine,
        symbol: "sc2612".into(),
        ..s.instruments[0].clone()
    });
    s.positions.push(PositionSnapshot {
        venue: Venue::Shfe,
        symbol: "cu2612".into(),
        side: Side::Buy,
        today: 2,
        yesterday: 3,
    });
    s
}
fn cn_core() -> TradingCore {
    let mut c = TradingCore::new(limits(), crate::yida::scope()).unwrap();
    c.restore(cn_snapshot()).unwrap();
    c
}
fn cn_intent(offset: Offset, lots: i32) -> Intent {
    Intent {
        venue: Venue::Shfe,
        symbol: "cu2612".into(),
        side: Side::Sell,
        offset,
        lots,
        ..intent(Side::Sell, offset, lots)
    }
}
fn cn_update(p: &PreparedOrder, traded: i32, status: OrderStatus) -> OrderUpdate {
    OrderUpdate {
        venue: p.intent.venue,
        ..update(p, traded, status)
    }
}
fn cn_fill(p: &PreparedOrder, id: &str, lots: i32) -> Fill {
    let mut f = fill(p, id, lots);
    f.identity.venue = p.intent.venue;
    f
}
#[test]
fn shfe_close_requires_exact_date_inventory() {
    let mut c = cn_core();
    assert!(c
        .prepare(cn_intent(Offset::Close, 1), Instant::now())
        .is_err());
    assert!(c
        .prepare(cn_intent(Offset::CloseToday, 3), Instant::now())
        .is_err());
    let p = c
        .prepare(cn_intent(Offset::CloseYesterday, 3), Instant::now())
        .unwrap();
    assert_eq!(
        (
            c.order(&p.key).unwrap().reserved_today,
            c.order(&p.key).unwrap().reserved_yesterday
        ),
        (0, 3)
    );
    c.on_order(cn_update(&p, 1, OrderStatus::Cancelled))
        .unwrap();
    c.on_fill(cn_fill(&p, "YESTERDAY-CLOSE", 1)).unwrap();
    let p = c.position(Venue::Shfe, "cu2612", Side::Buy);
    assert_eq!((p.today, p.yesterday, p.frozen()), (2, 2, 0));
}
#[test]
fn generic_close_reserves_both_dates_before_any_send() {
    let mut r = SingleLoop {
        core: cn_core(),
        trader: Fake::default(),
        execute: true,
    };
    let receipt = r
        .submit_planned_with(cn_intent(Offset::Close, 5), Instant::now(), |_| Ok(()))
        .unwrap();
    assert!(receipt.all_submitted_locally);
    assert_eq!(r.trader.inserts, 2);
    assert_eq!(
        receipt
            .children
            .iter()
            .map(|p| (p.offset, p.lots))
            .collect::<Vec<_>>(),
        vec![(Offset::CloseToday, 2), (Offset::CloseYesterday, 3)]
    );
    let p = r.core.position(Venue::Shfe, "cu2612", Side::Buy);
    assert_eq!((p.frozen_today, p.frozen_yesterday), (2, 3));
}
#[test]
fn split_fok_and_second_child_preflight_failure_send_nothing() {
    let mut r = SingleLoop {
        core: cn_core(),
        trader: Fake::default(),
        execute: true,
    };
    r.core.limits.enable_fak_fok = true;
    let mut i = cn_intent(Offset::Close, 5);
    i.time_in_force = TimeInForce::Fok;
    assert!(r
        .submit_planned_with(i, Instant::now(), |_| Ok(()))
        .is_err());
    let mut s = cn_snapshot();
    s.instruments[0].min_lots = 2;
    r.core.restore(s).unwrap();
    // First child has 2 lots; the 1-lot second child fails validation. No send occurs.
    assert!(r
        .submit_planned_with(cn_intent(Offset::Close, 3), Instant::now(), |_| Ok(()))
        .is_err());
    assert_eq!(r.trader.inserts, 0);
    assert_eq!(
        r.core.position(Venue::Shfe, "cu2612", Side::Buy).frozen(),
        0
    );
}
struct SecondFailure {
    calls: usize,
    uncertain: bool,
}
impl Trader for SecondFailure {
    fn insert(&mut self, _: &PreparedOrder) -> Result<i32> {
        self.calls += 1;
        if self.calls == 2 {
            if self.uncertain {
                bail!("send uncertain")
            }
            return Ok(20);
        }
        Ok(0)
    }
    fn cancel(&mut self, _: &PreparedCancel) -> Result<i32> {
        Ok(0)
    }
}
#[test]
fn split_send_failure_returns_every_identity_and_preserves_correct_reservations() {
    for uncertain in [false, true] {
        let mut r = SingleLoop {
            core: cn_core(),
            trader: SecondFailure {
                calls: 0,
                uncertain,
            },
            execute: true,
        };
        let receipt = r
            .submit_planned_with(cn_intent(Offset::Close, 5), Instant::now(), |_| Ok(()))
            .unwrap();
        assert_eq!(receipt.children.len(), 2);
        assert!(!receipt.all_submitted_locally);
        assert!(matches!(
            receipt.children[0].outcome,
            ChildOutcome::SubmittedLocally
        ));
        assert_eq!(
            r.core.position(Venue::Shfe, "cu2612", Side::Buy).frozen(),
            if uncertain { 5 } else { 2 }
        );
        assert_eq!(r.core.is_ready(), !uncertain);
        if uncertain {
            assert!(matches!(
                receipt.children[1].outcome,
                ChildOutcome::Uncertain
            ));
            assert!(r.core.restore(cn_snapshot()).is_err());
        } else {
            assert!(matches!(
                receipt.children[1].outcome,
                ChildOutcome::RejectedLocally
            ));
        }
    }
}
#[test]
fn native_cancel_before_system_identity_retains_reservations() {
    let mut r = SingleLoop {
        core: cn_core(),
        trader: Fake::default(),
        execute: true,
    };
    let p = r
        .core
        .prepare(cn_intent(Offset::CloseToday, 1), Instant::now())
        .unwrap();
    r.cancel(&p.key, Instant::now()).unwrap();
    assert!(r.core.order(&p.key).unwrap().cancel_pending);
    assert_eq!(r.trader.cancels, 1);
    r.core
        .on_order(cn_update(&p, 0, OrderStatus::Working))
        .unwrap();
    assert!(r.cancel(&p.key, Instant::now()).is_err());
    assert_eq!(r.trader.cancels, 1);
    assert_eq!(
        r.core.position(Venue::Shfe, "cu2612", Side::Buy).frozen(),
        1
    );
}
#[test]
fn native_system_ids_and_trade_ids_are_venue_scoped() {
    let mut c = cn_core();
    let a = c
        .prepare(cn_intent(Offset::Open, 1), Instant::now())
        .unwrap();
    let b = c
        .prepare(
            Intent {
                venue: Venue::Ine,
                symbol: "sc2612".into(),
                ..cn_intent(Offset::Open, 1)
            },
            Instant::now(),
        )
        .unwrap();
    for p in [&a, &b] {
        let mut u = cn_update(p, 0, OrderStatus::Working);
        u.system_id = "123".into();
        c.on_order(u).unwrap();
        let mut f = cn_fill(p, "456", 1);
        f.system_id = "123".into();
        c.on_fill(f).unwrap();
    }
    assert_eq!(c.position(Venue::Shfe, "cu2612", Side::Sell).today, 1);
    assert_eq!(c.position(Venue::Ine, "sc2612", Side::Sell).today, 1);
    assert!(c
        .prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .is_err());
}
#[test]
fn recovered_manual_reference_zero_does_not_erase_system_order_identity() {
    let mut c = cn_core();
    let mut s = cn_snapshot();
    for (venue, symbol) in [(Venue::Shfe, "cu2612"), (Venue::Ine, "sc2612")] {
        s.orders.push(OrderUpdate {
            key: OrderKey {
                session: format!("yida:20261109:{}:sys:77", venue.as_str()),
                order_ref: "0".into(),
            },
            venue,
            symbol: symbol.into(),
            system_id: "77".into(),
            side: Side::Sell,
            offset: Offset::Open,
            lots: 1,
            traded: 0,
            price_type: PriceType::Limit,
            limit_price: 4000.0,
            status: OrderStatus::Working,
            cancel_rejected: false,
            trading_day: s.trading_day.clone(),
        });
    }
    c.restore(s).unwrap();
    c.on_fill(Fill {
        identity: TradeIdentity {
            venue: Venue::Ine,
            symbol: "sc2612".into(),
            trading_day: "20261109".into(),
            trade_id: "22".into(),
            side: Side::Sell,
        },
        order_ref: "0".into(),
        order_key: None,
        system_id: "77".into(),
        offset: Offset::Open,
        lots: 1,
        price: 4000.0,
    })
    .unwrap();
    assert_eq!(c.position(Venue::Ine, "sc2612", Side::Sell).today, 1);
    assert_eq!(c.position(Venue::Shfe, "cu2612", Side::Sell).today, 0);
}

#[test]
fn recovery_cannot_forget_known_fills_or_change_pending_order_identity() {
    let mut c = core();
    let p = c
        .prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .unwrap();
    c.on_fill(fill(&p, "FACT", 1)).unwrap();
    assert!(c.restore(snapshot()).is_err());
    let mut c = core();
    let p = c
        .prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .unwrap();
    let mut s = snapshot();
    let mut u = update(&p, 0, OrderStatus::Working);
    u.side = Side::Sell;
    s.orders.push(u);
    assert!(c.restore(s).is_err());
    assert_eq!(c.order(&p.key).unwrap().request.side, Side::Buy);
}

fn market_core(minimum: i32, maximum: i32) -> TradingCore {
    let mut s = cn_snapshot();
    s.instruments[0].market_lots = Some(LotLimits { minimum, maximum });
    let mut c = TradingCore::new(limits(), crate::yida::scope()).unwrap();
    c.restore(s).unwrap();
    c
}
fn market_intent(side: Side, lots: i32) -> Intent {
    Intent {
        side,
        price_type: PriceType::Market,
        price_ticks: 0,
        ..cn_intent(Offset::Open, lots)
    }
}

#[test]
fn market_orders_use_native_quantity_rules_and_cannot_silently_become_limits() {
    let mut c = market_core(2, 4);
    for lots in [1, 3, 5, 6] {
        assert!(c
            .prepare(market_intent(Side::Sell, lots), Instant::now())
            .is_err());
    }
    let mut priced = market_intent(Side::Sell, 2);
    priced.price_ticks = 20000;
    assert!(c.prepare(priced, Instant::now()).is_err());
    let p = c
        .prepare(market_intent(Side::Sell, 2), Instant::now())
        .unwrap();
    assert_eq!(p.limit_price, 0.0);
    assert_eq!(
        c.order(&p.key).unwrap().request.price_type,
        PriceType::Market
    );
    // Unsupported contracts and the CTP adapter do not inherit limit-order limits.
    assert!(cn_core()
        .prepare(market_intent(Side::Sell, 1), Instant::now())
        .is_err());
    let mut ctp_market = intent(Side::Buy, Offset::Open, 1);
    ctp_market.price_type = PriceType::Market;
    ctp_market.price_ticks = 0;
    assert!(core().prepare(ctp_market, Instant::now()).is_err());
}

#[test]
fn market_self_trade_protection_covers_both_order_arrival_directions() {
    for side in [Side::Buy, Side::Sell] {
        for market_first in [false, true] {
            let mut c = market_core(1, 10);
            let mut first = cn_intent(Offset::Open, 1);
            first.side = side;
            // Choose a limit well away from a crossing price. A market order can still cross it.
            first.price_ticks = if side == Side::Buy { 1 } else { 100000 };
            let mut second = first.clone();
            second.side = side.opposite();
            if market_first {
                first = market_intent(side, 1);
            } else {
                second = market_intent(side.opposite(), 1);
            }
            c.prepare(first, Instant::now()).unwrap();
            assert!(c
                .prepare(second, Instant::now())
                .unwrap_err()
                .to_string()
                .contains("self-trade"));
        }
    }
}

#[test]
fn recovery_preserves_zero_price_market_orders_and_self_trade_protection() {
    let mut c = market_core(1, 10);
    let p = c
        .prepare(market_intent(Side::Sell, 1), Instant::now())
        .unwrap();
    let u = cn_update(&p, 0, OrderStatus::Working);
    c.on_order(u.clone()).unwrap();
    let mut s = cn_snapshot();
    s.instruments[0].market_lots = Some(LotLimits {
        minimum: 1,
        maximum: 10,
    });
    s.orders.push(u.clone());
    c.restore(s.clone()).unwrap();
    assert_eq!(
        c.order(&p.key).unwrap().request.price_type,
        PriceType::Market
    );
    let mut opposite = cn_intent(Offset::Open, 1);
    opposite.side = Side::Buy;
    opposite.price_ticks = 1;
    assert!(c.prepare(opposite, Instant::now()).is_err());
    let mut changed = u;
    changed.price_type = PriceType::Limit;
    changed.limit_price = 4000.0;
    assert!(c.on_order(changed.clone()).is_err());
    s.orders[0] = changed;
    assert!(c.restore(s).is_err());
    assert_eq!(
        c.order(&p.key).unwrap().request.price_type,
        PriceType::Market
    );
}

#[derive(Clone, Copy)]
enum BatchBehavior {
    Success,
    LocalReject,
    Uncertain,
}
struct BatchFake {
    behavior: BatchBehavior,
    insert_calls: usize,
    cancel_calls: usize,
}
impl Trader for BatchFake {
    fn insert(&mut self, _: &PreparedOrder) -> Result<i32> {
        panic!("batch must call native batch API")
    }
    fn cancel(&mut self, _: &PreparedCancel) -> Result<i32> {
        panic!("batch must call native batch API")
    }
    fn insert_batch(&mut self, _: &[PreparedOrder]) -> Result<BatchSend> {
        self.insert_calls += 1;
        self.result()
    }
    fn cancel_batch(&mut self, _: &[PreparedCancel]) -> Result<BatchSend> {
        self.cancel_calls += 1;
        self.result()
    }
}
impl BatchFake {
    fn result(&self) -> Result<BatchSend> {
        match self.behavior {
            BatchBehavior::Success => Ok(BatchSend::SubmittedLocally),
            BatchBehavior::LocalReject => Ok(BatchSend::RejectedBeforeSend { index: 1, code: 40 }),
            BatchBehavior::Uncertain => {
                anyhow::bail!("native batch partly sent; outcome uncertain")
            }
        }
    }
}
fn batch_loop(behavior: BatchBehavior) -> SingleLoop<BatchFake> {
    SingleLoop {
        core: cn_core(),
        trader: BatchFake {
            behavior,
            insert_calls: 0,
            cancel_calls: 0,
        },
        execute: true,
    }
}
fn close_batch() -> Vec<Intent> {
    vec![
        cn_intent(Offset::CloseToday, 1),
        cn_intent(Offset::CloseYesterday, 1),
    ]
}

#[test]
fn native_batch_outcomes_release_only_certainly_unsent_reservations() {
    for behavior in [
        BatchBehavior::Success,
        BatchBehavior::LocalReject,
        BatchBehavior::Uncertain,
    ] {
        let mut runtime = batch_loop(behavior);
        let receipt = runtime
            .submit_batch_with(close_batch(), Instant::now(), |_| Ok(()))
            .unwrap();
        assert_eq!(runtime.trader.insert_calls, 1);
        assert_eq!(
            receipt
                .children
                .iter()
                .map(|c| c.parent_index)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        let released = matches!(behavior, BatchBehavior::LocalReject);
        assert_eq!(
            runtime
                .core
                .position(Venue::Shfe, "cu2612", Side::Buy)
                .frozen(),
            if released { 0 } else { 2 }
        );
        assert_eq!(
            runtime.core.is_ready(),
            !matches!(behavior, BatchBehavior::Uncertain)
        );
        assert_eq!(
            receipt.all_submitted_locally,
            matches!(behavior, BatchBehavior::Success)
        );
        if matches!(behavior, BatchBehavior::Uncertain) {
            assert!(receipt
                .children
                .iter()
                .all(|c| matches!(c.outcome, ChildOutcome::Uncertain)));
            assert!(runtime.core.restore(cn_snapshot()).is_err());
        }
    }
}

#[test]
fn batch_preflight_and_audit_failure_send_nothing() {
    for audit_failure in [false, true] {
        let mut runtime = batch_loop(BatchBehavior::Success);
        let mut intents = close_batch();
        if !audit_failure {
            intents[1].lots = 10000;
        }
        let result = runtime.submit_batch_with(intents, Instant::now(), |_| {
            anyhow::bail!("audit unavailable")
        });
        assert_eq!(runtime.trader.insert_calls, 0);
        assert_eq!(
            runtime
                .core
                .position(Venue::Shfe, "cu2612", Side::Buy)
                .frozen(),
            0
        );
        if audit_failure {
            assert!(result
                .unwrap()
                .children
                .iter()
                .all(|c| matches!(c.outcome, ChildOutcome::NotSent)));
        } else {
            assert!(result.is_err());
        }
    }
    let mut runtime = batch_loop(BatchBehavior::Success);
    assert!(runtime
        .submit_batch_with(vec![], Instant::now(), |_| Ok(()))
        .is_err());
    assert!(runtime
        .submit_batch_with(
            vec![cn_intent(Offset::Open, 1); 17],
            Instant::now(),
            |_| Ok(())
        )
        .is_err());
    assert_eq!(runtime.trader.insert_calls, 0);
}

#[test]
fn batch_cancel_before_ack_keeps_frozen_inventory_until_factual_outcomes() {
    for behavior in [
        BatchBehavior::Success,
        BatchBehavior::LocalReject,
        BatchBehavior::Uncertain,
    ] {
        let mut runtime = batch_loop(behavior);
        let keys: Vec<_> = close_batch()
            .into_iter()
            .map(|i| runtime.core.prepare(i, Instant::now()).unwrap().key)
            .collect();
        assert!(keys
            .iter()
            .all(|k| runtime.core.order(k).unwrap().system_id.is_empty()));
        let receipt = runtime
            .cancel_batch_with(keys.clone(), Instant::now(), |_| Ok(()))
            .unwrap();
        assert_eq!(runtime.trader.cancel_calls, 1);
        assert_eq!(
            runtime
                .core
                .position(Venue::Shfe, "cu2612", Side::Buy)
                .frozen(),
            2
        );
        assert_eq!(
            receipt.all_submitted_locally,
            matches!(behavior, BatchBehavior::Success)
        );
        assert!(keys
            .iter()
            .all(|k| runtime.core.order(k).unwrap().cancel_pending
                != matches!(behavior, BatchBehavior::LocalReject)));
        assert_eq!(
            runtime.core.is_ready(),
            !matches!(behavior, BatchBehavior::Uncertain)
        );
    }
}

#[test]
fn duplicate_or_invalid_batch_cancel_never_leaves_a_false_pending_flag() {
    let mut runtime = batch_loop(BatchBehavior::Success);
    let key = runtime
        .core
        .prepare(cn_intent(Offset::CloseToday, 1), Instant::now())
        .unwrap()
        .key;
    assert!(runtime
        .cancel_batch_with(vec![key.clone(), key.clone()], Instant::now(), |_| Ok(()))
        .is_err());
    let bad = OrderKey {
        session: key.session.clone(),
        order_ref: "99999".into(),
    };
    assert!(runtime
        .cancel_batch_with(vec![key.clone(), bad], Instant::now(), |_| Ok(()))
        .is_err());
    assert!(!runtime.core.order(&key).unwrap().cancel_pending);
    assert_eq!(runtime.trader.cancel_calls, 0);
}

#[test]
fn fill_reference_from_another_native_group_cannot_match_a_local_order() {
    let mut runtime = cn_core();
    let order = runtime
        .prepare(cn_intent(Offset::Open, 1), Instant::now())
        .unwrap();
    let mut fill = cn_fill(&order, "group-collision", 1);
    fill.order_key = Some(OrderKey {
        session: crate::yida::group_session_key(20261109, 7),
        order_ref: order.key.order_ref.clone(),
    });
    assert!(runtime.on_fill(fill).is_err());
    assert_eq!(runtime.order(&order.key).unwrap().filled, 0);
}

fn option_snapshot() -> Snapshot {
    let mut s = cn_snapshot();
    let rule = &mut s.instruments[0];
    rule.symbol = "cu2612C106000".into();
    rule.price_tick = 2.0;
    rule.cash_budget_per_open_lot = 100.0;
    rule.market_lots = Some(LotLimits {
        minimum: 1,
        maximum: 30,
    });
    rule.option = Some(OptionContract {
        right: OptionRight::Call,
        underlying: "cu2612".into(),
        strike_price: 106000.0,
        contract_multiple: 5,
        expiry_date: 20261124,
        upper_limit_price: 12000.0,
        cash_buffer_per_buy_lot: 10.0,
    });
    s.positions.clear();
    for side in [Side::Buy, Side::Sell] {
        s.positions.push(PositionSnapshot {
            venue: Venue::Shfe,
            symbol: rule.symbol.clone(),
            side,
            today: 2,
            yesterday: 3,
        });
    }
    s
}
fn option_core() -> TradingCore {
    let mut c = TradingCore::new(limits(), crate::yida::scope()).unwrap();
    c.restore(option_snapshot()).unwrap();
    c
}
fn option_intent(side: Side, offset: Offset, lots: i32) -> Intent {
    Intent {
        symbol: "cu2612C106000".into(),
        side,
        price_ticks: 100,
        ..cn_intent(offset, lots)
    }
}

#[test]
fn option_buy_open_and_buy_close_reserve_premium_and_sells_do_not_credit_cash() {
    for offset in [Offset::Open, Offset::CloseToday, Offset::CloseYesterday] {
        let mut c = option_core();
        let buy = c
            .prepare(option_intent(Side::Buy, offset, 1), Instant::now())
            .unwrap();
        assert_eq!(c.order(&buy.key).unwrap().cash_reserved_per_lot, 1010.0);
        assert_eq!(c.view().available_local_budget, 1_000_000.0 - 1010.0);
        let mut c = option_core();
        let sell = c
            .prepare(option_intent(Side::Sell, offset, 1), Instant::now())
            .unwrap();
        let held = if offset == Offset::Open { 100.0 } else { 0.0 };
        assert_eq!(c.order(&sell.key).unwrap().local_cash_held, held);
        c.on_order(cn_update(&sell, 1, OrderStatus::Filled))
            .unwrap();
        c.on_fill(cn_fill(&sell, "option-sell", 1)).unwrap();
        assert_eq!(c.view().available_local_budget, 1_000_000.0 - held);
    }
}

#[test]
fn option_market_buy_reserves_the_native_daily_upper_bound_and_rejects_missing_or_unfunded_bounds()
{
    for offset in [Offset::Open, Offset::CloseToday] {
        let mut c = option_core();
        let mut i = option_intent(Side::Buy, offset, 1);
        i.price_type = PriceType::Market;
        i.price_ticks = 0;
        let order = c.prepare(i.clone(), Instant::now()).unwrap();
        assert_eq!(order.option_market_buy_ceiling, 12000.0);
        assert_eq!(c.order(&order.key).unwrap().local_cash_held, 60010.0);
        let mut snapshot = option_snapshot();
        snapshot.available_cash = 2000.0;
        let mut c = option_core();
        c.restore(snapshot).unwrap();
        assert!(c.prepare(i.clone(), Instant::now()).is_err());
        let mut snapshot = option_snapshot();
        snapshot.instruments[0]
            .option
            .as_mut()
            .unwrap()
            .upper_limit_price = 0.0;
        let mut c = option_core();
        c.restore(snapshot).unwrap();
        assert!(c.prepare(i, Instant::now()).is_err());
    }
}

#[test]
fn option_close_partial_cancel_refunds_original_cash_unit_and_retains_unseen_fill_freeze() {
    let mut c = option_core();
    let p = c
        .prepare(
            option_intent(Side::Buy, Offset::CloseToday, 2),
            Instant::now(),
        )
        .unwrap();
    let mut rules = option_snapshot().instruments;
    rules[0].option.as_mut().unwrap().cash_buffer_per_buy_lot = 500.0;
    c.replace_instruments(rules).unwrap();
    c.on_order(cn_update(&p, 1, OrderStatus::Cancelled))
        .unwrap();
    assert_eq!(c.order(&p.key).unwrap().local_cash_held, 1010.0);
    assert_eq!(
        c.position(Venue::Shfe, "cu2612C106000", Side::Sell)
            .frozen_today,
        1
    );
    assert_eq!(c.view().available_local_budget, 1_000_000.0 - 1010.0);
    let fill = cn_fill(&p, "option-close-partial", 1);
    c.on_fill(fill.clone()).unwrap();
    assert!(!c.on_fill(fill).unwrap());
    assert_eq!(
        c.position(Venue::Shfe, "cu2612C106000", Side::Sell).today,
        1
    );
    assert_eq!(
        c.position(Venue::Shfe, "cu2612C106000", Side::Sell)
            .frozen_today,
        0
    );
    assert_eq!(c.view().available_local_budget, 1_000_000.0 - 1010.0);
}

#[test]
fn options_cannot_bypass_scope_metadata_or_the_premium_check_when_closing() {
    let mut scope = crate::yida::scope();
    scope.allow_options = false;
    assert!(TradingCore::new(limits(), scope)
        .unwrap()
        .restore(option_snapshot())
        .is_err());
    let mut c = option_core();
    c.constrain_cash(1000.0).unwrap();
    assert!(c
        .prepare(
            option_intent(Side::Buy, Offset::CloseToday, 1),
            Instant::now()
        )
        .is_err());
    c.constrain_cash(-1.0).unwrap();
    // Selling an owned option remains possible without crediting its expected proceeds.
    c.prepare(
        option_intent(Side::Sell, Offset::CloseToday, 1),
        Instant::now(),
    )
    .unwrap();
    let mut s = option_snapshot();
    s.instruments[0].option = None;
    assert!(option_core().restore(s).is_err());
}
