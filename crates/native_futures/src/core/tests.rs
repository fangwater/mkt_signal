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
        front_id: 7,
        session_id: 11,
        max_order_ref: 100,
        available_cash: 1_000_000.0,
        instruments: vec![Instrument {
            symbol: "IF2612".into(),
            price_tick: 0.2,
            min_lots: 1,
            max_lots: 100,
            active: true,
            cash_budget_per_open_lot: 10000.0,
        }],
        positions: vec![],
        orders: vec![],
        trades: vec![],
    }
}
fn core() -> TradingCore {
    let mut core = TradingCore::new(limits()).unwrap();
    core.set_trading_status("IF", true);
    core.restore(snapshot()).unwrap();
    core
}
fn intent(side: Side, offset: Offset, lots: i32) -> Intent {
    Intent {
        symbol: "IF2612".into(),
        side,
        offset,
        lots,
        price_ticks: 20_000,
        time_in_force: TimeInForce::Day,
    }
}
fn update(request: &PreparedOrder, traded: i32, status: OrderStatus) -> OrderUpdate {
    OrderUpdate {
        key: request.key.clone(),
        symbol: request.intent.symbol.clone(),
        exchange: "CFFEX".into(),
        system_id: format!("SYS-{}", request.key.order_ref),
        side: request.intent.side,
        offset: request.intent.offset,
        lots: request.intent.lots,
        traded,
        limit_price: request.limit_price,
        status,
        cancel_rejected: false,
        trading_day: "20261109".into(),
    }
}
fn fill(request: &PreparedOrder, id: &str, lots: i32) -> Fill {
    Fill {
        identity: TradeIdentity {
            exchange: "CFFEX".into(),
            symbol: request.intent.symbol.clone(),
            trading_day: "20261109".into(),
            trade_id: id.into(),
            side: request.intent.side,
        },
        order_ref: request.key.order_ref.clone(),
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
    assert_eq!(runtime.core.position("IF2612", Side::Buy).total(), 0);
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
    assert_eq!(core.position("IF2612", Side::Buy).today, 1);
    let mut changed = first;
    changed.lots = 2;
    assert!(core.on_fill(changed).is_err());
    assert_eq!(core.position("IF2612", Side::Buy).today, 1);
    core.on_fill(fill(&request, "TRADE-B", 2)).unwrap();
    assert_eq!(core.position("IF2612", Side::Buy).today, 3);
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
    assert_eq!(core.position("IF2612", Side::Buy).total(), 0);
    assert_eq!(core.order(&request.key).unwrap().remaining(), 3);
    core.on_fill(fill(&request, "TRADE-A", 3)).unwrap();
    core.on_order(update(&request, 0, OrderStatus::Working))
        .unwrap();
    assert_eq!(
        core.order(&request.key).unwrap().status,
        OrderStatus::Filled
    );
    assert_eq!(core.position("IF2612", Side::Buy).total(), 3);
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
    assert_eq!(core.position("IF2612", Side::Buy).today, 1);
}
#[test]
fn close_allocation_reserves_dates_and_releases_only_cancelled_part() {
    let mut core = core();
    let mut s = snapshot();
    s.positions.push(PositionSnapshot {
        symbol: "IF2612".into(),
        side: Side::Buy,
        today: 1,
        yesterday: 2,
    });
    core.restore(s).unwrap();
    let request = core
        .prepare(intent(Side::Sell, Offset::Close, 3), Instant::now())
        .unwrap();
    assert_eq!(core.position("IF2612", Side::Buy).frozen(), 3);
    assert!(core
        .prepare(intent(Side::Sell, Offset::Close, 1), Instant::now())
        .is_err());
    core.on_order(update(&request, 1, OrderStatus::Cancelled))
        .unwrap();
    let p = core.position("IF2612", Side::Buy);
    assert_eq!((p.frozen_today, p.frozen_yesterday), (1, 0));
    core.on_fill(fill(&request, "CLOSE-ONE", 1)).unwrap();
    let p = core.position("IF2612", Side::Buy);
    assert_eq!((p.today, p.yesterday, p.frozen()), (0, 2, 0));
}
#[test]
fn rejected_close_restores_frozen_inventory() {
    let mut core = core();
    let mut s = snapshot();
    s.positions.push(PositionSnapshot {
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
    assert_eq!(core.position("IF2612", Side::Sell).frozen(), 0);
    assert_eq!(core.position("IF2612", Side::Sell).total(), 2);
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
    let mut core = TradingCore::new(limits()).unwrap();
    core.restore(snapshot()).unwrap();
    assert!(core
        .prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .is_err());
    core.set_trading_status("IF", true);
    core.prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .unwrap();
    core.set_trading_status("IF", false);
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
        symbol: "IF2612".into(),
        side: Side::Buy,
        today: 1,
        yesterday: 0,
    });
    s.max_order_ref = 101;
    core.restore(s).unwrap();
    assert!(!core.on_fill(f).unwrap());
    assert_eq!(core.position("IF2612", Side::Buy).today, 1);
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
            front_id: 1,
            session_id: 2,
            order_ref: "forced-close".into(),
        },
        intent: intent(Side::Sell, Offset::Close, 1),
        limit_price: 4000.0,
    };
    assert!(core
        .on_fill(fill(&request, "VENUE-LIQUIDATION", 1))
        .is_err());
    assert_eq!(core.position("IF2612", Side::Buy).total(), 0);
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
    let mut core = TradingCore::new(RiskLimits {
        close_priority: ClosePriority::YesterdayFirst,
        ..limits()
    })
    .unwrap();
    core.set_trading_status("IF", true);
    let mut s = snapshot();
    s.positions.push(PositionSnapshot {
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
    let p = core.position("IF2612", Side::Buy);
    assert_eq!((p.frozen_today, p.frozen_yesterday), (0, 1));
    core.on_fill(fill(&request, "OLD-CLOSE", 1)).unwrap();
    let p = core.position("IF2612", Side::Buy);
    assert_eq!((p.today, p.yesterday, p.frozen()), (2, 0, 0));
}
