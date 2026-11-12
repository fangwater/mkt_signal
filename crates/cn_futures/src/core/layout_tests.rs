//! Shared OMS layout coverage using Yida scope only; no CTP code or SDK execution.
use super::*;

fn core() -> TradingCore {
    let limits = RiskLimits {
        max_order_lots: 20,
        max_position_lots_per_side: 100,
        max_working_lots: 100,
        max_actions_per_second: 100_000,
        minimum_cash_remaining: 0.0,
        enable_fak_fok: true,
        close_priority: ClosePriority::TodayFirst,
    };
    let mut core = TradingCore::new(limits, crate::yida::scope()).unwrap();
    core.restore(Snapshot {
        trading_day: "20261112".into(),
        session: crate::yida::group_session_key(20261112, 1),
        max_order_ref: 0,
        available_cash: 1_000_000.0,
        instruments: vec![Instrument {
            venue: Venue::Shfe,
            symbol: "cu2612".into(),
            price_tick: 10.0,
            contract_multiple: 5,
            expiry_date: 20261215,
            commission: None,
            margin: None,
            min_lots: 1,
            max_lots: 100,
            market_lots: Some(LotLimits {
                minimum: 1,
                maximum: 100,
            }),
            active: true,
            cash_budget_per_open_lot: 1000.0,
            option: None,
        }],
        positions: vec![],
        orders: vec![],
        trades: vec![],
    })
    .unwrap();
    core
}
fn intent(side: Side, offset: Offset, lots: i32) -> Intent {
    Intent {
        venue: Venue::Shfe,
        symbol: "cu2612".into(),
        side,
        offset,
        lots,
        price_type: PriceType::Limit,
        price_ticks: 100,
        time_in_force: TimeInForce::Day,
        execution_route: None,
    }
}
fn update(p: &PreparedOrder, traded: i32, status: OrderStatus) -> OrderUpdate {
    OrderUpdate {
        key: p.key.clone(),
        venue: p.intent.venue,
        symbol: p.intent.symbol.clone(),
        system_id: format!("system-{}", p.key.order_ref),
        trading_day: "20261112".into(),
        side: p.intent.side,
        offset: p.intent.offset,
        price_type: p.intent.price_type,
        time_in_force: p.intent.time_in_force,
        lots: p.intent.lots,
        traded,
        limit_price: p.limit_price,
        status,
        cancel_rejected: false,
    }
}
fn fill(p: &PreparedOrder, trade: &str, lots: i32) -> Fill {
    Fill {
        identity: TradeIdentity {
            venue: p.intent.venue,
            symbol: p.intent.symbol.clone(),
            trading_day: "20261112".into(),
            trade_id: trade.into(),
            side: p.intent.side,
        },
        order_ref: p.key.order_ref.clone(),
        order_key: Some(p.key.clone()),
        system_id: format!("system-{}", p.key.order_ref),
        offset: p.intent.offset,
        lots,
        price: p.limit_price,
    }
}
fn assert_projection(core: &TradingCore, key: &OrderKey) {
    let view = core.order(key).unwrap();
    let canonical = view.to_order();
    assert_eq!(
        serde_json::to_value(view).unwrap(),
        serde_json::to_value(&canonical).unwrap()
    );
    let mutation = crate::persistence::projection_for(core, std::slice::from_ref(key), &[]);
    assert_eq!(
        serde_json::to_value(&mutation.orders[0].order).unwrap(),
        serde_json::to_value(canonical).unwrap()
    );
    let id = core.orders.id(key).unwrap();
    let hot = &core.orders.slots[id.0];
    assert_eq!(hot.remaining(), view.remaining());
    assert_eq!(hot.lots, view.request.lots);
    assert_eq!(hot.side, view.request.side);
    assert_eq!(hot.offset, view.request.offset);
    assert_eq!(hot.price_type, view.request.price_type);
    assert_eq!(core.orders.slots.len(), core.orders.identities.len());
    let total: i64 = core
        .orders
        .slots
        .iter()
        .map(|o| i64::from(o.remaining()))
        .sum();
    assert_eq!(total, core.working_lots);
    for (symbol, ids) in core.working_by_symbol.iter().enumerate() {
        for (index, id) in ids.iter().enumerate() {
            assert_eq!(core.orders.slots[id.0].symbol, SymbolId(symbol));
            assert_eq!(
                core.orders.slots[id.0].working_index.map(|i| i.get() - 1),
                Some(index)
            );
            assert!(core.orders.slots[id.0].remaining() > 0);
        }
    }
}

#[test]
fn dense_hot_state_is_smaller_than_the_previous_full_order_slot() {
    let old = std::mem::size_of::<Order>()
        + std::mem::size_of::<SymbolId>()
        + std::mem::size_of::<Option<usize>>();
    let hot = std::mem::size_of::<OrderSlot>();
    assert!(!std::mem::needs_drop::<OrderSlot>());
    assert!(hot * 2 < old, "hot={hot}, old={old}");
    eprintln!(
        "order layout: old slot={old}B, hot slot={hot}B, cold identity={}B",
        std::mem::size_of::<OrderIdentity>()
    );
}

#[test]
fn terminal_before_fill_retains_reservation_and_projects_each_mutation() {
    let mut c = core();
    let p = c
        .prepare(intent(Side::Buy, Offset::Open, 3), Instant::now())
        .unwrap();
    assert_projection(&c, &p.key);
    c.on_order(&update(&p, 3, OrderStatus::Filled)).unwrap();
    assert_eq!(c.order(&p.key).unwrap().remaining(), 3);
    assert_eq!(c.pending_open[0][0], 3);
    assert_projection(&c, &p.key);
    let first = fill(&p, "first", 1);
    assert!(c.on_fill(&first).unwrap());
    assert!(!c.on_fill(&first).unwrap());
    assert_projection(&c, &p.key);
    c.on_fill(&fill(&p, "second", 2)).unwrap();
    assert_projection(&c, &p.key);
    assert_eq!(c.working_lots, 0);
    assert_eq!(c.pending_open[0][0], 0);
    assert_eq!(c.position(Venue::Shfe, "cu2612", Side::Buy).today, 3);
}

#[test]
fn close_cancel_and_late_fill_release_exact_inventory() {
    let mut c = core();
    let open = c
        .prepare(intent(Side::Buy, Offset::Open, 3), Instant::now())
        .unwrap();
    c.on_fill(&fill(&open, "open", 3)).unwrap();
    let close = c
        .prepare(intent(Side::Sell, Offset::CloseToday, 3), Instant::now())
        .unwrap();
    c.prepare_cancel(&close.key, Instant::now()).unwrap();
    assert_projection(&c, &close.key);
    c.on_order(&update(&close, 1, OrderStatus::Cancelled))
        .unwrap();
    assert_eq!(c.order(&close.key).unwrap().remaining(), 1);
    assert_eq!(c.position(Venue::Shfe, "cu2612", Side::Buy).frozen_today, 1);
    assert_projection(&c, &close.key);
    c.on_fill(&fill(&close, "close", 1)).unwrap();
    assert_projection(&c, &close.key);
    let position = c.position(Venue::Shfe, "cu2612", Side::Buy);
    assert_eq!((position.today, position.frozen_today), (2, 0));
}

#[test]
fn invalid_reports_leave_hot_and_cold_state_unchanged() {
    let mut c = core();
    let p = c
        .prepare(intent(Side::Buy, Offset::Open, 2), Instant::now())
        .unwrap();
    let before = serde_json::to_value(c.view()).unwrap();
    let mut invalid = update(&p, 0, OrderStatus::Working);
    invalid.side = Side::Sell;
    assert!(c.on_order(&invalid).is_err());
    assert!(c.on_fill(&fill(&p, "too-much", 3)).is_err());
    assert_eq!(serde_json::to_value(c.view()).unwrap(), before);
    c.on_fill(&fill(&p, "one", 1)).unwrap();
    let before = serde_json::to_value(c.view()).unwrap();
    assert!(c.on_order(&update(&p, 0, OrderStatus::Rejected)).is_err());
    assert_eq!(serde_json::to_value(c.view()).unwrap(), before);
    assert_projection(&c, &p.key);
}

#[test]
fn growth_prefault_and_restore_keep_hot_cold_indexes_and_complete_identities() {
    let mut c = core();
    // Exceed the initial reserve and retain the earliest identity after Vec growth.
    let mut first = None;
    for _ in 0..4100 {
        let p = c
            .prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
            .unwrap();
        c.reject(&p.key).unwrap();
        first.get_or_insert(p.key);
    }
    let key = first.unwrap();
    let before = serde_json::to_value(c.view()).unwrap();
    c.reserve_orders(8192).unwrap();
    assert_eq!(serde_json::to_value(c.view()).unwrap(), before);
    assert_projection(&c, &key);
    let p = c
        .prepare(intent(Side::Buy, Offset::Open, 2), Instant::now())
        .unwrap();
    let mut report = update(&p, 0, OrderStatus::Working);
    report.system_id = "s".repeat(63);
    c.on_order(&report).unwrap();
    c.restore(Snapshot {
        trading_day: "20261112".into(),
        session: p.key.session.clone(),
        max_order_ref: p.key.order_ref.parse().unwrap(),
        available_cash: 998000.0,
        instruments: c.view().instruments.into_iter().cloned().collect(),
        positions: vec![],
        orders: vec![report],
        trades: vec![],
    })
    .unwrap();
    assert_projection(&c, &p.key);
    assert_eq!(c.order(&p.key).unwrap().system_id.len(), 63);
    assert!(!c.order(&p.key).unwrap().owned);
}

#[test]
fn self_trade_checks_use_hot_side_price_and_market_flag() {
    let mut c = core();
    let p = c
        .prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .unwrap();
    assert!(c
        .prepare(intent(Side::Sell, Offset::Open, 1), Instant::now())
        .is_err());
    let mut sell = intent(Side::Sell, Offset::Open, 1);
    sell.price_ticks = 101;
    let passive = c.prepare(sell, Instant::now()).unwrap();
    c.reject(&p.key).unwrap();
    let mut market = intent(Side::Buy, Offset::Open, 1);
    market.price_type = PriceType::Market;
    market.price_ticks = 0;
    assert!(c.prepare(market.clone(), Instant::now()).is_err());
    c.reject(&passive.key).unwrap();
    let p = c.prepare(market, Instant::now()).unwrap();
    assert_projection(&c, &p.key);
}

#[test]
fn removing_middle_working_order_updates_moved_index_and_pending_lots() {
    let mut c = core();
    let a = c
        .prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .unwrap();
    let b = c
        .prepare(intent(Side::Buy, Offset::Open, 2), Instant::now())
        .unwrap();
    let d = c
        .prepare(intent(Side::Buy, Offset::Open, 3), Instant::now())
        .unwrap();
    c.reject(&b.key).unwrap();
    assert_eq!(c.pending_open[0][0], 4);
    assert_projection(&c, &d.key);
    c.reject(&d.key).unwrap();
    assert_eq!(c.pending_open[0][0], 1);
    assert_projection(&c, &a.key);
    c.reject(&a.key).unwrap();
    assert!(c.working_by_symbol[0].is_empty());
    assert_eq!(c.pending_open[0][0], 0);
}

#[test]
fn arena_preserves_noncanonical_identities_and_replaces_both_halves_together() {
    let mut c = core();
    let p = c
        .prepare(intent(Side::Buy, Offset::Open, 1), Instant::now())
        .unwrap();
    let original = c.order(&p.key).unwrap().to_order();
    let mut arena = OrderArena::default();
    for reference in ["1", "01", "external", "18446744073709551616"] {
        let mut order = original.clone();
        order.key.order_ref = reference.into();
        order.system_id = format!("{}-{reference}", "s".repeat(40));
        let key = order.key.clone();
        arena.insert(order.clone(), SymbolId(0));
        assert_eq!(
            serde_json::to_value(arena.get(&key).unwrap()).unwrap(),
            serde_json::to_value(order).unwrap()
        );
    }
    assert_eq!(arena.slots.len(), 4);
    let mut replacement = original;
    replacement.key.order_ref = "01".into();
    replacement.filled = 1;
    replacement.status = OrderStatus::Filled;
    replacement.system_id = "replacement".into();
    let key = replacement.key.clone();
    let id = arena.id(&key).unwrap();
    assert_eq!(arena.insert(replacement.clone(), SymbolId(0)), (id, 1));
    assert_eq!(arena.slots.len(), 4);
    assert_eq!(arena.slots[id.0].remaining(), 0);
    assert_eq!(
        serde_json::to_value(arena.get(&key).unwrap()).unwrap(),
        serde_json::to_value(replacement).unwrap()
    );
    assert_eq!(
        arena
            .get(&OrderKey {
                session: key.session,
                order_ref: "1".into()
            })
            .unwrap()
            .remaining(),
        1
    );
}
