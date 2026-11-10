use super::*;
use session::Session;
fn config() -> config::Config {
    toml::from_str(include_str!("../../../../config/yida_trade.example.toml")).unwrap()
}

fn native_config(protocol: &str) -> String {
    format!("TradingServerIP=127.0.0.1\nTradingServerPort=1\nTradingProtocol={protocol}\nUDPTradingType=socket\nXTCPTradingType=automatic\nConnectTCPMarketData=no\nReceiveUDPMarketData=no\nRecalcMode=off\n")
}

#[test]
fn public_simulation_is_endpoint_pinned_and_has_one_lot_limits() {
    let mut c = config();
    c.public_simulation = true;
    c.validate().unwrap();
    assert!(c.validate_sdk_config_text(&native_config("XTCP")).is_err());
    let native = native_config("XTCP")
        .replace("127.0.0.1", "118.190.175.212")
        .replace("TradingServerPort=1\n", "TradingServerPort=41600\n");
    c.validate_sdk_config_text(&native).unwrap();
    for extra in [
        "TradingServerIP2=127.0.0.1\n",
        "RecoverySiteCount=1\n",
        "UDPTradingServerIP=127.0.0.1\n",
    ] {
        assert!(c
            .validate_sdk_config_text(&(native.clone() + extra))
            .is_err());
    }
    c.risk.max_order_lots = 2;
    assert!(c.validate().is_err());
    c.risk.max_order_lots = 1;
    c.risk.max_working_lots = 3;
    assert!(c.validate().is_err());
}

fn option_row(
    kind: i32,
    venue: &str,
    underlying: &str,
    right: OptionRight,
    strike: i32,
) -> RawEvent {
    let symbol = format!(
        "{underlying}{}{strike}",
        if right == OptionRight::Call { "C" } else { "P" }
    );
    let mut e = row(kind, venue, &symbol);
    e.product_class = 2;
    e.options_type = if right == OptionRight::Call { 1 } else { 2 };
    e.contract_multiple = if venue == "INE" { 1000 } else { 5 };
    e.expiry = 20261220;
    e.strike_price = f64::from(strike);
    e.upper_limit_price = 1000.0;
    put(&mut e.underlying, underlying);
    put(&mut e.underlying_exchange, venue);
    e
}

#[test]
fn options_inventory_uses_the_same_ledger_and_still_requires_a_trading_allowlist() {
    for public_simulation in [false, true] {
        let mut c = config();
        c.public_simulation = public_simulation;
        let mut e = option_row(POSITION, "INE", "sc2701", OptionRight::Call, 790);
        e.position_direction = 2;
        e.date = 2;
        e.lots = 19;
        let mut session = Session::default();
        begin(&mut session, &c);
        session.on_event(&e, &c, None).unwrap();
        let snapshot = session
            .on_event(
                &RawEvent {
                    kind: END,
                    day: 20261109,
                    ..RawEvent::default()
                },
                &c,
                None,
            )
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.positions.len(), 1);
        let mut core = crate::core::TradingCore::new(c.risk.clone(), scope()).unwrap();
        core.restore(snapshot).unwrap();
        assert_eq!(
            core.position(Venue::Ine, "sc2701C790", Side::Buy).yesterday,
            19
        );
        assert!(core
            .prepare(
                Intent {
                    venue: Venue::Ine,
                    symbol: "sc2701C790".into(),
                    side: Side::Sell,
                    offset: Offset::CloseYesterday,
                    lots: 1,
                    price_type: PriceType::Limit,
                    price_ticks: 2000,
                    time_in_force: TimeInForce::Day,
                    execution_route: None
                },
                std::time::Instant::now()
            )
            .is_err());
        begin(&mut session, &c);
        e.lots = -1;
        assert!(session.on_event(&e, &c, None).is_err());
        assert_eq!(
            core.position(Venue::Ine, "sc2701C790", Side::Buy).yesterday,
            19
        );
    }
}

#[test]
fn historical_account_notifications_do_not_establish_or_poison_the_recovery_baseline() {
    let c = config();
    let mut s = Session::default();
    for kind in [CONNECTED, LOGIN, INIT] {
        s.on_event(
            &RawEvent {
                kind,
                day: 20261109,
                ..RawEvent::default()
            },
            &c,
            None,
        )
        .unwrap();
    }
    for login_count in [0, 2, 1] {
        s.on_event(
            &RawEvent {
                kind: ACCOUNT,
                login_count,
                available: 123.0,
                ..RawEvent::default()
            },
            &c,
            None,
        )
        .unwrap();
        assert!(!s.ready);
    }
    begin(&mut s, &c);
    let snapshot = s
        .on_event(
            &RawEvent {
                kind: END,
                day: 20261109,
                ..RawEvent::default()
            },
            &c,
            None,
        )
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.available_cash, 1_000_000.0);
    assert!(s.ready);
    assert!(s
        .on_event(
            &RawEvent {
                kind: ACCOUNT,
                login_count: 0,
                available: 123.0,
                ..RawEvent::default()
            },
            &c,
            None
        )
        .is_err());
    assert!(!s.ready);
}

#[test]
fn native_protocol_must_match_and_udp_sender_cannot_silently_fall_back() {
    use config::TradingProtocol::*;
    let mut c = config();
    for protocol in [Tcp, Udp, Xtcp] {
        c.trading_protocol = protocol;
        let native = native_config(protocol.native_name());
        c.validate_sdk_config_text(&native).unwrap();
        assert!(c
            .validate_sdk_config_text(&native.replace("TradingProtocol=", "OtherProtocol="))
            .is_err());
        assert!(c
            .validate_sdk_config_text(&(native.clone() + "UDPTrading=yes\n"))
            .is_err());
        assert!(c
            .validate_sdk_config_text(&(native.clone() + "TradingProtocol=TCP\n"))
            .is_err());
        for other in [Tcp, Udp, Xtcp].into_iter().filter(|p| *p != protocol) {
            assert!(c
                .validate_sdk_config_text(&native_config(other.native_name()))
                .is_err());
        }
    }
    c.trading_protocol = Udp;
    assert!(c
        .validate_sdk_config_text(
            &native_config("UDP").replace("UDPTradingType=socket", "UDPTradingType=automatic")
        )
        .is_err());
}

#[test]
fn sdk_threads_must_stay_off_the_configured_business_core() {
    let mut c = config();
    c.business_core = Some(3);
    let native = native_config("XTCP");
    assert!(c.validate_sdk_config_text(&native).is_err());
    let native = native + "TCPTradingCPUID=4\nTimerCPUID=4\nXTCPTradingCPUID=4\n";
    c.validate_sdk_config_text(&native).unwrap();
    assert!(c
        .validate_sdk_config_text(&native.replace("TCPTradingCPUID=4", "TCPTradingCPUID=3"))
        .is_err());
}

#[test]
fn long_identities_do_not_collapse_to_truncated_exchange_ids() {
    let mut e = row(ORDER, "SHFE", "cu2612");
    e.system_id = (1_i64 << 40) + 55;
    e.legacy_system_id = 55;
    e.status = 1;
    e.lots = 1;
    e.price = 4000.0;
    let first = e.to_order(e.recovered_key().unwrap()).unwrap();
    e.system_id += 1_i64 << 32;
    let second = e.to_order(e.recovered_key().unwrap()).unwrap();
    assert_ne!(first.system_id, second.system_id);
    assert_ne!(first.key, second.key);
    e.kind = TRADE;
    e.trade_id = (1_i64 << 40) + 55;
    e.legacy_trade_id = 55;
    let first_fill = e.to_fill().unwrap();
    e.trade_id += 1_i64 << 32;
    assert_ne!(first_fill.identity, e.to_fill().unwrap().identity);
    e.order_group = 1;
    assert!(e.to_fill().is_err());
    e.order_group = 0;
    e.trade_flag = 1;
    assert!(e.to_fill().is_err());
}
fn put(out: &mut [u8], value: &str) {
    out[..value.len()].copy_from_slice(value.as_bytes());
}
fn row(kind: i32, venue: &str, symbol: &str) -> RawEvent {
    let mut e = RawEvent {
        kind,
        day: 20261109,
        product_class: 1,
        hedge: 1,
        replay: 1,
        ..RawEvent::default()
    };
    put(&mut e.exchange, venue);
    put(&mut e.symbol, symbol);
    e
}

fn valid_order() -> RawEvent {
    RawEvent {
        reference: 101,
        system_id: (1_i64 << 40) + 17,
        lots: 3,
        price: 4000.0,
        status: 1,
        ..row(ORDER, "SHFE", "cu2612")
    }
}

#[test]
fn order_parser_covers_all_native_statuses_sides_and_close_dates() {
    for (venue, symbol) in [("SHFE", "cu2612"), ("INE", "sc2612")] {
        for (direction, side) in [(0, Side::Buy), (1, Side::Sell)] {
            for (offset, expected_offset) in [
                (0, Offset::Open),
                (3, Offset::CloseToday),
                (4, Offset::CloseYesterday),
            ] {
                for (status, expected_status) in [
                    (0, OrderStatus::Pending),
                    (1, OrderStatus::Working),
                    (2, OrderStatus::Cancelled),
                    (3, OrderStatus::Filled),
                    (4, OrderStatus::Rejected),
                ] {
                    let mut e = RawEvent {
                        direction,
                        offset,
                        status,
                        ..valid_order()
                    };
                    e.exchange.fill(0);
                    e.symbol.fill(0);
                    put(&mut e.exchange, venue);
                    put(&mut e.symbol, symbol);
                    e.traded = match status {
                        2 => 1,
                        3 => 3,
                        _ => 0,
                    };
                    if status == 4 {
                        e.error = 25;
                    }
                    let parsed = e.to_order(e.recovered_key().unwrap()).unwrap();
                    assert_eq!(
                        (parsed.side, parsed.offset, parsed.status),
                        (side, expected_offset, expected_status)
                    );
                    assert_eq!(parsed.traded, e.traded);
                    assert_eq!(parsed.system_id, e.system_id.to_string());
                    assert_eq!(parsed.trading_day, "20261109");
                }
            }
        }
    }
}

#[test]
fn malformed_order_reports_never_become_valid_oms_updates() {
    let base = valid_order();
    let key = base.recovered_key().unwrap();
    let invalid = [
        RawEvent {
            kind: TRADE,
            ..base
        },
        RawEvent { status: 5, ..base },
        RawEvent {
            order_type: 2,
            ..base
        },
        RawEvent {
            order_type: 4,
            ..base
        },
        RawEvent {
            order_attribute: 2,
            ..base
        },
        RawEvent {
            direction: 48,
            ..base
        },
        RawEvent { offset: 1, ..base },
        RawEvent { lots: 0, ..base },
        RawEvent { traded: -1, ..base },
        RawEvent { traded: 4, ..base },
        RawEvent {
            price: f64::NAN,
            ..base
        },
        RawEvent {
            price: f64::INFINITY,
            ..base
        },
        RawEvent { price: 0.0, ..base },
        RawEvent {
            status: 3,
            traded: 2,
            ..base
        },
        RawEvent {
            status: 4,
            traded: 1,
            error: 25,
            ..base
        },
        RawEvent { error: 25, ..base },
        RawEvent {
            system_id: -1,
            ..base
        },
        RawEvent { hedge: 3, ..base },
        RawEvent {
            order_group: 256,
            ..base
        },
        RawEvent { flag: 1, ..base },
        RawEvent {
            product_class: 2,
            ..base
        },
        RawEvent { day: 0, ..base },
        RawEvent {
            symbol: [b'x'; 32],
            ..base
        },
    ];
    for (i, event) in invalid.into_iter().enumerate() {
        assert!(
            event.to_order(key.clone()).is_err(),
            "invalid order case {i}"
        );
    }
    for (venue, symbol) in [
        ("CFFEX", "IF2612"),
        ("SHFE", "cu2612C60000"),
        ("UNKNOWN", "cu2612"),
    ] {
        let mut e = base;
        e.exchange.fill(0);
        e.symbol.fill(0);
        put(&mut e.exchange, venue);
        put(&mut e.symbol, symbol);
        assert!(e.to_order(key.clone()).is_err());
    }
}

#[test]
fn native_market_reports_use_exchange_attribute_and_allow_zero_order_price_only() {
    for (venue, symbol) in [("SHFE", "cu2612"), ("INE", "sc2612")] {
        for order_type in [0, 1, 3] {
            for status in 0..=4 {
                let mut e = RawEvent {
                    order_type,
                    order_attribute: 1,
                    price: 0.0,
                    status,
                    traded: if status == 3 { 3 } else { 0 },
                    ..valid_order()
                };
                e.exchange.fill(0);
                e.symbol.fill(0);
                put(&mut e.exchange, venue);
                put(&mut e.symbol, symbol);
                let key = e.recovered_key().unwrap();
                let order = e.to_order(key.clone()).unwrap();
                assert_eq!(order.price_type, PriceType::Market);
                e.price = -1.0;
                assert!(e.to_order(key.clone()).is_err());
                e.price = f64::NAN;
                assert!(e.to_order(key).is_err());
                // A factual fill still requires a positive execution price.
                e.kind = TRADE;
                e.price = 0.0;
                e.trade_id = 88;
                assert!(e.to_fill().is_err());
            }
        }
    }
}

#[test]
fn historical_rejections_are_audit_only_before_the_snapshot_barrier() {
    let e = RawEvent {
        status: 4,
        error: 40,
        system_id: -1,
        price: 0.0,
        order_attribute: 1,
        replay: 0,
        ..valid_order()
    };
    let mut s = Session::default();
    assert!(s.is_pre_snapshot_rejection(&e));
    let key = OrderKey {
        session: session_key(e.day),
        order_ref: e.reference.to_string(),
    };
    e.to_order(key).unwrap();
    assert!(!s.is_pre_snapshot_rejection(&RawEvent { replay: 1, ..e }));
    assert!(!s.is_pre_snapshot_rejection(&RawEvent { status: 0, ..e }));
    let c = config();
    begin(&mut s, &c);
    assert!(!s.is_pre_snapshot_rejection(&e));
    // A live update inside the snapshot must still invalidate recovery.
    assert!(s.on_event(&e, &c, None).is_err());
    assert!(!s.is_pre_snapshot_rejection(&e));
}

#[test]
fn factual_fill_parser_preserves_identity_and_rejects_invalid_evidence() {
    let base = RawEvent {
        kind: TRADE,
        trade_id: i64::MAX,
        commission: 1.25,
        ..valid_order()
    };
    let fill = base.to_fill().unwrap();
    assert_eq!(fill.identity.trade_id, i64::MAX.to_string());
    assert_eq!(fill.system_id, base.system_id.to_string());
    assert_eq!(fill.lots, 3);
    for (i, event) in [
        RawEvent {
            kind: ORDER,
            ..base
        },
        RawEvent { lots: 0, ..base },
        RawEvent { lots: -1, ..base },
        RawEvent {
            price: f64::NAN,
            ..base
        },
        RawEvent {
            price: f64::INFINITY,
            ..base
        },
        RawEvent { price: 0.0, ..base },
        RawEvent {
            trade_id: 0,
            ..base
        },
        RawEvent {
            system_id: 0,
            ..base
        },
        RawEvent {
            commission: f64::NAN,
            ..base
        },
        RawEvent {
            no_order: 1,
            ..base
        },
        RawEvent {
            trade_flag: 1,
            ..base
        },
        RawEvent {
            order_group: 256,
            ..base
        },
        RawEvent { hedge: 3, ..base },
        RawEvent { day: 0, ..base },
    ]
    .into_iter()
    .enumerate()
    {
        assert!(event.to_fill().is_err(), "invalid fill case {i}");
    }
}

fn restored_core() -> crate::core::TradingCore {
    let c = config();
    let mut session = Session::default();
    begin(&mut session, &c);
    let snapshot = session
        .on_event(
            &RawEvent {
                kind: END,
                day: 20261109,
                ..RawEvent::default()
            },
            &c,
            None,
        )
        .unwrap()
        .unwrap();
    let mut risk = c.risk;
    risk.max_order_lots = 3;
    risk.max_position_lots_per_side = 3;
    risk.max_working_lots = 3;
    let mut core = crate::core::TradingCore::new(risk, scope()).unwrap();
    core.restore(snapshot).unwrap();
    core
}

#[test]
fn native_terminal_reports_before_fills_preserve_factual_positions_and_reservations() {
    for (status, traded) in [(3, 3), (2, 1)] {
        let mut core = restored_core();
        let prepared = core
            .prepare(
                Intent {
                    venue: Venue::Shfe,
                    symbol: "cu2612".into(),
                    side: Side::Buy,
                    offset: Offset::Open,
                    lots: 3,
                    price_ticks: 20000,
                    price_type: PriceType::Limit,
                    time_in_force: TimeInForce::Day,
                    execution_route: None,
                },
                std::time::Instant::now(),
            )
            .unwrap();
        let e = RawEvent {
            traded,
            status,
            reference: prepared.key.order_ref.parse().unwrap(),
            ..valid_order()
        };
        core.on_order(e.to_order(prepared.key.clone()).unwrap())
            .unwrap();
        assert_eq!(core.position(Venue::Shfe, "cu2612", Side::Buy).total(), 0);
        assert_eq!(core.order(&prepared.key).unwrap().filled, 0);
        assert!(core.order(&prepared.key).unwrap().local_cash_held > 0.0);
        let fill_event = RawEvent {
            kind: TRADE,
            lots: traded,
            trade_id: (1_i64 << 40) + i64::from(traded),
            ..e
        };
        let fill = fill_event.to_fill().unwrap();
        assert!(core.on_fill(fill.clone()).unwrap());
        assert!(!core.on_fill(fill).unwrap());
        assert_eq!(
            core.position(Venue::Shfe, "cu2612", Side::Buy).total(),
            i64::from(traded)
        );
        let conflicting = RawEvent {
            price: 4000.2,
            ..fill_event
        }
        .to_fill()
        .unwrap();
        assert!(core.on_fill(conflicting).is_err());
        assert_eq!(
            core.position(Venue::Shfe, "cu2612", Side::Buy).total(),
            i64::from(traded)
        );
    }
}

#[test]
fn permission_change_inside_recovery_poisoned_snapshot_cannot_be_committed() {
    let c = config();
    let mut s = Session::default();
    begin(&mut s, &c);
    assert!(s
        .on_event(
            &RawEvent {
                kind: INVALIDATED,
                ..RawEvent::default()
            },
            &c,
            None
        )
        .is_err());
    assert!(s
        .on_event(
            &RawEvent {
                kind: END,
                day: 20261109,
                ..RawEvent::default()
            },
            &c,
            None
        )
        .unwrap()
        .is_none());
    assert!(!s.ready);
}
fn begin(s: &mut Session, c: &config::Config) {
    for e in [
        RawEvent {
            kind: CONNECTED,
            ..RawEvent::default()
        },
        RawEvent {
            kind: LOGIN,
            day: 20261109,
            ..RawEvent::default()
        },
        RawEvent {
            kind: INIT,
            ..RawEvent::default()
        },
        RawEvent {
            kind: BEGIN,
            day: 20261109,
            reference: 100,
            ..RawEvent::default()
        },
    ] {
        assert!(s.on_event(&e, c, None).unwrap().is_none());
    }
    let a = RawEvent {
        kind: ACCOUNT,
        day: 20261109,
        login_count: 1,
        available: 1_000_000.0,
        replay: 1,
        ..RawEvent::default()
    };
    s.on_event(&a, c, None).unwrap();
    for (venue, symbol) in [("SHFE", "cu2612"), ("INE", "sc2612")] {
        let mut e = row(INSTRUMENT, venue, symbol);
        e.expiry = 20261220;
        e.tick = 0.2;
        e.minimum = 1;
        e.maximum = 100;
        s.on_event(&e, c, None).unwrap();
    }
}
#[test]
fn init_and_login_do_not_allow_trading_before_caught_up_end() {
    let c = config();
    c.validate().unwrap();
    let mut s = Session::default();
    begin(&mut s, &c);
    assert!(!s.ready);
    let snap = s
        .on_event(
            &RawEvent {
                kind: END,
                day: 20261109,
                ..RawEvent::default()
            },
            &c,
            None,
        )
        .unwrap()
        .unwrap();
    assert!(s.ready);
    assert_eq!(snap.instruments.len(), 2);
}
#[test]
fn replay_keeps_shfe_today_and_ine_yesterday_separate() {
    let c = config();
    let mut s = Session::default();
    begin(&mut s, &c);
    for (venue, symbol, date) in [("SHFE", "cu2612", 1), ("INE", "sc2612", 2)] {
        let mut e = row(POSITION, venue, symbol);
        e.position_direction = 2;
        e.date = date;
        e.lots = 3;
        s.on_event(&e, &c, None).unwrap();
    }
    let snap = s
        .on_event(
            &RawEvent {
                kind: END,
                day: 20261109,
                ..RawEvent::default()
            },
            &c,
            None,
        )
        .unwrap()
        .unwrap();
    assert_eq!(snap.positions[0].today, 3);
    assert_eq!(snap.positions[1].yesterday, 3);
}
#[test]
fn frozen_inventory_without_replayed_close_orders_blocks_snapshot() {
    let c = config();
    let mut s = Session::default();
    begin(&mut s, &c);
    let mut e = row(POSITION, "SHFE", "cu2612");
    e.position_direction = 2;
    e.date = 1;
    e.lots = 3;
    e.frozen = 1;
    s.on_event(&e, &c, None).unwrap();
    assert!(s
        .on_event(
            &RawEvent {
                kind: END,
                day: 20261109,
                ..RawEvent::default()
            },
            &c,
            None
        )
        .is_err());
    assert!(!s.ready);
}
#[test]
fn interleaving_live_fills_and_multiple_logins_are_rejected() {
    let c = config();
    let mut s = Session::default();
    begin(&mut s, &c);
    let mut e = row(TRADE, "SHFE", "cu2612");
    e.replay = 0;
    assert!(s.on_event(&e, &c, None).is_err());
    s = Session::default();
    begin(&mut s, &c);
    e = RawEvent {
        kind: ACCOUNT,
        available: 1.0,
        login_count: 2,
        replay: 1,
        ..RawEvent::default()
    };
    assert!(s.on_event(&e, &c, None).is_err());
}
#[test]
fn vendor_codes_are_integer_and_shfe_close_dates_are_explicit() {
    let mut e = row(ORDER, "SHFE", "cu2612");
    e.reference = 0;
    e.system_id = 123;
    e.direction = 0;
    e.offset = 3;
    e.lots = 2;
    e.status = 1;
    e.price = 4000.0;
    let o = e.to_order(e.recovered_key().unwrap()).unwrap();
    assert_eq!(o.offset, Offset::CloseToday);
    assert_eq!(o.key.order_ref, "0");
    e.direction = 48;
    assert!(e.to_order(o.key.clone()).is_err());
    e.direction = 0;
    e.offset = 1;
    assert!(e.to_order(o.key).is_err());
}
#[test]
fn unsupported_hedges_and_order_flags_preserve_evidence_and_fail() {
    let mut e = row(ORDER, "INE", "sc2612");
    e.system_id = 1;
    e.hedge = 2;
    assert!(e.to_order(e.recovered_key().unwrap()).is_err());
    e.hedge = 1;
    e.flag = 1;
    assert!(e.to_order(e.recovered_key().unwrap()).is_err());
}
#[test]
fn permission_changes_and_server_faults_require_recovery() {
    let c = config();
    let mut s = Session::default();
    begin(&mut s, &c);
    s.on_event(
        &RawEvent {
            kind: END,
            day: 20261109,
            ..RawEvent::default()
        },
        &c,
        None,
    )
    .unwrap();
    assert!(s
        .on_event(
            &RawEvent {
                kind: INVALIDATED,
                ..RawEvent::default()
            },
            &c,
            None
        )
        .is_err());
    assert!(!s.ready);
    let mut s = Session::default();
    assert!(s
        .on_event(
            &RawEvent {
                kind: FATAL,
                ..RawEvent::default()
            },
            &c,
            None
        )
        .is_err());
}

#[test]
fn failed_snapshot_cannot_be_reopened_by_late_end() {
    let c = config();
    let mut s = Session::default();
    begin(&mut s, &c);
    let mut e = row(TRADE, "SHFE", "cu2612");
    e.replay = 0;
    assert!(s.on_event(&e, &c, None).is_err());
    assert!(s
        .on_event(
            &RawEvent {
                kind: END,
                day: 20261109,
                ..RawEvent::default()
            },
            &c,
            None
        )
        .unwrap()
        .is_none());
    assert!(!s.ready);
    begin(&mut s, &c);
    assert!(s
        .on_event(
            &RawEvent {
                kind: END,
                day: 20261109,
                ..RawEvent::default()
            },
            &c,
            None
        )
        .unwrap()
        .is_some());
}

#[test]
fn server_accept_without_system_id_keeps_known_local_order_pending() {
    let mut e = row(ORDER, "SHFE", "cu2612");
    e.reference = 101;
    e.status = 0;
    e.lots = 1;
    e.price = 4000.0;
    let key = OrderKey {
        session: session_key(e.day),
        order_ref: "101".into(),
    };
    let o = e.to_order(key.clone()).unwrap();
    assert_eq!(o.status, OrderStatus::Pending);
    assert!(o.system_id.is_empty());
    e.status = 1;
    assert!(e.to_order(key).is_err());
}

#[test]
fn nonzero_groups_isolate_reference_identity_and_native_fills() {
    let mut first = valid_order();
    first.order_group = 7;
    let mut second = first;
    second.order_group = 8;
    let a = first.recovered_key().unwrap();
    let b = second.recovered_key().unwrap();
    assert_ne!(a, b);
    assert_eq!(parse_group_key(&a).unwrap(), (20261109, 7, 101));
    first.kind = TRADE;
    first.trade_id = 17;
    second.kind = TRADE;
    second.trade_id = 18;
    assert_eq!(first.to_fill().unwrap().order_key, Some(a));
    assert_eq!(second.to_fill().unwrap().order_key, Some(b));
    let mut c = config();
    c.order_group = 0;
    assert!(c.validate().is_err());
    c.order_group = 255;
    c.validate().unwrap();
}

#[test]
fn execution_routes_require_a_specific_seat_only_for_fixed_and_preferred() {
    for mode in [ConnectionSelection::Any, ConnectionSelection::Packed] {
        ExecutionRoute {
            mode,
            connection_id: None,
        }
        .validate()
        .unwrap();
        assert!(ExecutionRoute {
            mode,
            connection_id: Some(0)
        }
        .validate()
        .is_err());
    }
    for mode in [ConnectionSelection::Fixed, ConnectionSelection::Preferred] {
        assert!(ExecutionRoute {
            mode,
            connection_id: None
        }
        .validate()
        .is_err());
        assert!(ExecutionRoute {
            mode,
            connection_id: Some(64)
        }
        .validate()
        .is_err());
        ExecutionRoute {
            mode,
            connection_id: Some(63),
        }
        .validate()
        .unwrap();
    }
}

#[test]
fn option_parser_handles_calls_puts_sides_close_dates_and_all_ordinary_order_types() {
    for (venue, underlying, strike) in [("SHFE", "cu2612", 106000), ("INE", "sc2612", 640)] {
        for right in [OptionRight::Call, OptionRight::Put] {
            for direction in [0, 1] {
                for offset in [0, 3, 4] {
                    for order_type in [0, 1, 3] {
                        for market in [0, 1] {
                            for status in 0..=4 {
                                let mut e = option_row(ORDER, venue, underlying, right, strike);
                                e.reference = 101;
                                e.system_id = 17;
                                e.direction = direction;
                                e.offset = offset;
                                e.order_type = order_type;
                                e.order_attribute = market;
                                e.status = status;
                                e.lots = 3;
                                e.traded = if status == 3 { 3 } else { 0 };
                                e.price = if market == 1 { 0.0 } else { 25.0 };
                                let order = e.to_order(e.recovered_key().unwrap()).unwrap();
                                assert_eq!(
                                    order.price_type,
                                    if market == 1 {
                                        PriceType::Market
                                    } else {
                                        PriceType::Limit
                                    }
                                );
                                e.kind = TRADE;
                                e.trade_id = 19;
                                e.price = 25.0;
                                assert_eq!(e.to_fill().unwrap().lots, 3);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn option_metadata_mismatch_is_rejected_before_order_or_fill_conversion() {
    let mut base = option_row(ORDER, "INE", "sc2612", OptionRight::Put, 640);
    base.lots = 1;
    base.price = 20.0;
    base.system_id = 17;
    base.reference = 101;
    base.status = 1;
    for mut e in [
        RawEvent {
            options_type: 1,
            ..base
        },
        RawEvent {
            strike_price: 641.0,
            ..base
        },
        RawEvent {
            contract_multiple: 0,
            ..base
        },
        RawEvent { expiry: 0, ..base },
        RawEvent {
            upper_limit_price: f64::NAN,
            ..base
        },
        RawEvent {
            product_class: 1,
            ..base
        },
        RawEvent {
            underlying: [0; 32],
            ..base
        },
        RawEvent {
            underlying_exchange: [0; 12],
            ..base
        },
    ] {
        assert!(e.to_order(base.recovered_key().unwrap()).is_err());
        e.kind = TRADE;
        e.trade_id = 19;
        assert!(e.to_fill().is_err());
    }
    let mut c = config();
    c.symbols[0].symbol = "cu2612C106000".into();
    assert!(c.validate().is_err());
    c.symbols[0].cash_buffer_per_buy_lot = Some(10.0);
    c.validate().unwrap();
    c.symbols[0].cash_buffer_per_buy_lot = Some(f64::NAN);
    assert!(c.validate().is_err());
    assert!(option_symbol_parts(Venue::Cffex, "IO2612C4000").is_none());
    for symbol in [
        "cu2612c100000",
        "cu2612C0",
        "cu2612CNaN",
        "cu2612C1e3",
        "cu2613C1000",
        "SP cu2612&cu2701",
    ] {
        assert!(option_symbol_parts(Venue::Shfe, symbol).is_none());
    }
}
