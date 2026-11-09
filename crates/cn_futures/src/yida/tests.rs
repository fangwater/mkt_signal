use super::*;
use session::Session;
fn config() -> config::Config {
    toml::from_str(include_str!("../../../../config/yida_trade.example.toml")).unwrap()
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
