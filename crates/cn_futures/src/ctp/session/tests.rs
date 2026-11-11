use super::*;

#[test]
fn background_cash_and_complete_rule_queries_keep_business_session_ready() {
    let (mut session, mut now) = begin();
    for id in 1..=20 {
        let (_, event) = dispatch(&mut session, &mut now, id);
        if session.on_raw_event(&event, now).unwrap().is_some() {
            break;
        }
    }
    assert!(session.ready());
    now += Duration::from_secs(6);
    assert_eq!(session.request_due(now).unwrap(), Some(ACCOUNT));
    session.sent(ACCOUNT, 100, 0, now).unwrap();
    let mut account = response(ACCOUNT, 100);
    account.available = 123.0;
    assert!(session.on_raw_event(&account, now).unwrap().is_none());
    assert_eq!(session.take_cash_update(), Some(123.0));
    assert!(session.ready());
    now += Duration::from_secs(60);
    assert_eq!(session.request_due(now).unwrap(), Some(INSTRUMENT));
    session.sent(INSTRUMENT, 101, 0, now).unwrap();
    let mut absent = response(INSTRUMENT, 101);
    absent.present = 0;
    assert!(session.on_raw_event(&absent, now).unwrap().is_none());
    assert!(session.take_rule_update().unwrap().is_empty());
    assert!(session.ready());
}

fn config() -> Config {
    toml::from_str(include_str!("../../../../../config/ctp_trade.example.toml")).unwrap()
}
fn set<const N: usize>(out: &mut [u8; N], value: &str) {
    out[..value.len()].copy_from_slice(value.as_bytes());
}
fn response(kind: i32, id: i32) -> RawEvent {
    let mut e = RawEvent {
        kind,
        request: id,
        last: 1,
        ..RawEvent::default()
    };
    match kind {
        LOGGED_IN => {
            e.present = 1;
            e.front = 7;
            e.session = 11;
            set(&mut e.trading_day, "20261109");
            set(&mut e.max_order_ref, "100");
        }
        INSTRUMENT => {
            e.present = 1;
            set(&mut e.symbol, "IF2612");
            set(&mut e.exchange, "CFFEX");
            e.price_tick = 0.2;
            e.contract_multiple = 300;
            e.expiry = 20261218;
            set(&mut e.product, "IF");
            e.minimum = 1;
            e.maximum = 100;
            e.active = 1;
            e.product_class = i32::from(b'1');
        }
        SETTLEMENT_STATUS => {
            e.present = 1;
            set(&mut e.investor, "TEST");
            set(&mut e.trading_day, "20261109");
        }
        COMMISSION | MARGIN | EXCHANGE_MARGIN => {
            e.present = 1;
            set(&mut e.symbol, "IF2612");
            set(&mut e.exchange, "CFFEX");
            set(&mut e.investor, "TEST");
            e.investor_range = 51;
            e.hedge = 49;
            e.commission_rates = [0.000023, 0.0, 0.000023, 0.0, 0.00023, 0.0];
            e.margin_rates = [0.12, 0.0, 0.12, 0.0];
        }
        ACCOUNT => {
            e.present = 1;
            set(&mut e.investor, "TEST");
            set(&mut e.exchange, "CNY");
            e.available = 1_000_000.0;
        }
        POSITION => {
            e.present = 1;
            set(&mut e.symbol, "IF2612");
            set(&mut e.exchange, "CFFEX");
            set(&mut e.investor, "TEST");
            e.hedge = 49;
            e.position_direction = 50;
            e.position_date = 49;
            e.volume = 3;
            e.today = 1;
            e.yesterday = 100;
        }
        _ => {}
    }
    e
}
fn begin() -> (Session, Instant) {
    let now = Instant::now();
    let mut s = Session::new(config(), "TEST".into(), now);
    s.on_raw_event(
        &RawEvent {
            kind: CONNECTED,
            ..RawEvent::default()
        },
        now,
    )
    .unwrap();
    (s, now)
}
fn dispatch(s: &mut Session, now: &mut Instant, id: i32) -> (i32, RawEvent) {
    *now += Duration::from_secs(2);
    let kind = s.request_due(*now).unwrap().unwrap();
    s.sent(kind, id, 0, *now).unwrap();
    (kind, response(kind, id))
}

#[test]
fn bootstrap_requires_two_quiet_matching_passes_and_normalizes_yesterday() {
    let (mut s, mut now) = begin();
    let mut output = None;
    let mut counts = HashMap::new();
    for id in 1..=20 {
        let (kind, e) = dispatch(&mut s, &mut now, id);
        *counts.entry(kind).or_insert(0) += 1;
        output = s.on_raw_event(&e, now).unwrap();
        if output.is_some() {
            break;
        }
    }
    let image = output.unwrap();
    assert!(s.ready());
    assert_eq!(counts[&ACCOUNT], 2);
    assert_eq!(counts[&QUERY_TRADE], 2);
    assert_eq!(
        (image.positions[0].today, image.positions[0].yesterday),
        (1, 2)
    );
    assert_eq!(image.max_order_ref, 100);
}
#[test]
fn late_response_cannot_complete_a_different_request() {
    let (mut s, mut now) = begin();
    let (kind, mut e) = dispatch(&mut s, &mut now, 42);
    e.request = 41;
    assert!(s.on_raw_event(&e, now).unwrap().is_none());
    assert!(s.request_due(now).unwrap().is_none());
    e.request = 42;
    s.on_raw_event(&e, now).unwrap();
    assert_eq!(kind, AUTHENTICATED);
    assert_eq!(
        s.request_due(now + Duration::from_secs(2)).unwrap(),
        Some(LOGGED_IN)
    );
}
#[test]
fn rate_limit_rejection_retries_without_an_inflight_request() {
    let (mut s, now) = begin();
    s.sent(AUTHENTICATED, 1, -3, now).unwrap();
    assert!(s.request_due(now).unwrap().is_none());
    assert_eq!(
        s.request_due(now + Duration::from_secs(2)).unwrap(),
        Some(AUTHENTICATED)
    );
}
#[test]
fn partial_query_error_and_timeout_do_not_create_ready_state() {
    let (mut s, mut now) = begin();
    let (_, mut e) = dispatch(&mut s, &mut now, 1);
    e.error = 7;
    e.last = 0;
    assert!(s.on_raw_event(&e, now).is_err());
    assert!(!s.ready());
    assert!(s.request_due(now + Duration::from_secs(60)).is_err());
}
#[test]
fn missing_allowlisted_contract_is_a_complete_snapshot_failure() {
    let (mut s, mut now) = begin();
    for id in 1..=4 {
        let (kind, mut e) = dispatch(&mut s, &mut now, id);
        if kind == INSTRUMENT {
            e.present = 0;
            assert!(s.on_raw_event(&e, now).is_err());
        } else {
            s.on_raw_event(&e, now).unwrap();
        }
    }
    assert!(!s.ready());
}
#[test]
fn concurrent_live_event_invalidates_snapshot_pass() {
    let (mut s, mut now) = begin();
    let mut account_queries = 0;
    let mut ready = false;
    for id in 1..=30 {
        let (kind, e) = dispatch(&mut s, &mut now, id);
        if kind == ACCOUNT {
            account_queries += 1;
            if account_queries == 1 {
                let mut live = RawEvent {
                    kind: ORDER,
                    ..RawEvent::default()
                };
                set(&mut live.exchange, "CFFEX");
                set(&mut live.investor, "TEST");
                s.on_raw_event(&live, now).unwrap();
            }
        }
        if s.on_raw_event(&e, now).unwrap().is_some() {
            ready = true;
            break;
        }
    }
    assert!(ready);
    assert_eq!(account_queries, 3);
}
#[test]
fn unsupported_hedge_inventory_is_not_ignored() {
    let (mut s, mut now) = begin();
    for id in 1..=10 {
        let (kind, mut e) = dispatch(&mut s, &mut now, id);
        if kind == POSITION {
            e.hedge = 51;
            assert!(s.on_raw_event(&e, now).is_err());
            break;
        } else {
            s.on_raw_event(&e, now).unwrap();
        }
    }
    assert!(!s.ready());
}
#[test]
fn disconnect_abandons_old_correlation_and_requires_new_login() {
    let (mut s, mut now) = begin();
    let (_, e) = dispatch(&mut s, &mut now, 1);
    s.on_raw_event(
        &RawEvent {
            kind: DISCONNECTED,
            ..RawEvent::default()
        },
        now,
    )
    .unwrap();
    assert!(s.on_raw_event(&e, now).unwrap().is_none());
    assert!(s
        .request_due(now + Duration::from_secs(10))
        .unwrap()
        .is_none());
    s.on_raw_event(
        &RawEvent {
            kind: CONNECTED,
            ..RawEvent::default()
        },
        now,
    )
    .unwrap();
    assert_eq!(s.request_due(now).unwrap(), Some(AUTHENTICATED));
}

#[test]
fn relative_margin_adds_the_exchange_base_and_preserves_full_account_fields() {
    let (mut session, mut now) = begin();
    let mut snapshot = None;
    for id in 1..=30 {
        let (kind, mut e) = dispatch(&mut session, &mut now, id);
        if kind == MARGIN {
            e.relative = 1;
            e.margin_rates = [0.02, 2.0, 0.03, 3.0];
        }
        if kind == EXCHANGE_MARGIN {
            e.margin_rates = [0.10, 1.0, 0.11, 1.0];
        }
        if kind == ACCOUNT {
            e.account_values[17] = 1_200_000.0;
            e.account_values[18] = e.available;
            e.account_values[14] = 52.5;
        }
        if let Some(value) = session.on_raw_event(&e, now).unwrap() {
            snapshot = Some(value);
            break;
        }
    }
    let rule = &snapshot.unwrap().instruments[0];
    let rate = rule.margin.unwrap();
    assert!((rate.long_by_money - 0.12).abs() < 1e-12);
    assert!((rate.short_by_money - 0.14).abs() < 1e-12);
    assert_eq!(rate.long_by_volume, 3.0);
    assert_eq!(rate.short_by_volume, 4.0);
    assert_eq!(rule.contract_multiple, 300);
    assert_eq!(rule.expiry_date, 20261218);
    assert_eq!(session.account().unwrap().balance, 1_200_000.0);
    assert_eq!(session.account().unwrap().commission, 52.5);
}

#[test]
fn read_only_bootstrap_never_confirms_an_unconfirmed_settlement() {
    let (mut session, mut now) = begin();
    let mut complete = false;
    for id in 1..=30 {
        let (kind, mut e) = dispatch(&mut session, &mut now, id);
        assert_ne!(kind, SETTLED);
        if kind == SETTLEMENT_STATUS {
            e.present = 0;
        }
        if session.on_raw_event(&e, now).unwrap().is_some() {
            complete = true;
            break;
        }
    }
    assert!(complete);
    assert!(!session.settlement_confirmed());
}

#[test]
fn missing_fee_rows_do_not_release_trading_and_late_rates_cannot_match() {
    let (mut session, mut now) = begin();
    for id in 1..=10 {
        let (kind, mut e) = dispatch(&mut session, &mut now, id);
        if kind == COMMISSION {
            let mut late = e;
            late.request -= 1;
            assert!(session.on_raw_event(&late, now).unwrap().is_none());
            e.present = 0;
            assert!(session.on_raw_event(&e, now).is_err());
            assert!(!session.ready());
            return;
        }
        session.on_raw_event(&e, now).unwrap();
    }
    panic!("commission query was not scheduled");
}

impl Session {
    fn on_raw_event(&mut self, e: &RawEvent, now: Instant) -> Result<Option<Snapshot>> {
        self.on_event(e, now, &super::super::decode_event(e))
    }
}
