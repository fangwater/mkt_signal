use cn_futures_pbs::{
    config::Zmq,
    msg::{day, timestamp, MarketDataMsg, UNKNOWN_QUANTITY},
    sources::zmq::parse,
};

const SHFE: &[u8] = include_bytes!("fixtures/mdl_22_1.txt");
type QuoteCase = (
    &'static [u8],
    &'static str,
    &'static str,
    f64,
    u64,
    f64,
    u64,
    f64,
    u64,
    u64,
);
const CASES: [QuoteCase; 6] = [
    (
        include_bytes!("fixtures/mdl_21_1.txt"),
        "CFFEX",
        "IC2611",
        6485.2,
        20323,
        6485.2,
        3,
        6486.6,
        1,
        404812,
    ),
    (
        SHFE, "SHFE", "ss2704", 14010.0, 52, 14005.0, 4, 14030.0, 34, 5226257,
    ),
    (
        include_bytes!("fixtures/mdl_22_3.txt"),
        "INE",
        "lu2702",
        4887.0,
        23964,
        4885.0,
        5,
        4887.0,
        9,
        1108413,
    ),
    (
        include_bytes!("fixtures/mdl_23_1.txt"),
        "CZCE",
        "AP701",
        6214.0,
        257723,
        6213.0,
        15,
        6214.0,
        1,
        3748697,
    ),
    (
        include_bytes!("fixtures/mdl_24_1.txt"),
        "DCE",
        "m2701",
        3140.0,
        968108,
        3139.0,
        1096,
        3140.0,
        186,
        6656092,
    ),
    (
        include_bytes!("fixtures/mdl_26_1.txt"),
        "GFEX",
        "lc2611",
        91600.0,
        40,
        91380.0,
        5,
        91700.0,
        10,
        672489,
    ),
];
fn replace(frame: &[u8], index: usize, value: &str) -> Vec<u8> {
    let s = std::str::from_utf8(&frame[14..]).unwrap();
    let mut fields = s.split(',').collect::<Vec<_>>();
    fields[index] = value;
    [
        std::str::from_utf8(&frame[..14]).unwrap().to_owned(),
        fields.join(","),
    ]
    .concat()
    .into_bytes()
}

#[test]
fn six_live_market_fixtures_keep_actual_prices_quantities_and_native_symbols() {
    for (frame, venue, symbol, last, volume, bid, bid_qty, ask, ask_qty, sequence) in CASES {
        let msg = parse(frame, 123).unwrap();
        assert_eq!(msg.venue(), venue.as_bytes());
        assert_eq!(msg.symbol(), symbol.as_bytes());
        assert_eq!(msg.received_us, 123);
        assert_eq!(msg.last_price, last);
        assert_eq!(msg.volume, volume);
        assert_eq!(msg.source_sequence, sequence);
        assert_eq!((msg.bid[0].price, msg.bid[0].quantity), (bid, bid_qty));
        assert_eq!((msg.ask[0].price, msg.ask[0].quantity), (ask, ask_qty));
        assert_eq!(msg.trading_day, 20261113);
        assert_eq!(msg.action_day, 20261113);
        assert!(msg.exchange_ts_us > 0);
    }
}
#[test]
fn missing_levels_stay_missing_and_explicit_zero_is_preserved() {
    let msg = parse(CASES[5].0, 0).unwrap();
    assert!(msg.bid[1].price.is_nan());
    assert_eq!(msg.bid[1].quantity, UNKNOWN_QUANTITY);
    assert_eq!(
        parse(&replace(CASES[5].0, 24, "0"), 0).unwrap().bid[0].quantity,
        0
    );
    assert_eq!(std::mem::size_of::<MarketDataMsg>(), 328);
}
#[test]
fn night_uses_action_day_and_keeps_settlement_day_separate() {
    let frame = replace(
        &replace(&replace(SHFE, 1, "2026-11-16"), 2, "2026-11-14"),
        3,
        "00:00:00.123",
    );
    let msg = parse(&frame, 0).unwrap();
    assert_eq!(msg.trading_day, 20261116);
    assert_eq!(msg.action_day, 20261114);
    assert_eq!(
        msg.exchange_ts_us,
        chrono::DateTime::parse_from_rfc3339("2026-11-14T00:00:00.123+08:00")
            .unwrap()
            .timestamp_micros()
    );
}
#[test]
fn malformed_input_never_panics_or_turns_into_a_valid_quote() {
    for end in 0..SHFE.len() {
        assert!(std::panic::catch_unwind(|| parse(&SHFE[..end], 0)).is_ok());
    }
    for frame in [
        replace(SHFE, 4, "NaN"),
        replace(SHFE, 11, "-1"),
        replace(SHFE, 22, "18446744073709551616"),
        replace(SHFE, 2, "2026-02-30"),
        replace(SHFE, 3, "25:00:00.000"),
        replace(SHFE, 0, &"a".repeat(64)),
    ] {
        assert!(parse(&frame, 0).is_err());
    }
    assert!(parse(b"mdl.24.7.* || queue-data", 0).is_err());
    assert!(day(b"2026-11-131").is_err());
    assert!(timestamp(20261113, b"13:40:00.100", 10).is_err());
}
#[test]
fn additive_tail_never_changes_fixed_vendor_sequence_position() {
    let mut frame = SHFE.to_vec();
    frame.extend_from_slice(b",future_field,9223372036854775807");
    assert_eq!(parse(&frame, 0).unwrap().source_sequence, 5226257);
}
#[test]
fn company_intranet_is_explicit_and_does_not_accept_a_public_profile() {
    let mut config = Zmq {
        access_scope: "company_intranet".into(),
        endpoint: "tcp://10.61.20.12:13005".into(),
        receive_hwm: 100000,
    };
    assert!(config.validate().is_ok());
    config.access_scope = "colo".into();
    assert!(config.validate().is_err());
    config.access_scope = "company_intranet".into();
    config.endpoint = "tcp://8.8.8.8:13005".into();
    assert!(config.validate().is_err());
}
