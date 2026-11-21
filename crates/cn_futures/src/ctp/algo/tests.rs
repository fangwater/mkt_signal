use super::*;

fn key(reference: u64) -> OrderKey {
    OrderKey {
        session: "ctp:1:2".into(),
        order_ref: crate::model::Reference::decimal(reference).unwrap(),
    }
}
fn resting(reference: u64, side: Side, price_ticks: i64, remaining: i32) -> LiveOrder {
    LiveOrder {
        key: key(reference),
        side,
        price_ticks,
        remaining,
        time_in_force: TimeInForce::Day,
        terminal: false,
        cancel_pending: false,
        owned: true,
    }
}
fn step(delta: i64, live: &[LiveOrder], now_us: u64) -> Step<'_> {
    Step {
        delta,
        tradable: delta.abs(),
        bid_ticks: 100,
        ask_ticks: 101,
        live,
        now_us,
    }
}
fn chase() -> Algo {
    Algo::new(
        &AlgoConfig {
            algorithm: ExecutionAlgorithm::Chase,
            chase: ChaseParameters {
                maker_amend_cooldown_ms: 500,
                maker_timeout_ms: 3_000,
            },
        },
        100_000,
    )
}

#[test]
fn ctaker_crosses_once_and_waits_for_the_order_and_retry_interval() {
    let mut algo = Algo::new(&AlgoConfig::default(), 1_000_000);
    let mut out = Commands::new();
    assert_eq!(
        algo.step(&step(3, &[], 10), &mut out),
        "order_action_emitted"
    );
    assert_eq!(
        out[0],
        Command::Place {
            side: Side::Buy,
            lots: 3,
            price_ticks: 101,
            time_in_force: TimeInForce::Fak
        }
    );
    out.clear();
    let mut fak = resting(1, Side::Buy, 101, 3);
    fak.time_in_force = TimeInForce::Fak;
    assert_eq!(
        algo.step(&step(3, &[fak], 20), &mut out),
        "working_order_unresolved"
    );
    assert_eq!(algo.step(&step(3, &[], 30), &mut out), "retry_interval");
    algo.on_target();
    assert_eq!(
        algo.step(&step(-2, &[], 40), &mut out),
        "order_action_emitted"
    );
    assert!(matches!(
        out[0],
        Command::Place {
            side: Side::Sell,
            price_ticks: 100,
            ..
        }
    ));
}

#[test]
fn chase_joins_amends_after_the_cooldown_and_crosses_after_the_maker_timeout() {
    let mut algo = chase();
    let mut out = Commands::new();
    assert_eq!(algo.step(&step(2, &[], 0), &mut out), "maker_placed");
    assert_eq!(
        out[0],
        Command::Place {
            side: Side::Buy,
            lots: 2,
            price_ticks: 100,
            time_in_force: TimeInForce::Day
        }
    );
    out.clear();
    // The touch moved away, but not for long enough.
    let order = resting(1, Side::Buy, 99, 2);
    assert_eq!(
        algo.step(&step(2, &[order], 200_000), &mut out),
        "maker_working"
    );
    assert_eq!(
        algo.step(&step(2, &[order], 600_000), &mut out),
        "chase_amend"
    );
    assert_eq!(out[0], Command::Cancel(key(1)));
    out.clear();
    let mut pending = order;
    pending.cancel_pending = true;
    assert_eq!(
        algo.step(&step(2, &[pending], 700_000), &mut out),
        "cancel_pending"
    );
    assert_eq!(algo.step(&step(2, &[], 800_000), &mut out), "maker_placed");
    out.clear();
    let order = resting(2, Side::Buy, 100, 2);
    assert_eq!(
        algo.step(&step(2, &[order], 3_000_000), &mut out),
        "chase_maker_timeout"
    );
    assert_eq!(out[0], Command::Cancel(key(2)));
    out.clear();
    assert_eq!(algo.step(&step(2, &[], 3_100_000), &mut out), "chase_taker");
    assert!(matches!(
        out[0],
        Command::Place {
            price_ticks: 101,
            time_in_force: TimeInForce::Fak,
            ..
        }
    ));
}

#[test]
fn chase_cancels_wrong_side_oversized_and_reached_orders_but_never_foreign_ones() {
    let mut algo = chase();
    let mut out = Commands::new();
    let sell = resting(1, Side::Sell, 101, 1);
    assert_eq!(
        algo.step(&step(2, &[sell], 0), &mut out),
        "chase_cancel_stale"
    );
    assert_eq!(out.as_slice(), &[Command::Cancel(key(1))]);
    out.clear();
    let oversized = resting(2, Side::Buy, 100, 3);
    assert_eq!(
        algo.step(&step(2, &[oversized], 0), &mut out),
        "chase_cancel_stale"
    );
    out.clear();
    let fine = resting(3, Side::Buy, 100, 1);
    assert_eq!(
        algo.step(&step(0, &[fine], 0), &mut out),
        "chase_cancel_reached"
    );
    assert_eq!(out.as_slice(), &[Command::Cancel(key(3))]);
    out.clear();
    let mut foreign = fine;
    foreign.owned = false;
    assert_eq!(
        algo.step(&step(2, &[foreign], 0), &mut out),
        "working_order_unresolved"
    );
    assert!(out.is_empty());
    assert_eq!(algo.step(&step(0, &[], 0), &mut out), "target_reached");
}

#[test]
fn configuration_uses_crypto_algorithm_names_and_bounds_chase() {
    let config = AlgoConfig {
        algorithm: serde_json::from_str("\"chase\"").unwrap(),
        chase: toml::from_str("maker_amend_cooldown_ms = 200").unwrap(),
    };
    assert_eq!(config.algorithm.as_str(), "chase");
    assert_eq!(config.chase.maker_timeout_ms, 5_000);
    assert!(config.validate().is_ok());
    let mut bad = config;
    bad.chase.maker_amend_cooldown_ms = 50;
    assert!(bad.validate().is_err());
    bad.chase = ChaseParameters {
        maker_amend_cooldown_ms: 1_000,
        maker_timeout_ms: 500,
    };
    assert!(bad.validate().is_err());
    assert_eq!(AlgoConfig::default().algorithm, ExecutionAlgorithm::Ctaker);
    assert!(serde_json::from_str::<ExecutionAlgorithm>("\"taker\"").is_err());
}
