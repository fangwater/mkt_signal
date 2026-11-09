use super::*;
use crate::cta_factor_calc::config::*;
use period_pbs::pb::{PriceLevel, SymbolInfo};
use period_pbs::period::{period_for_timestamp_ms, DEFAULT_PERIOD_MS};

const HOUR: i64 = 3_600_000;
const START: i64 = 1_800_000_000_000;

fn record(end: i64, event_us: Option<i64>, mid: f64) -> PeriodMessage {
    PeriodMessage {
        period: period_for_timestamp_ms(end, DEFAULT_PERIOD_MS),
        ts: end,
        // Publication time must never advance the factor's data clock.
        post_ts: end + 100 * HOUR,
        poster_id: "hourly-test".into(),
        symbol_infos: event_us
            .map(|timestamp| SymbolInfo {
                symbol: "BTCUSDT".into(),
                incs: vec![IncrementOrderBookInfo {
                    timestamp,
                    is_snapshot: true,
                    bids: vec![PriceLevel {
                        price: mid - 0.5,
                        amount: 2.0,
                    }],
                    asks: vec![PriceLevel {
                        price: mid + 0.5,
                        amount: 1.0,
                    }],
                }],
                trades: vec![TradeInfo {
                    timestamp,
                    side: "buy".into(),
                    price: mid,
                    amount: 1.0,
                }],
            })
            .into_iter()
            .collect(),
    }
}

fn input() -> HourlySeries {
    HourlySeries {
        baseline: Some(LocalBaselineAggregator::for_completed_periods(HOUR)),
        ..Default::default()
    }
}

fn push(
    input: &mut HourlySeries,
    period: &PeriodMessage,
) -> (Vec<BaselineBar>, Vec<(Interval, i64, Option<f64>)>) {
    input.begin_period(period);
    for info in &period.symbol_infos {
        for book in &info.incs {
            let mid = (book.bids[0].price + book.asks[0].price) / 2.0;
            input.on_book(book, Some(mid));
        }
        for trade in &info.trades {
            input.on_trade(trade, true, None);
        }
    }
    input.finish_period()
}

#[test]
fn hourly_ids_windows_and_empty_position_scopes_are_supported_for_both_workers() {
    assert_eq!(START % HOUR, 0);
    for (worker, spread) in [(Worker::Perp, false), (Worker::Spread, true)] {
        let mut combination: Combination = serde_json::from_value(serde_json::json!({
            "name":"hourly", "strategy_group":if spread {"spread01"} else {"prep01"},
            "venue":if spread {"okex-futures"} else {"binance-futures"}, "interval":"1h",
            "factor_rules":[],
            "signal":{"enabled":false,"max_pos_u":100,"entry":{},"execution":{}}
        }))
        .unwrap();
        if spread {
            combination.reference_venue = Some(Market::BinanceFutures);
            combination.signal.as_mut().unwrap().spread = Some(SpreadFilters {
                long_quantile: 0.3,
                short_quantile: 0.7,
            });
        }
        combination.validate().unwrap();
        assert!(combination.manager_strategy_name().ends_with("-1h-pos"));
        combination.factor_rules.push(FactorRule {
            symbol: "BTCUSDT".into(),
            nq: "0.50".into(),
            weight: 1.0,
            factor: "avg_price".into(),
            venue: Some(Market::BinanceFutures),
            interval: None,
            reference_venue: None,
        });
        let plan = Plan::merge([combination]).unwrap();
        worker.validate_plan(&plan).unwrap();
        let ordinary = &plan.calculations["binance-futures-btcusdt-1h-avg_price"].parameters;
        assert_eq!(
            (ordinary.normalize_window, ordinary.normalize_min_periods),
            (24, 16)
        );
        assert_eq!((ordinary.rank_window, ordinary.rank_min_periods), (48, 24));
        let nq = &plan.calculations["binance-futures-btcusdt-1h-nq"].parameters;
        assert_eq!(
            (nq.lookback, nq.rank_window, nq.rank_min_periods),
            (120, 120, 60)
        );
        if spread {
            assert!(plan
                .calculations
                .contains_key("okex-futures-binance-futures-btcusdt-1h-spread"));
        }
    }
}

#[test]
fn hourly_ranks_include_all_ties_and_stay_unknown_until_24_valid_z_scores() {
    let id = FactorId::parse("binance-futures-btcusdt-1h-avg_price").unwrap();
    let mut transform = Transform::new(&Parameters::for_id(&id), Interval::Hour);
    for index in 0..80 {
        let point = transform.observe(Some(7.0), true);
        assert_eq!(point.value.is_some(), index >= 15);
        if index < 38 {
            assert_eq!(point.quantile, None);
            assert!(!point.ready);
        } else {
            assert_eq!(point.quantile, Some(1.0));
            assert!(point.ready);
        }
    }
    let missing = transform.observe(None, true);
    assert!(missing.value.is_none() && missing.quantile.is_none() && !missing.ready);
    assert_eq!(transform.ranks.len(), 47);
}

#[test]
fn hourly_bars_close_on_period_watermark_and_include_exact_boundary_events() {
    let mut input = input();
    let first = record(START + 3_000, Some(START * 1000 + 1), 100.0);
    assert!(push(&mut input, &first).0.is_empty());
    let before = record(START + HOUR - 3_000, None, 0.0);
    assert!(push(&mut input, &before).0.is_empty());
    let closing = record(START + HOUR, Some((START + HOUR) * 1000), 200.0);
    let (bars, quotes) = push(&mut input, &closing);
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].start_ms, START);
    assert_eq!(
        (bars[0].open, bars[0].close, bars[0].count),
        (100.0, 200.0, 2)
    );
    assert_eq!(bars[0].amount, 300.0);
    assert_eq!(quotes, vec![(Interval::Hour, START + HOUR, Some(200.0))]);
    assert!(push(&mut input, &closing).0.is_empty());
    let next = record(START + HOUR + 3_000, Some((START + HOUR) * 1000 + 1), 300.0);
    assert!(push(&mut input, &next).0.is_empty());
    let (bars, _) = push(&mut input, &record(START + 2 * HOUR, None, 0.0));
    assert_eq!(bars.len(), 1);
    assert_eq!(
        (bars[0].start_ms, bars[0].count, bars[0].close),
        (START + HOUR, 1, 300.0)
    );
}

#[test]
fn hourly_partial_inputs_are_computed_without_coverage_or_last_minute_requirements() {
    let mut input = input();
    // Begin halfway through the first hour and receive only one period.
    let end = START + HOUR / 2;
    push(&mut input, &record(end, Some(end * 1000), 123.0));
    let (bars, quotes) = push(&mut input, &record(START + HOUR, None, 0.0));
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].count, 1);
    assert_eq!(bars[0].close, 123.0);
    assert_eq!(quotes, vec![(Interval::Hour, START + HOUR, Some(123.0))]);
    // Fully absent hours are not made up from the preceding price/book.
    let (bars, quotes) = push(&mut input, &record(START + 3 * HOUR, None, 0.0));
    assert!(bars.is_empty() && quotes.is_empty());
    let end = START + 3 * HOUR + HOUR / 2;
    push(&mut input, &record(end, Some(end * 1000), 456.0));
    let (bars, quotes) = push(&mut input, &record(START + 4 * HOUR, None, 0.0));
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].start_ms, START + 3 * HOUR);
    assert_eq!(
        quotes,
        vec![(Interval::Hour, START + 4 * HOUR, Some(456.0))]
    );
}

#[test]
fn hourly_engine_warms_native_formula_nq_and_cross_market_spread_from_periods() {
    let combo: Combination = serde_json::from_value(serde_json::json!({
        "name":"hourly", "strategy_group":"spread01", "venue":"binance-futures", "interval":"1h",
        "factor_rules":[
            {"标的":"BTC","nq":"0.50","符号":1,"因子":"avg_price"},
            {"标的":"BTC","nq":"关闭","符号":1,"因子":"avg_price","venue":"okex-futures"},
            {"标的":"BTC","nq":"关闭","符号":1,"因子":"spread","venue":"okex-futures","reference_venue":"binance-futures"}
        ]
    })).unwrap();
    let config = Config::load(std::path::Path::new("config/cta_factor_spread.toml")).unwrap();
    let mut engine = Engine::new(Plan::merge([combo]).unwrap(), &config).unwrap();
    for hour in 0..330 {
        if hour == 150 {
            continue;
        }
        let end = START + (hour + 1) * HOUR;
        let price = 100.0 + (hour % 7) as f64;
        for (market, mid) in [
            (Market::BinanceFutures, price),
            (Market::OkexFutures, price + 1.0),
        ] {
            // One observed period per hour suffices. No completeness enforcement.
            let mut input = record(end, Some(end * 1000), mid);
            let mut sell = input.symbol_infos[0].trades[0].clone();
            sell.side = "sell".into();
            input.symbol_infos[0].trades.push(sell);
            let output = engine.on_period(market, &input).unwrap();
            assert!(output.iter().all(|p| p.ts_ms == end));
        }
    }
    for id in [
        "binance-futures-btcusdt-1h-avg_price",
        "okex-futures-btcusdt-1h-avg_price",
        "binance-futures-btcusdt-1h-nq",
        "okex-futures-binance-futures-btcusdt-1h-spread",
    ] {
        assert!(engine.latest[id].ready(), "{id}: {:?}", engine.latest[id]);
        assert_eq!(engine.latest[id].ts_ms, START + 330 * HOUR);
    }
    let key = (Market::BinanceFutures, "BTCUSDT".into());
    assert!(engine.series[&key].baseline.is_none());
    assert_eq!(
        engine.formula[&(Market::BinanceFutures, "BTCUSDT".into(), Interval::Hour)].transforms[0]
            .ranks
            .len(),
        48
    );
    let previous = &engine.previous_spreads["okex-futures-binance-futures-btcusdt-1h-spread"];
    assert_eq!(previous.ts_ms, START + 329 * HOUR);
    let Special::Nq(nq) = &engine.special["binance-futures-btcusdt-1h-nq"] else {
        panic!()
    };
    assert_eq!(nq.closes.len(), 120);
    // Removing hourly calculations must also stop native hourly aggregation.
    engine.retain_plan(Plan::default()).unwrap();
    assert!(engine.series.is_empty());
}
