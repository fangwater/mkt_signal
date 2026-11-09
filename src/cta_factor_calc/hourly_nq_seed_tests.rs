use super::*;
use crate::cta_factor_calc::{config::Config, engine::Engine};
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode as HttpStatus},
    routing::get,
    Json, Router,
};
use period_pbs::pb::{IncrementOrderBookInfo, PeriodMessage, PriceLevel, SymbolInfo};
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

const START: i64 = 1_800_000_000_000;
const ID: &str = "binance-futures-btcusdt-1h-nq";

fn plan(frequency: &str) -> Plan {
    let combination = serde_json::from_value(json!({
        "name":"seed-test", "strategy_group":"prep01", "venue":"binance-futures", "interval":frequency,
        "factor_rules":[{"标的":"BTC", "nq":"0.50", "符号":1, "因子":"avg_price"}]
    })).unwrap();
    Plan::merge([combination]).unwrap()
}
fn candle(end: i64, close: &str) -> Value {
    json!([end - HOUR_MS, "0", "0", "0", close, "0", end - 1])
}

#[test]
fn seeds_are_hourly_only_and_size_both_nested_windows() {
    for frequency in ["1min", "5min", "15min"] {
        let seed = HourlyNqSeed::new(&plan(frequency));
        assert_eq!(seed.status.phase, "not_needed");
        assert!(!seed.needs_start(Market::BinanceFutures));
    }
    let seed = HourlyNqSeed::new(&plan("1h"));
    assert_eq!(seed.requests.len(), 1);
    assert_eq!(seed.requests[0].bars, 239);
    assert!(seed.needs_start(Market::BinanceFutures));
    assert!(!seed.needs_start(Market::OkexFutures));
}

#[test]
fn closed_candle_parser_excludes_future_bars_and_accepts_sparse_history() {
    let value = json!([
        candle(START + HOUR_MS, "101.5"),
        candle(START + 3 * HOUR_MS, "103.5"),
        candle(START + 4 * HOUR_MS, "999.0")
    ]);
    let rows = parse_closed_candles(&value, START + 3 * HOUR_MS).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].end_ms, START + 3 * HOUR_MS);
    assert_eq!(rows[1].close, 103.5);
    assert!(parse_closed_candles(&json!({"code":-1121}), START).is_err());
    assert!(parse_closed_candles(&json!([candle(START, "NaN")]), START).is_err());
    assert!(parse_closed_candles(&json!([candle(START, "0")]), START).is_err());
    assert!(
        parse_closed_candles(&json!([candle(START, "100"), candle(START, "101")]), START).is_err()
    );
    assert!(parse_closed_candles(&json!([candle(START + 1, "100")]), START + HOUR_MS).is_err());
}

#[test]
fn historical_closes_warm_only_nq_then_current_kafka_mid_takes_over() {
    let merged = plan("1h");
    let config =
        Config::load(std::path::Path::new("config/cta_prep_binance_futures.toml")).unwrap();
    let mut engine = Engine::new(merged, &config).unwrap();
    let cutoff = START + 239 * HOUR_MS;
    let rows = (1..=239)
        .map(|i| HourlyClose {
            end_ms: START + i * HOUR_MS,
            close: if i == 239 { 80.0 } else { 100.0 },
        })
        .collect::<Vec<_>>();
    let mut future = rows.clone();
    future.last_mut().unwrap().end_ms = cutoff + HOUR_MS;
    assert!(engine.seed_hourly_nq(ID, &future, cutoff).is_err());
    engine.seed_hourly_nq(ID, &rows, cutoff).unwrap();
    assert!(
        engine.latest.is_empty(),
        "bootstrap must never publish historical decisions"
    );
    assert!(engine.seed_hourly_nq(ID, &rows, cutoff).is_err());
    let end = cutoff + HOUR_MS;
    let period = PeriodMessage {
        ts: end,
        post_ts: end,
        period: 0,
        poster_id: "seed-test".into(),
        symbol_infos: vec![SymbolInfo {
            symbol: "BTCUSDT".into(),
            trades: Vec::new(),
            incs: vec![IncrementOrderBookInfo {
                timestamp: end * 1000,
                is_snapshot: true,
                bids: vec![PriceLevel {
                    price: 119.0,
                    amount: 1.0,
                }],
                asks: vec![PriceLevel {
                    price: 121.0,
                    amount: 1.0,
                }],
            }],
        }],
    };
    engine.on_period(Market::BinanceFutures, &period).unwrap();
    let nq = &engine.latest[ID];
    assert_eq!(nq.ts_ms, end);
    assert!(nq.ready());
    assert_eq!(nq.fields["long"].raw, Some(0.5));
    assert_eq!(nq.fields["short"].raw, Some(0.0));
    assert!(!engine.latest["binance-futures-btcusdt-1h-avg_price"].ready());
    assert!(engine.seed_hourly_nq(ID, &rows, cutoff).is_err());
    let mut minute = Engine::new(plan("1min"), &config).unwrap();
    assert!(minute
        .seed_hourly_nq("binance-futures-btcusdt-1min-nq", &rows, cutoff)
        .is_err());
}

#[tokio::test]
async fn public_fetch_paginates_before_cutoff_without_credentials() {
    type Seen = Arc<Mutex<Vec<BTreeMap<String, String>>>>;
    async fn candles(
        State(seen): State<Seen>,
        headers: HeaderMap,
        Query(query): Query<BTreeMap<String, String>>,
    ) -> Json<Value> {
        assert!(!headers.contains_key("x-mbx-apikey") && !headers.contains_key("authorization"));
        assert_eq!(query["interval"], "1h");
        assert_eq!(query["symbol"], "BTCUSDT");
        let limit = query["limit"].parse::<usize>().unwrap();
        let end = query["endTime"].parse::<i64>().unwrap() + 1;
        seen.lock().unwrap().push(query);
        Json(Value::Array(
            (0..limit)
                .rev()
                .map(|i| candle(end - i as i64 * HOUR_MS, "100"))
                .collect(),
        ))
    }
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/fapi/v1/klines", get(candles))
        .with_state(seen.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let cutoff = START + 1000 * HOUR_MS;
    let mut client = KlineClient::new(&base, Duration::ZERO).unwrap();
    let rows = client.fetch("BTCUSDT", 600, cutoff).await.unwrap();
    assert_eq!(rows.len(), 600);
    assert_eq!(rows.last().unwrap().end_ms, cutoff);
    assert_eq!(rows.first().unwrap().end_ms, cutoff - 599 * HOUR_MS);
    let queries = seen.lock().unwrap();
    assert_eq!(queries.len(), 2);
    assert_eq!(queries[0]["limit"], "499");
    assert_eq!(queries[1]["limit"], "101");
    assert_eq!(
        queries[1]["endTime"],
        (cutoff - 499 * HOUR_MS - 1).to_string()
    );
    server.abort();
}

#[tokio::test]
async fn rate_limit_stops_the_batch_without_hammering_other_symbols() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().route(
        "/fapi/v1/klines",
        get(move || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                HttpStatus::TOO_MANY_REQUESTS
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let requests = [
        Request {
            factor_id: ID.into(),
            symbol: "BTCUSDT".into(),
            bars: 239,
        },
        Request {
            factor_id: "binance-futures-ethusdt-1h-nq".into(),
            symbol: "ETHUSDT".into(),
            bars: 239,
        },
    ];
    let batch = fetch_all(&base, &requests, START, Duration::ZERO).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(batch.errors.len(), 2);
    assert!(batch.histories.is_empty());
    server.abort();
}

#[tokio::test]
async fn bootstrap_uses_the_start_of_the_first_kafka_hour_as_its_cutoff() {
    let mut seed = HourlyNqSeed::new(&plan("1h"));
    seed.base = "http://127.0.0.1:1".into();
    seed.start(START + 3000).unwrap();
    assert_eq!(seed.status.cutoff_ms, Some(START));
    assert!(seed.loading());
    // This is a nonblocking poll even while the HTTP task has not run.
    assert!(seed.poll().is_none());
    drop(seed);
    let mut exact = HourlyNqSeed::new(&plan("1h"));
    exact.base = "http://127.0.0.1:1".into();
    exact.start(START).unwrap();
    assert_eq!(exact.status.cutoff_ms, Some(START - HOUR_MS));
}
