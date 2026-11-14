//! Seed missing Gate FR position marks after a grace period. Gate tickers are
//! event driven, so absence of a new tick alone does not invalidate a known mark.

use anyhow::{ensure, Context, Result};
use log::{info, warn};
use order_common::TradingVenue;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant};
use trade_signal::ArbMode;

use super::{monitor_channel::MonitorChannel, price_table::PriceTable};

const PUBLIC_BASE: &str = "https://api.gateio.ws";
const AFTER_ENV: &str = "PRE_TRADE_GATE_MARK_REST_AFTER_SECS";
const DEFAULT_AFTER: Duration = Duration::from_secs(30);
const CHECK_INTERVAL: Duration = Duration::from_secs(5);
const RETRY_INTERVAL: Duration = Duration::from_secs(30);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_GAP: Duration = Duration::from_millis(100);
const MAX_REQUESTS_PER_ROUND: usize = 8;

pub(super) fn enabled(open: TradingVenue, hedge: TradingVenue, mode: ArbMode) -> bool {
    mode == ArbMode::FundingArb
        && matches!(
            (open, hedge),
            (TradingVenue::GateMargin, TradingVenue::GateFutures)
                | (TradingVenue::GateFutures, TradingVenue::GateMargin)
        )
}

fn parse_after(value: &str) -> Result<Duration> {
    let seconds: u64 = value
        .trim()
        .parse()
        .context("parse missing mark grace seconds")?;
    ensure!(
        (5..=3600).contains(&seconds),
        "grace seconds must be in 5..=3600"
    );
    Ok(Duration::from_secs(seconds))
}

pub(super) fn has_mark(prices: &PriceTable, symbol: &str) -> bool {
    prices
        .mark_price(symbol)
        .is_some_and(|p| p.is_finite() && p > 0.0)
}

pub(super) fn seed_missing_mark(
    prices: &mut PriceTable,
    symbol: &str,
    mark: f64,
    timestamp_us: i64,
) -> bool {
    if !mark.is_finite() || mark <= 0.0 || has_mark(prices, symbol) {
        return false;
    }
    prices.update_mark_price(symbol, mark, timestamp_us);
    true
}

#[derive(Default)]
struct MissingMarks {
    next_attempt: BTreeMap<String, Instant>,
}

impl MissingMarks {
    fn observe(&mut self, symbols: &[String], now: Instant, after: Duration) {
        self.next_attempt.retain(|s, _| symbols.contains(s));
        for symbol in symbols {
            self.next_attempt
                .entry(symbol.clone())
                .or_insert(now + after);
        }
    }

    fn due(&self, now: Instant) -> Vec<String> {
        // Oldest due first: repeatedly failing contracts cannot starve others.
        let mut due: Vec<_> = self
            .next_attempt
            .iter()
            .filter(|(_, at)| **at <= now)
            .collect();
        due.sort_by(|(a, at), (b, bt)| at.cmp(bt).then_with(|| a.cmp(b)));
        due.into_iter()
            .take(MAX_REQUESTS_PER_ROUND)
            .map(|(s, _)| s.clone())
            .collect()
    }

    fn defer(&mut self, symbol: &str, now: Instant) {
        if let Some(at) = self.next_attempt.get_mut(symbol) {
            *at = now + RETRY_INTERVAL;
        }
    }
}

#[derive(Deserialize)]
struct ContractMark {
    name: String,
    mark_price: String,
}

fn parse_mark(body: &str, symbol: &str) -> Result<f64> {
    let contract: ContractMark = serde_json::from_str(body).context("decode Gate contract mark")?;
    ensure!(contract.name == symbol, "Gate contract name mismatch");
    let mark = contract
        .mark_price
        .parse::<f64>()
        .context("parse Gate mark_price")?;
    ensure!(mark.is_finite() && mark > 0.0, "invalid Gate mark_price");
    Ok(mark)
}

#[derive(Debug)]
enum FetchOutcome {
    Mark(f64),
    Backoff(Duration),
}

async fn fetch_mark(client: &Client, base: &str, symbol: &str) -> Result<FetchOutcome> {
    ensure!(
        symbol.ends_with("_USDT")
            && symbol
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'),
        "invalid Gate USDT contract symbol"
    );
    let response = client
        .get(format!("{base}/api/v4/futures/usdt/contracts/{symbol}"))
        .send()
        .await
        .context("GET Gate public contract mark")?;
    if matches!(
        response.status(),
        StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
    ) {
        let seconds = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(60)
            .clamp(60, 3600);
        return Ok(FetchOutcome::Backoff(Duration::from_secs(seconds)));
    }
    let body = response
        .error_for_status()
        .context("Gate contract mark HTTP status")?
        .text()
        .await
        .context("read Gate contract mark")?;
    Ok(FetchOutcome::Mark(parse_mark(&body, symbol)?))
}

pub(super) fn start_poll(prices: &Rc<RefCell<PriceTable>>) {
    let prices = Rc::downgrade(prices);
    let after = match std::env::var(AFTER_ENV) {
        Ok(value) => match parse_after(&value) {
            Ok(after) => after,
            Err(err) => {
                warn!("invalid {AFTER_ENV}: {err:#}; using default 30 seconds");
                DEFAULT_AFTER
            }
        },
        Err(_) => DEFAULT_AFTER,
    };
    tokio::task::spawn_local(async move {
        let client = match Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(REQUEST_TIMEOUT)
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                warn!("Gate missing mark REST client unavailable: {err}");
                return;
            }
        };
        info!("Gate missing mark REST supplement started: after_secs={} check_secs=5 retry_secs=30 max_requests=8", after.as_secs());
        let mut missing = MissingMarks::default();
        let mut backoff_until = Instant::now();
        let mut tick = tokio::time::interval(CHECK_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            if prices.upgrade().is_none() {
                return;
            }
            let mon = MonitorChannel::instance();
            let now = Instant::now();
            missing.observe(&mon.gate_mark_price_rest_symbols(), now, after);
            if now < backoff_until {
                continue;
            }
            for symbol in missing.due(now) {
                if !mon.gate_mark_price_rest_symbols().contains(&symbol) {
                    missing.next_attempt.remove(&symbol);
                    continue;
                }
                let outcome = fetch_mark(&client, PUBLIC_BASE, &symbol).await;
                missing.defer(&symbol, Instant::now());
                match outcome {
                    Ok(FetchOutcome::Mark(mark)) => {
                        if mon.seed_gate_mark_price_if_missing(&symbol, mark) {
                            info!("Gate missing mark supplemented: symbol={symbol} mark_price={mark} source=public_contract_rest");
                        }
                    }
                    Ok(FetchOutcome::Backoff(delay)) => {
                        backoff_until = Instant::now() + delay;
                        warn!(
                            "Gate missing mark REST backoff: symbol={symbol} seconds={}",
                            delay.as_secs()
                        );
                        break;
                    }
                    Err(err) => {
                        warn!("Gate missing mark REST unavailable: symbol={symbol} error={err:#}")
                    }
                }
                tokio::time::sleep(REQUEST_GAP).await;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_and_config_are_gate_fr_only() {
        assert!(enabled(
            TradingVenue::GateMargin,
            TradingVenue::GateFutures,
            ArbMode::FundingArb
        ));
        assert!(enabled(
            TradingVenue::GateFutures,
            TradingVenue::GateMargin,
            ArbMode::FundingArb
        ));
        assert!(!enabled(
            TradingVenue::GateMargin,
            TradingVenue::GateFutures,
            ArbMode::CrossArb
        ));
        assert!(!enabled(
            TradingVenue::BinanceMargin,
            TradingVenue::GateFutures,
            ArbMode::FundingArb
        ));
        assert_eq!(parse_after("45").unwrap(), Duration::from_secs(45));
        for invalid in ["0", "4", "3601", "-1", "bad"] {
            assert!(parse_after(invalid).is_err());
        }
    }

    #[test]
    fn waits_for_continuous_missing_period_and_resets_after_recovery_or_close() {
        let now = Instant::now();
        let mut state = MissingMarks::default();
        let symbols = vec!["BICO_USDT".into(), "PUNDIX_USDT".into()];
        state.observe(&symbols, now, DEFAULT_AFTER);
        assert!(state
            .due(now + DEFAULT_AFTER - Duration::from_nanos(1))
            .is_empty());
        assert_eq!(state.due(now + DEFAULT_AFTER), symbols);
        state.defer("BICO_USDT", now + DEFAULT_AFTER);
        assert_eq!(state.due(now + DEFAULT_AFTER), vec!["PUNDIX_USDT"]);
        state.observe(&[], now + DEFAULT_AFTER, DEFAULT_AFTER);
        state.observe(&symbols, now + DEFAULT_AFTER, DEFAULT_AFTER);
        assert!(state
            .due(now + DEFAULT_AFTER + Duration::from_secs(29))
            .is_empty());
        assert_eq!(state.due(now + DEFAULT_AFTER * 2), symbols);
    }

    #[test]
    fn failed_early_symbols_do_not_starve_later_symbols() {
        let now = Instant::now();
        let symbols = (0..20)
            .map(|i| format!("ASSET{i:02}_USDT"))
            .collect::<Vec<_>>();
        let mut state = MissingMarks::default();
        state.observe(&symbols, now, DEFAULT_AFTER);
        let due_at = now + DEFAULT_AFTER;
        let first = state.due(due_at);
        assert_eq!(first.len(), 8);
        for symbol in first {
            state.defer(&symbol, due_at);
        }
        assert_eq!(state.due(due_at), symbols[8..16]);
    }

    #[test]
    fn only_matching_positive_finite_mark_is_accepted() {
        assert_eq!(
            parse_mark(
                r#"{"name":"BICO_USDT","mark_price":"0.01315","last":"999"}"#,
                "BICO_USDT"
            )
            .unwrap(),
            0.01315
        );
        for body in [
            r#"{"name":"PUNDIX_USDT","mark_price":"1"}"#,
            r#"{"name":"BICO_USDT","last":"1"}"#,
            r#"{"name":"BICO_USDT","mark_price":"0"}"#,
            r#"{"name":"BICO_USDT","mark_price":"-1"}"#,
            r#"{"name":"BICO_USDT","mark_price":"NaN"}"#,
            r#"{"name":"BICO_USDT","mark_price":"inf"}"#,
            "[]",
        ] {
            assert!(parse_mark(body, "BICO_USDT").is_err(), "{body}");
        }
    }

    #[test]
    fn rest_fills_existing_price_table_but_never_overwrites_arriving_stream_mark() {
        let mut prices = PriceTable::new();
        prices.update_index_price("BICO_USDT", 99.0, 123);
        assert!(!seed_missing_mark(&mut prices, "BICO_USDT", f64::NAN, 124));
        assert!(seed_missing_mark(&mut prices, "BICO_USDT", 0.01315, 125));
        assert_eq!(prices.snapshot()["BICO_USDT"].mark_price, 0.01315);
        assert_eq!(prices.get("BICO_USDT").unwrap().update_time, 125);
        prices.update_mark_price("BICO_USDT", 0.014, 126);
        assert!(!seed_missing_mark(&mut prices, "BICO_USDT", 0.01, 127));
        assert_eq!(prices.mark_price("BICO_USDT"), Some(0.014));
        assert_eq!(prices.get("BICO_USDT").unwrap().update_time, 126);
    }

    #[tokio::test]
    async fn public_contract_get_and_rate_limit_backoff() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for response in [
                "HTTP/1.1 200 OK\r\nContent-Length: 40\r\nConnection: close\r\n\r\n{\"name\":\"BICO_USDT\",\"mark_price\":\"0.01\"}",
                "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 120\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                "HTTP/1.1 503 Service Unavailable\r\nRetry-After: 1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            ] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut buf = [0u8; 1024];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    request.extend_from_slice(&buf[..n]);
                    if request.windows(4).any(|w| w == b"\r\n\r\n") { break; }
                }
                let request = String::from_utf8(request).unwrap().to_lowercase();
                assert!(request.starts_with("get /api/v4/futures/usdt/contracts/bico_usdt http/1.1\r\n"));
                for header in ["\r\nkey:", "\r\nsign:", "\r\nauthorization:"] {
                    assert!(!request.contains(header));
                }
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let client = Client::builder()
            .no_proxy()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .unwrap();
        assert!(
            matches!(fetch_mark(&client, &base, "BICO_USDT").await.unwrap(), FetchOutcome::Mark(p) if p == 0.01)
        );
        assert!(
            matches!(fetch_mark(&client, &base, "BICO_USDT").await.unwrap(), FetchOutcome::Backoff(d) if d == Duration::from_secs(120))
        );
        assert!(
            matches!(fetch_mark(&client, &base, "BICO_USDT").await.unwrap(), FetchOutcome::Backoff(d) if d == Duration::from_secs(60))
        );
        assert!(fetch_mark(&client, &base, "BICO_USDT").await.is_err());
        server.await.unwrap();
    }
}
