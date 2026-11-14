//! Short-lived, public spot quotes used only by the FR concentration guard.
//! These quotes never enter the derivatives mark table or order pricing.

use anyhow::{bail, Context, Result};
use log::{debug, info, warn};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;
use std::time::{Duration, Instant};

use super::monitor_channel::MonitorChannel;

const PUBLIC_BASE: &str = "https://api.gateio.ws";
const REFRESH_INTERVAL: Duration = Duration::from_secs(10);
const QUOTE_TTL: Duration = Duration::from_secs(30);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_GAP: Duration = Duration::from_millis(100);
const MAX_REQUESTS_PER_ROUND: usize = 16;

#[derive(Debug)]
struct SpotQuote {
    mid: f64,
    received_at: Instant,
}

#[derive(Debug, Default)]
pub(crate) struct GateSpotValuation {
    quotes: HashMap<String, SpotQuote>,
}

impl GateSpotValuation {
    pub(crate) fn fresh_prices(&self, now: Instant) -> HashMap<String, f64> {
        self.quotes
            .iter()
            .filter(|(_, q)| {
                now.checked_duration_since(q.received_at)
                    .is_some_and(|age| age < QUOTE_TTL)
            })
            .map(|(asset, q)| (asset.clone(), q.mid))
            .collect()
    }

    fn record(&mut self, asset: String, mid: f64, received_at: Instant) {
        if mid.is_finite() && mid > 0.0 {
            self.quotes.insert(asset, SpotQuote { mid, received_at });
        } else {
            self.quotes.remove(&asset);
        }
    }
}

#[derive(Debug, Deserialize)]
struct SpotTicker {
    currency_pair: String,
    highest_bid: String,
    lowest_ask: String,
}

fn parse_spot_mid(body: &str, pair: &str) -> Result<f64> {
    let rows: Vec<SpotTicker> =
        serde_json::from_str(body).context("decode Gate public spot tickers")?;
    let mut matches = rows.iter().filter(|row| row.currency_pair == pair);
    let row = matches
        .next()
        .context("requested Gate spot ticker missing")?;
    if matches.next().is_some() {
        bail!("duplicate Gate spot ticker pair={pair}");
    }
    let bid = row.highest_bid.parse::<f64>().context("parse spot bid")?;
    let ask = row.lowest_ask.parse::<f64>().context("parse spot ask")?;
    if !(bid.is_finite() && ask.is_finite() && bid > 0.0 && ask >= bid) {
        bail!("invalid two-sided Gate spot quote pair={pair}");
    }
    Ok(bid + (ask - bid) * 0.5)
}

enum FetchOutcome {
    Quote(f64),
    RateLimited(Duration),
}

async fn fetch_spot_mid(client: &Client, base: &str, asset: &str) -> Result<FetchOutcome> {
    let pair = format!("{asset}_USDT");
    let response = client
        .get(format!("{base}/api/v4/spot/tickers"))
        .query(&[("currency_pair", &pair)])
        .send()
        .await
        .context("GET Gate public spot tickers")?;
    if response.status() == StatusCode::TOO_MANY_REQUESTS {
        let delay = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(60)
            .clamp(1, 3600);
        return Ok(FetchOutcome::RateLimited(Duration::from_secs(delay)));
    }
    let response = response
        .error_for_status()
        .context("Gate public spot tickers HTTP status")?;
    let body = response.text().await.context("read Gate spot tickers")?;
    Ok(FetchOutcome::Quote(parse_spot_mid(&body, &pair)?))
}

pub(crate) fn start_poll(cache: &Rc<RefCell<GateSpotValuation>>) {
    let cache = Rc::downgrade(cache);
    tokio::task::spawn_local(async move {
        let client = match Client::builder()
            .no_proxy()
            .timeout(REQUEST_TIMEOUT)
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                warn!("Gate spot concentration valuation client unavailable: {err}");
                return;
            }
        };
        let mut ticker = tokio::time::interval(REFRESH_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut backoff_until = Instant::now();
        let mut cursor = 0usize;
        loop {
            ticker.tick().await;
            let Some(cache) = cache.upgrade() else {
                return;
            };
            let assets = MonitorChannel::instance().gate_spot_valuation_assets();
            let requested: BTreeSet<_> = assets.iter().cloned().collect();
            cache
                .borrow_mut()
                .quotes
                .retain(|a, _| requested.contains(a));
            if assets.is_empty() || Instant::now() < backoff_until {
                continue;
            }
            let count = assets.len().min(MAX_REQUESTS_PER_ROUND);
            for offset in 0..count {
                let asset = &assets[(cursor + offset) % assets.len()];
                match fetch_spot_mid(&client, PUBLIC_BASE, asset).await {
                    Ok(FetchOutcome::Quote(mid)) => {
                        cache
                            .borrow_mut()
                            .record(asset.clone(), mid, Instant::now());
                        debug!("Gate concentration spot quote refreshed: asset={asset} mid={mid}");
                    }
                    Ok(FetchOutcome::RateLimited(delay)) => {
                        cache.borrow_mut().quotes.remove(asset);
                        backoff_until = Instant::now() + delay;
                        warn!(
                            "Gate concentration spot quote rate limited: backoff_secs={}",
                            delay.as_secs()
                        );
                        break;
                    }
                    Err(err) => {
                        cache.borrow_mut().quotes.remove(asset);
                        warn!("Gate concentration spot quote unavailable: asset={asset} error={err:#}");
                    }
                }
                tokio::time::sleep(REQUEST_GAP).await;
            }
            cursor = (cursor + count) % assets.len();
        }
    });
    info!("Gate spot concentration valuation poll started: interval_secs=10 ttl_secs=30 source=public_spot_bid_ask_mid");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_matching_current_book_instead_of_old_last_trade() {
        let body = r#"[{"currency_pair":"OTHER_USDT","highest_bid":"1","lowest_ask":"2"},{"currency_pair":"SLVON_USDT","highest_bid":"47.78","lowest_ask":"47.87","last":"999"}]"#;
        assert!((parse_spot_mid(body, "SLVON_USDT").unwrap() - 47.825).abs() < 1e-10);
        assert!(parse_spot_mid(body, "MISSING_USDT").is_err());
    }

    #[test]
    fn rejects_empty_crossed_non_finite_and_duplicate_quotes() {
        for (bid, ask) in [
            ("0", "1"),
            ("1", "0"),
            ("2", "1"),
            ("NaN", "3"),
            ("1", "inf"),
            ("-1", "2"),
        ] {
            let body = format!(
                r#"[{{"currency_pair":"A_USDT","highest_bid":"{bid}","lowest_ask":"{ask}"}}]"#
            );
            assert!(parse_spot_mid(&body, "A_USDT").is_err(), "{body}");
        }
        assert!(parse_spot_mid("[]", "A_USDT").is_err());
        let row = r#"{"currency_pair":"A_USDT","highest_bid":"1","lowest_ask":"2"}"#;
        assert!(parse_spot_mid(&format!("[{row},{row}]"), "A_USDT").is_err());
    }

    #[test]
    fn expired_or_future_quotes_are_unknown_and_invalid_updates_invalidate() {
        let mut cache = GateSpotValuation::default();
        let now = Instant::now();
        cache.record("A".into(), 20.0, now);
        assert_eq!(cache.fresh_prices(now).get("A"), Some(&20.0));
        assert!(cache.fresh_prices(now + QUOTE_TTL).is_empty());
        assert!(cache.fresh_prices(now - Duration::from_secs(1)).is_empty());
        cache.record("A".into(), f64::NAN, now);
        assert!(cache.fresh_prices(now).is_empty());
    }

    #[tokio::test]
    async fn fetch_is_an_unsigned_pair_specific_get_and_honors_rate_limit() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for rate_limited in [false, true] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut buf = [0u8; 1024];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    request.extend_from_slice(&buf[..n]);
                    if request.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                requests.push(String::from_utf8(request).unwrap());
                let response = if rate_limited {
                    "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 120\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string()
                } else {
                    let body =
                        r#"[{"currency_pair":"SLVON_USDT","highest_bid":"40","lowest_ask":"42"}]"#;
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                };
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        });
        let client = Client::builder().no_proxy().build().unwrap();
        assert!(matches!(
            fetch_spot_mid(&client, &base, "SLVON").await.unwrap(),
            FetchOutcome::Quote(41.0)
        ));
        assert!(
            matches!(fetch_spot_mid(&client, &base, "SLVON").await.unwrap(), FetchOutcome::RateLimited(d) if d == Duration::from_secs(120))
        );
        for request in server.await.unwrap() {
            assert!(request
                .starts_with("GET /api/v4/spot/tickers?currency_pair=SLVON_USDT HTTP/1.1\r\n"));
            for line in request.to_ascii_lowercase().lines() {
                assert!(
                    !line.starts_with("key:")
                        && !line.starts_with("sign:")
                        && !line.starts_with("authorization:")
                );
            }
        }
    }
}
