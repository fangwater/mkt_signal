use super::{Subscription, Venue};
use anyhow::{ensure, Context, Result};
use chrono::{Datelike, NaiveDate, Weekday};
use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
use reqwest::{blocking::Client, Url};
use serde::Deserialize;
use std::collections::HashSet;
use std::io::Read;
use std::time::{Duration, Instant};

const MAX_RESPONSE_BYTES: u64 = 256 * 1024;

pub(super) fn endpoint(raw: &str) -> Result<Url> {
    let url = Url::parse(raw).map_err(|_| anyhow::anyhow!("invalid shfe_dominant_url"))?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.path() == "/api/v1/dominant"
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "shfe_dominant_url must be an HTTP(S) /api/v1/dominant endpoint without credentials or query"
    );
    Ok(url)
}

#[derive(Deserialize)]
struct Response {
    trading_day: NaiveDate,
    latest_trading_day: NaiveDate,
    source: Source,
    calendar_verified: bool,
    count: usize,
    products: Vec<Product>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Source {
    History,
    Dominant,
    ScheduledNext,
}

#[derive(Deserialize)]
struct Product {
    exchange: String,
    product: String,
    dominant: String,
}

#[derive(Debug)]
pub struct List {
    pub trading_day: String,
    pub latest_trading_day: String,
    pub source: &'static str,
    pub calendar_verified: bool,
    pub symbols: Vec<Subscription>,
}

fn parse(body: &[u8], broker_day: &str) -> Result<List> {
    let day = NaiveDate::parse_from_str(broker_day, "%Y%m%d").context("invalid broker MD date")?;
    let response: Response = serde_json::from_slice(body)
        .map_err(|_| anyhow::anyhow!("invalid Market Replay dominant response"))?;
    ensure!(
        response.trading_day == day,
        "Market Replay dominant day differs from broker MD day"
    );
    ensure!(
        response.count == response.products.len() && response.count <= 1000,
        "invalid Market Replay dominant count"
    );
    let latest = response.latest_trading_day;
    let source = match response.source {
        Source::History if response.calendar_verified && day < latest => "history",
        Source::Dominant if response.calendar_verified && day == latest => "dominant",
        Source::ScheduledNext if day > latest => {
            // The API may return the same scheduled list for later dates. Without
            // its calendar confirmation, allow only the next regular weekday.
            let next = latest
                .iter_days()
                .skip(1)
                .find(|d| !matches!(d.weekday(), Weekday::Sat | Weekday::Sun));
            ensure!(
                response.calendar_verified || next == Some(day),
                "Market Replay has no verified dominant list for the broker MD day"
            );
            "scheduled_next"
        }
        _ => anyhow::bail!("inconsistent Market Replay dominant date and source"),
    };
    let mut seen = HashSet::new();
    let mut symbols = Vec::new();
    for row in response
        .products
        .into_iter()
        .filter(|r| r.exchange == "xsge")
    {
        let symbol = &row.dominant;
        let prefix_len = symbol.len().saturating_sub(4);
        ensure!(
            symbol.is_ascii()
                && (1..=3).contains(&prefix_len)
                && symbol.as_bytes()[..prefix_len]
                    .iter()
                    .all(u8::is_ascii_lowercase)
                && symbol.as_bytes()[prefix_len..]
                    .iter()
                    .all(u8::is_ascii_digit)
                && symbol[..prefix_len].to_ascii_uppercase() == row.product
                && crate::model::is_future_symbol(Venue::Shfe, symbol)
                && seen.insert(row.product),
            "invalid or duplicate SHFE dominant contract"
        );
        symbols.push(Subscription {
            venue: Venue::Shfe,
            symbol: row.dominant,
        });
    }
    ensure!(
        !symbols.is_empty(),
        "Market Replay SHFE dominant list is empty"
    );
    symbols.sort_unstable_by(|a, b| a.symbol.cmp(&b.symbol));
    Ok(List {
        trading_day: broker_day.to_owned(),
        latest_trading_day: latest.to_string(),
        source,
        calendar_verified: response.calendar_verified,
        symbols,
    })
}

fn fetch(client: &Client, url: &Url, day: &str) -> Result<List> {
    // Build the query with the broker's native date; no credentials are sent.
    let mut url = url.clone();
    url.query_pairs_mut().append_pair("trading_day", day);
    let response = client
        .get(url)
        .send()
        .map_err(|_| anyhow::anyhow!("Market Replay dominant HTTP request failed"))?;
    ensure!(
        response.status().is_success(),
        "Market Replay dominant HTTP status {}",
        response.status()
    );
    let mut bytes = Vec::new();
    response
        .take(MAX_RESPONSE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| anyhow::anyhow!("cannot read Market Replay dominant response"))?;
    ensure!(
        bytes.len() as u64 <= MAX_RESPONSE_BYTES,
        "Market Replay dominant response is too large"
    );
    parse(&bytes, day)
}

pub(super) struct Lookup {
    requests: Sender<(u64, String)>,
    results: Receiver<(u64, Result<List>)>,
    pending: Option<(u64, String)>,
    in_flight: bool,
    next_attempt: Instant,
    backoff: Duration,
}

impl Lookup {
    pub(super) fn new(url: &str, backoff: Duration) -> Result<Self> {
        let url = endpoint(url)?;
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(6))
            .build()
            .context("build Market Replay dominant HTTP client")?;
        let (requests, incoming) = bounded::<(u64, String)>(1);
        let (outgoing, results) = bounded(1);
        std::thread::Builder::new()
            .name("ctp-md-dominant".into())
            .spawn(move || {
                for (generation, day) in incoming {
                    if outgoing
                        .send((generation, fetch(&client, &url, &day)))
                        .is_err()
                    {
                        break;
                    }
                }
            })
            .context("spawn Market Replay dominant worker")?;
        Ok(Self {
            requests,
            results,
            pending: None,
            in_flight: false,
            next_attempt: Instant::now(),
            backoff,
        })
    }

    pub(super) fn begin(&mut self, generation: u64, day: &str, now: Instant) {
        self.pending = Some((generation, day.to_owned()));
        self.next_attempt = now;
    }

    pub(super) fn reset(&mut self) {
        self.pending = None;
    }

    pub(super) fn pending(&self) -> bool {
        self.pending.is_some()
    }

    pub(super) fn poll(&mut self, now: Instant) -> Result<Option<Result<List>>> {
        match self.results.try_recv() {
            Ok((generation, result)) => {
                self.in_flight = false;
                if self
                    .pending
                    .as_ref()
                    .is_some_and(|(active, _)| *active == generation)
                {
                    if result.is_ok() {
                        self.pending = None;
                    } else {
                        self.next_attempt = now + self.backoff;
                    }
                    return Ok(Some(result));
                }
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                anyhow::bail!("Market Replay dominant worker stopped")
            }
        }
        if let Some(request) = &self.pending {
            if !self.in_flight && now >= self.next_attempt {
                self.requests
                    .try_send(request.clone())
                    .context("queue Market Replay dominant request")?;
                self.in_flight = true;
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn scheduled(latest: &str, day: &str) -> serde_json::Value {
        json!({
            "trading_day": day, "latest_trading_day": latest,
            "source": "scheduled_next", "calendar_verified": false, "count": 2,
            "products": [
                {"exchange":"xsge","product":"AU","dominant":"au2702","previous":"au2612","rolls":true},
                {"exchange":"xsie","product":"SC","dominant":"sc2701","previous":"sc2612","rolls":true}
            ]
        })
    }

    #[test]
    fn night_session_uses_effective_list_and_excludes_ine() {
        let body = scheduled("2026-11-17", "2026-11-18").to_string();
        let list = parse(body.as_bytes(), "20261118").unwrap();
        assert_eq!(list.trading_day, "20261118");
        assert_eq!(list.symbols.len(), 1);
        assert_eq!(list.symbols[0].symbol, "au2702");
        assert!(parse(body.as_bytes(), "20261119").is_err());
    }

    #[test]
    fn unverified_schedule_rejects_missing_weekday_but_handles_weekend() {
        let mut stale = scheduled("2026-11-17", "2026-11-19");
        assert!(parse(stale.to_string().as_bytes(), "20261119").is_err());
        stale["calendar_verified"] = json!(true);
        assert!(parse(stale.to_string().as_bytes(), "20261119").is_ok());

        let weekend = scheduled("2026-11-20", "2026-11-23");
        assert!(parse(weekend.to_string().as_bytes(), "20261123").is_ok());
    }

    #[test]
    fn malformed_shfe_rows_fail_the_whole_list() {
        let mut payload = scheduled("2026-11-17", "2026-11-18");
        payload["products"][1] = payload["products"][0].clone();
        assert!(parse(payload.to_string().as_bytes(), "20261118").is_err());
        payload["products"][1]["product"] = json!("RB");
        assert!(parse(payload.to_string().as_bytes(), "20261118").is_err());
    }
}
