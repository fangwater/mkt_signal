//! Closed hourly candle prices bootstrap only the pre-Kafka portion of NQ.
use super::config::{Interval, Market, Plan};
use anyhow::{anyhow, ensure, Context, Result};
use reqwest::{Client, StatusCode, Url};
use serde::Serialize;
use serde_json::Value;
use std::{collections::BTreeMap, time::Duration};
use tokio::{sync::oneshot, task::JoinHandle};

const HOUR_MS: i64 = 3_600_000;
const PAGE_LIMIT: usize = 499;
const PUBLIC_BASE_ENV: &str = "BINANCE_FUTURES_PUBLIC_API_BASE";
const DEFAULT_PUBLIC_BASE: &str = "https://fapi.binance.com";

#[derive(Clone, Debug, Serialize)]
pub(crate) struct HourlyClose {
    pub end_ms: i64,
    pub close: f64,
}

#[derive(Clone, Debug)]
struct Request {
    factor_id: String,
    symbol: String,
    bars: usize,
}

#[derive(Default)]
pub(crate) struct SeedBatch {
    pub histories: BTreeMap<String, Vec<HourlyClose>>,
    pub errors: BTreeMap<String, String>,
}

#[derive(Serialize)]
pub(crate) struct SeedStatus {
    pub phase: &'static str,
    pub cutoff_ms: Option<i64>,
    pub requested_symbols: usize,
    pub seeded_rows: BTreeMap<String, usize>,
    pub errors: BTreeMap<String, String>,
}

/// HTTP runs outside the generation poll loop, which continues servicing the
/// active generation and position tables during a replacement's bootstrap.
pub(crate) struct HourlyNqSeed {
    requests: Vec<Request>,
    base: String,
    task: Option<JoinHandle<()>>,
    receiver: Option<oneshot::Receiver<SeedBatch>>,
    pub status: SeedStatus,
}
impl HourlyNqSeed {
    pub fn exclude_seeded(&mut self, seeded: &BTreeMap<String, usize>) {
        self.requests.retain(|r| !seeded.contains_key(&r.factor_id));
        self.status.requested_symbols = self.requests.len();
        if self.requests.is_empty() {
            self.status.phase = "seeded_from_hourly_history";
        }
    }

    pub fn new(plan: &Plan) -> Self {
        let requests = plan
            .calculations
            .values()
            .filter(|c| {
                c.id.interval == Interval::Hour
                    && c.id.factor == "nq"
                    && c.id.market == Market::BinanceFutures
            })
            .map(|c| Request {
                factor_id: c.id.name(),
                symbol: c.id.symbol.clone(),
                // At lookback + rank_window - 1, every retained change has a
                // complete extrema lookback, including the oldest ranked change.
                bars: c.parameters.lookback + c.parameters.rank_window - 1,
            })
            .collect::<Vec<_>>();
        let count = requests.len();
        Self {
            requests,
            base: std::env::var(PUBLIC_BASE_ENV)
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_PUBLIC_BASE.into()),
            task: None,
            receiver: None,
            status: SeedStatus {
                phase: if count == 0 {
                    "not_needed"
                } else {
                    "waiting_for_kafka"
                },
                cutoff_ms: None,
                requested_symbols: count,
                seeded_rows: BTreeMap::new(),
                errors: BTreeMap::new(),
            },
        }
    }
    pub fn needs_start(&self, market: Market) -> bool {
        market == Market::BinanceFutures && self.status.phase == "waiting_for_kafka"
    }
    pub fn start(&mut self, first_period_end_ms: i64) -> Result<()> {
        ensure!(
            first_period_end_ms > 0,
            "invalid first hourly NQ Kafka period"
        );
        // Exact boundary events belong to the closing hour. Seed through the
        // start of that first source hour, never through its own close.
        let cutoff_ms = (first_period_end_ms - 1).div_euclid(HOUR_MS) * HOUR_MS;
        self.status.cutoff_ms = Some(cutoff_ms);
        self.status.phase = "loading";
        let requests = self.requests.clone();
        let base = self.base.clone();
        let (sender, receiver) = oneshot::channel();
        self.receiver = Some(receiver);
        self.task = Some(tokio::spawn(async move {
            let result = fetch_all(&base, &requests, cutoff_ms, Duration::from_secs(1)).await;
            let _ = sender.send(result);
        }));
        Ok(())
    }
    pub fn loading(&self) -> bool {
        self.status.phase == "loading"
    }
    pub fn poll(&mut self) -> Option<SeedBatch> {
        let receiver = self.receiver.as_mut()?;
        let result = match receiver.try_recv() {
            Ok(batch) => batch,
            Err(oneshot::error::TryRecvError::Empty) => return None,
            Err(oneshot::error::TryRecvError::Closed) => SeedBatch {
                errors: self
                    .requests
                    .iter()
                    .map(|r| {
                        (
                            r.factor_id.clone(),
                            "hourly NQ bootstrap task ended without a result".into(),
                        )
                    })
                    .collect(),
                ..Default::default()
            },
        };
        self.receiver = None;
        self.task = None;
        Some(result)
    }
    pub fn finish(&mut self, rows: BTreeMap<String, usize>, errors: BTreeMap<String, String>) {
        self.status.seeded_rows = rows;
        self.status.errors = errors;
        self.status.phase = if self.status.errors.is_empty() {
            "complete"
        } else {
            "partial"
        };
    }
}
impl Drop for HourlyNqSeed {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct KlineClient {
    http: Client,
    url: String,
    pause: Duration,
    next_request: tokio::time::Instant,
    rate_limited: bool,
}
impl KlineClient {
    fn new(base: &str, pause: Duration) -> Result<Self> {
        let parsed = Url::parse(base.trim()).context("parse hourly NQ public API base")?;
        ensure!(
            matches!(parsed.scheme(), "http" | "https")
                && parsed.host_str().is_some()
                && parsed.username().is_empty()
                && parsed.password().is_none()
                && parsed.query().is_none()
                && parsed.fragment().is_none(),
            "invalid hourly NQ public API base"
        );
        Ok(Self {
            http: Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(15))
                .user_agent("mkt-signal-cta-hourly-nq")
                .build()
                .context("build hourly NQ public client")?,
            url: format!("{}/fapi/v1/klines", base.trim().trim_end_matches('/')),
            pause,
            next_request: tokio::time::Instant::now(),
            rate_limited: false,
        })
    }
    async fn fetch(
        &mut self,
        symbol: &str,
        bars: usize,
        cutoff_ms: i64,
    ) -> Result<Vec<HourlyClose>> {
        let mut history = BTreeMap::new();
        let mut end_ms = cutoff_ms - 1;
        while history.len() < bars && end_ms > 0 {
            ensure!(
                !self.rate_limited,
                "hourly NQ public API bootstrap stopped after rate limiting"
            );
            tokio::time::sleep_until(self.next_request).await;
            self.next_request = tokio::time::Instant::now() + self.pause;
            let limit = (bars - history.len()).min(PAGE_LIMIT);
            let response = self
                .http
                .get(&self.url)
                .query(&[
                    ("symbol", symbol.to_string()),
                    ("interval", "1h".into()),
                    ("endTime", end_ms.to_string()),
                    ("limit", limit.to_string()),
                ])
                .send()
                .await
                .context("fetch hourly NQ closed candles")?;
            if matches!(
                response.status(),
                StatusCode::TOO_MANY_REQUESTS | StatusCode::IM_A_TEAPOT
            ) {
                self.rate_limited = true;
                return Err(anyhow!(
                    "hourly NQ public API HTTP {}; stop this bootstrap without immediate retries",
                    response.status()
                ));
            }
            let value: Value = response
                .error_for_status()
                .context("hourly NQ kline HTTP status")?
                .json()
                .await
                .context("decode hourly NQ candles")?;
            let page = parse_closed_candles(&value, cutoff_ms)?;
            let Some(oldest) = page.first() else {
                break;
            };
            ensure!(
                oldest.end_ms - HOUR_MS <= end_ms,
                "hourly NQ kline pagination did not advance"
            );
            let next_end_ms = oldest.end_ms - HOUR_MS - 1;
            let count = page.len();
            for row in page {
                if let Some(previous) = history.insert(row.end_ms, row.close) {
                    ensure!(
                        previous == row.close,
                        "conflicting hourly NQ candle at {}",
                        row.end_ms
                    );
                }
            }
            ensure!(next_end_ms < end_ms, "hourly NQ kline pagination stalled");
            end_ms = next_end_ms;
            if count < limit {
                break;
            }
        }
        let mut rows = history
            .into_iter()
            .map(|(end_ms, close)| HourlyClose { end_ms, close })
            .collect::<Vec<_>>();
        if rows.len() > bars {
            rows.drain(..rows.len() - bars);
        }
        Ok(rows)
    }
}

async fn fetch_all(base: &str, requests: &[Request], cutoff_ms: i64, pause: Duration) -> SeedBatch {
    let mut batch = SeedBatch::default();
    let mut client = match KlineClient::new(base, pause) {
        Ok(client) => client,
        Err(error) => {
            for r in requests {
                batch.errors.insert(r.factor_id.clone(), error.to_string());
            }
            return batch;
        }
    };
    for request in requests {
        match client.fetch(&request.symbol, request.bars, cutoff_ms).await {
            Ok(rows) => {
                batch.histories.insert(request.factor_id.clone(), rows);
            }
            Err(error) => {
                batch
                    .errors
                    .insert(request.factor_id.clone(), format!("{error:#}"));
            }
        }
    }
    batch
}

fn parse_closed_candles(value: &Value, cutoff_ms: i64) -> Result<Vec<HourlyClose>> {
    let mut history = BTreeMap::new();
    for value in value
        .as_array()
        .context("hourly NQ candle response must be an array")?
    {
        let row = value.as_array().context("invalid hourly NQ candle row")?;
        ensure!(row.len() >= 7, "short hourly NQ candle row");
        let open = row[0]
            .as_i64()
            .context("invalid hourly NQ candle open time")?;
        let end = open
            .checked_add(HOUR_MS)
            .context("hourly NQ candle time overflow")?;
        if end > cutoff_ms {
            continue;
        }
        ensure!(
            open > 0 && open % HOUR_MS == 0 && row[6].as_i64() == Some(end - 1),
            "invalid hourly NQ candle time bounds"
        );
        let close = row[4]
            .as_str()
            .context("invalid hourly NQ close string")?
            .parse::<f64>()
            .context("parse hourly NQ close")?;
        ensure!(
            close.is_finite() && close > 0.0,
            "invalid hourly NQ close price"
        );
        if let Some(previous) = history.insert(end, close) {
            ensure!(previous == close, "conflicting hourly NQ candle at {end}");
        }
    }
    Ok(history
        .into_iter()
        .map(|(end_ms, close)| HourlyClose { end_ms, close })
        .collect())
}

#[cfg(test)]
#[path = "hourly_nq_seed_tests.rs"]
mod tests;
