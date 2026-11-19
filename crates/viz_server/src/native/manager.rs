//! Read-only authenticated Manager observations and attribution.
use super::{config::Config, l1::SharedQuotes};
use anyhow::{ensure, Context, Result};
use axum::http::{header, HeaderMap, StatusCode};
use cn_futures_state::{Adapter, InstrumentMark, PositionRow, MAX_POSITIONS};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

const SESSION: &str = "cn_cta_session=";
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct Positions {
    sources: Arc<BTreeMap<String, (String, Adapter)>>,
    origin: String,
    client: reqwest::Client,
    quotes: Option<SharedQuotes>,
}

impl Positions {
    pub fn new(config: &Config) -> Result<Self> {
        let sources = config
            .counters
            .iter()
            .filter_map(|counter| {
                let id = counter.manager_source_id.as_ref()?;
                Some((counter.id.clone(), (id.clone(), counter.adapter)))
            })
            .collect();
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(3))
            .build()
            .context("build Manager observation HTTP client")?;
        Ok(Self {
            sources: Arc::new(sources),
            origin: format!("http://127.0.0.1:{}", config.manager_port),
            client,
            quotes: None,
        })
    }

    pub fn with_quotes(mut self, quotes: Option<SharedQuotes>) -> Self {
        self.quotes = quotes;
        self
    }

    pub fn source_ids(&self) -> Vec<&str> {
        self.sources.keys().map(String::as_str).collect()
    }

    pub async fn read(
        &self,
        source: &str,
        headers: &HeaderMap,
    ) -> Result<BTreeMap<String, ContractPositions>, StatusCode> {
        let (id, _, ledger): (_, _, Ledger) = self.read_json(source, headers, "netting").await?;
        if ledger.portfolio.source_id != id {
            return Err(StatusCode::BAD_GATEWAY);
        }
        ledger.project().map_err(|error| {
            log::warn!("invalid Manager ledger for Viz source {source}: {error}");
            StatusCode::BAD_GATEWAY
        })
    }

    pub async fn account(
        &self,
        source: &str,
        headers: &HeaderMap,
    ) -> Result<AccountObservation, StatusCode> {
        let (id, adapter, response): (_, _, ManagerSnapshot) =
            self.read_json(source, headers, "snapshot").await?;
        if response.source_id != id {
            return Err(StatusCode::BAD_GATEWAY);
        }
        let mut observation = response.project(super::now_us());
        if let (Adapter::Ctp, Some(quotes), Some(position)) =
            (adapter, &self.quotes, &mut observation.position)
        {
            if let Ok(day) = position.trading_day.parse() {
                let marks = || {
                    quotes
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .marks(day, &position.positions)
                };
                let (mut prices, mut connected) = marks();
                if prices.iter().any(|m| {
                    m.last_price.is_none() && m.bid_price.is_none() && m.ask_price.is_none()
                }) {
                    // A new observation can queue its first same-day L1 lookup.
                    // Allow one history polling cycle without re-querying Manager.
                    tokio::time::sleep(Duration::from_millis(1250)).await;
                    (prices, connected) = marks();
                }
                position.marks = prices;
                position.l1_connected = connected;
            }
        }
        Ok(observation)
    }

    async fn read_json<T: DeserializeOwned>(
        &self,
        source: &str,
        headers: &HeaderMap,
        endpoint: &str,
    ) -> Result<(&str, Adapter, T), StatusCode> {
        let (id, adapter) = self.sources.get(source).ok_or(StatusCode::NOT_FOUND)?;
        let cookie = headers
            .get(header::COOKIE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| {
                value
                    .split(';')
                    .map(str::trim)
                    .find(|part| part.starts_with(SESSION))
            })
            .ok_or(StatusCode::UNAUTHORIZED)?;
        let mut response = self
            .client
            .get(format!(
                "{}/manager/api/native/sources/{id}/{endpoint}",
                self.origin
            ))
            .header(header::COOKIE, cookie)
            .send()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?;
        match response.status() {
            StatusCode::OK => {}
            code @ (StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::NOT_FOUND) => {
                return Err(code)
            }
            _ => return Err(StatusCode::BAD_GATEWAY),
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?
        {
            if bytes.len() + chunk.len() > MAX_RESPONSE_BYTES {
                return Err(StatusCode::BAD_GATEWAY);
            }
            bytes.extend_from_slice(&chunk);
        }
        let data = serde_json::from_slice(&bytes).map_err(|_| StatusCode::BAD_GATEWAY)?;
        Ok((id, *adapter, data))
    }
}

#[derive(Deserialize)]
struct ManagerSnapshot {
    source_id: String,
    state_observed_us: Option<u64>,
    trading_day: Option<String>,
    snapshot_ready: bool,
    snapshot_current_runtime: bool,
    positions_may_lag: bool,
    projection_errors: u64,
    replication_stale: bool,
    status: String,
    positions: Vec<PositionRow>,
    funds: Option<ManagerFunds>,
    funds_current_runtime: bool,
    funds_stale: bool,
}

#[derive(Deserialize)]
struct ManagerFunds {
    observed_us: u64,
    currency: Option<String>,
    balance: Option<f64>,
    available: Option<f64>,
    margin: Option<f64>,
    native_fields: BTreeMap<String, f64>,
}

#[derive(Serialize)]
pub struct AccountObservation {
    position: Option<ObservedPositions>,
    account: Option<ObservedFunds>,
}

#[derive(Serialize)]
struct ObservedPositions {
    observed_us: u64,
    trading_day: String,
    current: bool,
    positions: Vec<PositionRow>,
    marks: Vec<InstrumentMark>,
    l1_connected: bool,
}

#[derive(Serialize)]
struct ObservedFunds {
    observed_us: u64,
    current: bool,
    currency: &'static str,
    balance: Option<f64>,
    available: Option<f64>,
    curr_margin: Option<f64>,
    withdraw_quota: Option<f64>,
    position_profit: Option<f64>,
    close_profit: Option<f64>,
    commission: Option<f64>,
}

impl ManagerSnapshot {
    fn project(self, now_us: u64) -> AccountObservation {
        let valid_time = |time| time > 0 && time <= now_us;
        let position = self
            .trading_day
            .zip(self.state_observed_us)
            .and_then(|(day, time)| {
                let mut seen = std::collections::HashSet::new();
                let valid = day.len() == 8
                    && day.bytes().all(|b| b.is_ascii_digit())
                    && valid_time(time)
                    && self.snapshot_ready
                    && !self.positions_may_lag
                    && self.projection_errors == 0
                    && self.positions.len() <= MAX_POSITIONS
                    && self.positions.iter().all(|row| {
                        !row.symbol.as_str().is_empty()
                            && row.position.validate().is_ok()
                            && seen.insert((row.venue, row.symbol.as_str(), row.side))
                    });
                valid.then(|| ObservedPositions {
                    observed_us: time,
                    trading_day: day,
                    current: self.snapshot_current_runtime
                        && !self.replication_stale
                        && self.status == "observing",
                    positions: self.positions,
                    marks: Vec::new(),
                    l1_connected: false,
                })
            });
        let account = self
            .funds
            .filter(|funds| {
                funds.currency.as_deref() == Some("CNY") && valid_time(funds.observed_us)
            })
            .map(|funds| ObservedFunds {
                observed_us: funds.observed_us,
                currency: "CNY",
                current: self.funds_current_runtime && !self.funds_stale && !self.replication_stale,
                balance: funds.balance,
                available: funds.available,
                curr_margin: funds.margin,
                withdraw_quota: funds.native_fields.get("withdraw_quota").copied(),
                position_profit: funds.native_fields.get("position_profit").copied(),
                close_profit: funds.native_fields.get("close_profit").copied(),
                commission: funds.native_fields.get("commission").copied(),
            });
        AccountObservation { position, account }
    }
}

#[derive(Serialize)]
pub struct ContractPositions {
    target_lots: i64,
    attributed_lots: i64,
    strategies: Vec<StrategyPosition>,
}

#[derive(Serialize)]
struct StrategyPosition {
    name: String,
    target_lots: i64,
    attributed_lots: i64,
}

#[derive(Deserialize)]
struct Ledger {
    portfolio: Portfolio,
    net_targets: BTreeMap<String, NetTarget>,
}

#[derive(Deserialize)]
struct Portfolio {
    source_id: String,
    strategy_groups: BTreeMap<String, StrategyGroup>,
    targets: BTreeMap<String, BTreeMap<String, i64>>,
    positions: BTreeMap<String, BTreeMap<String, Position>>,
}

#[derive(Deserialize)]
struct StrategyGroup {
    held: bool,
    members: BTreeSet<String>,
}

#[derive(Deserialize)]
struct Position {
    fifo: Vec<Lot>,
}

#[derive(Deserialize)]
struct Lot {
    lots: i64,
}

#[derive(Deserialize)]
struct NetTarget {
    target_lots: i64,
    attributed_lots: i64,
}

impl Ledger {
    fn project(self) -> Result<BTreeMap<String, ContractPositions>> {
        let held = self
            .portfolio
            .strategy_groups
            .values()
            .filter(|group| group.held)
            .flat_map(|group| &group.members)
            .collect::<BTreeSet<_>>();
        self.net_targets
            .into_iter()
            .map(|(market, net)| {
                let strategies = self
                    .portfolio
                    .targets
                    .iter()
                    .map(|(name, targets)| {
                        let position = self
                            .portfolio
                            .positions
                            .get(name)
                            .and_then(|positions| positions.get(&market))
                            .context("missing attributed position")?;
                        let attributed_lots = position.fifo.iter().try_fold(0i64, |sum, lot| {
                            sum.checked_add(lot.lots)
                                .context("attributed lots overflow")
                        })?;
                        let target_lots = if held.contains(name) {
                            attributed_lots
                        } else {
                            *targets.get(&market).context("missing strategy target")?
                        };
                        Ok(StrategyPosition {
                            name: name.clone(),
                            target_lots,
                            attributed_lots,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let (target_lots, attributed_lots) =
                    strategies
                        .iter()
                        .try_fold((0i64, 0i64), |(target, attributed), row| {
                            Ok::<_, anyhow::Error>((
                                target
                                    .checked_add(row.target_lots)
                                    .context("target lots overflow")?,
                                attributed
                                    .checked_add(row.attributed_lots)
                                    .context("attributed lots overflow")?,
                            ))
                        })?;
                ensure!(
                    (target_lots, attributed_lots) == (net.target_lots, net.attributed_lots),
                    "strategy totals do not match the Manager ledger"
                );
                Ok((
                    market,
                    ContractPositions {
                        target_lots,
                        attributed_lots,
                        strategies,
                    },
                ))
            })
            .collect()
    }
}
