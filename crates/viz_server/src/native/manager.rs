//! Read-only projection of the authenticated Manager attribution ledger.
use super::config::Config;
use anyhow::{ensure, Context, Result};
use axum::http::{header, HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

const SESSION: &str = "cn_cta_session=";
const MAX_LEDGER_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct Positions {
    sources: Arc<BTreeMap<String, String>>,
    origin: String,
    client: reqwest::Client,
}

impl Positions {
    pub fn new(config: &Config) -> Result<Self> {
        let sources = config
            .counters
            .iter()
            .filter_map(|counter| {
                let id = counter.manager_source_id.as_ref()?;
                Some((counter.id.clone(), id.clone()))
            })
            .collect();
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(3))
            .build()
            .context("build Manager ledger HTTP client")?;
        Ok(Self {
            sources: Arc::new(sources),
            origin: format!("http://127.0.0.1:{}", config.manager_port),
            client,
        })
    }

    pub fn source_ids(&self) -> Vec<&str> {
        self.sources.keys().map(String::as_str).collect()
    }

    pub async fn read(
        &self,
        source: &str,
        headers: &HeaderMap,
    ) -> Result<BTreeMap<String, ContractPositions>, StatusCode> {
        let id = self.sources.get(source).ok_or(StatusCode::NOT_FOUND)?;
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
                "{}/manager/api/native/sources/{id}/netting",
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
            if bytes.len() + chunk.len() > MAX_LEDGER_BYTES {
                return Err(StatusCode::BAD_GATEWAY);
            }
            bytes.extend_from_slice(&chunk);
        }
        let ledger: Ledger = serde_json::from_slice(&bytes).map_err(|_| StatusCode::BAD_GATEWAY)?;
        if &ledger.portfolio.source_id != id {
            return Err(StatusCode::BAD_GATEWAY);
        }
        ledger.project().map_err(|error| {
            log::warn!("invalid Manager ledger for Viz source {source}: {error}");
            StatusCode::BAD_GATEWAY
        })
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
