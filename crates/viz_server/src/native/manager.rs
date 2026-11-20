//! Read-only authenticated Manager observations and attribution.
use super::{config::Config, cta, l1::SharedQuotes};
use anyhow::{ensure, Context, Result};
use axum::http::{header, HeaderMap, StatusCode};
use cn_futures_state::Adapter;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

const SESSION: &str = "cn_cta_session=";
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct Positions {
    sources: Arc<BTreeMap<String, (String, Adapter)>>,
    origin: String,
    client: reqwest::Client,
    cta_signal_port: Option<u16>,
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
            cta_signal_port: config.cta_signal_port,
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
        let mut signals = BTreeMap::new();
        if let Some(port) = self.cta_signal_port {
            let groups = ledger
                .portfolio
                .strategy_groups
                .keys()
                .map(String::as_str)
                .collect();
            match cta::factor_signals(&self.client, port, id, &groups).await {
                Ok(observed) => signals = observed,
                Err(error) => {
                    log::warn!("read CTA signals for Viz source {source}: {error:#}")
                }
            }
        }
        ledger.project(signals).map_err(|error| {
            log::warn!("invalid Manager ledger for Viz source {source}: {error}");
            StatusCode::BAD_GATEWAY
        })
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
        let response = self
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
                return Err(code);
            }
            _ => return Err(StatusCode::BAD_GATEWAY),
        }
        let data = decode_json(response)
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?;
        Ok((id, *adapter, data))
    }
}

pub(super) async fn decode_json<T: DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    let mut response = response.error_for_status()?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= MAX_RESPONSE_BYTES,
            "Viz upstream response too large"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

#[derive(Serialize)]
pub struct ContractPositions {
    target_lots: i64,
    attributed_lots: i64,
    strategies: Vec<StrategyPosition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    factor_signals: Option<cta::FactorSignals>,
}

#[derive(Serialize)]
struct StrategyPosition {
    name: String,
    signal_target_lots: i64,
    target_lots: i64,
    attributed_lots: i64,
}

#[derive(Deserialize)]
struct Ledger {
    portfolio: Portfolio,
    net_targets: BTreeMap<String, i64>,
    /// Exec-owned live strategy attribution; absent rows hold zero lots.
    exec_ledger: BTreeMap<String, BTreeMap<String, i64>>,
}

#[derive(Deserialize)]
struct Portfolio {
    source_id: String,
    /// Group names select the CTA signal views; members come from `targets`.
    strategy_groups: BTreeMap<String, serde::de::IgnoredAny>,
    targets: BTreeMap<String, BTreeMap<String, i64>>,
}

impl Ledger {
    fn project(
        self,
        mut signals: BTreeMap<String, (String, cta::FactorSignals)>,
    ) -> Result<BTreeMap<String, ContractPositions>> {
        self.net_targets
            .into_iter()
            .map(|(market, net)| {
                let attributed = self.exec_ledger.get(&market);
                let mut strategies = self
                    .portfolio
                    .targets
                    .iter()
                    .map(|(name, targets)| {
                        let target_lots =
                            *targets.get(&market).context("missing strategy target")?;
                        Ok(StrategyPosition {
                            name: name.clone(),
                            signal_target_lots: target_lots,
                            target_lots,
                            attributed_lots: attributed
                                .and_then(|row| row.get(name))
                                .copied()
                                .unwrap_or(0),
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
                    target_lots == net,
                    "strategy totals do not match the Manager net target"
                );
                let factor_signals = signals.remove(&market).map(|(group, signals)| {
                    let aggregate = format!("{group}-1h");
                    strategies.retain(|row| {
                        row.name == aggregate
                            || row.signal_target_lots != 0
                            || row.attributed_lots != 0
                    });
                    signals
                });
                Ok((
                    market,
                    ContractPositions {
                        target_lots,
                        attributed_lots,
                        strategies,
                        factor_signals,
                    },
                ))
            })
            .collect()
    }
}
