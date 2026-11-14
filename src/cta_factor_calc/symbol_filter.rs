//! Remove inactive Binance contracts while preparing configuration edits.
use super::config::{Combination, Market};
use anyhow::{ensure, Context, Result};
use reqwest::{Client, Url};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

type Snapshot = BTreeMap<String, String>;

pub struct ConfigurationSymbolFilter {
    client: Client,
    url: Url,
    snapshot: Mutex<Option<(Instant, Snapshot)>>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct ExcludedSymbol {
    pub symbol: String,
    pub market: Market,
    pub status: String,
    pub removed_rules: usize,
}

impl ConfigurationSymbolFilter {
    pub fn from_env() -> Result<Self> {
        Self::new(
            &std::env::var("BINANCE_FUTURES_PUBLIC_API_BASE")
                .unwrap_or_else(|_| "https://fapi.binance.com".into()),
        )
    }

    pub fn new(public_base: &str) -> Result<Self> {
        let mut url = Url::parse(public_base).context("parse Binance public API base")?;
        ensure!(
            matches!(url.scheme(), "http" | "https")
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "Binance public API base must be an HTTP(S) URL without credentials/query/fragment"
        );
        url.set_path(&format!(
            "{}/fapi/v1/exchangeInfo",
            url.path().trim_end_matches('/')
        ));
        Ok(Self {
            client: Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(15))
                .build()?,
            url,
            snapshot: Mutex::new(None),
        })
    }

    pub async fn filter(
        &self,
        combination: Combination,
    ) -> Result<(Combination, Vec<ExcludedSymbol>)> {
        combination.validate()?;
        let ids = combination
            .factor_rules
            .iter()
            .map(|rule| combination.rule_id(rule))
            .collect::<Result<Vec<_>>>()?;
        let trading_binance = combination.signal.is_some()
            && (combination.venue == Market::BinanceFutures
                || combination.reference_venue == Some(Market::BinanceFutures));
        if !trading_binance
            && !ids
                .iter()
                .any(|id| id.markets().any(|market| market == Market::BinanceFutures))
        {
            return Ok((combination, vec![]));
        }
        let mut snapshot = self.snapshot.lock().await;
        if snapshot
            .as_ref()
            .is_none_or(|(at, _)| at.elapsed() >= Duration::from_secs(60))
        {
            let rows = crate::common::exchange_info::fetch_binance_futures_listing(
                &self.client,
                self.url.as_str(),
            )
            .await
            .context("cannot verify Binance contract status; configuration was not saved")?;
            *snapshot = Some((
                Instant::now(),
                rows.into_iter()
                    .map(|row| (row.normalized_symbol, row.status))
                    .collect(),
            ));
        }
        filter_with_snapshot(combination, &snapshot.as_ref().unwrap().1)
    }
}

fn filter_with_snapshot(
    mut combination: Combination,
    snapshot: &Snapshot,
) -> Result<(Combination, Vec<ExcludedSymbol>)> {
    let trading_binance = combination.signal.is_some()
        && (combination.venue == Market::BinanceFutures
            || combination.reference_venue == Some(Market::BinanceFutures));
    let mut excluded = BTreeMap::<String, ExcludedSymbol>::new();
    let mut retained = Vec::new();
    for rule in &combination.factor_rules {
        let id = combination.rule_id(rule)?;
        if trading_binance || id.markets().any(|market| market == Market::BinanceFutures) {
            let status = snapshot
                .get(&id.symbol)
                .map_or("NOT_LISTED", String::as_str);
            if status != "TRADING" {
                excluded
                    .entry(id.symbol.clone())
                    .or_insert_with(|| ExcludedSymbol {
                        symbol: id.symbol,
                        market: Market::BinanceFutures,
                        status: status.into(),
                        removed_rules: 0,
                    })
                    .removed_rules += 1;
                continue;
            }
        }
        retained.push(rule.clone());
    }
    if excluded.is_empty() {
        return Ok((combination, vec![]));
    }
    combination.factor_rules = retained;
    let factors = combination.factors()?;
    combination
        .parameters
        .retain(|name, _| factors.contains_key(name));
    if let Some(signal) = &mut combination.signal {
        signal
            .symbol_overrides
            .retain(|symbol, _| !excluded.contains_key(symbol));
    }
    combination
        .validate()
        .context("no valid combination remains after filtering inactive Binance contracts")?;
    Ok((combination, excluded.into_values().collect()))
}

#[cfg(test)]
#[path = "symbol_filter_tests.rs"]
mod tests;
