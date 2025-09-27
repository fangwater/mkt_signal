use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{anyhow, Context, Result};
use log::{info, warn};
use reqwest::Client;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct PriceEntry {
    pub symbol: String,
    pub mark_price: f64,
    pub index_price: f64,
    pub update_time: i64,
}

impl PriceEntry {
    fn new(symbol: String) -> Self {
        Self {
            symbol,
            mark_price: 0.0,
            index_price: 0.0,
            update_time: 0,
        }
    }
}

impl Default for PriceEntry {
    fn default() -> Self {
        Self {
            symbol: String::new(),
            mark_price: 0.0,
            index_price: 0.0,
            update_time: 0,
        }
    }
}

#[derive(Debug, Default)]
pub struct PriceTable {
    client: Client,
    entries: BTreeMap<String, PriceEntry>,
}

impl PriceTable {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            entries: BTreeMap::new(),
        }
    }

    pub async fn init(&mut self, interested: &BTreeSet<String>) -> Result<()> {
        if interested.is_empty() {
            info!("no symbols provided for mark price table");
            self.entries.clear();
            return Ok(());
        }

        let resp = self
            .client
            .get("https://fapi.binance.com/fapi/v1/premiumIndex")
            .send()
            .await?;

        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(anyhow!(
                "GET /fapi/v1/premiumIndex failed: {} - {}",
                status,
                body
            ));
        }

        let raw_list: Vec<RawPremiumIndex> = serde_json::from_str(&body)
            .context("failed to parse /fapi/v1/premiumIndex response")?;

        let mut map: HashMap<String, PriceEntry> = HashMap::new();
        for raw in raw_list {
            let symbol = raw.symbol.to_uppercase();
            if !interested.contains(&symbol) {
                continue;
            }
            let entry = PriceEntry {
                symbol: symbol.clone(),
                mark_price: parse_decimal(&raw.mark_price, "markPrice", &symbol)?,
                index_price: parse_decimal(&raw.index_price, "indexPrice", &symbol)?,
                update_time: raw.time,
            };
            map.insert(symbol, entry);
        }

        let mut entries: BTreeMap<String, PriceEntry> = interested
            .iter()
            .map(|sym| (sym.clone(), PriceEntry::new(sym.clone())))
            .collect();

        for (sym, entry) in map {
            entries.insert(sym.clone(), entry);
        }

        let missing: Vec<String> = interested
            .iter()
            .filter(|sym| {
                entries
                    .get(*sym)
                    .map(|entry| entry.mark_price == 0.0)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        if !missing.is_empty() {
            warn!("missing mark price entries for symbols: {:?}", missing);
        }

        self.entries = entries;
        Ok(())
    }

    pub fn update_mark_price(&mut self, symbol: &str, mark_price: f64, timestamp: i64) {
        let symbol_upper = symbol.to_uppercase();
        let entry = self
            .entries
            .entry(symbol_upper.clone())
            .or_insert_with(|| PriceEntry::new(symbol_upper.clone()));
        entry.mark_price = mark_price;
        entry.update_time = timestamp;
    }

    pub fn update_index_price(&mut self, symbol: &str, index_price: f64, timestamp: i64) {
        let symbol_upper = symbol.to_uppercase();
        let entry = self
            .entries
            .entry(symbol_upper.clone())
            .or_insert_with(|| PriceEntry::new(symbol_upper.clone()));
        entry.index_price = index_price;
        entry.update_time = timestamp;
    }

    pub fn snapshot(&self) -> BTreeMap<String, PriceEntry> {
        self.entries.clone()
    }
}

#[derive(Debug, Deserialize)]
struct RawPremiumIndex {
    #[serde(rename = "symbol")]
    symbol: String,
    #[serde(rename = "markPrice")]
    mark_price: String,
    #[serde(rename = "indexPrice")]
    index_price: String,
    #[serde(rename = "time")]
    time: i64,
}

fn parse_decimal(value: &str, field: &str, symbol: &str) -> Result<f64> {
    value
        .parse::<f64>()
        .with_context(|| format!("symbol={} field={}", symbol, field))
}
