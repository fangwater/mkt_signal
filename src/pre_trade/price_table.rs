use std::collections::{BTreeSet, HashMap};

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
    entries: HashMap<String, PriceEntry>,
}

impl PriceTable {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            entries: HashMap::new(),
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
            let symbol = price_symbol_key(&raw.symbol);
            if !interested
                .iter()
                .any(|item| price_symbol_key(item) == symbol)
            {
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

        let mut entries: HashMap<String, PriceEntry> = interested
            .iter()
            .map(|sym| {
                let key = price_symbol_key(sym);
                (key.clone(), PriceEntry::new(key))
            })
            .collect();

        for (sym, entry) in map {
            entries.insert(sym.clone(), entry);
        }

        let missing: Vec<String> = interested
            .iter()
            .filter(|sym| {
                entries
                    .get(&price_symbol_key(sym))
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
        let symbol_upper = price_symbol_key(symbol);
        let entry = self
            .entries
            .entry(symbol_upper.clone())
            .or_insert_with(|| PriceEntry::new(symbol_upper.clone()));
        entry.mark_price = mark_price;
        entry.update_time = timestamp;
    }

    pub fn update_index_price(&mut self, symbol: &str, index_price: f64, timestamp: i64) {
        let symbol_upper = price_symbol_key(symbol);
        let entry = self
            .entries
            .entry(symbol_upper.clone())
            .or_insert_with(|| PriceEntry::new(symbol_upper.clone()));
        entry.index_price = index_price;
        entry.update_time = timestamp;
    }

    pub fn mark_price(&self, symbol: &str) -> Option<f64> {
        let key = price_symbol_key(symbol);
        self.entries
            .get(&key)
            .map(|entry| entry.mark_price)
            .filter(|price| *price > 0.0)
    }

    pub fn get(&self, symbol: &str) -> Option<&PriceEntry> {
        self.entries.get(&price_symbol_key(symbol))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &PriceEntry)> {
        self.entries.iter()
    }

    pub fn snapshot(&self) -> HashMap<String, PriceEntry> {
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

fn price_symbol_key(symbol: &str) -> String {
    let upper = symbol.trim().to_ascii_uppercase();
    let normalized = upper.replace(['-', '_', '/'], "");
    let is_coin_perpetual = normalized.ends_with("USDPERP");
    let is_bitget_coin_futures = normalized
        .strip_suffix("CM")
        .is_some_and(|root| root.ends_with("USD") && root.len() > "USD".len());
    let is_coin_delivery = normalized.len() > 6
        && normalized
            .get(normalized.len() - 6..)
            .is_some_and(|suffix| suffix.bytes().all(|byte| byte.is_ascii_digit()))
        && normalized
            .get(..normalized.len() - 6)
            .is_some_and(|root| root.ends_with("USD"));
    if is_coin_perpetual || is_coin_delivery || is_bitget_coin_futures {
        normalized
    } else {
        upper
    }
}

fn parse_decimal(value: &str, field: &str, symbol: &str) -> Result<f64> {
    value
        .parse::<f64>()
        .with_context(|| format!("symbol={} field={}", symbol, field))
}

#[cfg(test)]
mod tests {
    use super::PriceTable;

    #[test]
    fn coin_futures_mark_price_keys_accept_exchange_and_internal_symbols() {
        let mut table = PriceTable::new();
        table.update_mark_price("BTCUSD_PERP", 50_000.0, 123);
        assert_eq!(table.mark_price("BTCUSD_PERP"), Some(50_000.0));
        assert_eq!(table.mark_price("BTCUSDPERP"), Some(50_000.0));
        assert_eq!(table.get("BTCUSD_PERP").unwrap().symbol, "BTCUSDPERP");
    }

    #[test]
    fn bitget_coin_futures_mark_price_keys_accept_exchange_and_internal_symbols() {
        let mut table = PriceTable::new();
        table.update_mark_price("BTCUSD_CM", 50_000.0, 123);
        assert_eq!(table.mark_price("BTCUSD_CM"), Some(50_000.0));
        assert_eq!(table.mark_price("BTCUSDCM"), Some(50_000.0));
        assert_eq!(table.get("BTCUSD_CM").unwrap().symbol, "BTCUSDCM");
    }

    #[test]
    fn non_coin_underscore_symbols_remain_distinct() {
        let mut table = PriceTable::new();
        table.update_mark_price("BTC_USDT", 50_000.0, 123);
        assert_eq!(table.mark_price("BTC_USDT"), Some(50_000.0));
        assert_eq!(table.mark_price("BTCUSDT"), None);
    }
}
