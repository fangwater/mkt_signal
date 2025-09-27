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

#[derive(Debug, Clone)]
pub struct MinQtyEntry {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub min_qty: f64,
}

#[derive(Debug, Default)]
pub struct PriceTable {
    client: Client,
    entries: BTreeMap<String, PriceEntry>,
    spot_min_qty: HashMap<String, MinQtyEntry>,
    futures_um_min_qty: HashMap<String, MinQtyEntry>,
}

impl PriceTable {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            entries: BTreeMap::new(),
            spot_min_qty: HashMap::new(),
            futures_um_min_qty: HashMap::new(),
        }
    }

    pub async fn init(&mut self, interested: &BTreeSet<String>) -> Result<()> {
        let spot_min_qty = self
            .fetch_exchange_min_qty(
                "https://api.binance.com/api/v3/exchangeInfo",
                "binance_spot",
            )
            .await?;
        let futures_um_min_qty = self
            .fetch_exchange_min_qty(
                "https://fapi.binance.com/fapi/v1/exchangeInfo",
                "binance_um_futures",
            )
            .await?;

        self.spot_min_qty = spot_min_qty;
        self.futures_um_min_qty = futures_um_min_qty;

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

    pub fn spot_min_qty_by_symbol(&self, symbol: &str) -> Option<f64> {
        let key = symbol.to_uppercase();
        self.spot_min_qty.get(&key).map(|entry| entry.min_qty)
    }

    pub fn spot_min_qty(&self, base_asset: &str, quote_asset: &str) -> Option<f64> {
        let symbol = format!(
            "{}{}",
            base_asset.to_uppercase(),
            quote_asset.to_uppercase()
        );
        self.spot_min_qty_by_symbol(&symbol)
    }

    pub fn futures_um_min_qty_by_symbol(&self, symbol: &str) -> Option<f64> {
        let key = symbol.to_uppercase();
        self.futures_um_min_qty.get(&key).map(|entry| entry.min_qty)
    }

    pub fn futures_um_min_qty(&self, base_asset: &str, quote_asset: &str) -> Option<f64> {
        let symbol = format!(
            "{}{}",
            base_asset.to_uppercase(),
            quote_asset.to_uppercase()
        );
        self.futures_um_min_qty_by_symbol(&symbol)
    }

    async fn fetch_exchange_min_qty(
        &self,
        url: &str,
        label: &str,
    ) -> Result<HashMap<String, MinQtyEntry>> {
        let resp = self.client.get(url).send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(anyhow!("GET {} failed: {} - {}", url, status, body));
        }

        let exchange_info: RawExchangeInfo = serde_json::from_str(&body)
            .with_context(|| format!("failed to parse {} exchange info", label))?;

        let mut map = HashMap::new();
        for raw_symbol in exchange_info.symbols {
            match min_qty_from_filters(&raw_symbol.filters, &raw_symbol.symbol)? {
                Some(min_qty) => {
                    let symbol = raw_symbol.symbol.to_uppercase();
                    let entry = MinQtyEntry {
                        symbol: symbol.clone(),
                        base_asset: raw_symbol.base_asset.to_uppercase(),
                        quote_asset: raw_symbol.quote_asset.to_uppercase(),
                        min_qty,
                    };
                    map.insert(symbol, entry);
                }
                None => {
                    if raw_symbol.status.eq_ignore_ascii_case("TRADING") {
                        warn!(
                            "{} missing LOT_SIZE minQty, symbol={}",
                            label, raw_symbol.symbol
                        );
                    }
                }
            }
        }

        Ok(map)
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

#[derive(Debug, Deserialize)]
struct RawExchangeInfo {
    #[serde(rename = "symbols")]
    symbols: Vec<RawExchangeSymbol>,
}

#[derive(Debug, Deserialize)]
struct RawExchangeSymbol {
    #[serde(rename = "symbol")]
    symbol: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    #[serde(default, rename = "status")]
    status: String,
    #[serde(default, rename = "filters")]
    filters: Vec<RawExchangeFilter>,
}

#[derive(Debug, Deserialize)]
struct RawExchangeFilter {
    #[serde(rename = "filterType")]
    filter_type: String,
    #[serde(rename = "minQty")]
    min_qty: Option<String>,
}

fn min_qty_from_filters(filters: &[RawExchangeFilter], symbol: &str) -> Result<Option<f64>> {
    for filter in filters {
        if filter.filter_type == "LOT_SIZE" {
            if let Some(min_qty) = &filter.min_qty {
                let qty = parse_decimal(min_qty, "minQty", symbol)?;
                return Ok(Some(qty));
            }
        }
    }
    Ok(None)
}
