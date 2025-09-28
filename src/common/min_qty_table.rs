use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use log::{info, warn};
use reqwest::Client;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct MinQtyEntry {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub min_qty: f64,
    pub price_tick: Option<f64>,
}

#[derive(Debug)]
pub struct MinQtyTable {
    client: Client,
    spot: HashMap<String, MinQtyEntry>,
    futures_um: HashMap<String, MinQtyEntry>,
}

impl Default for MinQtyTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MinQtyTable {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            spot: HashMap::new(),
            futures_um: HashMap::new(),
        }
    }

    /// 刷新币安现货与永续合约最小下单量
    pub async fn refresh_binance(&mut self) -> Result<()> {
        let spot = self
            .fetch_exchange_min_qty(
                "https://api.binance.com/api/v3/exchangeInfo",
                "binance_spot",
            )
            .await?;
        let futures = self
            .fetch_exchange_min_qty(
                "https://fapi.binance.com/fapi/v1/exchangeInfo",
                "binance_um_futures",
            )
            .await?;

        info!(
            "刷新最小下单量: spot={} 条目, futures={} 条目",
            spot.len(),
            futures.len()
        );

        self.spot = spot;
        self.futures_um = futures;
        Ok(())
    }

    pub fn spot_min_qty_by_symbol(&self, symbol: &str) -> Option<f64> {
        let key = symbol.to_uppercase();
        self.spot.get(&key).map(|entry| entry.min_qty)
    }

    pub fn spot_price_tick_by_symbol(&self, symbol: &str) -> Option<f64> {
        let key = symbol.to_uppercase();
        self.spot
            .get(&key)
            .and_then(|entry| entry.price_tick)
            .filter(|tick| *tick > 0.0)
    }

    pub fn futures_um_min_qty_by_symbol(&self, symbol: &str) -> Option<f64> {
        let key = symbol.to_uppercase();
        self.futures_um.get(&key).map(|entry| entry.min_qty)
    }

    pub fn futures_um_price_tick_by_symbol(&self, symbol: &str) -> Option<f64> {
        let key = symbol.to_uppercase();
        self.futures_um
            .get(&key)
            .and_then(|entry| entry.price_tick)
            .filter(|tick| *tick > 0.0)
    }

    pub fn spot_min_qty(&self, base_asset: &str, quote_asset: &str) -> Option<f64> {
        let symbol = format!(
            "{}{}",
            base_asset.to_uppercase(),
            quote_asset.to_uppercase()
        );
        self.spot_min_qty_by_symbol(&symbol)
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
            let filters = extract_filter_values(&raw_symbol.filters, &raw_symbol.symbol)?;
            if let Some(min_qty) = filters.min_qty {
                let symbol = raw_symbol.symbol.to_uppercase();
                let entry = MinQtyEntry {
                    symbol: symbol.clone(),
                    base_asset: raw_symbol.base_asset.to_uppercase(),
                    quote_asset: raw_symbol.quote_asset.to_uppercase(),
                    min_qty,
                    price_tick: filters.price_tick,
                };
                map.insert(symbol, entry);
            } else if raw_symbol.status.eq_ignore_ascii_case("TRADING") {
                warn!(
                    "{} missing LOT_SIZE minQty, symbol={}",
                    label, raw_symbol.symbol
                );
            }
        }

        Ok(map)
    }
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
    #[serde(rename = "stepSize")]
    step_size: Option<String>,
    #[serde(rename = "tickSize")]
    tick_size: Option<String>,
}

struct SymbolFilterValues {
    min_qty: Option<f64>,
    price_tick: Option<f64>,
}

fn extract_filter_values(
    filters: &[RawExchangeFilter],
    symbol: &str,
) -> Result<SymbolFilterValues> {
    let mut min_qty: Option<f64> = None;
    let mut price_tick: Option<f64> = None;

    for filter in filters {
        match filter.filter_type.as_str() {
            "LOT_SIZE" => {
                if let Some(value) = &filter.step_size {
                    min_qty = Some(parse_decimal(value, "stepSize", symbol)?);
                } else if let Some(value) = &filter.min_qty {
                    min_qty = Some(parse_decimal(value, "minQty", symbol)?);
                }
            }
            "PRICE_FILTER" => {
                if let Some(value) = &filter.tick_size {
                    let tick = parse_decimal(value, "tickSize", symbol)?;
                    if tick > 0.0 {
                        price_tick = Some(tick);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(SymbolFilterValues {
        min_qty,
        price_tick,
    })
}

fn parse_decimal(value: &str, field: &str, symbol: &str) -> Result<f64> {
    value
        .parse::<f64>()
        .with_context(|| format!("symbol={} field={}", symbol, field))
}
