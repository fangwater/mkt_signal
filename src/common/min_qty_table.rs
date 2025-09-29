use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use log::{debug, info, warn};
use reqwest::Client;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct MinQtyEntry {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    /// 最小可下单量（minQty）
    pub min_qty: f64,
    /// 数量步进（stepSize）
    pub step_size: f64,
    /// 价格步进（tickSize）
    pub price_tick: Option<f64>,
    /// 名义金额下限（minNotional）
    pub min_notional: Option<f64>,
}

#[derive(Debug)]
pub struct MinQtyTable {
    client: Client,
    spot: HashMap<String, MinQtyEntry>,
    futures_um: HashMap<String, MinQtyEntry>,
    margin: HashMap<String, MinQtyEntry>,
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
            margin: HashMap::new(),
        }
    }

    /// 刷新币安现货/UM永续/保证金的交易对过滤器（minQty/stepSize/tickSize）
    pub async fn refresh_binance(&mut self) -> Result<()> {
        let spot = self
            .fetch_exchange_filters(
                "https://api.binance.com/api/v3/exchangeInfo",
                "binance_spot",
                false,
            )
            .await?;
        let futures = self
            .fetch_exchange_filters(
                "https://fapi.binance.com/fapi/v1/exchangeInfo",
                "binance_um_futures",
                false,
            )
            .await?;
        let margin = match self
            .fetch_exchange_filters(
                "https://api.binance.com/sapi/v1/margin/exchangeInfo",
                "binance_margin",
                true,
            )
            .await
        {
            Ok(m) => m,
            Err(err) => {
                warn!("refresh margin exchange info failed: {err:#}");
                HashMap::new()
            }
        };

        info!(
            "刷新交易对过滤器: spot={} 条目, futures={} 条目, margin={} 条目",
            spot.len(),
            futures.len(),
            margin.len()
        );

        self.spot = spot;
        self.futures_um = futures;
        self.margin = margin;
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

    pub fn spot_step_by_symbol(&self, symbol: &str) -> Option<f64> {
        let key = symbol.to_uppercase();
        self.spot.get(&key).map(|e| e.step_size).filter(|v| *v > 0.0)
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

    pub fn futures_um_step_by_symbol(&self, symbol: &str) -> Option<f64> {
        let key = symbol.to_uppercase();
        self.futures_um
            .get(&key)
            .map(|e| e.step_size)
            .filter(|v| *v > 0.0)
    }

    pub fn margin_min_qty_by_symbol(&self, symbol: &str) -> Option<f64> {
        let key = symbol.to_uppercase();
        self.margin.get(&key).map(|entry| entry.min_qty)
    }

    pub fn margin_price_tick_by_symbol(&self, symbol: &str) -> Option<f64> {
        let key = symbol.to_uppercase();
        self.margin
            .get(&key)
            .and_then(|entry| entry.price_tick)
            .filter(|tick| *tick > 0.0)
    }

    pub fn margin_step_by_symbol(&self, symbol: &str) -> Option<f64> {
        let key = symbol.to_uppercase();
        self.margin.get(&key).map(|e| e.step_size).filter(|v| *v > 0.0)
    }

    pub fn spot_min_notional_by_symbol(&self, symbol: &str) -> Option<f64> {
        let key = symbol.to_uppercase();
        self.spot.get(&key).and_then(|e| e.min_notional).filter(|v| *v > 0.0)
    }

    pub fn margin_min_notional_by_symbol(&self, symbol: &str) -> Option<f64> {
        let key = symbol.to_uppercase();
        self.margin.get(&key).and_then(|e| e.min_notional).filter(|v| *v > 0.0)
    }

    async fn fetch_exchange_filters(
        &self,
        url: &str,
        label: &str,
        with_api_key: bool,
    ) -> Result<HashMap<String, MinQtyEntry>> {
        let mut req = self.client.get(url);

        if with_api_key {
            match std::env::var("BINANCE_API_KEY") {
                Ok(key) if !key.trim().is_empty() => {
                    req = req.header("X-MBX-APIKEY", key.trim());
                }
                _ => {
                    debug!(
                        "{} request without X-MBX-APIKEY (env BINANCE_API_KEY not set)",
                        label
                    );
                }
            }
        }

        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        debug!(
            "GET {} ({}) -> status={} bytes={}",
            url,
            label,
            status.as_u16(),
            body.len()
        );
        if !status.is_success() {
            return Err(anyhow!("GET {} failed: {} - {}", url, status, body));
        }
        if body.trim().is_empty() {
            return Err(anyhow!(
                "GET {} returned empty body (status={})",
                url, status
            ));
        }

        let exchange_info: RawExchangeInfo = serde_json::from_str(&body)
            .with_context(|| format!("failed to parse {} exchange info", label))?;

        let mut map = HashMap::new();
        for raw_symbol in exchange_info.symbols {
            let filters = extract_filter_values(&raw_symbol.filters, &raw_symbol.symbol)?;
            let symbol = raw_symbol.symbol.to_uppercase();
            let entry = MinQtyEntry {
                symbol: symbol.clone(),
                base_asset: raw_symbol.base_asset.to_uppercase(),
                quote_asset: raw_symbol.quote_asset.to_uppercase(),
                min_qty: filters.min_qty.unwrap_or(0.0),
                step_size: filters.step_size.unwrap_or(0.0),
                price_tick: filters.price_tick,
                min_notional: filters
                    .min_notional,
            };
            map.insert(symbol, entry);
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
    #[serde(rename = "minNotional")]
    min_notional: Option<String>,
}

struct SymbolFilterValues {
    min_qty: Option<f64>,
    step_size: Option<f64>,
    price_tick: Option<f64>,
    min_notional: Option<f64>,
}

fn extract_filter_values(
    filters: &[RawExchangeFilter],
    symbol: &str,
) -> Result<SymbolFilterValues> {
    let mut min_qty: Option<f64> = None;
    let mut step_size: Option<f64> = None;
    let mut price_tick: Option<f64> = None;
    let mut min_notional: Option<f64> = None;

    for filter in filters {
        match filter.filter_type.as_str() {
            "LOT_SIZE" => {
                if let Some(value) = &filter.min_qty {
                    min_qty = Some(parse_decimal(value, "minQty", symbol)?);
                }
                if let Some(value) = &filter.step_size {
                    step_size = Some(parse_decimal(value, "stepSize", symbol)?);
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
            "MIN_NOTIONAL" | "NOTIONAL" => {
                if let Some(value) = &filter.min_notional {
                    let v = parse_decimal(value, "minNotional", symbol)?;
                    if v > 0.0 {
                        min_notional = Some(v);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(SymbolFilterValues {
        min_qty,
        step_size,
        price_tick,
        min_notional,
    })
}

fn parse_decimal(value: &str, field: &str, symbol: &str) -> Result<f64> {
    value
        .parse::<f64>()
        .with_context(|| format!("symbol={} field={}", symbol, field))
}
