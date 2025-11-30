use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use log::{debug, info};
use reqwest::Client;
use serde::Deserialize;

use super::exchange::Exchange;

// ============================================================================
// Core Data Structures
// ============================================================================

#[derive(Debug, Clone)]
pub struct MinQtyEntry {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub min_qty: f64,         // 最小可下单量（minQty）
    pub step_size: f64,       // 数量步进（stepSize）
    pub price_tick: Option<f64>,    // 价格步进（tickSize）
    pub min_notional: Option<f64>,  // 名义金额下限（minNotional）
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketType {
    Spot,
    Futures,
    Margin,
}

// ============================================================================
// ExchangeInfoProvider Trait
// ============================================================================

/// Trait for exchange info providers.
pub trait ExchangeInfoProvider {
    fn exchange(&self) -> Exchange;
    fn supported_market_types(&self) -> Vec<MarketType>;
    fn margin_reuses_spot(&self) -> bool { false }
}

// ============================================================================
// Binance Provider
// ============================================================================

pub struct BinanceProvider;

impl BinanceProvider {
    pub fn new() -> Self { Self }

    fn get_api_url(&self, market_type: MarketType) -> &'static str {
        match market_type {
            MarketType::Spot | MarketType::Margin => "https://api.binance.com/api/v3/exchangeInfo",
            MarketType::Futures => "https://fapi.binance.com/fapi/v1/exchangeInfo",
        }
    }

    pub async fn fetch_filters(&self, client: &Client, market_type: MarketType) -> Result<HashMap<String, MinQtyEntry>> {
        let url = self.get_api_url(market_type);
        let label = match market_type {
            MarketType::Spot => "binance_spot",
            MarketType::Futures => "binance_um_futures",
            MarketType::Margin => "binance_margin",
        };
        let resp = client.get(url).send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        debug!("GET {} ({}) -> status={} bytes={}", url, label, status.as_u16(), body.len());
        if !status.is_success() {
            return Err(anyhow!("GET {} failed: {} - {}", url, status, body));
        }
        if body.trim().is_empty() {
            return Err(anyhow!("GET {} returned empty body (status={})", url, status));
        }
        let exchange_info: BinanceRawExchangeInfo = serde_json::from_str(&body)
            .with_context(|| format!("failed to parse {} exchange info", label))?;
        let mut map = HashMap::new();
        for raw_symbol in exchange_info.symbols {
            let filters = binance_extract_filter_values(&raw_symbol.filters, &raw_symbol.symbol)?;
            let symbol = raw_symbol.symbol.to_uppercase();
            let entry = MinQtyEntry {
                symbol: symbol.clone(),
                base_asset: raw_symbol.base_asset.to_uppercase(),
                quote_asset: raw_symbol.quote_asset.to_uppercase(),
                min_qty: filters.min_qty.unwrap_or(0.0),
                step_size: filters.step_size.unwrap_or(0.0),
                price_tick: filters.price_tick,
                min_notional: filters.min_notional,
            };
            map.insert(symbol, entry);
        }
        Ok(map)
    }
}

impl Default for BinanceProvider {
    fn default() -> Self { Self::new() }
}

impl ExchangeInfoProvider for BinanceProvider {
    fn exchange(&self) -> Exchange { Exchange::Binance }
    fn supported_market_types(&self) -> Vec<MarketType> {
        vec![MarketType::Spot, MarketType::Futures, MarketType::Margin]
    }
    fn margin_reuses_spot(&self) -> bool { true }
}

// Binance-specific raw data structures
#[derive(Debug, Deserialize)]
struct BinanceRawExchangeInfo {
    #[serde(rename = "symbols")]
    symbols: Vec<BinanceRawExchangeSymbol>,
}

#[derive(Debug, Deserialize)]
struct BinanceRawExchangeSymbol {
    #[serde(rename = "symbol")]
    symbol: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    #[serde(default, rename = "status")]
    _status: String,
    #[serde(default, rename = "filters")]
    filters: Vec<BinanceRawExchangeFilter>,
}

#[derive(Debug, Deserialize)]
struct BinanceRawExchangeFilter {
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

struct BinanceSymbolFilterValues {
    min_qty: Option<f64>,
    step_size: Option<f64>,
    price_tick: Option<f64>,
    min_notional: Option<f64>,
}

fn binance_extract_filter_values(filters: &[BinanceRawExchangeFilter], symbol: &str) -> Result<BinanceSymbolFilterValues> {
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
                    if tick > 0.0 { price_tick = Some(tick); }
                }
            }
            "MIN_NOTIONAL" | "NOTIONAL" => {
                if let Some(value) = &filter.min_notional {
                    let v = parse_decimal(value, "minNotional", symbol)?;
                    if v > 0.0 { min_notional = Some(v); }
                }
            }
            _ => {}
        }
    }
    Ok(BinanceSymbolFilterValues { min_qty, step_size, price_tick, min_notional })
}

fn parse_decimal(value: &str, field: &str, symbol: &str) -> Result<f64> {
    value.parse::<f64>().with_context(|| format!("symbol={} field={}", symbol, field))
}

// ============================================================================
// MinQtyTable - Single Exchange Instance
// ============================================================================

type FilterStorage = HashMap<MarketType, HashMap<String, MinQtyEntry>>;

#[derive(Debug)]
pub struct MinQtyTable {
    client: Client,
    exchange: Exchange,
    filters: FilterStorage,
}

impl MinQtyTable {
    pub fn new(exchange: Exchange) -> Self {
        Self { client: Client::new(), exchange, filters: HashMap::new() }
    }

    pub fn exchange(&self) -> Exchange { self.exchange }

    /// Refresh exchange filters
    pub async fn refresh(&mut self) -> Result<()> {
        match self.exchange {
            Exchange::Binance | Exchange::BinanceFutures => {
                let provider = BinanceProvider::new();
                for market_type in provider.supported_market_types() {
                    if market_type == MarketType::Margin && provider.margin_reuses_spot() {
                        if let Some(spot_data) = self.filters.get(&MarketType::Spot).cloned() {
                            debug!("reuse spot exchange filters for {}_margin", self.exchange);
                            self.filters.insert(MarketType::Margin, spot_data);
                            continue;
                        }
                    }
                    let data = provider.fetch_filters(&self.client, market_type).await?;
                    info!("刷新交易对过滤器: exchange={} market_type={:?} count={}", self.exchange, market_type, data.len());
                    self.filters.insert(market_type, data);
                }
                Ok(())
            }
            // TODO: Add other exchanges
            _ => Err(anyhow!("exchange {} not supported yet", self.exchange)),
        }
    }

    // Query Methods
    pub fn get_entry(&self, market_type: MarketType, symbol: &str) -> Option<&MinQtyEntry> {
        let key = symbol.to_uppercase();
        self.filters.get(&market_type).and_then(|m| m.get(&key))
    }
    pub fn min_qty(&self, market_type: MarketType, symbol: &str) -> Option<f64> {
        self.get_entry(market_type, symbol).map(|e| e.min_qty)
    }
    pub fn step_size(&self, market_type: MarketType, symbol: &str) -> Option<f64> {
        self.get_entry(market_type, symbol).map(|e| e.step_size).filter(|v| *v > 0.0)
    }
    pub fn price_tick(&self, market_type: MarketType, symbol: &str) -> Option<f64> {
        self.get_entry(market_type, symbol).and_then(|e| e.price_tick).filter(|v| *v > 0.0)
    }
    pub fn min_notional(&self, market_type: MarketType, symbol: &str) -> Option<f64> {
        self.get_entry(market_type, symbol).and_then(|e| e.min_notional).filter(|v| *v > 0.0)
    }

}
