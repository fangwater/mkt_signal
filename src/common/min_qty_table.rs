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
    pub min_qty: f64,              // 最小可下单量（minQty）
    pub step_size: f64,            // 数量步进（stepSize）
    pub price_tick: Option<f64>,   // 价格步进（tickSize）
    pub min_notional: Option<f64>, // 名义金额下限（minNotional）
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
    fn margin_reuses_spot(&self) -> bool {
        false
    }
}

// ============================================================================
// Binance Provider
// ============================================================================

pub struct BinanceProvider;

impl BinanceProvider {
    pub fn new() -> Self {
        Self
    }

    fn get_api_url(&self, market_type: MarketType) -> &'static str {
        match market_type {
            MarketType::Spot | MarketType::Margin => "https://api.binance.com/api/v3/exchangeInfo",
            MarketType::Futures => "https://fapi.binance.com/fapi/v1/exchangeInfo",
        }
    }

    pub async fn fetch_filters(
        &self,
        client: &Client,
        market_type: MarketType,
    ) -> Result<HashMap<String, MinQtyEntry>> {
        let url = self.get_api_url(market_type);
        let label = match market_type {
            MarketType::Spot => "binance_spot",
            MarketType::Futures => "binance_um_futures",
            MarketType::Margin => "binance_margin",
        };
        let resp = client.get(url).send().await?;
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
                url,
                status
            ));
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
    fn default() -> Self {
        Self::new()
    }
}

impl ExchangeInfoProvider for BinanceProvider {
    fn exchange(&self) -> Exchange {
        Exchange::Binance
    }
    fn supported_market_types(&self) -> Vec<MarketType> {
        vec![MarketType::Spot, MarketType::Futures, MarketType::Margin]
    }
    fn margin_reuses_spot(&self) -> bool {
        true
    }
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
    // Binance UM futures uses `notional` (not `minNotional`) under MIN_NOTIONAL.
    #[serde(rename = "notional")]
    notional: Option<String>,
}

struct BinanceSymbolFilterValues {
    min_qty: Option<f64>,
    step_size: Option<f64>,
    price_tick: Option<f64>,
    min_notional: Option<f64>,
}

fn binance_extract_filter_values(
    filters: &[BinanceRawExchangeFilter],
    symbol: &str,
) -> Result<BinanceSymbolFilterValues> {
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
                let value = filter
                    .min_notional
                    .as_deref()
                    .or(filter.notional.as_deref());
                if let Some(value) = value {
                    let v = parse_decimal(value, "minNotional", symbol)?;
                    if v > 0.0 {
                        min_notional = Some(v);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(BinanceSymbolFilterValues {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binance_min_notional_parses_min_notional_field() {
        let json = serde_json::json!([
            {"filterType":"LOT_SIZE","minQty":"1","stepSize":"1"},
            {"filterType":"MIN_NOTIONAL","minNotional":"5"}
        ]);
        let filters: Vec<BinanceRawExchangeFilter> = serde_json::from_value(json).unwrap();
        let out = binance_extract_filter_values(&filters, "DOGEUSDT").unwrap();
        assert_eq!(out.min_notional, Some(5.0));
    }

    #[test]
    fn binance_min_notional_parses_notional_field_for_futures() {
        let json = serde_json::json!([
            {"filterType":"LOT_SIZE","minQty":"1","stepSize":"1"},
            {"filterType":"MIN_NOTIONAL","notional":"5"}
        ]);
        let filters: Vec<BinanceRawExchangeFilter> = serde_json::from_value(json).unwrap();
        let out = binance_extract_filter_values(&filters, "DOGEUSDT").unwrap();
        assert_eq!(out.min_notional, Some(5.0));
    }

    #[test]
    fn okex_swap_contract_multiplier_is_ct_val_times_ct_mult() {
        let provider = OkexProvider::new();
        let instruments = vec![OkexInstrument {
            inst_id: "FIL-USDT-SWAP".to_string(),
            base_ccy: None,
            quote_ccy: None,
            settle_ccy: Some("USDT".to_string()),
            ct_type: Some("linear".to_string()),
            ct_val: Some("0.1".to_string()),
            ct_mult: Some("1".to_string()),
            lot_sz: "1".to_string(),
            tick_sz: "0.001".to_string(),
            min_sz: "1".to_string(),
        }];

        let (_entries, multipliers) = provider.parse_swap_response(&instruments).unwrap();
        assert_eq!(multipliers.get("FILUSDT").copied(), Some(0.1));
    }

    #[test]
    fn bitget_futures_multiplier_defaults_to_one_in_unified_mode() {
        let provider = BitgetProvider::new();
        let response = BitgetResponse {
            code: "00000".to_string(),
            msg: "success".to_string(),
            data: vec![BitgetInstrument {
                symbol: "BTCUSDT".to_string(),
                base_coin: "BTC".to_string(),
                quote_coin: "USDT".to_string(),
                min_order_qty: "0.001".to_string(),
                price_multiplier: Some("0.1".to_string()),
                quantity_multiplier: Some("0.001".to_string()),
                price_precision: Some("1".to_string()),
                quantity_precision: Some("3".to_string()),
                min_order_amount: Some("5".to_string()),
            }],
        };

        let (_entries, multipliers) = provider
            .parse_response(response, MarketType::Futures)
            .unwrap();
        assert_eq!(multipliers.get("BTCUSDT").copied(), Some(1.0));
    }

    #[test]
    fn bitget_margin_precision_fields_fallback_to_ticks() {
        let provider = BitgetProvider::new();
        let response = BitgetResponse {
            code: "00000".to_string(),
            msg: "success".to_string(),
            data: vec![BitgetInstrument {
                symbol: "BRETTUSDT".to_string(),
                base_coin: "BRETT".to_string(),
                quote_coin: "USDT".to_string(),
                min_order_qty: "0.01".to_string(),
                price_multiplier: None,
                quantity_multiplier: None,
                price_precision: Some("5".to_string()),
                quantity_precision: Some("2".to_string()),
                min_order_amount: Some("1".to_string()),
            }],
        };

        let (entries, multipliers) = provider
            .parse_response(response, MarketType::Margin)
            .unwrap();
        let entry = entries.get("BRETTUSDT").unwrap();

        assert!(multipliers.is_empty());
        assert_eq!(entry.step_size, 0.01);
        assert!((entry.price_tick.unwrap() - 0.00001).abs() < 1e-12);
    }

    #[test]
    fn bybit_linear_multiplier_defaults_to_one_in_unified_mode() {
        let provider = BybitProvider::new();
        let response = BybitInstrumentsResponse {
            ret_code: 0,
            ret_msg: "OK".to_string(),
            result: Some(BybitInstrumentsResult {
                list: vec![BybitInstrument {
                    symbol: "BTCUSDT".to_string(),
                    status: "Trading".to_string(),
                    base_coin: "BTC".to_string(),
                    quote_coin: "USDT".to_string(),
                    contract_type: "LinearPerpetual".to_string(),
                    lot_size_filter: BybitLotSizeFilter {
                        min_order_qty: Some("0.001".to_string()),
                        qty_step: Some("0.001".to_string()),
                        min_notional_value: Some("5".to_string()),
                        ..Default::default()
                    },
                    price_filter: BybitPriceFilter {
                        tick_size: Some("0.1".to_string()),
                    },
                }],
                next_page_cursor: None,
            }),
        };

        let (_entries, multipliers) = provider
            .parse_response_for_test(response, MarketType::Futures)
            .unwrap();
        assert_eq!(multipliers.get("BTCUSDT").copied(), Some(1.0));
    }

    #[test]
    fn bybit_spot_uses_base_precision_and_min_order_amt() {
        let provider = BybitProvider::new();
        let response = BybitInstrumentsResponse {
            ret_code: 0,
            ret_msg: "OK".to_string(),
            result: Some(BybitInstrumentsResult {
                list: vec![BybitInstrument {
                    symbol: "DOGEUSDT".to_string(),
                    status: "Trading".to_string(),
                    base_coin: "DOGE".to_string(),
                    quote_coin: "USDT".to_string(),
                    contract_type: "".to_string(),
                    lot_size_filter: BybitLotSizeFilter {
                        min_order_qty: Some("0.1".to_string()),
                        base_precision: Some("0.1".to_string()),
                        min_order_amt: Some("5".to_string()),
                        ..Default::default()
                    },
                    price_filter: BybitPriceFilter {
                        tick_size: Some("0.00001".to_string()),
                    },
                }],
                next_page_cursor: None,
            }),
        };

        let (entries, multipliers) = provider
            .parse_response_for_test(response, MarketType::Margin)
            .unwrap();
        let entry = entries.get("DOGEUSDT").expect("spot entry present");
        assert!((entry.step_size - 0.1).abs() < 1e-12);
        assert!((entry.min_qty - 0.1).abs() < 1e-12);
        assert!((entry.price_tick.unwrap() - 0.00001).abs() < 1e-12);
        assert!((entry.min_notional.unwrap() - 5.0).abs() < 1e-12);
        assert!(multipliers.is_empty());
    }

    #[test]
    #[should_panic(expected = "lotSizeFilter.basePrecision")]
    fn bybit_spot_panics_when_required_field_missing() {
        let provider = BybitProvider::new();
        let response = BybitInstrumentsResponse {
            ret_code: 0,
            ret_msg: "OK".to_string(),
            result: Some(BybitInstrumentsResult {
                list: vec![BybitInstrument {
                    symbol: "DOGEUSDT".to_string(),
                    status: "Trading".to_string(),
                    base_coin: "DOGE".to_string(),
                    quote_coin: "USDT".to_string(),
                    contract_type: "".to_string(),
                    lot_size_filter: BybitLotSizeFilter {
                        min_order_qty: Some("0.1".to_string()),
                        // basePrecision intentionally absent → must panic
                        min_order_amt: Some("5".to_string()),
                        ..Default::default()
                    },
                    price_filter: BybitPriceFilter {
                        tick_size: Some("0.00001".to_string()),
                    },
                }],
                next_page_cursor: None,
            }),
        };

        let _ = provider.parse_response_for_test(response, MarketType::Margin);
    }
}

// ============================================================================
// Gate Provider
// ============================================================================

pub struct GateProvider;

impl GateProvider {
    pub fn new() -> Self {
        Self
    }

    fn get_api_url(&self, market_type: MarketType) -> &'static str {
        match market_type {
            MarketType::Spot | MarketType::Margin => {
                "https://api.gateio.ws/api/v4/spot/currency_pairs"
            }
            MarketType::Futures => "https://api.gateio.ws/api/v4/futures/usdt/contracts",
        }
    }

    pub async fn fetch_filters(
        &self,
        client: &Client,
        market_type: MarketType,
    ) -> Result<HashMap<String, MinQtyEntry>> {
        let (entries, _) = self
            .fetch_filters_with_multipliers(client, market_type)
            .await?;
        Ok(entries)
    }

    pub async fn fetch_filters_with_multipliers(
        &self,
        client: &Client,
        market_type: MarketType,
    ) -> Result<(HashMap<String, MinQtyEntry>, HashMap<String, f64>)> {
        let url = self.get_api_url(market_type);
        let label = match market_type {
            MarketType::Spot => "gate_spot",
            MarketType::Futures => "gate_futures",
            MarketType::Margin => "gate_margin",
        };
        let resp = client.get(url).send().await?;
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
                url,
                status
            ));
        }
        match market_type {
            MarketType::Spot | MarketType::Margin => {
                let entries = self.parse_spot_response(&body, label)?;
                Ok((entries, HashMap::new()))
            }
            MarketType::Futures => self.parse_futures_response(&body, label),
        }
    }

    fn parse_spot_response(&self, body: &str, label: &str) -> Result<HashMap<String, MinQtyEntry>> {
        let pairs: Vec<GateSpotCurrencyPair> = serde_json::from_str(body)
            .with_context(|| format!("failed to parse {} exchange info", label))?;
        let mut map = HashMap::new();
        for pair in pairs {
            let symbol = pair.id.to_uppercase().replace('_', ""); // ETH_USDT -> ETHUSDT
            let step_size = precision_to_step(pair.amount_precision);
            let price_tick = precision_to_step(pair.precision);
            let entry = MinQtyEntry {
                symbol: symbol.clone(),
                base_asset: pair.base.to_uppercase(),
                quote_asset: pair.quote.to_uppercase(),
                min_qty: pair.min_base_amount.parse().unwrap_or(0.0),
                step_size,
                price_tick: if price_tick > 0.0 {
                    Some(price_tick)
                } else {
                    None
                },
                min_notional: pair
                    .min_quote_amount
                    .and_then(|v| v.parse().ok())
                    .filter(|v| *v > 0.0),
            };
            map.insert(symbol, entry);
        }
        Ok(map)
    }

    fn parse_futures_response(
        &self,
        body: &str,
        label: &str,
    ) -> Result<(HashMap<String, MinQtyEntry>, HashMap<String, f64>)> {
        let contracts: Vec<GateFuturesContract> = serde_json::from_str(body)
            .with_context(|| format!("failed to parse {} exchange info", label))?;
        let mut entries = HashMap::new();
        let mut multipliers = HashMap::new();
        for contract in contracts {
            let symbol = contract.name.to_uppercase().replace('_', ""); // ZEC_USDT -> ZECUSDT
            let (base, quote) = contract
                .name
                .split_once('_')
                .unwrap_or((&contract.name, "USDT"));
            let price_tick: f64 = contract.order_price_round.parse().unwrap_or(0.0);
            let quanto: f64 = contract.quanto_multiplier.parse().unwrap_or(1.0);
            // Gate futures: 下单 size 使用 contracts。
            // 统一口径：
            // - min_qty / step_size 使用 contracts 单位（与 OKX futures 一致）
            // - contract multiplier 记录 base/contract（quanto_multiplier）
            let entry = MinQtyEntry {
                symbol: symbol.clone(),
                base_asset: base.to_uppercase(),
                quote_asset: quote.to_uppercase(),
                min_qty: contract.order_size_min as f64,
                step_size: 1.0,
                price_tick: if price_tick > 0.0 {
                    Some(price_tick)
                } else {
                    None
                },
                min_notional: None,
            };
            entries.insert(symbol.clone(), entry);
            multipliers.insert(symbol, quanto);
        }
        Ok((entries, multipliers))
    }
}

impl Default for GateProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ExchangeInfoProvider for GateProvider {
    fn exchange(&self) -> Exchange {
        Exchange::Gate
    }
    fn supported_market_types(&self) -> Vec<MarketType> {
        vec![MarketType::Spot, MarketType::Futures, MarketType::Margin]
    }
    fn margin_reuses_spot(&self) -> bool {
        true
    }
}

#[derive(Debug, Deserialize)]
struct GateSpotCurrencyPair {
    id: String,    // e.g. "ETH_USDT"
    base: String,  // e.g. "ETH"
    quote: String, // e.g. "USDT"
    #[serde(default)]
    min_base_amount: String, // e.g. "0.001"
    min_quote_amount: Option<String>, // e.g. "1.0"
    #[serde(default)]
    amount_precision: i32, // e.g. 3
    #[serde(default)]
    precision: i32, // e.g. 6
}

#[derive(Debug, Deserialize)]
struct GateFuturesContract {
    name: String,              // e.g. "ZEC_USDT"
    order_size_min: i64,       // e.g. 1
    order_price_round: String, // e.g. "0.01"
    quanto_multiplier: String, // e.g. "0.01"
}

fn precision_to_step(precision: i32) -> f64 {
    if precision >= 0 {
        10_f64.powi(-precision)
    } else {
        0.0
    }
}

// ============================================================================
// OKEx Provider
// ============================================================================

pub struct OkexProvider;

impl OkexProvider {
    pub fn new() -> Self {
        Self
    }

    fn get_api_url(&self, market_type: MarketType) -> &'static str {
        match market_type {
            MarketType::Spot => "https://openapi.okx.com/api/v5/public/instruments?instType=SPOT",
            MarketType::Margin => "https://openapi.okx.com/api/v5/public/instruments?instType=MARGIN",
            MarketType::Futures => "https://openapi.okx.com/api/v5/public/instruments?instType=SWAP",
        }
    }

    pub async fn fetch_filters(
        &self,
        client: &Client,
        market_type: MarketType,
    ) -> Result<HashMap<String, MinQtyEntry>> {
        let (entries, _) = self
            .fetch_filters_with_multipliers(client, market_type)
            .await?;
        Ok(entries)
    }

    pub async fn fetch_filters_with_multipliers(
        &self,
        client: &Client,
        market_type: MarketType,
    ) -> Result<(HashMap<String, MinQtyEntry>, HashMap<String, f64>)> {
        let url = self.get_api_url(market_type);
        let label = match market_type {
            MarketType::Spot => "okex_spot",
            MarketType::Futures => "okex_swap",
            MarketType::Margin => "okex_margin",
        };
        let resp = client.get(url).send().await?;
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
        let response: OkexResponse = serde_json::from_str(&body)
            .with_context(|| format!("failed to parse {} response", label))?;
        if response.code != "0" {
            return Err(anyhow!(
                "OKEx API error: {} - {}",
                response.code,
                response.msg
            ));
        }
        match market_type {
            MarketType::Spot | MarketType::Margin => self.parse_spot_response(&response.data),
            MarketType::Futures => self.parse_swap_response(&response.data),
        }
    }

    fn parse_spot_response(
        &self,
        data: &[OkexInstrument],
    ) -> Result<(HashMap<String, MinQtyEntry>, HashMap<String, f64>)> {
        let mut entries = HashMap::new();
        for inst in data {
            if !inst.inst_id.ends_with("-USDT") {
                continue;
            }
            let symbol = inst.inst_id.replace('-', ""); // BTC-USDT -> BTCUSDT
            let entry = MinQtyEntry {
                symbol: symbol.clone(),
                base_asset: inst.base_ccy.clone().unwrap_or_default().to_uppercase(),
                quote_asset: inst.quote_ccy.clone().unwrap_or_default().to_uppercase(),
                min_qty: inst.min_sz.parse().unwrap_or(0.0),
                step_size: inst.lot_sz.parse().unwrap_or(0.0),
                price_tick: inst.tick_sz.parse().ok().filter(|v| *v > 0.0),
                min_notional: None,
            };
            entries.insert(symbol, entry);
        }
        Ok((entries, HashMap::new()))
    }

    fn parse_swap_response(
        &self,
        data: &[OkexInstrument],
    ) -> Result<(HashMap<String, MinQtyEntry>, HashMap<String, f64>)> {
        let mut entries = HashMap::new();
        let mut multipliers = HashMap::new();
        for inst in data {
            // 只要 USDT 本位正向合约
            if inst.ct_type.as_deref() != Some("linear") {
                continue;
            }
            if inst.settle_ccy.as_deref() != Some("USDT") {
                continue;
            }
            // BTC-USDT-SWAP -> BTCUSDT
            let symbol = inst.inst_id.replace("-SWAP", "").replace('-', "");

            // 计算合约面值 = ctVal × ctMult
            // 对于 USDT 永续合约，通常 ctVal 是实际面值（如 0.01 BTC），ctMult = 1
            let ct_val: f64 = inst
                .ct_val
                .as_deref()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1.0);
            let ct_mult: f64 = inst
                .ct_mult
                .as_deref()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1.0);
            let contract_size = ct_val * ct_mult;

            let entry = MinQtyEntry {
                symbol: symbol.clone(),
                base_asset: inst.inst_id.split('-').next().unwrap_or("").to_uppercase(),
                quote_asset: "USDT".to_string(),
                min_qty: inst.min_sz.parse().unwrap_or(0.0),
                step_size: inst.lot_sz.parse().unwrap_or(0.0),
                price_tick: inst.tick_sz.parse().ok().filter(|v| *v > 0.0),
                min_notional: None,
            };
            entries.insert(symbol.clone(), entry);
            multipliers.insert(symbol, contract_size);
        }
        Ok((entries, multipliers))
    }
}

impl Default for OkexProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ExchangeInfoProvider for OkexProvider {
    fn exchange(&self) -> Exchange {
        Exchange::Okex
    }
    fn supported_market_types(&self) -> Vec<MarketType> {
        vec![MarketType::Spot, MarketType::Futures, MarketType::Margin]
    }
    fn margin_reuses_spot(&self) -> bool {
        false
    }
}

#[derive(Debug, Deserialize)]
struct OkexResponse {
    code: String,
    msg: String,
    data: Vec<OkexInstrument>,
}

#[derive(Debug, Deserialize)]
struct OkexInstrument {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "baseCcy")]
    base_ccy: Option<String>,
    #[serde(rename = "quoteCcy")]
    quote_ccy: Option<String>,
    #[serde(rename = "settleCcy")]
    settle_ccy: Option<String>,
    #[serde(rename = "ctType")]
    ct_type: Option<String>,
    #[serde(rename = "ctVal")]
    ct_val: Option<String>,
    #[serde(rename = "ctMult")]
    ct_mult: Option<String>,
    #[serde(rename = "lotSz")]
    lot_sz: String,
    #[serde(rename = "tickSz")]
    tick_sz: String,
    #[serde(rename = "minSz")]
    min_sz: String,
}

// ============================================================================
// Bybit Provider
// ============================================================================

pub struct BybitProvider;

impl BybitProvider {
    pub fn new() -> Self {
        Self
    }

    fn get_api_url(&self, market_type: MarketType) -> &'static str {
        match market_type {
            MarketType::Spot => "https://api.bybit.com/v5/market/instruments-info?category=spot",
            MarketType::Margin => "https://api.bybit.com/v5/market/instruments-info?category=spot",
            MarketType::Futures => {
                "https://api.bybit.com/v5/market/instruments-info?category=linear"
            }
        }
    }

    pub async fn fetch_filters(
        &self,
        client: &Client,
        market_type: MarketType,
    ) -> Result<HashMap<String, MinQtyEntry>> {
        let (entries, _) = self
            .fetch_filters_with_multipliers(client, market_type)
            .await?;
        Ok(entries)
    }

    pub async fn fetch_filters_with_multipliers(
        &self,
        client: &Client,
        market_type: MarketType,
    ) -> Result<(HashMap<String, MinQtyEntry>, HashMap<String, f64>)> {
        let url = self.get_api_url(market_type);
        let label = match market_type {
            MarketType::Spot => "bybit_spot",
            MarketType::Futures => "bybit_linear",
            MarketType::Margin => "bybit_margin",
        };
        let mut all_entries = HashMap::new();
        let mut all_multipliers = HashMap::new();
        let mut cursor: Option<String> = None;
        let mut page = 0usize;

        loop {
            page += 1;
            if page > 20 {
                return Err(anyhow!(
                    "Bybit instruments pagination exceeded safety limit for {}",
                    label
                ));
            }

            let mut req = client.get(url);
            if market_type == MarketType::Futures {
                req = req.query(&[("limit", "1000")]);
            }
            if let Some(cursor) = cursor.as_deref() {
                req = req.query(&[("cursor", cursor)]);
            }

            let resp = req.send().await?;
            let status = resp.status();
            let body = resp.text().await?;
            debug!(
                "GET {} ({}) page={} cursor={:?} -> status={} bytes={}",
                url,
                label,
                page,
                cursor,
                status.as_u16(),
                body.len()
            );
            if !status.is_success() {
                return Err(anyhow!("GET {} failed: {} - {}", url, status, body));
            }

            let response: BybitInstrumentsResponse = serde_json::from_str(&body)
                .with_context(|| format!("failed to parse {} response", label))?;
            let next_cursor = response
                .result
                .as_ref()
                .and_then(|result| result.next_page_cursor.as_deref())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
            let (entries, multipliers) = self.parse_response(response, market_type)?;
            all_entries.extend(entries);
            all_multipliers.extend(multipliers);

            if next_cursor.is_none() {
                break;
            }
            cursor = next_cursor;
        }

        Ok((all_entries, all_multipliers))
    }

    fn parse_response(
        &self,
        response: BybitInstrumentsResponse,
        market_type: MarketType,
    ) -> Result<(HashMap<String, MinQtyEntry>, HashMap<String, f64>)> {
        if response.ret_code != 0 {
            return Err(anyhow!(
                "Bybit API error: {} - {}",
                response.ret_code,
                response.ret_msg
            ));
        }

        let result = response
            .result
            .ok_or_else(|| anyhow!("Bybit response missing result"))?;

        let mut entries = HashMap::new();
        let mut multipliers = HashMap::new();

        for inst in result.list {
            if !inst.status.eq_ignore_ascii_case("trading") {
                continue;
            }
            if !inst.quote_coin.eq_ignore_ascii_case("USDT") {
                continue;
            }

            if market_type == MarketType::Futures
                && !inst.contract_type.eq_ignore_ascii_case("LinearPerpetual")
            {
                continue;
            }

            let symbol = inst.symbol.to_uppercase();
            let lot = &inst.lot_size_filter;

            // Per-market-type required-field map (panic loudly so we notice when
            // Bybit changes the schema instead of silently degrading to step_size=0).
            //   spot/margin (category=spot)   → basePrecision, minOrderAmt
            //   futures   (category=linear)   → qtyStep,       minNotionalValue
            // minOrderQty / tickSize are required on every category.
            let (step_field_name, step_raw, notional_field_name, notional_raw) = match market_type {
                MarketType::Spot | MarketType::Margin => (
                    "lotSizeFilter.basePrecision",
                    lot.base_precision.as_deref(),
                    "lotSizeFilter.minOrderAmt",
                    lot.min_order_amt.as_deref(),
                ),
                MarketType::Futures => (
                    "lotSizeFilter.qtyStep",
                    lot.qty_step.as_deref(),
                    "lotSizeFilter.minNotionalValue",
                    lot.min_notional_value.as_deref(),
                ),
            };

            let min_qty = lot
                .min_order_qty
                .as_deref()
                .and_then(|v| v.parse::<f64>().ok())
                .filter(|v| *v > 0.0)
                .unwrap_or_else(|| {
                    panic!(
                        "Bybit {:?} {}: required field lotSizeFilter.minOrderQty missing or unparseable (raw={:?}); exchange API may have changed",
                        market_type, symbol, lot.min_order_qty
                    )
                });

            let step_size = step_raw
                .and_then(|v| v.parse::<f64>().ok())
                .filter(|v| *v > 0.0)
                .unwrap_or_else(|| {
                    panic!(
                        "Bybit {:?} {}: required field {} missing or unparseable (raw={:?}); exchange API may have changed",
                        market_type, symbol, step_field_name, step_raw
                    )
                });

            let price_tick = Some(
                inst.price_filter
                    .tick_size
                    .as_deref()
                    .and_then(|v| v.parse::<f64>().ok())
                    .filter(|v| *v > 0.0)
                    .unwrap_or_else(|| {
                        panic!(
                            "Bybit {:?} {}: required field priceFilter.tickSize missing or unparseable (raw={:?}); exchange API may have changed",
                            market_type, symbol, inst.price_filter.tick_size
                        )
                    }),
            );

            let min_notional = Some(
                notional_raw
                    .and_then(|v| v.parse::<f64>().ok())
                    .filter(|v| *v > 0.0)
                    .unwrap_or_else(|| {
                        panic!(
                            "Bybit {:?} {}: required field {} missing or unparseable (raw={:?}); exchange API may have changed",
                            market_type, symbol, notional_field_name, notional_raw
                        )
                    }),
            );

            let entry = MinQtyEntry {
                symbol: symbol.clone(),
                base_asset: inst.base_coin.to_uppercase(),
                quote_asset: inst.quote_coin.to_uppercase(),
                min_qty,
                step_size,
                price_tick,
                min_notional,
            };
            entries.insert(symbol.clone(), entry);

            if market_type == MarketType::Futures {
                // Bybit U本位线性永续（统一账户）按 base qty 口径处理，乘数固定 1。
                multipliers.insert(symbol, 1.0);
            }
        }
        Ok((entries, multipliers))
    }

    #[cfg(test)]
    fn parse_response_for_test(
        &self,
        response: BybitInstrumentsResponse,
        market_type: MarketType,
    ) -> Result<(HashMap<String, MinQtyEntry>, HashMap<String, f64>)> {
        self.parse_response(response, market_type)
    }
}

impl Default for BybitProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ExchangeInfoProvider for BybitProvider {
    fn exchange(&self) -> Exchange {
        Exchange::Bybit
    }
    fn supported_market_types(&self) -> Vec<MarketType> {
        vec![MarketType::Spot, MarketType::Futures, MarketType::Margin]
    }
    fn margin_reuses_spot(&self) -> bool {
        true
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitInstrumentsResponse {
    ret_code: i32,
    ret_msg: String,
    result: Option<BybitInstrumentsResult>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitInstrumentsResult {
    #[serde(default)]
    list: Vec<BybitInstrument>,
    #[serde(default)]
    next_page_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitInstrument {
    symbol: String,
    status: String,
    #[serde(default)]
    base_coin: String,
    #[serde(default)]
    quote_coin: String,
    #[serde(default)]
    contract_type: String,
    #[serde(default)]
    lot_size_filter: BybitLotSizeFilter,
    #[serde(default)]
    price_filter: BybitPriceFilter,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitLotSizeFilter {
    #[serde(default)]
    min_order_qty: Option<String>,
    #[serde(default)]
    qty_step: Option<String>,
    #[serde(default)]
    min_notional_value: Option<String>,
    #[serde(default)]
    base_precision: Option<String>,
    #[serde(default)]
    min_order_amt: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitPriceFilter {
    #[serde(default)]
    tick_size: Option<String>,
}

// ============================================================================
// Bitget Provider
// ============================================================================

pub struct BitgetProvider;

impl BitgetProvider {
    pub fn new() -> Self {
        Self
    }

    fn get_api_url(&self, market_type: MarketType) -> &'static str {
        match market_type {
            MarketType::Spot => "https://api.bitget.com/api/v3/market/instruments?category=SPOT",
            MarketType::Margin => {
                "https://api.bitget.com/api/v3/market/instruments?category=MARGIN"
            }
            MarketType::Futures => {
                "https://api.bitget.com/api/v3/market/instruments?category=USDT-FUTURES"
            }
        }
    }

    pub async fn fetch_filters(
        &self,
        client: &Client,
        market_type: MarketType,
    ) -> Result<HashMap<String, MinQtyEntry>> {
        let (entries, _) = self
            .fetch_filters_with_multipliers(client, market_type)
            .await?;
        Ok(entries)
    }

    pub async fn fetch_filters_with_multipliers(
        &self,
        client: &Client,
        market_type: MarketType,
    ) -> Result<(HashMap<String, MinQtyEntry>, HashMap<String, f64>)> {
        let url = self.get_api_url(market_type);
        let label = match market_type {
            MarketType::Spot => "bitget_spot",
            MarketType::Futures => "bitget_futures",
            MarketType::Margin => "bitget_margin",
        };
        let resp = client.get(url).send().await?;
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
        let response: BitgetResponse = serde_json::from_str(&body)
            .with_context(|| format!("failed to parse {} response", label))?;
        self.parse_response(response, market_type)
    }

    fn parse_response(
        &self,
        response: BitgetResponse,
        market_type: MarketType,
    ) -> Result<(HashMap<String, MinQtyEntry>, HashMap<String, f64>)> {
        if response.code != "00000" {
            return Err(anyhow!(
                "Bitget API error: {} - {}",
                response.code,
                response.msg
            ));
        }
        let mut map = HashMap::new();
        let mut multipliers = HashMap::new();
        for inst in response.data {
            let symbol = inst.symbol.to_uppercase();
            let step_size = parse_positive_decimal(inst.quantity_multiplier.as_deref())
                .or_else(|| precision_field_to_step(inst.quantity_precision.as_deref()))
                .unwrap_or(0.0);
            let price_tick = parse_positive_decimal(inst.price_multiplier.as_deref())
                .or_else(|| precision_field_to_step(inst.price_precision.as_deref()))
                .unwrap_or(0.0);
            let entry = MinQtyEntry {
                symbol: symbol.clone(),
                base_asset: inst.base_coin.to_uppercase(),
                quote_asset: inst.quote_coin.to_uppercase(),
                min_qty: inst.min_order_qty.parse().unwrap_or(0.0),
                step_size,
                price_tick: if price_tick > 0.0 {
                    Some(price_tick)
                } else {
                    None
                },
                min_notional: inst
                    .min_order_amount
                    .and_then(|v| v.parse().ok())
                    .filter(|v| *v > 0.0),
            };
            if market_type == MarketType::Futures {
                // 统一账户 USDT-FUTURES: qty 使用 base coin 口径，合约乘数按 1.0 处理。
                multipliers.insert(symbol.clone(), 1.0);
            }
            map.insert(symbol, entry);
        }
        Ok((map, multipliers))
    }
}

impl Default for BitgetProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ExchangeInfoProvider for BitgetProvider {
    fn exchange(&self) -> Exchange {
        Exchange::Bitget
    }
    fn supported_market_types(&self) -> Vec<MarketType> {
        vec![MarketType::Spot, MarketType::Futures, MarketType::Margin]
    }
    fn margin_reuses_spot(&self) -> bool {
        false
    }
}

#[derive(Debug, Deserialize)]
struct BitgetResponse {
    code: String,
    msg: String,
    data: Vec<BitgetInstrument>,
}

#[derive(Debug, Deserialize)]
struct BitgetInstrument {
    symbol: String,
    #[serde(rename = "baseCoin")]
    base_coin: String,
    #[serde(rename = "quoteCoin")]
    quote_coin: String,
    #[serde(rename = "minOrderQty")]
    min_order_qty: String,
    #[serde(rename = "priceMultiplier", default)]
    price_multiplier: Option<String>,
    #[serde(rename = "quantityMultiplier", default)]
    quantity_multiplier: Option<String>,
    #[serde(rename = "pricePrecision", default)]
    price_precision: Option<String>,
    #[serde(rename = "quantityPrecision", default)]
    quantity_precision: Option<String>,
    #[serde(rename = "minOrderAmount", default)]
    min_order_amount: Option<String>,
}

fn parse_positive_decimal(value: Option<&str>) -> Option<f64> {
    value
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| *v > 0.0)
}

fn precision_field_to_step(value: Option<&str>) -> Option<f64> {
    value
        .and_then(|v| v.parse::<i32>().ok())
        .map(precision_to_step)
        .filter(|v| *v > 0.0)
}

// ============================================================================
// MinQtyTable - Single Exchange Instance
// ============================================================================

type FilterStorage = HashMap<MarketType, HashMap<String, MinQtyEntry>>;
type ContractMultiplierStorage = HashMap<String, f64>; // symbol -> multiplier

#[derive(Debug)]
pub struct MinQtyTable {
    client: Client,
    exchange: Exchange,
    filters: FilterStorage,
    contract_multipliers: ContractMultiplierStorage,
}

impl MinQtyTable {
    pub fn new(exchange: Exchange) -> Self {
        Self {
            client: Client::new(),
            exchange,
            filters: HashMap::new(),
            contract_multipliers: HashMap::new(),
        }
    }

    pub fn exchange(&self) -> Exchange {
        self.exchange
    }

    /// Refresh exchange filters
    pub async fn refresh(&mut self) -> Result<()> {
        match self.exchange {
            Exchange::Binance => self.refresh_binance().await,
            Exchange::Gate => self.refresh_gate().await,
            Exchange::Bitget => self.refresh_bitget().await,
            Exchange::Okex => self.refresh_okex().await,
            Exchange::Bybit => self.refresh_bybit().await,
            Exchange::Hyperliquid | Exchange::Aster => {
                Err(anyhow!("exchange {} not supported yet", self.exchange))
            }
        }
    }

    async fn refresh_binance(&mut self) -> Result<()> {
        let provider = BinanceProvider::new();
        for market_type in provider.supported_market_types() {
            if market_type == MarketType::Margin && provider.margin_reuses_spot() {
                if let Some(spot_data) = self.filters.get(&MarketType::Spot).cloned() {
                    debug!("reuse spot filters for {}_margin", self.exchange);
                    self.filters.insert(MarketType::Margin, spot_data);
                    continue;
                }
            }
            let data = provider.fetch_filters(&self.client, market_type).await?;
            info!(
                "刷新交易对过滤器: exchange={} market_type={:?} count={}",
                self.exchange,
                market_type,
                data.len()
            );
            self.filters.insert(market_type, data);
        }
        Ok(())
    }

    async fn refresh_gate(&mut self) -> Result<()> {
        let provider = GateProvider::new();
        for market_type in provider.supported_market_types() {
            if market_type == MarketType::Margin && provider.margin_reuses_spot() {
                if let Some(spot_data) = self.filters.get(&MarketType::Spot).cloned() {
                    debug!("reuse spot filters for {}_margin", self.exchange);
                    self.filters.insert(MarketType::Margin, spot_data);
                    continue;
                }
            }
            let (data, multipliers) = provider
                .fetch_filters_with_multipliers(&self.client, market_type)
                .await?;
            info!(
                "刷新交易对过滤器: exchange={} market_type={:?} count={}",
                self.exchange,
                market_type,
                data.len()
            );
            self.filters.insert(market_type, data);
            if !multipliers.is_empty() {
                self.contract_multipliers.extend(multipliers);
            }
        }
        Ok(())
    }

    async fn refresh_bitget(&mut self) -> Result<()> {
        let provider = BitgetProvider::new();
        for market_type in provider.supported_market_types() {
            let (data, multipliers) = provider
                .fetch_filters_with_multipliers(&self.client, market_type)
                .await?;
            info!(
                "刷新交易对过滤器: exchange={} market_type={:?} count={}",
                self.exchange,
                market_type,
                data.len()
            );
            self.filters.insert(market_type, data);
            if !multipliers.is_empty() {
                self.contract_multipliers.extend(multipliers);
            }
        }
        Ok(())
    }

    async fn refresh_okex(&mut self) -> Result<()> {
        let provider = OkexProvider::new();
        for market_type in provider.supported_market_types() {
            let (data, multipliers) = provider
                .fetch_filters_with_multipliers(&self.client, market_type)
                .await?;
            info!(
                "刷新交易对过滤器: exchange={} market_type={:?} count={}",
                self.exchange,
                market_type,
                data.len()
            );
            self.filters.insert(market_type, data);
            if !multipliers.is_empty() {
                self.contract_multipliers.extend(multipliers);
            }
        }
        Ok(())
    }

    async fn refresh_bybit(&mut self) -> Result<()> {
        let provider = BybitProvider::new();
        for market_type in provider.supported_market_types() {
            if market_type == MarketType::Margin && provider.margin_reuses_spot() {
                if let Some(spot_data) = self.filters.get(&MarketType::Spot).cloned() {
                    debug!("reuse spot filters for {}_margin", self.exchange);
                    self.filters.insert(MarketType::Margin, spot_data);
                    continue;
                }
            }

            let (data, multipliers) = provider
                .fetch_filters_with_multipliers(&self.client, market_type)
                .await?;
            info!(
                "刷新交易对过滤器: exchange={} market_type={:?} count={}",
                self.exchange,
                market_type,
                data.len()
            );
            self.filters.insert(market_type, data);
            if !multipliers.is_empty() {
                self.contract_multipliers.extend(multipliers);
            }
        }
        Ok(())
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
        self.get_entry(market_type, symbol)
            .map(|e| e.step_size)
            .filter(|v| *v > 0.0)
    }
    pub fn price_tick(&self, market_type: MarketType, symbol: &str) -> Option<f64> {
        self.get_entry(market_type, symbol)
            .and_then(|e| e.price_tick)
            .filter(|v| *v > 0.0)
    }
    pub fn min_notional(&self, market_type: MarketType, symbol: &str) -> Option<f64> {
        self.get_entry(market_type, symbol)
            .and_then(|e| e.min_notional)
            .filter(|v| *v > 0.0)
    }

    pub fn contract_multiplier(&self, symbol: &str) -> f64 {
        match self.exchange {
            Exchange::Binance | Exchange::Aster => 1.0, // Binance/Aster UM 合约面值默认为 1
            Exchange::Gate | Exchange::Okex | Exchange::Bitget | Exchange::Bybit => {
                let key = symbol.to_uppercase();
                *self.contract_multipliers.get(&key).unwrap_or(&1.0)
            }
            Exchange::Hyperliquid => 1.0,
        }
    }
}
