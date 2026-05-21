use crate::signal::common::TradingVenue;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fmt;
use std::time::Duration;

type HmacSha256 = Hmac<Sha256>;

const BINANCE_PERPETUAL_DEFAULT_DELIVERY_MS: i64 = 4_133_404_800_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelistMarket {
    Spot,
    Futures,
}

impl fmt::Display for DelistMarket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DelistMarket::Spot => write!(f, "spot"),
            DelistMarket::Futures => write!(f, "futures"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DelistEvent {
    pub venue: TradingVenue,
    pub market: DelistMarket,
    pub symbol: String,
    pub delist_time: Option<DateTime<Utc>>,
    pub risk_type: &'static str,
    pub source: &'static str,
    pub status: Option<String>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DelistScheduleQuery {
    pub now: DateTime<Utc>,
    pub window: ChronoDuration,
}

impl DelistScheduleQuery {
    pub fn next_days(days: i64) -> Self {
        Self {
            now: Utc::now(),
            window: ChronoDuration::days(days),
        }
    }

    fn contains(&self, ts: DateTime<Utc>) -> bool {
        ts >= self.now && ts <= self.now + self.window
    }
}

#[async_trait]
pub trait DelistScheduleProvider: Send + Sync {
    fn venue(&self) -> TradingVenue;

    async fn future_delist_events(&self, query: &DelistScheduleQuery) -> Result<Vec<DelistEvent>>;
}

pub fn provider_for_venue(venue: TradingVenue) -> Box<dyn DelistScheduleProvider> {
    match venue {
        TradingVenue::BinanceMargin => Box::new(BinanceSpotDelistProvider::default()),
        TradingVenue::BinanceFutures => Box::new(BinanceFuturesDelistProvider::default()),
        TradingVenue::AsterFutures => Box::new(AsterFuturesDelistProvider::default()),
        TradingVenue::OkexMargin => Box::new(OkxSpotDelistProvider::default()),
        TradingVenue::OkexFutures => Box::new(OkxSwapDelistProvider::default()),
        TradingVenue::BybitFutures => Box::new(BybitLinearDelistProvider::default()),
        TradingVenue::BitgetMargin => Box::new(BitgetSpotDelistProvider::default()),
        TradingVenue::BitgetFutures => Box::new(BitgetUsdtFuturesDelistProvider::default()),
        TradingVenue::GateMargin => Box::new(GateSpotRiskProvider::default()),
        TradingVenue::GateFutures => Box::new(GateFuturesDelistProvider::default()),
        TradingVenue::BybitMargin
        | TradingVenue::AsterMargin
        | TradingVenue::HyperliquidMargin
        | TradingVenue::HyperliquidFutures => Box::new(UnsupportedDelistProvider { venue }),
    }
}

pub fn default_monitored_venues() -> Vec<TradingVenue> {
    vec![
        TradingVenue::BinanceMargin,
        TradingVenue::BinanceFutures,
        TradingVenue::OkexMargin,
        TradingVenue::OkexFutures,
        TradingVenue::BybitFutures,
        TradingVenue::BitgetMargin,
        TradingVenue::BitgetFutures,
        TradingVenue::GateMargin,
        TradingVenue::GateFutures,
        TradingVenue::AsterFutures,
    ]
}

fn http_client() -> Result<Client> {
    Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("build delist schedule HTTP client failed")
}

fn datetime_from_millis(ms: i64, field: &str) -> Result<DateTime<Utc>> {
    Utc.timestamp_millis_opt(ms)
        .single()
        .ok_or_else(|| anyhow!("invalid {field} millis timestamp: {ms}"))
}

fn parse_millis_str(raw: &str, field: &str) -> Result<Option<DateTime<Utc>>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "0" {
        return Ok(None);
    }
    let ms = trimmed
        .parse::<i64>()
        .with_context(|| format!("parse {field} millis timestamp: {trimmed}"))?;
    Ok(Some(datetime_from_millis(ms, field)?))
}

fn sign_binance_query(query: &str, secret: &str) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| anyhow!("invalid binance secret"))?;
    mac.update(query.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

struct UnsupportedDelistProvider {
    venue: TradingVenue,
}

#[async_trait]
impl DelistScheduleProvider for UnsupportedDelistProvider {
    fn venue(&self) -> TradingVenue {
        self.venue
    }

    async fn future_delist_events(&self, _query: &DelistScheduleQuery) -> Result<Vec<DelistEvent>> {
        anyhow::bail!(
            "delist schedule provider is not implemented for venue={}",
            self.venue.data_pub_slug()
        )
    }
}

#[derive(Default)]
struct BinanceSpotDelistProvider;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceSpotDelistGroup {
    delist_time: i64,
    symbols: Vec<String>,
}

#[async_trait]
impl DelistScheduleProvider for BinanceSpotDelistProvider {
    fn venue(&self) -> TradingVenue {
        TradingVenue::BinanceMargin
    }

    async fn future_delist_events(&self, query: &DelistScheduleQuery) -> Result<Vec<DelistEvent>> {
        let api_key = std::env::var("BINANCE_API_KEY")
            .context("BINANCE_API_KEY is required for spot delist schedule")?;
        let api_secret = std::env::var("BINANCE_API_SECRET")
            .context("BINANCE_API_SECRET is required for spot delist schedule")?;
        if api_key.trim().is_empty() || api_secret.trim().is_empty() {
            anyhow::bail!("BINANCE_API_KEY/BINANCE_API_SECRET must not be empty");
        }

        let timestamp = Utc::now().timestamp_millis();
        let query_string = format!("timestamp={timestamp}");
        let signature = sign_binance_query(&query_string, &api_secret)?;
        let url = format!(
            "https://api.binance.com/sapi/v1/spot/delist-schedule?{query_string}&signature={signature}"
        );

        let response = http_client()?
            .get(url)
            .header("X-MBX-APIKEY", api_key)
            .send()
            .await
            .context("request Binance spot delist schedule failed")?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("read Binance spot delist schedule response failed")?;
        if !status.is_success() {
            anyhow::bail!(
                "Binance spot delist schedule request failed: status={} body={}",
                status,
                body
            );
        }

        let groups: Vec<BinanceSpotDelistGroup> = serde_json::from_str(&body)
            .context("parse Binance spot delist schedule JSON failed")?;
        let mut events = Vec::new();
        for group in groups {
            let delist_time = datetime_from_millis(group.delist_time, "delistTime")?;
            if !query.contains(delist_time) {
                continue;
            }
            for symbol in group.symbols {
                events.push(DelistEvent {
                    venue: self.venue(),
                    market: DelistMarket::Spot,
                    symbol,
                    delist_time: Some(delist_time),
                    risk_type: "scheduled_delist",
                    source: "binance_spot_delist_schedule",
                    status: None,
                    detail: None,
                });
            }
        }
        Ok(events)
    }
}

#[derive(Default)]
struct BinanceFuturesDelistProvider;

#[derive(Default)]
struct AsterFuturesDelistProvider;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceFuturesExchangeInfo {
    server_time: i64,
    symbols: Vec<BinanceFuturesSymbol>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceFuturesSymbol {
    symbol: String,
    status: String,
    contract_type: String,
    delivery_date: i64,
}

async fn fetch_binance_style_futures_delists(
    venue: TradingVenue,
    url: &'static str,
    source: &'static str,
    query: &DelistScheduleQuery,
) -> Result<Vec<DelistEvent>> {
    let response = http_client()?
        .get(url)
        .send()
        .await
        .with_context(|| format!("request {source} exchangeInfo failed"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("read {source} exchangeInfo response failed"))?;
    if !status.is_success() {
        anyhow::bail!(
            "{source} exchangeInfo request failed: status={} body={}",
            status,
            body
        );
    }

    let info: BinanceFuturesExchangeInfo = serde_json::from_str(&body)
        .with_context(|| format!("parse {source} exchangeInfo JSON failed"))?;
    let effective_now = query
        .now
        .max(datetime_from_millis(info.server_time, "serverTime")?);
    let effective_query = DelistScheduleQuery {
        now: effective_now,
        window: query.window,
    };

    let events = info
        .symbols
        .into_iter()
        .filter(|symbol| symbol.contract_type == "PERPETUAL")
        .filter(|symbol| symbol.delivery_date != BINANCE_PERPETUAL_DEFAULT_DELIVERY_MS)
        .filter_map(|symbol| {
            let delist_time = datetime_from_millis(symbol.delivery_date, "deliveryDate").ok()?;
            effective_query
                .contains(delist_time)
                .then_some(DelistEvent {
                    venue,
                    market: DelistMarket::Futures,
                    symbol: symbol.symbol,
                    delist_time: Some(delist_time),
                    risk_type: "scheduled_delist",
                    source,
                    status: Some(symbol.status),
                    detail: None,
                })
        })
        .collect();
    Ok(events)
}

#[async_trait]
impl DelistScheduleProvider for BinanceFuturesDelistProvider {
    fn venue(&self) -> TradingVenue {
        TradingVenue::BinanceFutures
    }

    async fn future_delist_events(&self, query: &DelistScheduleQuery) -> Result<Vec<DelistEvent>> {
        fetch_binance_style_futures_delists(
            self.venue(),
            "https://fapi.binance.com/fapi/v1/exchangeInfo",
            "binance_futures_exchange_info",
            query,
        )
        .await
    }
}

#[async_trait]
impl DelistScheduleProvider for AsterFuturesDelistProvider {
    fn venue(&self) -> TradingVenue {
        TradingVenue::AsterFutures
    }

    async fn future_delist_events(&self, query: &DelistScheduleQuery) -> Result<Vec<DelistEvent>> {
        fetch_binance_style_futures_delists(
            self.venue(),
            "https://fapi.asterdex.com/fapi/v1/exchangeInfo",
            "aster_futures_exchange_info",
            query,
        )
        .await
    }
}

#[derive(Default)]
struct OkxSwapDelistProvider;

#[derive(Default)]
struct OkxSpotDelistProvider;

#[derive(Debug, Deserialize)]
struct OkxResponse<T> {
    code: String,
    msg: String,
    data: Vec<T>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OkxInstrument {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "instType")]
    inst_type: String,
    #[serde(rename = "ctType")]
    #[serde(default)]
    ct_type: String,
    #[serde(rename = "settleCcy")]
    #[serde(default)]
    settle_ccy: String,
    #[serde(rename = "expTime")]
    exp_time: String,
    state: String,
}

async fn fetch_okx_delists(
    venue: TradingVenue,
    market: DelistMarket,
    inst_type: &'static str,
    query: &DelistScheduleQuery,
) -> Result<Vec<DelistEvent>> {
    let url = format!("https://openapi.okx.com/api/v5/public/instruments?instType={inst_type}");
    let response = http_client()?
        .get(&url)
        .send()
        .await
        .with_context(|| format!("request OKX instruments failed: instType={inst_type}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("read OKX instruments response failed")?;
    if !status.is_success() {
        anyhow::bail!(
            "OKX instruments request failed: status={} body={}",
            status,
            body
        );
    }
    let parsed: OkxResponse<OkxInstrument> =
        serde_json::from_str(&body).context("parse OKX instruments JSON failed")?;
    if parsed.code != "0" {
        anyhow::bail!(
            "OKX instruments API error: code={} msg={}",
            parsed.code,
            parsed.msg
        );
    }

    let mut events = Vec::new();
    for item in parsed.data {
        if item.inst_type != inst_type {
            continue;
        }
        if inst_type == "SWAP" && (item.ct_type != "linear" || item.settle_ccy != "USDT") {
            continue;
        }
        let Some(delist_time) = parse_millis_str(&item.exp_time, "expTime")? else {
            continue;
        };
        if query.contains(delist_time) {
            events.push(DelistEvent {
                venue,
                market,
                symbol: item.inst_id,
                delist_time: Some(delist_time),
                risk_type: "scheduled_delist",
                source: "okx_public_instruments",
                status: Some(item.state),
                detail: None,
            });
        }
    }
    Ok(events)
}

#[async_trait]
impl DelistScheduleProvider for OkxSwapDelistProvider {
    fn venue(&self) -> TradingVenue {
        TradingVenue::OkexFutures
    }

    async fn future_delist_events(&self, query: &DelistScheduleQuery) -> Result<Vec<DelistEvent>> {
        fetch_okx_delists(self.venue(), DelistMarket::Futures, "SWAP", query).await
    }
}

#[async_trait]
impl DelistScheduleProvider for OkxSpotDelistProvider {
    fn venue(&self) -> TradingVenue {
        TradingVenue::OkexMargin
    }

    async fn future_delist_events(&self, query: &DelistScheduleQuery) -> Result<Vec<DelistEvent>> {
        fetch_okx_delists(self.venue(), DelistMarket::Spot, "SPOT", query).await
    }
}

#[derive(Default)]
struct BybitLinearDelistProvider;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitInstrumentsResponse {
    ret_code: i64,
    ret_msg: String,
    result: Option<BybitInstrumentsResult>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitInstrumentsResult {
    #[serde(default)]
    list: Vec<BybitInstrument>,
    #[serde(default)]
    next_page_cursor: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitInstrument {
    symbol: String,
    status: String,
    #[serde(default)]
    contract_type: Option<String>,
    #[serde(default)]
    delivery_time: String,
}

#[async_trait]
impl DelistScheduleProvider for BybitLinearDelistProvider {
    fn venue(&self) -> TradingVenue {
        TradingVenue::BybitFutures
    }

    async fn future_delist_events(&self, query: &DelistScheduleQuery) -> Result<Vec<DelistEvent>> {
        const URL: &str = "https://api.bybit.com/v5/market/instruments-info";
        let client = http_client()?;
        let mut cursor = String::new();
        let mut events = Vec::new();

        loop {
            let mut request = client
                .get(URL)
                .query(&[("category", "linear"), ("limit", "1000")]);
            if !cursor.is_empty() {
                request = request.query(&[("cursor", cursor.as_str())]);
            }
            let response = request
                .send()
                .await
                .context("request Bybit instruments-info failed")?;
            let status = response.status();
            let body = response
                .text()
                .await
                .context("read Bybit instruments-info response failed")?;
            if !status.is_success() {
                anyhow::bail!(
                    "Bybit instruments-info request failed: status={} body={}",
                    status,
                    body
                );
            }
            let parsed: BybitInstrumentsResponse =
                serde_json::from_str(&body).context("parse Bybit instruments-info JSON failed")?;
            if parsed.ret_code != 0 {
                anyhow::bail!(
                    "Bybit instruments-info API error: code={} msg={}",
                    parsed.ret_code,
                    parsed.ret_msg
                );
            }

            let Some(result) = parsed.result else {
                break;
            };
            for item in result.list {
                if item.contract_type.as_deref() != Some("LinearPerpetual") {
                    continue;
                }
                let Some(delist_time) = parse_millis_str(&item.delivery_time, "deliveryTime")?
                else {
                    continue;
                };
                if query.contains(delist_time) {
                    events.push(DelistEvent {
                        venue: self.venue(),
                        market: DelistMarket::Futures,
                        symbol: item.symbol,
                        delist_time: Some(delist_time),
                        risk_type: "scheduled_delist",
                        source: "bybit_instruments_info",
                        status: Some(item.status),
                        detail: None,
                    });
                }
            }

            let next_cursor = result.next_page_cursor.trim().to_string();
            if next_cursor.is_empty() {
                break;
            }
            if next_cursor == cursor {
                anyhow::bail!("Bybit instruments-info cursor did not advance: {cursor}");
            }
            cursor = next_cursor;
        }
        Ok(events)
    }
}

#[derive(Default)]
struct BitgetUsdtFuturesDelistProvider;

#[derive(Default)]
struct BitgetSpotDelistProvider;

#[derive(Debug, Deserialize)]
struct BitgetResponse<T> {
    code: String,
    msg: String,
    data: Vec<T>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BitgetInstrument {
    symbol: String,
    status: String,
    #[serde(default)]
    delivery_time: Option<String>,
    #[serde(default)]
    off_time: Option<String>,
}

#[async_trait]
impl DelistScheduleProvider for BitgetUsdtFuturesDelistProvider {
    fn venue(&self) -> TradingVenue {
        TradingVenue::BitgetFutures
    }

    async fn future_delist_events(&self, query: &DelistScheduleQuery) -> Result<Vec<DelistEvent>> {
        let url = "https://api.bitget.com/api/v3/market/instruments?category=USDT-FUTURES";
        let response = http_client()?
            .get(url)
            .send()
            .await
            .context("request Bitget instruments failed")?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("read Bitget instruments response failed")?;
        if !status.is_success() {
            anyhow::bail!(
                "Bitget instruments request failed: status={} body={}",
                status,
                body
            );
        }
        let parsed: BitgetResponse<BitgetInstrument> =
            serde_json::from_str(&body).context("parse Bitget instruments JSON failed")?;
        if parsed.code != "00000" {
            anyhow::bail!(
                "Bitget instruments API error: code={} msg={}",
                parsed.code,
                parsed.msg
            );
        }

        let mut events = Vec::new();
        for item in parsed.data {
            let raw_time = item
                .off_time
                .as_deref()
                .filter(|value| {
                    let value = value.trim();
                    !value.is_empty() && value != "0" && value != "-1"
                })
                .or_else(|| item.delivery_time.as_deref());
            let Some(raw_time) = raw_time else {
                continue;
            };
            let Some(delist_time) = parse_millis_str(raw_time, "offTime/deliveryTime")? else {
                continue;
            };
            if query.contains(delist_time) {
                events.push(DelistEvent {
                    venue: self.venue(),
                    market: DelistMarket::Futures,
                    symbol: item.symbol,
                    delist_time: Some(delist_time),
                    risk_type: "scheduled_delist",
                    source: "bitget_market_instruments",
                    status: Some(item.status),
                    detail: None,
                });
            }
        }
        Ok(events)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BitgetSpotSymbol {
    symbol: String,
    status: String,
    #[serde(default)]
    off_time: Option<String>,
}

#[async_trait]
impl DelistScheduleProvider for BitgetSpotDelistProvider {
    fn venue(&self) -> TradingVenue {
        TradingVenue::BitgetMargin
    }

    async fn future_delist_events(&self, query: &DelistScheduleQuery) -> Result<Vec<DelistEvent>> {
        let url = "https://api.bitget.com/api/v2/spot/public/symbols";
        let response = http_client()?
            .get(url)
            .send()
            .await
            .context("request Bitget spot symbols failed")?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("read Bitget spot symbols response failed")?;
        if !status.is_success() {
            anyhow::bail!(
                "Bitget spot symbols request failed: status={} body={}",
                status,
                body
            );
        }
        let parsed: BitgetResponse<BitgetSpotSymbol> =
            serde_json::from_str(&body).context("parse Bitget spot symbols JSON failed")?;
        if parsed.code != "00000" {
            anyhow::bail!(
                "Bitget spot symbols API error: code={} msg={}",
                parsed.code,
                parsed.msg
            );
        }

        let mut events = Vec::new();
        for item in parsed.data {
            if let Some(raw_time) = item.off_time.as_deref() {
                let trimmed = raw_time.trim();
                if !trimmed.is_empty() && trimmed != "0" && trimmed != "-1" {
                    let Some(delist_time) = parse_millis_str(trimmed, "offTime")? else {
                        continue;
                    };
                    if query.contains(delist_time) {
                        events.push(DelistEvent {
                            venue: self.venue(),
                            market: DelistMarket::Spot,
                            symbol: item.symbol.clone(),
                            delist_time: Some(delist_time),
                            risk_type: "scheduled_delist",
                            source: "bitget_spot_symbols",
                            status: Some(item.status.clone()),
                            detail: None,
                        });
                    }
                }
            }

            if item.status != "online" {
                events.push(DelistEvent {
                    venue: self.venue(),
                    market: DelistMarket::Spot,
                    symbol: item.symbol,
                    delist_time: None,
                    risk_type: "status_not_online",
                    source: "bitget_spot_symbols",
                    status: Some(item.status),
                    detail: None,
                });
            }
        }
        Ok(events)
    }
}

#[derive(Default)]
struct GateSpotRiskProvider;

#[derive(Default)]
struct GateFuturesDelistProvider;

#[derive(Debug, Deserialize)]
struct GateFuturesContract {
    name: String,
    status: String,
    #[serde(default)]
    in_delisting: bool,
}

#[derive(Debug, Deserialize)]
struct GateCurrency {
    currency: String,
    #[serde(default)]
    delisted: bool,
    #[serde(default)]
    trade_disabled: bool,
    #[serde(default)]
    deposit_disabled: bool,
    #[serde(default)]
    withdraw_disabled: bool,
}

async fn fetch_gate_low_cap_currencies() -> Result<Vec<String>> {
    let api_key =
        std::env::var("GATE_API_KEY").context("GATE_API_KEY is required for Gate low-cap list")?;
    let api_secret = std::env::var("GATE_API_SECRET")
        .context("GATE_API_SECRET is required for Gate low-cap list")?;
    if api_key.trim().is_empty() || api_secret.trim().is_empty() {
        anyhow::bail!("GATE_API_KEY/GATE_API_SECRET must not be empty");
    }

    let method = "GET";
    let path = "/api/v4/wallet/getLowCapExchangeList";
    let query = "";
    let payload_hash = hex::encode(sha2::Sha512::digest(b""));
    let timestamp = Utc::now().timestamp().to_string();
    let sign_string = format!("{method}\n{path}\n{query}\n{payload_hash}\n{timestamp}");
    let mut mac = hmac::Hmac::<sha2::Sha512>::new_from_slice(api_secret.as_bytes())
        .map_err(|_| anyhow!("invalid gate secret"))?;
    mac.update(sign_string.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    let url = "https://api.gateio.ws/api/v4/wallet/getLowCapExchangeList";
    let response = http_client()?
        .get(url)
        .header("Accept", "application/json")
        .header("KEY", api_key)
        .header("Timestamp", timestamp)
        .header("SIGN", signature)
        .send()
        .await
        .context("request Gate low-cap currency list failed")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("read Gate low-cap currency list response failed")?;
    if !status.is_success() {
        anyhow::bail!(
            "Gate low-cap currency list request failed: status={} body={}",
            status,
            body
        );
    }
    let currencies: Vec<String> =
        serde_json::from_str(&body).context("parse Gate low-cap currency list JSON failed")?;
    Ok(currencies)
}

#[async_trait]
impl DelistScheduleProvider for GateSpotRiskProvider {
    fn venue(&self) -> TradingVenue {
        TradingVenue::GateMargin
    }

    async fn future_delist_events(&self, _query: &DelistScheduleQuery) -> Result<Vec<DelistEvent>> {
        let mut events = Vec::new();
        for currency in fetch_gate_low_cap_currencies().await? {
            events.push(DelistEvent {
                venue: self.venue(),
                market: DelistMarket::Spot,
                symbol: currency,
                delist_time: None,
                risk_type: "low_cap_coin",
                source: "gate_low_cap_exchange_list",
                status: None,
                detail: Some("treated_as_delist_risk".to_string()),
            });
        }

        let url = "https://api.gateio.ws/api/v4/spot/currencies";
        let response = http_client()?
            .get(url)
            .send()
            .await
            .context("request Gate spot currencies failed")?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("read Gate spot currencies response failed")?;
        if !status.is_success() {
            anyhow::bail!(
                "Gate spot currencies request failed: status={} body={}",
                status,
                body
            );
        }
        let currencies: Vec<GateCurrency> =
            serde_json::from_str(&body).context("parse Gate spot currencies JSON failed")?;
        for currency in currencies {
            if !currency.delisted && !currency.trade_disabled {
                continue;
            }
            let risk_type = if currency.delisted {
                "currency_delisted"
            } else {
                "trade_disabled"
            };
            events.push(DelistEvent {
                venue: self.venue(),
                market: DelistMarket::Spot,
                symbol: currency.currency,
                delist_time: None,
                risk_type,
                source: "gate_spot_currencies",
                status: Some(format!(
                    "delisted={} trade_disabled={} deposit_disabled={} withdraw_disabled={}",
                    currency.delisted,
                    currency.trade_disabled,
                    currency.deposit_disabled,
                    currency.withdraw_disabled
                )),
                detail: None,
            });
        }
        Ok(events)
    }
}

#[async_trait]
impl DelistScheduleProvider for GateFuturesDelistProvider {
    fn venue(&self) -> TradingVenue {
        TradingVenue::GateFutures
    }

    async fn future_delist_events(&self, _query: &DelistScheduleQuery) -> Result<Vec<DelistEvent>> {
        let url = "https://api.gateio.ws/api/v4/futures/usdt/contracts";
        let response = http_client()?
            .get(url)
            .send()
            .await
            .context("request Gate futures contracts failed")?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("read Gate futures contracts response failed")?;
        if !status.is_success() {
            anyhow::bail!(
                "Gate futures contracts request failed: status={} body={}",
                status,
                body
            );
        }
        let contracts: Vec<GateFuturesContract> =
            serde_json::from_str(&body).context("parse Gate futures contracts JSON failed")?;

        let events = contracts
            .into_iter()
            .filter(|contract| contract.in_delisting)
            .map(|contract| DelistEvent {
                venue: self.venue(),
                market: DelistMarket::Futures,
                symbol: contract.name,
                delist_time: None,
                risk_type: "futures_in_delisting",
                source: "gate_futures_contracts",
                status: Some(contract.status),
                detail: Some("no_delist_timestamp_in_api".to_string()),
            })
            .collect();
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn query_contains_is_inclusive() {
        let now = Utc.with_ymd_and_hms(2026, 5, 12, 0, 0, 0).unwrap();
        let query = DelistScheduleQuery {
            now,
            window: ChronoDuration::days(7),
        };
        assert!(query.contains(now));
        assert!(query.contains(now + ChronoDuration::days(7)));
        assert!(!query.contains(now - ChronoDuration::milliseconds(1)));
        assert!(!query.contains(now + ChronoDuration::days(7) + ChronoDuration::milliseconds(1)));
    }

    #[test]
    fn parse_empty_or_zero_millis_as_none() {
        assert!(parse_millis_str("", "x").unwrap().is_none());
        assert!(parse_millis_str("0", "x").unwrap().is_none());
    }

    #[test]
    fn binance_default_delivery_constant_is_2100_12_25_utc() {
        let dt =
            datetime_from_millis(BINANCE_PERPETUAL_DEFAULT_DELIVERY_MS, "deliveryDate").unwrap();
        assert_eq!(dt.to_rfc3339(), "2100-12-25T08:00:00+00:00");
    }
}
