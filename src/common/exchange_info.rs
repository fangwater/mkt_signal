//! Public exchangeInfo / instrument catalogs.
//! Distinguishes listed vs pending-delist vs already gone, without API keys.

use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

use crate::common::delist_risk::{normalize_symbol, RiskQueryResponse};

const LISTED_STATUS: &[&str] = &["trading", "online", "tradable", "listed", "normal"];

#[derive(Debug, Clone, Default)]
pub struct ListingIndex {
    /// venue -> compact symbol -> row
    books: BTreeMap<String, BTreeMap<String, ListingRow>>,
}

#[derive(Debug, Clone)]
pub struct ListingRow {
    pub status: String,
    pub listed: bool,
    pub pending: bool,
}

impl ListingIndex {
    pub fn listing_for(&self, venue: &str, symbols: &[String], assets: &[String]) -> String {
        let Some(book) = self.books.get(venue) else {
            return "unknown".to_string();
        };
        let keys = lookup_keys(symbols, assets);
        if keys.is_empty() {
            return "unknown".to_string();
        }
        let mut any_found = false;
        let mut any_listed = false;
        let mut any_pending = false;
        for key in keys {
            if let Some(row) = book.get(&key) {
                any_found = true;
                if row.listed {
                    any_listed = true;
                }
                if row.pending {
                    any_pending = true;
                }
            }
        }
        if !any_found {
            "delisted".to_string()
        } else if any_pending {
            "pending".to_string()
        } else if any_listed {
            "listed".to_string()
        } else {
            "delisted".to_string()
        }
    }

    pub fn decorate(&self, resp: &mut RiskQueryResponse) {
        for bucket in resp.exchanges.values_mut() {
            for item in &mut bucket.items {
                item.listing = self.listing_for(&item.venue, &item.symbols, &item.assets);
            }
        }
    }

    fn insert(&mut self, venue: &str, symbol: &str, row: ListingRow) {
        let key = normalize_symbol(symbol);
        if key.is_empty() {
            return;
        }
        self.books
            .entry(venue.to_string())
            .or_default()
            .insert(key, row);
    }
}

fn lookup_keys(symbols: &[String], assets: &[String]) -> Vec<String> {
    let mut keys = Vec::new();
    for symbol in symbols {
        let key = normalize_symbol(symbol);
        if !key.is_empty() && !keys.iter().any(|seen| seen == &key) {
            keys.push(key);
        }
    }
    for asset in assets {
        let asset = normalize_symbol(asset);
        if asset.is_empty() {
            continue;
        }
        let as_usdt = format!("{asset}USDT");
        if !keys.iter().any(|seen| seen == &as_usdt) {
            keys.push(as_usdt);
        }
    }
    keys
}

fn row(status: &str, pending: bool) -> ListingRow {
    let listed = is_listed_status(status) && !pending;
    ListingRow {
        status: status.to_string(),
        listed,
        pending,
    }
}

fn is_listed_status(status: &str) -> bool {
    let key = status.trim().to_ascii_lowercase();
    LISTED_STATUS.iter().any(|want| key == *want)
}

fn future_ms(raw: Option<i64>, now_ms: i64) -> bool {
    raw.is_some_and(|ms| ms > now_ms)
}

pub async fn fetch_listing_index(client: &Client) -> (ListingIndex, Vec<(String, String)>) {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut index = ListingIndex::default();
    let mut errors = Vec::new();
    for (source, result) in [
        (
            "binance_spot_exchange_info",
            fetch_binance_spot(client, &mut index, now_ms).await,
        ),
        (
            "binance_futures_exchange_info",
            fetch_binance_futures(client, &mut index, now_ms).await,
        ),
        (
            "bitget_spot_exchange_info",
            fetch_bitget_spot(client, &mut index, now_ms).await,
        ),
        (
            "bitget_futures_exchange_info",
            fetch_bitget_futures(client, &mut index, now_ms).await,
        ),
        (
            "gate_spot_exchange_info",
            fetch_gate_spot(client, &mut index, now_ms).await,
        ),
        (
            "gate_futures_exchange_info",
            fetch_gate_futures(client, &mut index, now_ms).await,
        ),
    ] {
        if let Err(err) = result {
            errors.push((source.to_string(), format!("{err:#}")));
        }
    }
    (index, errors)
}

async fn fetch_binance_spot(client: &Client, index: &mut ListingIndex, now_ms: i64) -> Result<()> {
    let body = get_json(
        client,
        "https://api.binance.com/api/v3/exchangeInfo",
        "Binance spot exchangeInfo",
    )
    .await?;
    ingest_binance_symbols(&body, "binance-margin", index, now_ms);
    Ok(())
}

async fn fetch_binance_futures(
    client: &Client,
    index: &mut ListingIndex,
    now_ms: i64,
) -> Result<()> {
    let body = get_json(
        client,
        "https://fapi.binance.com/fapi/v1/exchangeInfo",
        "Binance futures exchangeInfo",
    )
    .await?;
    ingest_binance_symbols(&body, "binance-futures", index, now_ms);
    Ok(())
}

fn ingest_binance_symbols(body: &Value, venue: &str, index: &mut ListingIndex, now_ms: i64) {
    let Some(symbols) = body.get("symbols").and_then(Value::as_array) else {
        return;
    };
    for item in symbols {
        let symbol = item.get("symbol").and_then(Value::as_str).unwrap_or("");
        if symbol.is_empty() {
            continue;
        }
        let status = item.get("status").and_then(Value::as_str).unwrap_or("");
        let delivery = item
            .get("deliveryDate")
            .and_then(Value::as_i64)
            .filter(|ms| *ms > now_ms && *ms < now_ms + 400 * 86_400_000);
        index.insert(venue, symbol, row(status, delivery.is_some()));
    }
}

async fn fetch_bitget_spot(client: &Client, index: &mut ListingIndex, now_ms: i64) -> Result<()> {
    let parsed: BitgetEnvelope<Vec<BitgetSpot>> = get_typed(
        client,
        "https://api.bitget.com/api/v2/spot/public/symbols",
        "Bitget spot symbols",
    )
    .await?;
    for item in parsed.data {
        let pending = future_ms(parse_ms(item.off_time.as_deref()), now_ms);
        index.insert("bitget-margin", &item.symbol, row(&item.status, pending));
    }
    Ok(())
}

async fn fetch_bitget_futures(
    client: &Client,
    index: &mut ListingIndex,
    now_ms: i64,
) -> Result<()> {
    let parsed: BitgetEnvelope<Vec<BitgetMix>> = get_typed(
        client,
        "https://api.bitget.com/api/v3/market/instruments?category=USDT-FUTURES",
        "Bitget USDT futures",
    )
    .await?;
    for item in parsed.data {
        let off =
            parse_ms(item.off_time.as_deref()).or_else(|| parse_ms(item.delivery_time.as_deref()));
        let pending = future_ms(off, now_ms);
        index.insert("bitget-futures", &item.symbol, row(&item.status, pending));
    }
    Ok(())
}

async fn fetch_gate_spot(client: &Client, index: &mut ListingIndex, now_ms: i64) -> Result<()> {
    let body = get_text(
        client,
        "https://api.gateio.ws/api/v4/spot/currency_pairs",
        "Gate spot currency_pairs",
    )
    .await?;
    let pairs: Vec<GateSpot> =
        serde_json::from_str(&body).context("parse Gate spot currency_pairs JSON failed")?;
    let now_sec = now_ms / 1000;
    for pair in pairs {
        let off_ms = pair
            .delisting_time
            .filter(|ts| *ts > now_sec)
            .map(|ts| ts.saturating_mul(1000));
        let pending = off_ms.is_some();
        index.insert("gate-margin", &pair.id, row(&pair.trade_status, pending));
    }
    Ok(())
}

async fn fetch_gate_futures(client: &Client, index: &mut ListingIndex, now_ms: i64) -> Result<()> {
    let _ = now_ms;
    let body = get_text(
        client,
        "https://api.gateio.ws/api/v4/futures/usdt/contracts",
        "Gate USDT futures",
    )
    .await?;
    let contracts: Vec<GateFutures> =
        serde_json::from_str(&body).context("parse Gate USDT futures JSON failed")?;
    for contract in contracts {
        index.insert(
            "gate-futures",
            &contract.name,
            row(&contract.status, contract.in_delisting),
        );
    }
    Ok(())
}

async fn get_json(client: &Client, url: &str, label: &str) -> Result<Value> {
    let body = get_text(client, url, label).await?;
    serde_json::from_str(&body).with_context(|| format!("parse {label} JSON failed"))
}

async fn get_typed<T: serde::de::DeserializeOwned>(
    client: &Client,
    url: &str,
    label: &str,
) -> Result<BitgetEnvelope<T>> {
    let body = get_text(client, url, label).await?;
    let parsed: BitgetEnvelope<T> =
        serde_json::from_str(&body).with_context(|| format!("parse {label} JSON failed"))?;
    if parsed.code != "00000" {
        bail!("{label} rejected: code={} msg={}", parsed.code, parsed.msg);
    }
    Ok(parsed)
}

async fn get_text(client: &Client, url: &str, label: &str) -> Result<String> {
    let response = client
        .get(url)
        .header("Accept", "application/json")
        .send()
        .await
        .with_context(|| format!("request {label} failed"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("read {label} failed"))?;
    if !status.is_success() {
        bail!(
            "{label} failed: status={status} body={}",
            body.chars().take(300).collect::<String>()
        );
    }
    Ok(body)
}

fn parse_ms(raw: Option<&str>) -> Option<i64> {
    let trimmed = raw?.trim();
    if trimmed.is_empty() || trimmed == "0" || trimmed == "-1" {
        return None;
    }
    trimmed.parse::<i64>().ok().filter(|ms| *ms > 0)
}

#[derive(Debug, Deserialize)]
struct BitgetEnvelope<T> {
    code: String,
    #[serde(default)]
    msg: String,
    data: T,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BitgetSpot {
    symbol: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    off_time: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BitgetMix {
    symbol: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    off_time: Option<String>,
    #[serde(default)]
    delivery_time: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GateSpot {
    id: String,
    #[serde(default)]
    trade_status: String,
    #[serde(default)]
    delisting_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct GateFutures {
    name: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    in_delisting: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_symbol_is_delisted() {
        let mut index = ListingIndex::default();
        index.insert("binance-margin", "BTCUSDT", row("TRADING", false));
        assert_eq!(
            index.listing_for("binance-margin", &["SUIBNB".into()], &[]),
            "delisted"
        );
        assert_eq!(
            index.listing_for("binance-margin", &["BTCUSDT".into()], &[]),
            "listed"
        );
        assert_eq!(
            index.listing_for("binance-futures", &["BTCUSDT".into()], &[]),
            "unknown"
        );
    }

    #[test]
    fn asset_looks_up_usdt_pair() {
        let mut index = ListingIndex::default();
        index.insert("binance-futures", "ICXUSDT", row("TRADING", true));
        assert_eq!(
            index.listing_for("binance-futures", &[], &["ICX".into()]),
            "pending"
        );
    }

    #[test]
    fn pair_only_event_does_not_use_other_quotes() {
        let mut index = ListingIndex::default();
        index.insert("binance-margin", "SUIUSDT", row("TRADING", false));
        assert_eq!(
            index.listing_for("binance-margin", &["SUIBNB".into()], &[]),
            "delisted"
        );
    }

    #[test]
    fn halted_catalog_row_is_delisted() {
        let mut index = ListingIndex::default();
        index.insert("binance-margin", "AAAUSDT", row("BREAK", false));
        assert_eq!(
            index.listing_for("binance-margin", &["AAAUSDT".into()], &[]),
            "delisted"
        );
    }
}
