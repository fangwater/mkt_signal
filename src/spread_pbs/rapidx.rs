//! RapidX (LiquidityTech) public market-data adapter.
//!
//! RapidX is a provider, not an execution venue.  The caller keeps the native
//! `TradingVenue` for symbol discovery and IPC service names while this adapter
//! maps it to RapidX's `{EXCHANGE}_{TYPE}_{BASE}_{QUOTE}` wire symbols.

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use mkt_parsers::msg::mkt_msg::{FundingRateMsg, IndexPriceMsg, Level, MarkPriceMsg};
use mkt_parsers::msg::open_interest_msg::OpenInterestMsg;
use order_common::TradingVenue;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::watch;
use tokio_tungstenite_v030::tungstenite::Message;

use crate::spread_pbs::adapter::{
    BboDedupPolicy, BboFrame, IncrementalDedupPolicy, IncrementalFrame, KeepaliveSpec,
    TradeDedupPolicy, TradeFrame, VenueAdapter,
};

pub const RAPIDX_PUBLIC_WS_URL: &str =
    "wss://md.liquiditytech.com/marketdata/v2/public?binary=false";
const RAPIDX_SUBSCRIBE_PAIRS: usize = 5;
const RAPIDX_FUNDING_URL: &str = "https://api.liquiditytech.com/api/v1/market/fundingRate";
// 3 requests / 10 seconds: four seconds leaves a small scheduling margin.
const RAPIDX_FUNDING_REQUEST_SPACING: Duration = Duration::from_secs(4);
const RAPIDX_FUNDING_HTTP_TIMEOUT: Duration = Duration::from_secs(5);
// Five unauthenticated pairs. This is a worst-case request-cycle bound, not a
// promise of exchange data freshness: 5 * (5s HTTP timeout + 4s spacing).
const RAPIDX_FUNDING_MAX_CYCLE: Duration = Duration::from_secs(45);

#[derive(Debug, Default)]
struct FundingSymbolScheduler {
    symbols: Vec<String>,
    next_index: usize,
}

impl FundingSymbolScheduler {
    fn new(symbols: Vec<String>) -> Self {
        let mut scheduler = Self::default();
        scheduler.replace(symbols);
        scheduler
    }

    fn replace(&mut self, symbols: Vec<String>) {
        self.symbols = symbols;
        self.next_index = 0;
    }

    fn next(&mut self) -> Option<String> {
        let symbol = self.symbols.get(self.next_index)?.clone();
        self.next_index = (self.next_index + 1) % self.symbols.len();
        Some(symbol)
    }
}

pub struct RapidXAdapter {
    venue: TradingVenue,
}

impl RapidXAdapter {
    pub fn new(venue: TradingVenue) -> Result<Self> {
        rapidx_prefix(venue)?;
        Ok(Self { venue })
    }

    fn wire_symbol(&self, symbol: &str) -> Result<String> {
        rapidx_symbol(self.venue, symbol)
    }

    pub fn funding_wire_symbol(&self, symbol: &str) -> Result<String> {
        self.wire_symbol(symbol)
    }

    fn internal_symbol(&self, wire: &str) -> Result<String> {
        let expected = rapidx_prefix(self.venue)?;
        let suffix = wire
            .strip_prefix(expected)
            .ok_or_else(|| anyhow!("RapidX symbol {wire:?} does not match {expected:?}"))?;
        Ok(suffix.replace('_', "").to_ascii_uppercase())
    }

    fn build_channel_subscribe(&self, symbols: &[String], channel: &str) -> Vec<Value> {
        symbols
            .chunks(RAPIDX_SUBSCRIBE_PAIRS)
            .map(|chunk| {
                let args = chunk
                    .iter()
                    .filter_map(|symbol| self.wire_symbol(symbol).ok())
                    .map(|sym| serde_json::json!({ "channel": channel, "sym": sym }))
                    .collect::<Vec<_>>();
                serde_json::json!({ "event": "subscribe", "arg": args })
            })
            .filter(|message| !message["arg"].as_array().is_some_and(Vec::is_empty))
            .collect()
    }
}

pub fn spawn_funding_poller(
    adapter: RapidXAdapter,
    mut symbols_rx: watch::Receiver<Vec<String>>,
    publisher: std::rc::Rc<crate::spread_pbs::publisher::SpreadDerivativesPublisher>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_local(async move {
        let client = reqwest::Client::builder()
            .timeout(RAPIDX_FUNDING_HTTP_TIMEOUT)
            .build();
        let Ok(client) = client else { return };
        let mut scheduler = FundingSymbolScheduler::new(symbols_rx.borrow().clone());
        log::info!(
            "RapidX funding poller started symbols={} request_spacing_s={} max_cycle_s={}",
            scheduler.symbols.len(),
            RAPIDX_FUNDING_REQUEST_SPACING.as_secs(),
            RAPIDX_FUNDING_MAX_CYCLE.as_secs(),
        );
        loop {
            if scheduler.symbols.is_empty() {
                tokio::select! {
                    changed = symbols_rx.changed() => match changed {
                        Ok(()) => scheduler.replace(symbols_rx.borrow().clone()),
                        Err(_) => return,
                    },
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() { return; }
                    }
                }
                continue;
            }
            let Some(symbol) = scheduler.next() else {
                continue;
            };
            let result = async {
                let wire = adapter.funding_wire_symbol(&symbol)?;
                let response = client
                    .get(RAPIDX_FUNDING_URL)
                    .query(&[("sym", wire)])
                    .send()
                    .await
                    .context("RapidX funding request")?
                    .error_for_status()
                    .context("RapidX funding HTTP status")?;
                let value = response
                    .json::<Value>()
                    .await
                    .context("RapidX funding JSON")?;
                let bytes = parse_funding_response(&value, &adapter)?;
                publisher.publish(&bytes).context("publish RapidX funding")
            }
            .await;
            if let Err(err) = result {
                log::warn!("RapidX funding poll failed: {err:#}");
            }
            tokio::select! {
                biased;
                changed = symbols_rx.changed() => {
                    if changed.is_err() { return; }
                    scheduler.replace(symbols_rx.borrow().clone());
                    log::info!("RapidX funding poller symbol set refreshed symbols={}", scheduler.symbols.len());
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() { return; }
                }
                _ = tokio::time::sleep(RAPIDX_FUNDING_REQUEST_SPACING) => {}
            }
        }
    })
}

fn parse_funding_response(value: &Value, adapter: &RapidXAdapter) -> Result<Bytes> {
    let code = value
        .get("code")
        .and_then(|code| {
            code.as_i64()
                .or_else(|| code.as_str().and_then(|raw| raw.parse().ok()))
        })
        .context("RapidX funding missing code")?;
    if !matches!(code, 200 | 200000) {
        bail!("RapidX funding API code {code}");
    }
    let row = value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(Value::as_object)
        .context("RapidX funding missing data row")?;
    let symbol = adapter.internal_symbol(required_str(row, "sym")?)?;
    let funding_rate = required_f64(row, "fundingRate")?;
    let funding_time = millis_to_micros(required_i64(row, "fundingTime")?)?;
    let next_funding_time = millis_to_micros(required_i64(row, "nextFundingTime")?)?;
    Ok(FundingRateMsg::create(symbol, funding_rate, next_funding_time, funding_time).to_bytes())
}

impl VenueAdapter for RapidXAdapter {
    fn name(&self) -> &'static str {
        "rapidx"
    }

    fn ws_url(&self) -> String {
        RAPIDX_PUBLIC_WS_URL.to_string()
    }

    fn build_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        self.build_channel_subscribe(symbols, "BBO")
    }

    fn build_trade_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        self.build_channel_subscribe(symbols, "TRADE")
    }

    fn build_incremental_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        symbols
            .chunks(RAPIDX_SUBSCRIBE_PAIRS)
            .map(|chunk| {
                let args = chunk
                    .iter()
                    .filter_map(|symbol| self.wire_symbol(symbol).ok())
                    .map(|sym| {
                        serde_json::json!({
                            "channel": "ORDER_BOOK", "sym": sym, "level": 50
                        })
                    })
                    .collect::<Vec<_>>();
                serde_json::json!({ "event": "subscribe", "arg": args })
            })
            .filter(|message| !message["arg"].as_array().is_some_and(Vec::is_empty))
            .collect()
    }

    fn build_derivatives_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        if !is_perp(self.venue) {
            return Vec::new();
        }
        let mut out = Vec::new();
        for channel in ["MARK_PRICE", "INDEX_PRICE", "OPEN_INTEREST"] {
            out.extend(self.build_channel_subscribe(symbols, channel));
        }
        out
    }

    fn bbo_dedup_policy(&self) -> BboDedupPolicy {
        // The current public BBO schema has no sequence field.
        BboDedupPolicy::RecentIdentity
    }

    fn trade_dedup_policy(&self) -> TradeDedupPolicy {
        TradeDedupPolicy::RecentIdentity
    }

    fn incremental_dedup_policy(&self) -> IncrementalDedupPolicy {
        // A mirrored leg can deliver its subscribe snapshot after the other
        // leg has already advanced. RapidX sequences are provider-global, so
        // accepting that older snapshot would rewind the shared book state.
        IncrementalDedupPolicy::MonotonicIncludingSnapshots
    }

    fn reconnect_on_parse_error(&self) -> bool {
        true
    }

    fn reconnect_on_incremental_gap(&self) -> bool {
        true
    }

    fn parse_frame(
        &self,
        value: &Value,
        emit: &mut dyn FnMut(BboFrame) -> Result<()>,
    ) -> Result<()> {
        if channel(value) != Some("BBO") {
            return Ok(());
        }
        let sym = arg_symbol(value)?;
        let data = data(value)?;
        emit(BboFrame {
            symbol: self.internal_symbol(sym)?,
            ts_us: millis_to_micros(required_i64(data, "ts")?)?,
            // There is no documented BBO sequence. RecentIdentity is selected above.
            seq_id: 0,
            reset_seq: false,
            bid_price: required_f64(data, "bid")?,
            bid_amount: required_f64(data, "bidqty")?,
            ask_price: required_f64(data, "ask")?,
            ask_amount: required_f64(data, "askqty")?,
        })
    }

    fn parse_trade_frame(&self, value: &Value) -> Result<Vec<TradeFrame>> {
        if channel(value) != Some("TRADE") {
            return Ok(Vec::new());
        }
        let data = data(value)?;
        let trade_id = required_i64(data, "tradeid")?;
        let side = match required_str(data, "side")?.to_ascii_lowercase().as_str() {
            "buy" => 'B',
            "sell" => 'S',
            other => bail!("RapidX TRADE invalid side {other:?}"),
        };
        Ok(vec![TradeFrame {
            symbol: self.internal_symbol(arg_symbol(value)?)?,
            timestamp_us: millis_to_micros(required_i64(data, "ts")?)?,
            seq_id: trade_id,
            trade_id,
            side,
            price: required_f64(data, "price")?,
            amount: required_f64(data, "qty")?,
        }])
    }

    fn parse_incremental_frame(&self, value: &Value) -> Result<Vec<IncrementalFrame>> {
        if channel(value) != Some("ORDER_BOOK") {
            return Ok(Vec::new());
        }
        let arg = value
            .get("arg")
            .and_then(Value::as_object)
            .context("RapidX ORDER_BOOK missing arg")?;
        let update_type = arg
            .get("updatetype")
            .and_then(Value::as_str)
            .context("RapidX ORDER_BOOK missing arg.updatetype")?;
        let is_snapshot = match update_type {
            "all" => true,
            "update" => false,
            other => bail!("RapidX ORDER_BOOK invalid updatetype {other:?}"),
        };
        let data = data(value)?;
        let seq_id = required_i64(data, "seq")?;
        Ok(vec![IncrementalFrame::Book {
            symbol: self.internal_symbol(arg_symbol(value)?)?,
            timestamp: millis_to_micros(required_i64(data, "ts")?)?,
            seq_id,
            prev_seq_id: required_i64(data, "pre_seq")?,
            first_update_id: seq_id,
            final_update_id: seq_id,
            gap_check: !is_snapshot,
            is_snapshot,
            bids: parse_levels(data, "Bids")?,
            asks: parse_levels(data, "Asks")?,
        }])
    }

    fn parse_derivatives_frame(&self, value: &Value) -> Result<Vec<Bytes>> {
        let Some(channel) = channel(value) else {
            return Ok(Vec::new());
        };
        if !matches!(channel, "MARK_PRICE" | "INDEX_PRICE" | "OPEN_INTEREST") {
            return Ok(Vec::new());
        }
        let data = data(value)?;
        let symbol = self.internal_symbol(arg_symbol(value)?)?;
        let ts = millis_to_micros(required_i64(data, "ts")?)?;
        let msg = match channel {
            "MARK_PRICE" => {
                MarkPriceMsg::create(symbol, required_f64(data, "markpx")?, ts).to_bytes()
            }
            "INDEX_PRICE" => {
                IndexPriceMsg::create(symbol, required_f64(data, "indexpx")?, ts).to_bytes()
            }
            "OPEN_INTEREST" => {
                OpenInterestMsg::create(symbol, required_f64(data, "open_interest")?, ts).to_bytes()
            }
            _ => unreachable!(),
        };
        Ok(vec![msg])
    }

    fn keepalive(&self) -> Option<KeepaliveSpec> {
        Some(KeepaliveSpec::dynamic(Duration::from_secs(20), || {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(0);
            Message::Text(serde_json::json!({ "ping": now_ms }).to_string().into())
        }))
    }
}

fn rapidx_prefix(venue: TradingVenue) -> Result<&'static str> {
    match venue {
        TradingVenue::BinanceMargin => Ok("BINANCE_SPOT_"),
        TradingVenue::BinanceFutures => Ok("BINANCE_PERP_"),
        TradingVenue::OkexMargin => Ok("OKX_SPOT_"),
        // RapidX documents TRADE qty as base currency, while its order and
        // position APIs document OKX quantities as contracts. Its public BBO,
        // ORDER_BOOK, and OPEN_INTEREST pages merely say "quantity" / "total
        // open interest" without an OKX perpetual unit. We therefore cannot
        // determine whether a ctVal conversion is needed (or would double
        // convert), so reject rather than publish mixed units.
        TradingVenue::OkexFutures => bail!(
            "RapidX OKX perpetual disabled: public BBO, ORDER_BOOK, and OPEN_INTEREST documentation does not specify whether quantities are base units or OKX contracts"
        ),
        other => bail!("RapidX provider does not support native venue {other:?}"),
    }
}

fn is_perp(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceFutures | TradingVenue::OkexFutures
    )
}

fn rapidx_symbol(venue: TradingVenue, internal: &str) -> Result<String> {
    let compact = internal.to_ascii_uppercase().replace(['-', '_'], "");
    let quote = ["USDT", "USDC", "USD", "BTC", "ETH"]
        .into_iter()
        .find(|quote| compact.ends_with(quote))
        .context("RapidX symbol has no supported quote suffix")?;
    let base = compact
        .strip_suffix(quote)
        .filter(|base| !base.is_empty())
        .with_context(|| format!("RapidX invalid internal symbol {internal:?}"))?;
    Ok(format!("{}{}_{}", rapidx_prefix(venue)?, base, quote))
}

fn channel(value: &Value) -> Option<&str> {
    value.pointer("/arg/channel").and_then(Value::as_str)
}

fn arg_symbol(value: &Value) -> Result<&str> {
    value
        .pointer("/arg/sym")
        .and_then(Value::as_str)
        .context("RapidX frame missing arg.sym")
}

fn data(value: &Value) -> Result<&serde_json::Map<String, Value>> {
    value
        .get("data")
        .and_then(Value::as_object)
        .context("RapidX frame missing data object")
}

fn required_str<'a>(data: &'a serde_json::Map<String, Value>, field: &str) -> Result<&'a str> {
    data.get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("RapidX frame missing/invalid {field}"))
}

fn required_i64(data: &serde_json::Map<String, Value>, field: &str) -> Result<i64> {
    let value = data
        .get(field)
        .with_context(|| format!("RapidX frame missing {field}"))?;
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|raw| raw.parse().ok()))
        .with_context(|| format!("RapidX frame invalid {field}"))
}

fn required_f64(data: &serde_json::Map<String, Value>, field: &str) -> Result<f64> {
    let value = data
        .get(field)
        .with_context(|| format!("RapidX frame missing {field}"))?;
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|raw| raw.parse().ok()))
        .filter(|value| value.is_finite())
        .with_context(|| format!("RapidX frame invalid {field}"))
}

fn millis_to_micros(timestamp_ms: i64) -> Result<i64> {
    timestamp_ms
        .checked_mul(1_000)
        .context("RapidX timestamp milliseconds overflow")
}

fn parse_levels(data: &serde_json::Map<String, Value>, field: &str) -> Result<Vec<Level>> {
    data.get(field)
        .and_then(Value::as_array)
        .with_context(|| format!("RapidX ORDER_BOOK missing {field}"))?
        .iter()
        .map(|level| {
            let pair = level
                .as_array()
                .filter(|pair| pair.len() == 2)
                .with_context(|| format!("RapidX ORDER_BOOK invalid {field} level"))?;
            let price = pair[0]
                .as_f64()
                .or_else(|| pair[0].as_str().and_then(|raw| raw.parse().ok()))
                .context("RapidX ORDER_BOOK invalid level price")?;
            let size = pair[1]
                .as_f64()
                .or_else(|| pair[1].as_str().and_then(|raw| raw.parse().ok()))
                .context("RapidX ORDER_BOOK invalid level size")?;
            if !price.is_finite() || !size.is_finite() {
                bail!("RapidX ORDER_BOOK non-finite level")
            }
            Ok(Level {
                price,
                amount: size,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn adapter() -> RapidXAdapter {
        RapidXAdapter::new(TradingVenue::BinanceFutures).unwrap()
    }

    #[test]
    fn subscribes_current_channels_in_unauthenticated_pair_batches() {
        let symbols = (0..6).map(|n| format!("X{n}USDT")).collect::<Vec<_>>();
        let messages = adapter().build_incremental_subscribe(&symbols);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["arg"].as_array().unwrap().len(), 5);
        assert_eq!(messages[0]["arg"][0]["sym"], "BINANCE_PERP_X0_USDT");
        assert_eq!(messages[0]["arg"][0]["level"], 50);
    }

    #[test]
    fn rejects_okx_perp_when_public_market_data_units_are_undocumented() {
        let err = RapidXAdapter::new(TradingVenue::OkexFutures).err().unwrap();
        assert!(err.to_string().contains("does not specify"));
    }

    #[test]
    fn parses_bbo_without_manufacturing_a_sequence() {
        let frame = serde_json::json!({"arg":{"channel":"BBO","sym":"BINANCE_PERP_BTC_USDT"},"data":{"bid":"100","bidqty":"1","ask":"101","askqty":"2","ts":"1700000000123"}});
        let frames = adapter().collect_frame(&frame).unwrap();
        assert_eq!(adapter().bbo_dedup_policy(), BboDedupPolicy::RecentIdentity);
        assert_eq!(frames[0].seq_id, 0);
        assert_eq!(frames[0].ts_us, 1_700_000_000_123_000);
    }

    #[test]
    fn parses_trade_and_order_book_snapshot_then_delta() {
        let trade = serde_json::json!({"arg":{"channel":"TRADE","sym":"BINANCE_PERP_BTC_USDT"},"data":{"price":"100","qty":"0.3","side":"sell","tradeid":"42","ts":"1700000000123"}});
        let trades = adapter().parse_trade_frame(&trade).unwrap();
        assert_eq!(trades[0].side, 'S');
        assert_eq!(trades[0].trade_id, 42);
        let snapshot = serde_json::json!({"arg":{"channel":"ORDER_BOOK","sym":"BINANCE_PERP_BTC_USDT","updatetype":"all"},"data":{"ts":"1700000000123","seq":"11","pre_seq":"10","Bids":[["100","1"]],"Asks":[["101","2"]]}});
        let delta = serde_json::json!({"arg":{"channel":"ORDER_BOOK","sym":"BINANCE_PERP_BTC_USDT","updatetype":"update"},"data":{"ts":"1700000000124","seq":"12","pre_seq":"11","Bids":[["100","0"]],"Asks":[]}});
        let snapshots = adapter().parse_incremental_frame(&snapshot).unwrap();
        let deltas = adapter().parse_incremental_frame(&delta).unwrap();
        let IncrementalFrame::Book {
            is_snapshot,
            gap_check,
            bids,
            ..
        } = &snapshots[0]
        else {
            panic!()
        };
        assert!(*is_snapshot);
        assert!(!*gap_check);
        assert_eq!(bids[0].amount, 1.0);
        let IncrementalFrame::Book {
            is_snapshot,
            gap_check,
            prev_seq_id,
            ..
        } = &deltas[0]
        else {
            panic!()
        };
        assert!(!*is_snapshot);
        assert!(*gap_check);
        assert_eq!(*prev_seq_id, 11);
    }

    #[test]
    fn uses_monotonic_snapshots_so_a_slow_mirror_cannot_rewind_depth() {
        assert_eq!(
            adapter().incremental_dedup_policy(),
            IncrementalDedupPolicy::MonotonicIncludingSnapshots
        );
    }

    #[test]
    fn parses_mark_index_and_open_interest() {
        for (channel, field) in [
            ("MARK_PRICE", "markpx"),
            ("INDEX_PRICE", "indexpx"),
            ("OPEN_INTEREST", "open_interest"),
        ] {
            let frame = serde_json::json!({"arg":{"channel":channel,"sym":"BINANCE_PERP_BTC_USDT"},"data":{field:"100.5","ts":"1700000000123"}});
            assert_eq!(adapter().parse_derivatives_frame(&frame).unwrap().len(), 1);
        }
    }

    #[test]
    fn parses_funding_with_exchange_timestamps() {
        let response = serde_json::json!({
            "code": 200000,
            "data": [{
                "sym": "BINANCE_PERP_BTC_USDT",
                "fundingRate": "0.00000499",
                "fundingTime": "1722395955000",
                "nextFundingTime": "1722412800000"
            }]
        });
        let bytes = parse_funding_response(&response, &adapter()).unwrap();
        assert_eq!(FundingRateMsg::get_symbol(&bytes), "BTCUSDT");
        assert_eq!(FundingRateMsg::get_timestamp(&bytes), 1_722_395_955_000_000);
        assert_eq!(
            FundingRateMsg::get_next_funding_time(&bytes),
            1_722_412_800_000_000
        );
    }

    #[test]
    fn funding_scheduler_rotates_current_symbols_without_stale_or_empty_access() {
        let mut scheduler = FundingSymbolScheduler::new(vec!["BTCUSDT".into(), "ETHUSDT".into()]);
        assert_eq!(scheduler.next().as_deref(), Some("BTCUSDT"));
        assert_eq!(scheduler.next().as_deref(), Some("ETHUSDT"));
        assert_eq!(scheduler.next().as_deref(), Some("BTCUSDT"));

        scheduler.replace(vec!["SOLUSDT".into()]);
        assert_eq!(scheduler.next().as_deref(), Some("SOLUSDT"));
        assert_eq!(scheduler.next().as_deref(), Some("SOLUSDT"));

        scheduler.replace(Vec::new());
        assert_eq!(scheduler.next(), None);

        scheduler.replace(vec!["XRPUSDT".into(), "DOGEUSDT".into()]);
        assert_eq!(scheduler.next().as_deref(), Some("XRPUSDT"));
        assert_eq!(scheduler.next().as_deref(), Some("DOGEUSDT"));
    }

    #[test]
    fn accepts_both_documented_funding_success_codes() {
        for code in [
            serde_json::json!(200),
            serde_json::json!(200000),
            serde_json::json!("200000"),
        ] {
            let response = serde_json::json!({
                "code": code,
                "data": [{
                    "sym": "BINANCE_PERP_BTC_USDT",
                    "fundingRate": "0",
                    "fundingTime": "1",
                    "nextFundingTime": "2"
                }]
            });
            assert!(parse_funding_response(&response, &adapter()).is_ok());
        }
    }
}
