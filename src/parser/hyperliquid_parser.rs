use crate::common::mkt_msg::{
    FundingRateMsg, KlineMsg, MarkPriceMsg, SignalMsg, SignalSource, TradeMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

fn parse_num(value: Option<&serde_json::Value>) -> Option<f64> {
    let value = value?;
    if let Some(number) = value.as_f64() {
        return Some(number);
    }
    value.as_str().and_then(|text| text.parse::<f64>().ok())
}

fn parse_i64(value: Option<&serde_json::Value>) -> Option<i64> {
    let value = value?;
    if let Some(number) = value.as_i64() {
        return Some(number);
    }
    if let Some(number) = value.as_u64() {
        return i64::try_from(number).ok();
    }
    if let Some(text) = value.as_str() {
        if let Ok(number) = text.parse::<i64>() {
            return Some(number);
        }
        if let Ok(number) = text.parse::<u64>() {
            return i64::try_from(number).ok();
        }
    }
    None
}

fn normalize_timestamp_to_ms(timestamp: i64) -> i64 {
    if timestamp >= 1_000_000_000_000_000 {
        timestamp / 1_000_000
    } else if timestamp >= 1_000_000_000_000 {
        timestamp
    } else if timestamp >= 1_000_000_000 {
        timestamp * 1000
    } else {
        timestamp
    }
}

fn now_ts_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn side_to_char(side: &str) -> Option<char> {
    match side.to_ascii_uppercase().as_str() {
        "B" | "BUY" | "BID" => Some('B'),
        "A" | "ASK" | "S" | "SELL" => Some('S'),
        _ => None,
    }
}

fn hash_text_to_i64(text: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish() as i64
}

fn normalize_hyperliquid_symbol(coin: &str) -> String {
    let normalized = coin.trim().to_uppercase().replace(['/', '-', '_'], "");
    if normalized.ends_with("USDC") {
        normalized
    } else {
        format!("{}USDC", normalized)
    }
}

fn normalized_symbol_from_value(item: &serde_json::Value) -> Option<String> {
    item.get("coin")
        .and_then(|value| value.as_str())
        .or_else(|| item.get("s").and_then(|value| value.as_str()))
        .map(normalize_hyperliquid_symbol)
}

fn parse_timestamp(json_value: &serde_json::Value) -> i64 {
    parse_i64(json_value.get("time"))
        .or_else(|| parse_i64(json_value.get("timeMs")))
        .or_else(|| {
            json_value
                .get("data")
                .and_then(|value| parse_i64(value.get("time")))
        })
        .or_else(|| {
            json_value
                .get("data")
                .and_then(|value| parse_i64(value.get("timeMs")))
        })
        .map(normalize_timestamp_to_ms)
        .unwrap_or(0)
}

fn parse_trade_id(item: &serde_json::Value) -> i64 {
    parse_i64(item.get("tid"))
        .or_else(|| parse_i64(item.get("id")))
        .or_else(|| {
            item.get("hash")
                .and_then(|value| value.as_str())
                .map(hash_text_to_i64)
        })
        .unwrap_or(0)
}

fn parse_trade_timestamp(item: &serde_json::Value, fallback_ts: i64) -> i64 {
    parse_i64(item.get("time"))
        .or_else(|| parse_i64(item.get("ts")))
        .map(normalize_timestamp_to_ms)
        .unwrap_or(fallback_ts)
}

fn parse_trade_field(item: &serde_json::Value, first_key: &str, second_key: &str) -> Option<f64> {
    parse_num(item.get(first_key)).or_else(|| parse_num(item.get(second_key)))
}

fn parse_trade_side(item: &serde_json::Value) -> Option<char> {
    item.get("side")
        .and_then(|value| value.as_str())
        .or_else(|| item.get("S").and_then(|value| value.as_str()))
        .and_then(side_to_char)
}

fn parse_kline_timestamp(item: &serde_json::Value) -> Option<i64> {
    parse_i64(item.get("t"))
        .or_else(|| parse_i64(item.get("openTime")))
        .or_else(|| parse_i64(item.get("time")))
        .map(normalize_timestamp_to_ms)
}

fn parse_kline_close_timestamp(item: &serde_json::Value) -> Option<i64> {
    parse_i64(item.get("T"))
        .or_else(|| parse_i64(item.get("closeTime")))
        .map(normalize_timestamp_to_ms)
}

fn parse_kline_count(item: &serde_json::Value) -> u64 {
    parse_i64(item.get("n"))
        .or_else(|| parse_i64(item.get("count")))
        .unwrap_or(0)
        .max(0) as u64
}

fn parse_kline_field(item: &serde_json::Value, short_key: &str, long_key: &str) -> Option<f64> {
    parse_num(item.get(short_key)).or_else(|| parse_num(item.get(long_key)))
}

fn is_closed_kline(close_ts: i64) -> bool {
    close_ts <= now_ts_ms() + 1_000
}

#[derive(Clone)]
pub struct HyperliquidSignalParser {
    source: SignalSource,
}

impl HyperliquidSignalParser {
    pub fn new(is_ipc: bool) -> Self {
        Self {
            source: if is_ipc {
                SignalSource::Ipc
            } else {
                SignalSource::Tcp
            },
        }
    }
}

impl Parser for HyperliquidSignalParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let Ok(json_str) = std::str::from_utf8(&msg) else {
            return 0;
        };
        let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) else {
            return 0;
        };

        let channel = json_value
            .get("channel")
            .and_then(|value| value.as_str())
            .unwrap_or_default();

        if channel != "allMids" {
            return 0;
        }

        let signal_msg = SignalMsg::create(self.source, parse_timestamp(&json_value));
        if tx.send(signal_msg.to_bytes()).is_ok() {
            return 1;
        }
        0
    }
}

#[derive(Clone)]
pub struct HyperliquidTradeParser;

impl HyperliquidTradeParser {
    pub fn new() -> Self {
        Self
    }

    fn parse_trade_item(
        &self,
        item: &serde_json::Value,
        fallback_ts: i64,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let Some(symbol) = normalized_symbol_from_value(item) else {
            return 0;
        };
        let Some(side) = parse_trade_side(item) else {
            return 0;
        };
        let Some(price) = parse_trade_field(item, "px", "price") else {
            return 0;
        };
        let Some(amount) = parse_trade_field(item, "sz", "size") else {
            return 0;
        };

        if price <= 0.0 || amount <= 0.0 {
            return 0;
        }

        let trade_msg = TradeMsg::create(
            symbol,
            parse_trade_id(item),
            parse_trade_timestamp(item, fallback_ts),
            side,
            price,
            amount,
        );
        if tx.send(trade_msg.to_bytes()).is_ok() {
            return 1;
        }
        0
    }
}

impl Parser for HyperliquidTradeParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let Ok(json_str) = std::str::from_utf8(&msg) else {
            return 0;
        };
        let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) else {
            return 0;
        };

        let channel = json_value
            .get("channel")
            .and_then(|value| value.as_str())
            .unwrap_or_default();

        if channel != "trades" {
            return 0;
        }

        let fallback_ts = parse_timestamp(&json_value);
        let Some(data) = json_value.get("data") else {
            return 0;
        };

        if data.is_object() {
            return self.parse_trade_item(data, fallback_ts, tx);
        }

        let Some(items) = data.as_array() else {
            return 0;
        };

        let mut parsed = 0usize;
        for item in items {
            if item.is_object() {
                parsed += self.parse_trade_item(item, fallback_ts, tx);
            }
        }
        parsed
    }
}

#[derive(Clone)]
pub struct HyperliquidKlineParser {
    only_closed: bool,
}

impl HyperliquidKlineParser {
    pub fn new(only_closed: bool) -> Self {
        Self { only_closed }
    }

    fn parse_kline_item(
        &self,
        item: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let Some(symbol) = normalized_symbol_from_value(item) else {
            return 0;
        };
        let Some(open_price) = parse_kline_field(item, "o", "open") else {
            return 0;
        };
        let Some(high_price) = parse_kline_field(item, "h", "high") else {
            return 0;
        };
        let Some(low_price) = parse_kline_field(item, "l", "low") else {
            return 0;
        };
        let Some(close_price) = parse_kline_field(item, "c", "close") else {
            return 0;
        };
        let Some(volume) = parse_kline_field(item, "v", "volume") else {
            return 0;
        };
        let Some(timestamp) = parse_kline_timestamp(item) else {
            return 0;
        };

        let close_ts = parse_kline_close_timestamp(item).unwrap_or(timestamp + 60_000);
        if self.only_closed && !is_closed_kline(close_ts) {
            return 0;
        }

        let kline_msg = KlineMsg::create_with_count(
            symbol,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            timestamp,
            parse_kline_count(item),
        );
        if tx.send(kline_msg.to_bytes()).is_ok() {
            return 1;
        }
        0
    }
}

impl Parser for HyperliquidKlineParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let Ok(json_str) = std::str::from_utf8(&msg) else {
            return 0;
        };
        let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) else {
            return 0;
        };

        let channel = json_value
            .get("channel")
            .and_then(|value| value.as_str())
            .unwrap_or_default();

        if channel != "candle" {
            return 0;
        }

        let Some(data) = json_value.get("data") else {
            return 0;
        };

        if data.is_object() {
            return self.parse_kline_item(data, tx);
        }

        let Some(items) = data.as_array() else {
            return 0;
        };

        let mut parsed = 0usize;
        for item in items {
            if item.is_object() {
                parsed += self.parse_kline_item(item, tx);
            }
        }
        parsed
    }
}

#[derive(Clone)]
pub struct HyperliquidDerivativesMetricsParser;

impl HyperliquidDerivativesMetricsParser {
    pub fn new() -> Self {
        Self
    }

    fn parse_active_asset_ctx(
        &self,
        item: &serde_json::Value,
        fallback_ts: i64,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let Some(coin) = item.get("coin").and_then(|value| value.as_str()) else {
            return 0;
        };

        let symbol = normalize_hyperliquid_symbol(coin);
        let ctx = item.get("ctx").unwrap_or(item);

        let timestamp = parse_i64(ctx.get("time"))
            .or_else(|| parse_i64(ctx.get("timeMs")))
            .map(normalize_timestamp_to_ms)
            .unwrap_or(fallback_ts);

        let mut parsed = 0usize;

        if let Some(mark_price) =
            parse_num(ctx.get("markPx")).or_else(|| parse_num(ctx.get("mark")))
        {
            let mark_price_msg = MarkPriceMsg::create(symbol.clone(), mark_price, timestamp);
            if tx.send(mark_price_msg.to_bytes()).is_ok() {
                parsed += 1;
            }
        }

        if let Some(funding_rate) =
            parse_num(ctx.get("funding")).or_else(|| parse_num(ctx.get("fundingRate")))
        {
            let next_funding_time = parse_i64(ctx.get("nextFundingTime"))
                .map(normalize_timestamp_to_ms)
                .unwrap_or(0);
            let funding_rate_msg =
                FundingRateMsg::create(symbol, funding_rate, next_funding_time, timestamp);
            if tx.send(funding_rate_msg.to_bytes()).is_ok() {
                parsed += 1;
            }
        }

        parsed
    }
}

impl Parser for HyperliquidDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let Ok(json_str) = std::str::from_utf8(&msg) else {
            return 0;
        };
        let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) else {
            return 0;
        };

        let channel = json_value
            .get("channel")
            .and_then(|value| value.as_str())
            .unwrap_or_default();

        if channel != "activeAssetCtx" {
            return 0;
        }

        let fallback_ts = parse_timestamp(&json_value);

        if let Some(data) = json_value.get("data") {
            if data.is_object() {
                return self.parse_active_asset_ctx(data, fallback_ts, tx);
            }

            if let Some(items) = data.as_array() {
                let mut parsed = 0usize;
                for item in items {
                    if item.is_object() {
                        parsed += self.parse_active_asset_ctx(item, fallback_ts, tx);
                        continue;
                    }

                    if let Some(pair) = item.as_array() {
                        if pair.len() >= 2 {
                            if let Some(coin) = pair[0].as_str() {
                                let wrapped = serde_json::json!({
                                    "coin": coin,
                                    "ctx": pair[1].clone(),
                                });
                                parsed += self.parse_active_asset_ctx(&wrapped, fallback_ts, tx);
                            }
                        }
                    }
                }
                return parsed;
            }
        }

        0
    }
}
