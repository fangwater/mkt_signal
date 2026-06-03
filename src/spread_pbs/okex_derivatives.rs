use bytes::Bytes;
use serde_json::Value;
use std::collections::HashSet;

use crate::common::mkt_msg::{FundingRateMsg, IndexPriceMsg, LiquidationMsg, MarkPriceMsg};
use crate::spread_pbs::okex::normalize_okex_symbol;

pub const OKEX_PUBLIC_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";

pub fn build_okex_derivatives_subscribe_msgs(symbols: &[String], batch_size: usize) -> Vec<Value> {
    let batch_size = batch_size.max(1);
    let mut out = Vec::new();

    for chunk in symbols.chunks(batch_size) {
        out.push(serde_json::json!({
            "op": "subscribe",
            "args": chunk.iter().map(|symbol| serde_json::json!({
                "channel": "mark-price",
                "instId": symbol,
            })).collect::<Vec<_>>()
        }));

        out.push(serde_json::json!({
            "op": "subscribe",
            "args": chunk.iter().map(|symbol| {
                let index_symbol = symbol
                    .replace("-USDT-SWAP", "-USD")
                    .replace("-USDT", "-USD");
                serde_json::json!({
                    "channel": "index-tickers",
                    "instId": index_symbol,
                })
            }).collect::<Vec<_>>()
        }));

        out.push(serde_json::json!({
            "op": "subscribe",
            "args": chunk.iter().map(|symbol| serde_json::json!({
                "channel": "funding-rate",
                "instId": symbol,
            })).collect::<Vec<_>>()
        }));
    }

    out.push(serde_json::json!({
        "op": "subscribe",
        "args": [{
            "channel": "liquidation-orders",
            "instType": "SWAP"
        }]
    }));

    out
}

pub fn parse_okex_derivatives_frame(value: &Value, active_symbols: &HashSet<String>) -> Vec<Bytes> {
    let Some(channel) = value
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .and_then(|v| v.as_str())
    else {
        return Vec::new();
    };

    match channel {
        "liquidation-orders" => parse_liquidation(value, active_symbols),
        "mark-price" => parse_mark_price(value),
        "funding-rate" => parse_funding_rate(value),
        "index-tickers" => parse_index_price(value),
        _ => Vec::new(),
    }
}

fn parse_liquidation(value: &Value, active_symbols: &HashSet<String>) -> Vec<Bytes> {
    let Some(data_array) = value.get("data").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in data_array {
        let Some(inst_id) = item.get("instId").and_then(|v| v.as_str()) else {
            continue;
        };
        if !active_symbols.contains(inst_id) {
            continue;
        }
        let symbol = normalize_okex_symbol(inst_id);
        let Some(details) = item.get("details").and_then(|v| v.as_array()) else {
            continue;
        };
        for detail in details {
            let (Some(side), Some(sz), Some(px), Some(ts)) = (
                detail.get("side").and_then(|v| v.as_str()),
                detail.get("sz").and_then(|v| v.as_str()),
                detail.get("bkPx").and_then(|v| v.as_str()),
                detail.get("ts").and_then(|v| v.as_str()),
            ) else {
                continue;
            };
            let (Ok(size), Ok(price), Ok(timestamp)) =
                (sz.parse::<f64>(), px.parse::<f64>(), ts.parse::<i64>())
            else {
                continue;
            };
            let liquidation_side = match side {
                "buy" => 'B',
                "sell" => 'S',
                _ => continue,
            };
            out.push(
                LiquidationMsg::create(
                    symbol.clone(),
                    liquidation_side,
                    size,
                    price,
                    normalize_ts_to_us(timestamp),
                )
                .to_bytes(),
            );
        }
    }
    out
}

fn parse_mark_price(value: &Value) -> Vec<Bytes> {
    let Some(data_array) = value.get("data").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in data_array {
        let (Some(inst_id), Some(mark_px), Some(ts)) = (
            item.get("instId").and_then(|v| v.as_str()),
            item.get("markPx").and_then(|v| v.as_str()),
            item.get("ts").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        let (Ok(mark_price), Ok(timestamp)) = (mark_px.parse::<f64>(), ts.parse::<i64>()) else {
            continue;
        };
        out.push(
            MarkPriceMsg::create(
                normalize_okex_symbol(inst_id),
                mark_price,
                normalize_ts_to_us(timestamp),
            )
            .to_bytes(),
        );
    }
    out
}

fn parse_funding_rate(value: &Value) -> Vec<Bytes> {
    let Some(data_array) = value.get("data").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in data_array {
        let (Some(inst_id), Some(rate), Some(next_time), Some(ts)) = (
            item.get("instId").and_then(|v| v.as_str()),
            item.get("fundingRate").and_then(|v| v.as_str()),
            item.get("nextFundingTime").and_then(|v| v.as_str()),
            item.get("ts").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        let (Ok(funding_rate), Ok(next_funding_time), Ok(timestamp)) = (
            rate.parse::<f64>(),
            next_time.parse::<i64>(),
            ts.parse::<i64>(),
        ) else {
            continue;
        };
        out.push(
            FundingRateMsg::create(
                normalize_okex_symbol(inst_id),
                funding_rate,
                normalize_ts_to_us(next_funding_time),
                normalize_ts_to_us(timestamp),
            )
            .to_bytes(),
        );
    }
    out
}

fn parse_index_price(value: &Value) -> Vec<Bytes> {
    let Some(data_array) = value.get("data").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in data_array {
        let (Some(inst_id), Some(idx_px), Some(ts)) = (
            item.get("instId").and_then(|v| v.as_str()),
            item.get("idxPx").and_then(|v| v.as_str()),
            item.get("ts").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        let (Ok(index_price), Ok(timestamp)) = (idx_px.parse::<f64>(), ts.parse::<i64>()) else {
            continue;
        };
        out.push(
            IndexPriceMsg::create(
                normalize_okex_symbol(inst_id),
                index_price,
                normalize_ts_to_us(timestamp),
            )
            .to_bytes(),
        );
    }
    out
}

fn normalize_ts_to_us(timestamp: i64) -> i64 {
    let abs = timestamp.abs();
    if abs >= 1_000_000_000_000_000_000 {
        timestamp / 1000
    } else if abs >= 1_000_000_000_000_000 {
        timestamp
    } else if abs >= 1_000_000_000_000 {
        timestamp.saturating_mul(1000)
    } else {
        timestamp.saturating_mul(1_000_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::mkt_msg::{FundingRateMsg, MarkPriceMsg};

    #[test]
    fn builds_mark_index_funding_and_liquidation_subs() {
        let symbols = vec!["BTC-USDT-SWAP".to_string(), "ETH-USDT-SWAP".to_string()];
        let msgs = build_okex_derivatives_subscribe_msgs(&symbols, 1);
        assert_eq!(msgs.len(), 7);
        assert_eq!(msgs[0]["args"][0]["channel"], "mark-price");
        assert_eq!(msgs[1]["args"][0]["channel"], "index-tickers");
        assert_eq!(msgs[1]["args"][0]["instId"], "BTC-USD");
        assert_eq!(msgs[2]["args"][0]["channel"], "funding-rate");
        assert_eq!(msgs[6]["args"][0]["channel"], "liquidation-orders");
    }

    #[test]
    fn parses_mark_and_funding_messages_directly() {
        let active = HashSet::new();
        let mark = serde_json::json!({
            "arg": {"channel": "mark-price", "instId": "BTC-USDT-SWAP"},
            "data": [{"instId":"BTC-USDT-SWAP", "markPx":"123.45", "ts":"1700000000000"}]
        });
        let out = parse_okex_derivatives_frame(&mark, &active);
        assert_eq!(out.len(), 1);
        assert_eq!(MarkPriceMsg::get_symbol(&out[0]), "BTCUSDT");
        assert_eq!(MarkPriceMsg::get_timestamp(&out[0]), 1_700_000_000_000_000);

        let funding = serde_json::json!({
            "arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
            "data": [{"instId":"BTC-USDT-SWAP", "fundingRate":"0.0001", "nextFundingTime":"1700003600000", "ts":"1700000000000"}]
        });
        let out = parse_okex_derivatives_frame(&funding, &active);
        assert_eq!(out.len(), 1);
        assert_eq!(FundingRateMsg::get_symbol(&out[0]), "BTCUSDT");
        assert_eq!(
            FundingRateMsg::get_next_funding_time(&out[0]),
            1_700_003_600_000_000
        );
        assert_eq!(
            FundingRateMsg::get_timestamp(&out[0]),
            1_700_000_000_000_000
        );
    }
}
