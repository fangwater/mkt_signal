use bytes::Bytes;
use mkt_parsers::okex as okex_codec;
use serde_json::Value;
use std::collections::HashSet;

use mkt_parsers::msg::mkt_msg::{FundingRateMsg, IndexPriceMsg, LiquidationMsg, MarkPriceMsg};

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
    let mut out = Vec::new();
    for derivative in okex_codec::parse_derivatives_json(value, Some(active_symbols)) {
        out.push(derivative_to_bytes(derivative));
    }
    out
}

fn derivative_to_bytes(derivative: okex_codec::Derivative) -> Bytes {
    match derivative {
        okex_codec::Derivative::MarkPrice {
            symbol,
            price,
            timestamp_us,
        } => MarkPriceMsg::create(symbol, price, timestamp_us).to_bytes(),
        okex_codec::Derivative::IndexPrice {
            symbol,
            price,
            timestamp_us,
        } => IndexPriceMsg::create(symbol, price, timestamp_us).to_bytes(),
        okex_codec::Derivative::FundingRate {
            symbol,
            funding_rate,
            next_funding_time_us,
            timestamp_us,
        } => FundingRateMsg::create(symbol, funding_rate, next_funding_time_us, timestamp_us)
            .to_bytes(),
        okex_codec::Derivative::Liquidation {
            symbol,
            side,
            amount,
            price,
            timestamp_us,
        } => LiquidationMsg::create(symbol, side, amount, price, timestamp_us).to_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::mkt_msg::{FundingRateMsg, MarkPriceMsg};

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
