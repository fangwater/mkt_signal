use anyhow::{Context, Result};
use bytes::Bytes;
use mkt_parsers::msg::mkt_msg::MarkPriceMsg;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct RawGateFuturesTicker {
    contract: String,
    mark_price: String,
}

#[derive(Debug)]
pub struct GateMarkPriceSnapshotParse {
    pub msgs: Vec<Bytes>,
    pub rows_total: usize,
    pub rows_invalid: usize,
}

pub fn parse_gate_mark_price_snapshot(
    json: &str,
    timestamp_us: i64,
) -> Result<GateMarkPriceSnapshotParse> {
    let raw_list: Vec<RawGateFuturesTicker> =
        serde_json::from_str(json).context("failed to parse Gate futures tickers response")?;
    let rows_total = raw_list.len();
    let mut rows_invalid = 0usize;
    let mut msgs = Vec::with_capacity(rows_total);

    for raw in raw_list {
        let symbol = raw.contract.trim().to_ascii_uppercase();
        let mark_price = raw.mark_price.parse::<f64>().ok();
        let Some(mark_price) = mark_price.filter(|price| price.is_finite() && *price > 0.0) else {
            rows_invalid += 1;
            continue;
        };
        if symbol.is_empty() {
            rows_invalid += 1;
            continue;
        }
        msgs.push(MarkPriceMsg::create(symbol, mark_price, timestamp_us).to_bytes());
    }

    Ok(GateMarkPriceSnapshotParse {
        msgs,
        rows_total,
        rows_invalid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_each_valid_ticker_as_standard_mark_price_msg() {
        let json = r#"[
            {"contract":"XDC_USDT","mark_price":"0.02714"},
            {"contract":"BTC_USDT","mark_price":"68000.5"},
            {"contract":"BAD_USDT","mark_price":"0"}
        ]"#;
        let parsed = parse_gate_mark_price_snapshot(json, 123_456).unwrap();

        assert_eq!(parsed.rows_total, 3);
        assert_eq!(parsed.rows_invalid, 1);
        assert_eq!(parsed.msgs.len(), 2);
        assert_eq!(MarkPriceMsg::get_symbol(&parsed.msgs[0]), "XDC_USDT");
        assert_eq!(MarkPriceMsg::get_mark_price(&parsed.msgs[0]), 0.02714);
        assert_eq!(MarkPriceMsg::get_timestamp(&parsed.msgs[0]), 123_456);
        assert_eq!(MarkPriceMsg::get_symbol(&parsed.msgs[1]), "BTC_USDT");
        assert_eq!(MarkPriceMsg::get_mark_price(&parsed.msgs[1]), 68000.5);
    }
}
