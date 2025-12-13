use crate::common::basic_account_msg::{BasicBalanceMsg, BasicBorrowInterestMsg};
use bytes::Bytes;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct BinancePmBalanceRow {
    #[serde(default)]
    asset: String,
    #[serde(default, rename = "crossMarginAsset")]
    cross_margin_asset: String,
    #[serde(default, rename = "crossMarginBorrowed")]
    cross_margin_borrowed: String,
    #[serde(default, rename = "crossMarginInterest")]
    cross_margin_interest: String,
    #[serde(default, rename = "updateTime")]
    update_time: i64,
}

fn parse_f64(v: &str) -> f64 {
    v.parse::<f64>().unwrap_or(0.0)
}

pub fn parse_binance_pm_balance_snapshot(json: &str) -> Option<Vec<Bytes>> {
    let rows: Vec<BinancePmBalanceRow> = serde_json::from_str(json).ok()?;
    let mut out = Vec::new();
    for row in rows {
        if row.asset.is_empty() {
            continue;
        }
        let ts = row.update_time;
        let asset = row.asset.to_ascii_uppercase();
        let balance = parse_f64(&row.cross_margin_asset);
        let borrowed = parse_f64(&row.cross_margin_borrowed);
        let interest = parse_f64(&row.cross_margin_interest);

        // Align with Binance ACCOUNT_UPDATE parser semantics:
        // - BalanceUpdate uses "cw" (cross wallet balance), here approximated by crossMarginAsset.
        out.push(BasicBalanceMsg::create(ts, asset.clone(), balance).to_bytes());
        out.push(BasicBorrowInterestMsg::create(ts, asset, borrowed, interest).to_bytes());
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::{BasicAccountEventType, BasicBalanceMsg};

    #[test]
    fn parse_balance_snapshot_to_basic_msgs() {
        let json = r#"[{
            "asset": "BTC",
            "crossMarginAsset": "1.234",
            "crossMarginBorrowed": "0.1",
            "crossMarginInterest": "0.01",
            "updateTime": 1700000000000
        }]"#;
        let msgs = parse_binance_pm_balance_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 2);
        let bal = BasicBalanceMsg::from_bytes(&msgs[0]).expect("bal ok");
        assert_eq!(bal.msg_type as u32, BasicAccountEventType::BalanceUpdate as u32);
        assert_eq!(bal.symbol, "BTC");
        assert!((bal.balance - 1.234).abs() < 1e-12);
    }
}

